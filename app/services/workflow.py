from typing import TypedDict, Optional, Annotated
import operator
import json
from langchain_core.messages import BaseMessage
from langgraph.graph import END, StateGraph
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate
from loguru import logger
from langchain_core.messages import HumanMessage, SystemMessage

from app.services.sql_agent import SQLService
from app.services.analytics import PythonExecutorService
from app.core.config import settings
from app.db.base import async_session_maker  

from app.services.rag import RAGService

# --- Определение состояния ---
class GraphState(TypedDict):
    question: str            # Вопрос пользователя
    user_id: int             # ID пользователя (нужен для памяти)
    session_id: str          # (Опционально)
    
    intent: Optional[str]    # Классификация
    
    sql_query: Optional[str]
    sql_result: Optional[str]
    
    python_code: Optional[str]
    plot_base64: Optional[str]
    
    final_answer: Optional[str]
    messages: Annotated[list[BaseMessage], operator.add]

async def router_node(state: GraphState):
    """Маршрутизатор: определяет намерение пользователя"""
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL, 
        format="json",
        temperature=0
    )
    
    prompt = PromptTemplate.from_template(
        """
        Твоя задача — классифицировать вопрос пользователя в одну из 4 категорий.
        
        КАТЕГОРИИ:
        1. "sql" — Поиск людей, контактов, компаний, должностей.
           Ключевые слова: "Кто работает", "В какой компании", "Телефон", "Email", "Должность", "Найди сотрудника".
        
        2. "python" — Работа с Excel файлами и графиками.
           Ключевые слова: "Построй график", "Проанализируй файл", "Сравни данные".
        
        3. "rag" — Вопросы по тексту ДОКУМЕНТОВ (PDF/Word).
           Ключевые слова: "Договор", "Приказ", "Инструкция", "Как рассчитать", "Какие правила".
        
        4. "general" — Приветствия, философия, вопросы "Как дела?".

        Вопрос: {question}

        ВЕРНИ ТОЛЬКО JSON: {{"intent": "выбранная_категория"}}
        """
    )
    
    chain = prompt | llm
    try:
        response = await chain.ainvoke({"question": state["question"]})
        content = response.content if hasattr(response, 'content') else str(response)
        data = json.loads(content)
        intent = data.get("intent", "general")
    except Exception as e:
        logger.error(f"Router error: {e}")
        intent = "general"
        
    logger.info(f"Router decision: {intent}")
    return {"intent": intent}

async def sql_node(state: GraphState):
    """Обработка SQL запросов"""
    service = SQLService()
    response = await service.generate_response(state["question"])
    return {
        "sql_query": response.get("sql"),
        "sql_result": response.get("result"),
        "final_answer": response.get("answer")
    }

async def python_node(state: GraphState):
    """Обработка Excel/Python запросов"""
    async with async_session_maker() as session:
        executor = PythonExecutorService(session)
        response = await executor.run_analysis(state["question"], state["user_id"])
    
    return {
        "python_code": response.executed_code,
        "plot_base64": response.plot_base64,
        "final_answer": response.answer_text
    }

async def rag_node(state: GraphState):
    """Ищет информацию в документах и формирует ответ"""
    question = state["question"]
    
    async with async_session_maker() as session:
        rag_service = RAGService(session)
        found_chunks = await rag_service.search(question, limit=3)
    
    if not found_chunks:
        return {"final_answer": "К сожалению, я не нашел информации об этом в загруженных документах."}

    context_text = "\n---\n".join(found_chunks)
    
    system_prompt = f"""
    Ты — эксперт по документации.
    Используй ТОЛЬКО представленный ниже контекст, чтобы ответить на вопрос пользователя.
    Если в контексте нет ответа, так и скажи. Не выдумывай факты.
    
    КОНТЕКСТ ИЗ ДОКУМЕНТОВ:
    {context_text}
    """
    
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL,
        temperature=0.2 
    )
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=question)
    ]
    
    response = await llm.ainvoke(messages)
    return {"final_answer": response.content}

async def general_node(state: GraphState):
    """Болталка"""
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL,
        temperature=0.3
    )
    
    messages = [
        SystemMessage(content=(
            "Ты — русскоязычный ассистент Parus AI. "
            "Отвечай СТРОГО на русском языке."
        )),
        HumanMessage(content=state["question"])
    ]
    
    response = await llm.ainvoke(messages)
    return {"final_answer": response.content}

# --- Построение графа ---

def build_graph():
    workflow = StateGraph(GraphState)

    workflow.add_node("router", router_node)
    workflow.add_node("sql_agent", sql_node)
    workflow.add_node("python_agent", python_node)
    workflow.add_node("rag_agent", rag_node)       
    workflow.add_node("general_agent", general_node)

    workflow.set_entry_point("router")

    def route_condition(state):
        return state["intent"] + "_agent"

    workflow.add_conditional_edges(
        "router",
        route_condition,
        {
            "sql_agent": "sql_agent",
            "python_agent": "python_agent",
            "rag_agent": "rag_agent",        
            "general_agent": "general_agent"
        }
    )

    # Все идут к выходу
    workflow.add_edge("sql_agent", END)
    workflow.add_edge("python_agent", END)
    workflow.add_edge("rag_agent", END)
    workflow.add_edge("general_agent", END)

    return workflow.compile()

app_workflow = build_graph()