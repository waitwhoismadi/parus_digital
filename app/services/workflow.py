import json
from loguru import logger
from langgraph.graph import StateGraph, END
from typing import TypedDict
from langchain_core.messages import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatOllama

from app.core.config import settings
from app.db.base import async_session_maker
from app.services.rag import RAGService
from app.services.sql_agent import SQLService

# --- ИЗМЕНЕНИЕ: Импортируем твой сервис ---
from app.services.analytics import PythonExecutorService 

# --- СОСТОЯНИЕ ГРАФА ---
class GraphState(TypedDict):
    question: str
    intent: str
    sql_query: str
    sql_result: str
    final_answer: str
    user_id: int
    session_id: str
    plot_base64: str

# ... (router_node оставляем без изменений) ...
async def router_node(state: GraphState):
    # ... (код роутера тот же) ...
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL, 
        format="json",
        temperature=0
    )
    
    prompt = PromptTemplate.from_template(
        """
        Твоя задача — классифицировать вопрос пользователя.
        
        КАТЕГОРИИ:
        1. "sql" — Люди, контакты, компании, должности ("Кто работает", "В какой компании", "Email").
        2. "python" — EXCEL, ГРАФИКИ, Анализ данных ("Построй", "Сравни", "Проанализируй", "Сделай красным").
        3. "rag" — Текстовые документы PDF/DOCX ("Договор", "Приказ", "Инструкция").
        4. "general" — Болталка.

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


# ... (sql_node оставляем без изменений) ...
async def sql_node(state: GraphState):
    question = state["question"]
    async with async_session_maker() as session:
        service = SQLService(session)
        response = await service.generate_response(question)
    
    return {
        "sql_query": response.get("sql"),
        "sql_result": response.get("result"),
        "final_answer": response.get("answer")
    }

# !!! ОБНОВЛЕННЫЙ PYTHON NODE !!!
async def python_node(state: GraphState):
    """Агент-аналитик (Python)"""
    user_id = state.get("user_id")
    question = state["question"]
    
    # Твой сервис требует сессию БД, поэтому создаем контекстный менеджер
    async with async_session_maker() as session:
        service = PythonExecutorService(session)
        # Вызываем твой метод
        result_obj = await service.run_analysis(question, user_id)
    
    # Распаковываем объект AnalyticsResponse в словарь для графа
    return {
        "final_answer": result_obj.answer_text,
        "plot_base64": result_obj.plot_base64
    }

# ... (rag_node оставляем без изменений) ...
async def rag_node(state: GraphState):
    question = state["question"]
    async with async_session_maker() as session:
        rag_service = RAGService(session)
        found_chunks = await rag_service.search(question, limit=3)
    
    if not found_chunks:
        return {"final_answer": "К сожалению, я не нашел информации об этом в загруженных документах."}

    context_text = "\n---\n".join(found_chunks)
    system_prompt = f"Ты эксперт. Контекст:\n{context_text}"
    
    llm = ChatOllama(model=settings.OLLAMA_MODEL, base_url=settings.OLLAMA_BASE_URL, temperature=0.2)
    messages = [SystemMessage(content=system_prompt), HumanMessage(content=question)]
    response = await llm.ainvoke(messages)
    return {"final_answer": response.content}

# ... (general_node оставляем без изменений) ...
async def general_node(state: GraphState):
    llm = ChatOllama(model=settings.OLLAMA_MODEL, base_url=settings.OLLAMA_BASE_URL, temperature=0.7)
    messages = [
        SystemMessage(content="Отвечай кратко на русском."),
        HumanMessage(content=state["question"])
    ]
    response = await llm.ainvoke(messages)
    return {"final_answer": response.content}

# ... (build_graph оставляем без изменений) ...
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

    workflow.add_edge("sql_agent", END)
    workflow.add_edge("python_agent", END)
    workflow.add_edge("rag_agent", END)
    workflow.add_edge("general_agent", END)

    return workflow.compile()

app_workflow = build_graph()