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
from app.db.base import async_session_maker  # Импортируем фабрику сессий

# --- 1. Определение состояния ---
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

# --- 2. Узлы (Nodes) ---

async def router_node(state: GraphState):
    """Определяет маршрут: sql, python или general"""
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL, 
        format="json"
    )
    
    prompt = PromptTemplate.from_template(
        """
        Ты — маршрутизатор запросов для ИИ-аналитика. 
        Твоя задача — классифицировать вопрос пользователя в одну из трех категорий.
        
        КРИТЕРИИ КЛАССИФИКАЦИИ:
        
        1. "sql" — Вопросы о СТРУКТУРЕ компании, списках сотрудников, справочниках (то, что лежит в базе данных).
        
        2. "python" — АНАЛИТИКА ФАЙЛОВ и ГРАФИКИ.
           ВАЖНО: Сюда относятся любые уточнения по графикам:
           - "Сделай красным", "Поменяй цвет", "Добавь подписи".
           - "А что насчет марта?", "Убеди фильтр".
           - Любые команды по визуализации ("Построй", "Нарисуй").
           
        3. "general" — Приветствия ("Привет", "Как дела") или вопросы, не связанные с данными (погода, философия).

        Вопрос: {question}

        ВЕРНИ СТРОГО JSON: {{"intent": "выбранная_категория"}}
        """
    )
    
    chain = prompt | llm
    try:
        response = await chain.ainvoke({"question": state["question"]})
        content = response.content if hasattr(response, 'content') else str(response)
        data = json.loads(content)
        intent = data.get("intent", "general")
    except Exception as e:
        logger.error(f"Router parse error: {e}")
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
    # ВАЖНО: Открываем сессию БД здесь, внутри узла
    async with async_session_maker() as session:
        executor = PythonExecutorService(session)
        # Передаем user_id, чтобы сервис мог подтянуть историю переписки
        response = await executor.run_analysis(state["question"], state["user_id"])
    
    return {
        "python_code": response.executed_code,
        "plot_base64": response.plot_base64,
        "final_answer": response.answer_text
    }

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

# --- 3. Построение графа ---

def build_graph():
    workflow = StateGraph(GraphState)

    workflow.add_node("router", router_node)
    workflow.add_node("sql_agent", sql_node)
    workflow.add_node("python_agent", python_node)
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
            "general_agent": "general_agent"
        }
    )

    workflow.add_edge("sql_agent", END)
    workflow.add_edge("python_agent", END)
    workflow.add_edge("general_agent", END)

    return workflow.compile()

app_workflow = build_graph()