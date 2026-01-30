from typing import TypedDict, Optional, Annotated
import operator
import json
from langchain_core.messages import BaseMessage
from langgraph.graph import END, StateGraph
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate
from loguru import logger

from app.services.sql_agent import SQLService
from app.services.analytics import PythonExecutorService
from app.core.config import settings
from app.db.base import async_session_maker

# --- 1. Определение состояния (СНАЧАЛА) ---
class GraphState(TypedDict):
    """
    Состояние, передаваемое между узлами графа.
    """
    question: str                # Исходный вопрос пользователя
    session_id: str              # ID сессии
    
    # Классификация
    intent: Optional[str]        # 'sql', 'python', 'general'
    
    # Данные для SQL ветки
    sql_query: Optional[str]     # Сгенерированный SQL
    sql_result: Optional[str]    # Результат выборки из БД
    
    # Данные для Python ветки
    python_code: Optional[str]   # Сгенерированный Python код
    plot_base64: Optional[str]   # График (если есть)
    
    # Итоговый ответ
    final_answer: Optional[str]  # Текст для отправки пользователю
    messages: Annotated[list[BaseMessage], operator.add] # История

# --- 2. Узлы (Nodes) ---

async def router_node(state: GraphState):
    """Определяет маршрут: sql, python или general"""
    # format="json" форсирует JSON output в Qwen/Llama
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL, 
        format="json"
    )
    
    prompt = PromptTemplate.from_template(
        """
        Ты — маршрутизатор запросов. Твоя задача — классифицировать вопрос пользователя в одну из трех категорий:
        
        1. "sql" — если вопрос касается справочных данных, списков проектов, сотрудников, планов, которые лежат в базе данных (НЕ в файлах).
        2. "python" — если вопрос касается АНАЛИЗА загруженных Excel файлов, построения графиков, расчетов, сравнения версий.
        3. "general" — приветствия, общие вопросы, или если непонятно.

        Вопрос: {question}

        ВЕРНИ JSON: {{"intent": "выбранная_категория"}}
        """
    )
    
    chain = prompt | llm
    try:
        response = await chain.ainvoke({"question": state["question"]})
        # Иногда LLM возвращает контент в response.content, иногда в response
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
    async with async_session_maker() as session:
        executor = PythonExecutorService(session)
        response = await executor.run_analysis(state["question"])
    
    return {
        "python_code": response.executed_code,
        "plot_base64": response.plot_base64,
        "final_answer": response.answer_text
    }

async def general_node(state: GraphState):
    """Болталка"""
    # Используем temperature=0.3, чтобы он был менее "креативным" и не выдумывал языки
    llm = ChatOllama(
        model=settings.OLLAMA_MODEL, 
        base_url=settings.OLLAMA_BASE_URL,
        temperature=0.3
    )
    
    from langchain_core.messages import HumanMessage, SystemMessage
    
    # Жесткая инструкция
    messages = [
        SystemMessage(content=(
            "Ты — русскоязычный ассистент Parus AI. "
            "Твоя задача — помогать сотрудникам ПЭО. "
            "Отвечай СТРОГО на русском языке. "
            "Никогда не используй китайские иероглифы или английский язык, если тебя об этом прямо не попросили."
        )),
        HumanMessage(content=state["question"])
    ]
    
    response = await llm.ainvoke(messages)
    return {"final_answer": response.content}

# ...
# --- 3. Построение графа ---

def build_graph():
    workflow = StateGraph(GraphState)

    # Добавляем узлы
    workflow.add_node("router", router_node)
    workflow.add_node("sql_agent", sql_node)
    workflow.add_node("python_agent", python_node)
    workflow.add_node("general_agent", general_node)

    # Входная точка
    workflow.set_entry_point("router")

    # Условные переходы
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

    # Все агенты завершают работу (идут к END)
    workflow.add_edge("sql_agent", END)
    workflow.add_edge("python_agent", END)
    workflow.add_edge("general_agent", END)

    return workflow.compile()

# Инициализация графа (синглтон)
app_workflow = build_graph()