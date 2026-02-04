from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from langchain_community.chat_models import ChatOllama
from langchain.prompts import PromptTemplate
from loguru import logger
from app.core.config import settings

class SQLService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL, 
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0 # Важно: 0 креативности, только логика
        )

    async def generate_response(self, user_question: str) -> dict:
        """Главный метод: Вопрос -> SQL -> Данные -> Ответ"""
        
        # 1. Генерируем SQL
        sql_query = await self._text_to_sql(user_question)
        
        # !!! ЛОГИРОВАНИЕ !!!
        logger.info(f"🔎 [SQL Agent] Question: {user_question}")
        logger.info(f"📝 [SQL Agent] Generated SQL: {sql_query}")
        
        if "NO_SQL" in sql_query or not sql_query:
             return {"answer": "Для этого вопроса не нужно искать данные в базе."}
        
        # 2. Выполняем SQL
        try:
            result = await self.session.execute(text(sql_query))
            rows = result.fetchall()
            columns = result.keys()
            
            # Логируем, что нашли
            logger.info(f"📊 [SQL Agent] Found {len(rows)} rows: {rows}")

            # Превращаем результат в строку для LLM
            if not rows:
                data_str = "Результат запроса пуст (No data found)."
            else:
                data_str = f"Columns: {list(columns)}\nData:\n"
                for row in rows:
                    data_str += str(row) + "\n"
                
        except Exception as e:
            logger.error(f"❌ [SQL Agent] Execution error: {e}")
            return {"answer": f"Ошибка выполнения SQL запроса. Попробуйте переформулировать.", "sql": sql_query}

        # 3. Формируем человеческий ответ
        final_answer = await self._data_to_text(user_question, data_str)
        return {
            "sql": sql_query,
            "result": data_str,
            "answer": final_answer
        }

    async def _text_to_sql(self, question: str) -> str:
        # Максимально подробная схема
        schema = """
        1. table "users" (Сотрудники):
           - id, first_name, last_name (Фамилия), middle_name
           - email, phone_number
           - position_id (link to positions), company_id (link to companies)

        2. table "companies" (Компании):
           - id, name (Название компании)
        
        3. table "positions" (Должности):
           - id, name (Название должности)

        Связи:
        - users.company_id = companies.id
        - users.position_id = positions.id
        """
        
        prompt = PromptTemplate.from_template(
            """
            Ты — Senior SQL Developer. Напиши PostgreSQL запрос для ответа на вопрос.
            
            Схема БД:
            {schema}
            
            Вопрос: {question}
            
            ЖЕСТКИЕ ПРАВИЛА:
            1. Возвращай ТОЛЬКО SQL код. Никаких объяснений.
            2. Для поиска по тексту (имя, фамилия, название) ВСЕГДА используй ILIKE и символы %.
               Пример: WHERE u.last_name ILIKE '%Пушпаков%'
            3. ВСЕГДА делай JOIN с таблицами companies и positions, чтобы вывести названия, а не ID.
            4. Если имя написано на русском, ищи в базе на русском.
            
            SQL Query:
            """
        )
        
        chain = prompt | self.llm
        response = await chain.ainvoke({"schema": schema, "question": question})
        
        # Чистим мусор (markdown)
        sql = response.content.replace("```sql", "").replace("```", "").strip()
        return sql

    async def _data_to_text(self, question: str, data: str) -> str:
        prompt = PromptTemplate.from_template(
            """
            Вопрос: {question}
            Данные из БД:
            {data}
            
            Если данных нет, ответь: "К сожалению, информации о сотруднике не найдено."
            Если данные есть, ответь кратко и четко на русском языке.
            """
        )
        chain = prompt | self.llm
        response = await chain.ainvoke({"question": question, "data": data})
        return response.content