from langchain_community.utilities import SQLDatabase
from langchain_community.chat_models import ChatOllama
from langchain.chains import create_sql_query_chain
from langchain_core.prompts import PromptTemplate
from app.core.config import settings

class SQLService:
    def __init__(self):
        sync_db_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
        
        self.db = SQLDatabase.from_uri(sync_db_url)
        
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL,
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0
        )

    async def generate_response(self, question: str) -> dict:
        """
        Генерирует SQL, исполняет и формирует ответ.
        """
        # Генерация SQL
        chain = create_sql_query_chain(self.llm, self.db)
        generated_sql = await chain.ainvoke({"question": question})
        
        # --- БЛОК ОЧИСТКИ (FIX) ---
        # Если есть блок markdown ```sql ... ```, берем то, что внутри
        if "```" in generated_sql:
            # Иногда пишет ```sql, иногда просто ```
            generated_sql = generated_sql.split("```")[1].replace("sql", "")
            
        # Если модель повторила "SQLQuery:", обрезаем всё до этого момента
        if "SQLQuery:" in generated_sql:
            generated_sql = generated_sql.split("SQLQuery:")[1]

        # Самая надежная защита: ищем первое слово SELECT
        # и берем всё, начиная с него
        select_index = generated_sql.upper().find("SELECT")
        if select_index != -1:
            clean_sql = generated_sql[select_index:].strip()
        else:
            # Если SELECT не найден, скорее всего модель отказалась писать SQL
            return {
                "sql": "Not generated",
                "result": "",
                "answer": generated_sql # Возвращаем текст ошибки от модели
            }
        # ---------------------------
        
        # Исполнение SQL
        try:
            result_str = self.db.run(clean_sql)
        except Exception as e:
            return {
                "sql": clean_sql,
                "error": str(e),
                "answer": f"Ошибка выполнения SQL: {e}"
            }

        # Интерпретация результата
        interpret_prompt = PromptTemplate.from_template(
            """
            Вопрос: {question}
            SQL запрос: {query}
            Результат из БД: {result}
            
            Дай краткий ответ на русском языке.
            """
        )
        interpret_chain = interpret_prompt | self.llm
        final_answer = await interpret_chain.ainvoke({
            "question": question, 
            "query": clean_sql, 
            "result": result_str
        })

        return {
            "sql": clean_sql,
            "result": result_str,
            "answer": final_answer.content
        }
    
    async def _text_to_sql(self, question: str) -> str:
        # Обновленная схема
        schema = """
        Table: users
        Columns: id, first_name, last_name, middle_name, email, birth_date, phone_number, telegram_id, position_id, company_id
        
        Table: positions
        Columns: id, name, role
        
        Table: companies
        Columns: id, name, parent_company_id
        
        Relationships:
        - users.position_id -> positions.id
        - users.company_id -> companies.id
        - companies.parent_company_id -> companies.id
        """
        
        prompt = PromptTemplate.from_template(
            """
            Ты — SQL эксперт. Твоя задача — написать SQL запрос (PostgreSQL) для ответа на вопрос пользователя.
            
            Схема базы данных:
            {schema}
            
            Вопрос: {question}
            
            ПРАВИЛА:
            1. Возвращай ТОЛЬКО код SQL.
            2. Используй JOIN, чтобы получить названия должностей и компаний, а не только их ID.
             Например: SELECT u.last_name, p.name FROM users u JOIN positions p ON u.position_id = p.id
            3. Для поиска имен используй ILIKE.
            
            SQL Query:
            """
        )
        
        chain = prompt | self.llm
        response = await chain.ainvoke({"schema": schema, "question": question})
        
        sql = response.content.replace("```sql", "").replace("```", "").strip()
        return sql