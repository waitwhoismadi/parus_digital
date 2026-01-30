import pandas as pd
import matplotlib
# Используем неинтерактивный бэкенд, чтобы не зависало на сервере
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import io
import base64
import traceback
from typing import Dict, List, Any
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatOllama
from loguru import logger

from app.core.config import settings
from app.db.models import FileMetadata
from app.services.storage import StorageService
from app.schemas.analytics import AnalyticsResponse

class PythonExecutorService:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.storage = StorageService()
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL, 
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0.1 
        )
        self.max_retries = 3

    async def run_analysis(self, user_query: str) -> AnalyticsResponse:
        """Главный метод: от запроса до результата."""
        files_meta = await self._get_relevant_files_metadata()
        
        if not files_meta:
            return AnalyticsResponse(
                answer_text="Нет доступных файлов для анализа. Загрузите Excel.", 
                executed_code=""
            )

        # Загрузка данных
        dfs = {}
        schemas_desc = []
        
        for meta in files_meta:
            try:
                file_obj = self.storage.get_file(meta.minio_path)
                if meta.filename.endswith('.csv'):
                    df = pd.read_csv(file_obj)
                else:
                    df = pd.read_excel(file_obj)
                
                safe_name = f"df_{meta.id}"
                dfs[safe_name] = df
                
                # --- ИСПРАВЛЕНИЕ: Красивое форматирование схемы ---
                # Превращаем JSON {"\u04...": "..."} в читаемый текст
                readable_schema = []
                if isinstance(meta.columns_schema, dict):
                    for col_name, col_desc in meta.columns_schema.items():
                        # col_name и col_desc автоматически станут нормальными строками в Python
                        readable_schema.append(f"- Колонка '{col_name}': {col_desc}")
                
                schema_text = "\n".join(readable_schema)
                # --------------------------------------------------

                # Добавляем названия колонок явно, чтобы модель точно знала, как к ним обращаться
                columns_list = ", ".join([f"'{c}'" for c in df.columns])
                
                schemas_desc.append(
                    f"DATASET NAME: '{safe_name}' (Original File: {meta.filename})\n"
                    f"ACTUAL PYTHON COLUMN NAMES: [{columns_list}]\n"
                    f"COLUMN MEANINGS:\n{schema_text}\n"
                )
            except Exception as e:
                logger.error(f"Failed to load file {meta.filename}: {e}")

        if not dfs:
            return AnalyticsResponse(answer_text="Ошибка загрузки данных.", executed_code="", is_error=True)

        # Self-healing loop
        last_error = None
        code = ""
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Generation attempt {attempt + 1}")
                code = await self._generate_code(user_query, schemas_desc, last_error, previous_code=code)
                
                logger.debug(f"Executing code:\n{code}")

                result, plot_b64 = self._execute_safe(code, dfs)
                
                return AnalyticsResponse(
                    answer_text=str(result),
                    plot_base64=plot_b64,
                    executed_code=code
                )
                
            except Exception as e:
                # Добавляем Traceback, чтобы модель видела, где именно она ошиблась
                last_error = f"{type(e).__name__}: {str(e)}\nTraceback: {traceback.format_exc()}"
                logger.warning(f"Code execution failed: {e}")

        return AnalyticsResponse(
            answer_text=f"Не удалось выполнить анализ. Ошибка: {last_error}",
            executed_code=code,
            is_error=True
        )

    async def _get_relevant_files_metadata(self) -> List[FileMetadata]:
        result = await self.db.execute(select(FileMetadata).order_by(FileMetadata.id.desc()).limit(5))
        return result.scalars().all()

    async def _generate_code(self, query: str, schemas: List[str], error: str = None, previous_code: str = "") -> str:
        
        system_prompt = """
            You are a Python Data Analyst. Write Python code using Pandas to answer the user's question.
            
            AVAILABLE DATAFRAMES:
            {schemas}

            RULES:
            1. Use ONLY `pandas` and `matplotlib.pyplot`.
            2. Assign the text answer (string or number) to the variable `final_result`.
            3. DO NOT use `print()`. Use `final_result = ...`
            4. COLUMN NAMES: Use exact column names from the "ACTUAL PYTHON COLUMN NAMES" list provided above.
            5. FILTERING: If the user asks for a specific item (e.g. "Mountain mass"), look for it in the text columns using string partial matching.
            Example: `df[df['ColumnName'].astype(str).str.contains('text', case=False, na=False)]`
            6. PLOTTING: If a plot is needed, create it using `plt.plot()` or `df.plot()`. DO NOT call `plt.show()`.
            
            Output JUST the executable Python code. No markdown.
            """
        
        user_msg = f"User Question: {query}"
        
        if error:
            user_msg += f"\n\nPrevious code failed with error:\n{error}\n\nPlease fix the code."

        prompt = PromptTemplate(
            template=f"{system_prompt}\n\n{{input}}",
            input_variables=["input", "schemas"]
        )
        
        chain = prompt | self.llm
        response = await chain.ainvoke({"input": user_msg, "schemas": "\n".join(schemas)})
        
        return response.content.replace("```python", "").replace("```", "").strip()

    def _execute_safe(self, code: str, dfs: Dict[str, pd.DataFrame]):
        # 1. Очищаем старые графики, но НЕ создаем новую фигуру
        plt.close('all')
        
        local_scope = {"pd": pd, "plt": plt}
        local_scope.update(dfs)
        
        # 2. Исполнение
        exec(code, {}, local_scope)
        
        # 3. Извлекаем результат
        final_result = local_scope.get("final_result")
        
        # Если final_result равен None (или не задан), пробуем найти хоть что-то
        if final_result is None:
             final_result = "Код выполнен успешно, но переменная 'final_result' пустая. Проверьте график."

        # 4. Проверяем, был ли нарисован график
        plot_b64 = None
        # get_fignums() вернет список номеров фигур, если они были созданы кодом
        if plt.get_fignums():
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            plot_b64 = base64.b64encode(buf.read()).decode('utf-8')
            plt.close('all')
            
        return final_result, plot_b64