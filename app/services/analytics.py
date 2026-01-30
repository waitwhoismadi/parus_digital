import pandas as pd
import matplotlib
# Устанавливаем бэкенд Agg СРАЗУ, до импорта pyplot. Это критично для Docker.
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
        """Главный метод"""
        files_meta = await self._get_relevant_files_metadata()
        
        if not files_meta:
            return AnalyticsResponse(answer_text="Нет файлов. Загрузите Excel.", executed_code="")

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
                
                # Формирование описания
                readable_schema = []
                if isinstance(meta.columns_schema, dict):
                    for col, desc in meta.columns_schema.items():
                        readable_schema.append(f"   - Column '{col}': {desc}")
                
                first_col_name = df.columns[0]
                first_col_values = df[first_col_name].astype(str).head(5).tolist()
                
                schema_text_block = (
                    f"DATASET: variable '{safe_name}' (Filename: {meta.filename})\n"
                    f"ALL COLUMN NAMES: {list(df.columns)}\n"
                    f"COLUMN MEANINGS:\n" + "\n".join(readable_schema) + "\n"
                    f"SAMPLE VALUES IN FIRST COLUMN ('{first_col_name}'): {first_col_values}...\n"
                )
                schemas_desc.append(schema_text_block)
            except Exception as e:
                logger.error(f"Error loading {meta.filename}: {e}")

        if not dfs:
            return AnalyticsResponse(answer_text="Ошибка данных", executed_code="", is_error=True)

        last_error = None
        code = ""
        
        for attempt in range(self.max_retries):
            try:
                code = await self._generate_code(user_query, schemas_desc, last_error)
                logger.info(f"--- ATTEMPT {attempt+1} GENERATED CODE ---\n{code}\n----------------")
                result, plot_b64 = self._execute_safe(code, dfs)
                
                return AnalyticsResponse(
                    answer_text=str(result),
                    plot_base64=plot_b64,
                    executed_code=code
                )
            except Exception as e:
                last_error = f"{type(e).__name__}: {str(e)}\nTraceback: {traceback.format_exc()}"
                logger.warning(f"Attempt {attempt+1} failed: {last_error}")

        return AnalyticsResponse(
            answer_text=f"Не удалось. Ошибка: {last_error}", 
            executed_code=code, 
            is_error=True
        )

    async def _get_relevant_files_metadata(self) -> List[FileMetadata]:
        result = await self.db.execute(select(FileMetadata).order_by(FileMetadata.id.desc()).limit(5))
        return result.scalars().all()

    async def _generate_code(self, query: str, schemas: List[str], error: str = None, previous_code: str = "") -> str:
        
        system_prompt = """
        You are a Senior Python Data Analyst.
        
        CONTEXT DATA:
        {schemas}

        USER REQUEST: {input}

        TASK: Write Python code (Pandas + Matplotlib) to fulfill the request.
        
        CRITICAL RULES (FOLLOW OR CODE WILL CRASH):
        
        1. VARIABLE: 
           - Start with `df = df_X.copy()` (use the correct variable name).
           - Initialize `final_result = "Data not found"`.
        
        2. DATA CLEANING: 
           - Convert ONLY value columns (Months, Quarters) to numeric: 
             `df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)`
           - NEVER touch the first column (Text Description).

        3. FILTERING:
           - Filter row: `filtered = df[df['NameCol'].astype(str).str.contains('Term', case=False, na=False)]`
           - ALWAYS check `if not filtered.empty:`
           - If empty, keep `final_result = "Data not found"`.

        4. GRAPHICS / PLOTTING (STRICT):
           - DECIDE: Does the user ask for "chart", "plot", "graph" (график)?
           - YES -> DRAW:
             a) Extract X (names) and Y (values) explicitly.
                Example: 
                `months = ['Январь', 'Февраль', 'Март']`
                `values = filtered.iloc[0][months].values`
             b) Plot: `plt.plot(months, values)` or `plt.bar(months, values)`.
             c) Add title: `plt.title('...')`.
             d) Set `final_result = "График построен."`
             e) FORBIDDEN: NEVER write `plt.show()`. NEVER write `plt.savefig()`.
           - NO -> CALCULATE:
             a) Just calculate the number.
             b) Set `final_result = calculated_value`.

        5. OUTPUT:
           - `final_result` must be a single string or number.
           - NO `print()` statements.

        RETURN ONLY CLEAN PYTHON CODE.
        """
        
        msg = query
        if error:
            msg += f"\n\nPREVIOUS ERROR: {error}\nHINT: Remove plt.show()! Check your column names."

        prompt = PromptTemplate(
            template=system_prompt,
            input_variables=["input", "schemas"]
        )
        
        chain = prompt | self.llm
        
        logger.info(f"--- SENDING PROMPT TO LLM ---\nQuestion: {msg}\n----------------")
        
        response = await chain.ainvoke({"input": msg, "schemas": "\n".join(schemas)})
        return response.content.replace("```python", "").replace("```", "").strip()

    def _execute_safe(self, code: str, dfs: Dict[str, pd.DataFrame]):
        # 1. Очищаем все прошлые графики
        plt.close('all')
        
        local_scope = {"pd": pd, "plt": plt}
        local_scope.update(dfs)
        
        # 2. Исполняем код
        exec(code, {}, local_scope)
        
        # 3. Достаем результат
        final_result = local_scope.get("final_result", "Код выполнен.")
        
        # 4. Проверяем наличие графиков
        plot_b64 = None
        # get_fignums() вернет список номеров фигур, если plt.plot() был вызван
        if plt.get_fignums():
            buf = io.BytesIO()
            # Сохраняем текущую фигуру
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
            buf.seek(0)
            plot_b64 = base64.b64encode(buf.read()).decode('utf-8')
            plt.close('all')
            
            # Если график есть, переписываем текст ответа, чтобы не пугать пользователя массивами
            if not isinstance(final_result, str):
                final_result = "График построен."
        
        return final_result, plot_b64