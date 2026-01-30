import pandas as pd
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
            temperature=0.1 # Низкая температура для стабильности кода
        )
        self.max_retries = 3

    async def run_analysis(self, user_query: str) -> AnalyticsResponse:
        """
        Главный метод: от запроса до результата.
        """
        # 1. Получаем контекст (какие файлы доступны)
        # В реальной системе здесь нужен Vector Store поиск, 
        # для MVP берем последние 5 загруженных файлов.
        files_meta = await self._get_relevant_files_metadata()
        
        if not files_meta:
            return AnalyticsResponse(
                answer_text="Нет доступных файлов для анализа. Загрузите Excel.", 
                executed_code=""
            )

        # 2. Загружаем данные из MinIO в Pandas (Data Loading)
        dfs = {}
        schemas_desc = []
        
        for meta in files_meta:
            try:
                file_obj = self.storage.get_file(meta.minio_path)
                # Читаем в зависимости от расширения
                if meta.filename.endswith('.csv'):
                    df = pd.read_csv(file_obj)
                else:
                    df = pd.read_excel(file_obj)
                
                # Очищаем имя переменной для Python (убираем пробелы и т.д.)
                safe_name = f"df_{meta.id}"
                dfs[safe_name] = df
                
                # Формируем описание для промпта
                schemas_desc.append(
                    f"Variable '{safe_name}' (Original: {meta.filename}):\n"
                    f"Columns: {meta.columns_schema}\n"
                    f"Description: {meta.description}"
                )
            except Exception as e:
                logger.error(f"Failed to load file {meta.filename}: {e}")

        if not dfs:
            return AnalyticsResponse(answer_text="Ошибка загрузки данных.", executed_code="", is_error=True)

        # 3. Цикл генерации и исполнения (Self-healing loop)
        last_error = None
        code = ""
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Generation attempt {attempt + 1}")
                
                # Генерируем код (с учетом ошибки, если она была)
                code = await self._generate_code(user_query, schemas_desc, last_error, previous_code=code)
                
                # Исполняем код
                result, plot_b64 = self._execute_safe(code, dfs)
                
                return AnalyticsResponse(
                    answer_text=str(result),
                    plot_base64=plot_b64,
                    executed_code=code
                )
                
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Code execution failed: {last_error}")
                # Идем на следующий круг цикла с этой ошибкой

        return AnalyticsResponse(
            answer_text=f"Не удалось выполнить анализ после {self.max_retries} попыток. Ошибка: {last_error}",
            executed_code=code,
            is_error=True
        )

    async def _get_relevant_files_metadata(self) -> List[FileMetadata]:
        """Получает список файлов из БД"""
        result = await self.db.execute(select(FileMetadata).order_by(FileMetadata.id.desc()).limit(5))
        return result.scalars().all()

    async def _generate_code(self, query: str, schemas: List[str], error: str = None, previous_code: str = "") -> str:
        """Генерирует Python код через LLM"""
        
        system_prompt = """
        Ты — Python Data Expert. Твоя задача — написать код на Python (Pandas) для ответа на вопрос пользователя.
        
        ТЕБЕ ДОСТУПНЫ СЛЕДУЮЩИЕ DATAFRAME ПЕРЕМЕННЫЕ:
        {schemas}

        ТРЕБОВАНИЯ:
        1. Используй ТОЛЬКО библиотеку pandas и matplotlib.
        2. Результат анализа (текстовый ответ) должен быть сохранен в переменную `final_result`.
        3. Если нужен график: построй его через matplotlib, но НЕ вызывай plt.show(). График сохранится автоматически системой.
        4. Не используй input(), print() или загрузку файлов (данные уже в памяти).
        5. Код должен быть чистым, без Markdown блоков (без ```python ... ```). Просто код.
        """
        
        user_msg = f"Вопрос пользователя: {query}"
        
        if error:
            user_msg += f"\n\nВ предыдущем коде была ошибка:\nCODE:\n{previous_code}\n\nERROR:\n{error}\n\nПожалуйста, исправь код."

        prompt = PromptTemplate(
            template=f"{system_prompt}\n\n{{input}}",
            input_variables=["input", "schemas"]
        )
        
        chain = prompt | self.llm
        response = await chain.ainvoke({"input": user_msg, "schemas": "\n".join(schemas)})
        
        # Очистка от markdown, если LLM всё же их добавила
        clean_code = response.content.replace("```python", "").replace("```", "").strip()
        return clean_code

    def _execute_safe(self, code: str, dfs: Dict[str, pd.DataFrame]):
        """
        Изолированное исполнение кода.
        Возвращает (значение final_result, base64 графика)
        """
        # Подготовка локального скоупа
        local_scope = {"pd": pd, "plt": plt}
        local_scope.update(dfs) # Внедряем датафреймы
        
        # Перехват графика
        plt.figure() # Сбрасываем предыдущие графики
        
        try:
            # EXECUTE
            exec(code, {}, local_scope)
            
            # Извлекаем результат
            final_result = local_scope.get("final_result", "Код выполнен, но переменная final_result не найдена.")
            
            # Обработка графика
            plot_b64 = None
            if plt.get_fignums():
                buf = io.BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plot_b64 = base64.b64encode(buf.read()).decode('utf-8')
                plt.close()
                
            return final_result, plot_b64
            
        except Exception as e:
            # Пробрасываем ошибку выше для Self-healing
            raise e