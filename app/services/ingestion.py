import pandas as pd
import io
import json
from datetime import datetime
from langchain_community.chat_models import ChatOllama
from langchain.prompts import PromptTemplate
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import FileMetadata
from app.services.storage import StorageService
from app.core.config import settings
from loguru import logger

class IngestionService:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.storage = StorageService()
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL, 
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0,
            format="json" # Force JSON output mode in Ollama
        )

    async def process_file(self, file_content: bytes, filename: str):
        """Полный цикл: MinIO -> Анализ структуры -> Postgres"""
        
        # 1. Загрузка в MinIO
        unique_name = f"{int(datetime.now().timestamp())}_{filename}"
        minio_path = self.storage.upload_file(file_content, unique_name)

        # 2. Предварительный просмотр данных (Pandas)
        df_preview = self._get_preview(file_content, filename)
        
        # 3. Анализ структуры через LLM
        schema_info = await self._analyze_schema_with_llm(df_preview)

        # 4. Сохранение метаданных в БД
        new_file = FileMetadata(
            filename=filename,
            minio_path=minio_path,
            file_type=filename.split('.')[-1],
            columns_schema=schema_info.get("columns", {}),
            description=schema_info.get("summary", "Нет описания")
        )
        self.db.add(new_file)
        await self.db.commit()
        await self.db.refresh(new_file)
        
        logger.success(f"File {filename} processed and schema saved.")
        return new_file

    def _get_preview(self, content: bytes, filename: str) -> pd.DataFrame:
        """Читает первые 5 строк для контекста LLM"""
        try:
            if filename.endswith(".xlsx"):
                return pd.read_excel(io.BytesIO(content), nrows=5)
            elif filename.endswith(".csv"):
                return pd.read_csv(io.BytesIO(content), nrows=5)
            else:
                raise ValueError("Unsupported format")
        except Exception as e:
            logger.error(f"Error reading file preview: {e}")
            raise

    async def _analyze_schema_with_llm(self, df: pd.DataFrame) -> dict:
        """Генерирует JSON описание колонок"""
        
        # Превращаем данные в строку для промпта
        csv_preview = df.to_csv(index=False)
        columns_list = list(df.columns)

        prompt = PromptTemplate(
            template="""
            Ты — Data Analyst. Твоя задача — проанализировать структуру таблицы.
            Вот первые 5 строк данных:
            {data_preview}

            Для каждой колонки из списка {columns}:
            1. Определи тип данных (число, дата, категория, текст).
            2. Дай краткое описание на русском языке (что это за данные).
            
            Также напиши общее резюме (summary) о том, что содержит этот файл.

            ВЕРНИ ТОЛЬКО JSON следующего формата, без лишнего текста:
            {{
                "columns": {{
                    "col_name_1": "тип: описание",
                    "col_name_2": "тип: описание"
                }},
                "summary": "Краткое описание файла"
            }}
            """,
            input_variables=["data_preview", "columns"]
        )

        chain = prompt | self.llm
        
        try:
            response = await chain.ainvoke({
                "data_preview": csv_preview,
                "columns": columns_list
            })
            # Парсинг ответа. В LangChain response.content это строка
            return json.loads(response.content)
        except Exception as e:
            logger.error(f"LLM Schema analysis failed: {e}")
            # Fallback
            return {"columns": {c: "unknown" for c in columns_list}, "summary": "Ошибка анализа"}