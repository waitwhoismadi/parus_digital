import pandas as pd
import io
import json
from datetime import datetime
from langchain_community.chat_models import ChatOllama
from langchain.prompts import PromptTemplate
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.db.models import FileMetadata
from app.services.storage import StorageService
from app.core.config import settings
from app.services.rag import RAGService  # Импорт RAG

class IngestionService:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.storage = StorageService()
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL, 
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0,
            format="json"
        )

    async def process_file(self, file_content: bytes, filename: str):
        """Полный цикл: MinIO -> (Анализ или RAG) -> Postgres"""
        
        # 1. Загрузка в MinIO (общая для всех)
        unique_name = f"{int(datetime.now().timestamp())}_{filename}"
        minio_path = self.storage.upload_file(file_content, unique_name)

        # Переменные по умолчанию
        schema_info = {"columns": {}, "summary": "Файл загружен"}

        # 2. Ветвление логики по типу файла
        
        # --- ВЕТКА 1: Таблицы (Excel/CSV) ---
        if filename.endswith(('.xlsx', '.csv')):
            try:
                # Предпросмотр и LLM анализ структуры
                df_preview = self._get_preview(file_content, filename)
                schema_info = await self._analyze_schema_with_llm(df_preview)
            except Exception as e:
                logger.error(f"Schema analysis failed: {e}")
                schema_info["summary"] = "Ошибка анализа структуры"

        # 3. Сохранение метаданных в БД
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

        # --- ВЕТКА 2: Документы (PDF) ---
        # Запускаем RAG только после того, как файл сохранен в БД (нужен new_file.id)
        if filename.endswith(".pdf"):
            logger.info(f"Starting RAG indexing for {filename}...")
            try:
                rag = RAGService(self.db)
                await rag.index_document(new_file.id, file_content, filename)
                
                # Можно обновить описание, что файл проиндексирован
                new_file.description = "PDF документ (Проиндексирован для поиска)"
                await self.db.commit()
                
            except Exception as e:
                logger.error(f"RAG Indexing error: {e}")

        logger.success(f"File {filename} processed successfully.")
        return new_file
    
    def _get_preview(self, content: bytes, filename: str) -> pd.DataFrame:
        """Читает первые 5 строк для контекста LLM"""
        if filename.endswith(".xlsx"):
            return pd.read_excel(io.BytesIO(content), nrows=5)
        elif filename.endswith(".csv"):
            return pd.read_csv(io.BytesIO(content), nrows=5)
        return pd.DataFrame()

    async def _analyze_schema_with_llm(self, df: pd.DataFrame) -> dict:
        """Генерирует JSON описание колонок"""
        csv_preview = df.to_csv(index=False)
        columns_list = list(df.columns)

        prompt = PromptTemplate(
            template="""
            Ты — Data Analyst. Твоя задача — проанализировать структуру таблицы.
            Вот первые 5 строк данных:
            {data_preview}

            ВЕРНИ ТОЛЬКО JSON следующего формата:
            {{
                "columns": {{ "col_name": "тип: описание" }},
                "summary": "Краткое описание файла"
            }}
            """,
            input_variables=["data_preview"]
        )

        chain = prompt | self.llm
        try:
            response = await chain.ainvoke({
                "data_preview": csv_preview
            })
            content = response.content if hasattr(response, 'content') else str(response)
            return json.loads(content)
        except Exception as e:
            logger.error(f"LLM Schema analysis failed: {e}")
            return {"columns": {}, "summary": "Ошибка LLM"}