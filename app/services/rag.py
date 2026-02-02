import io
import httpx
from pypdf import PdfReader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.models import DocumentChunk, FileMetadata
from app.core.config import settings
from loguru import logger

class RAGService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.ollama_url = f"{settings.OLLAMA_BASE_URL}/api/embeddings"
        self.model = "nomic-embed-text" 

    async def get_embedding(self, text: str) -> list[float]:
        """Превращает текст в список из 768 чисел"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.ollama_url,
                    json={"model": self.model, "prompt": text},
                    timeout=30.0
                )
                response.raise_for_status()
                return response.json()["embedding"]
            except Exception as e:
                logger.error(f"Embedding error: {e}")
                raise

    def extract_text_from_pdf(self, file_bytes: bytes) -> list[tuple[int, str]]:
        """Возвращает список (номер_страницы, текст)"""
        reader = PdfReader(io.BytesIO(file_bytes))
        result = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text()
            if text and len(text.strip()) > 50: 
                result.append((i + 1, text))
        return result

    def split_text(self, text: str, chunk_size: int = 500) -> list[str]:
        """Разбивает длинный текст на куски по ~500 символов"""
        words = text.split()
        chunks = []
        current_chunk = []
        current_len = 0
        
        for word in words:
            current_chunk.append(word)
            current_len += len(word) + 1
            if current_len >= chunk_size:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_len = 0
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        return chunks

    async def index_document(self, file_id: int, file_bytes: bytes, filename: str):
        """Главный метод: читает, режет, векторизует и сохраняет"""
        logger.info(f"Indexing document: {filename}")
        
        # 1. Извлекаем текст
        pages = []
        if filename.lower().endswith(".pdf"):
            pages = self.extract_text_from_pdf(file_bytes)
        # (Сюда можно добавить docx)
        
        if not pages:
            logger.warning("No text found in document")
            return

        # 2. Обрабатываем каждую страницу
        chunk_objects = []
        for page_num, text in pages:
            # Режем страницу на кусочки
            text_chunks = self.split_text(text)
            
            for chunk_text in text_chunks:
                # Генерируем вектор
                vector = await self.get_embedding(chunk_text)
                
                # Создаем объект для БД
                db_obj = DocumentChunk(
                    file_id=file_id,
                    chunk_text=chunk_text,
                    page_number=page_num,
                    embedding=vector
                )
                self.session.add(db_obj)
        
        # 3. Сохраняем всё разом
        await self.session.commit()
        logger.info(f"Successfully indexed {len(pages)} pages")

    async def search(self, query: str, limit: int = 3) -> list[str]:
        """Ищет похожие куски текста по вектору"""
        # 1. Превращаем вопрос пользователя в вектор
        query_vector = await self.get_embedding(query)

        # 2. Ищем ближайших соседей в базе (оператор <-> это L2 distance)
        stmt = select(DocumentChunk).order_by(
            DocumentChunk.embedding.l2_distance(query_vector)
        ).limit(limit)

        result = await self.session.execute(stmt)
        chunks = result.scalars().all()
        
        # 3. Возвращаем только текст найденных кусков
        return [chunk.chunk_text for chunk in chunks]