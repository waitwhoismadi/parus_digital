from minio import Minio
from app.core.config import settings
from loguru import logger
import io

class StorageService:
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(settings.MINIO_BUCKET_NAME):
            self.client.make_bucket(settings.MINIO_BUCKET_NAME)
            logger.info(f"Bucket {settings.MINIO_BUCKET_NAME} created")

    def upload_file(self, file_data: bytes, object_name: str) -> str:
        """Загружает байты в MinIO и возвращает путь"""
        try:
            self.client.put_object(
                settings.MINIO_BUCKET_NAME,
                object_name,
                io.BytesIO(file_data),
                length=len(file_data)
            )
            logger.info(f"File uploaded to MinIO: {object_name}")
            return object_name
        except Exception as e:
            logger.error(f"MinIO upload failed: {e}")
            raise

    def get_file(self, object_name: str) -> io.BytesIO:
        """Получает файл для обработки"""
        response = None
        try:
            response = self.client.get_object(settings.MINIO_BUCKET_NAME, object_name)
            return io.BytesIO(response.read())
        finally:
            if response:
                response.close()
                response.release_conn()