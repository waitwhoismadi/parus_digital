# app/db/models.py

from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, BigInteger, func
from datetime import datetime
# Убрали лишний импорт declarative_base
from app.db.base import Base  # <--- Используем ТОЛЬКО этот Base из base.py

# УДАЛЕНО: Base = declarative_base() <--- Эта строка всё ломала!

class FileMetadata(Base):
    """Хранит информацию о загруженных Excel/CSV файлах"""
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    minio_path = Column(String, nullable=False, unique=True)
    file_type = Column(String) 
    upload_date = Column(DateTime, default=datetime.utcnow)
    
    columns_schema = Column(JSON, nullable=False)
    description = Column(Text, nullable=True) 

class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, index=True)  # Telegram ID пользователя
    role = Column(String)  # 'user' или 'assistant'
    content = Column(Text) # Текст сообщения
    created_at = Column(DateTime(timezone=True), server_default=func.now())