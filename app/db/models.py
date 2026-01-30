from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class FileMetadata(Base):
    """Хранит информацию о загруженных Excel/CSV файлах"""
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    minio_path = Column(String, nullable=False, unique=True)
    file_type = Column(String) # xlsx, csv
    upload_date = Column(DateTime, default=datetime.utcnow)
    
    # Ключевое поле: JSON описание структуры.
    # Пример: {"date": "Дата платежа", "amount": "Сумма в рублях", ...}
    columns_schema = Column(JSON, nullable=False)
    description = Column(Text, nullable=True) # Общее описание содержимого

class ChatHistory(Base):
    """История сообщений для контекста"""
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, index=True) # ID сессии пользователя/чата
    role = Column(String) # user, assistant, system
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)