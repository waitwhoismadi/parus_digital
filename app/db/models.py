from sqlalchemy import Column, Integer, String, BigInteger, Text, DateTime, JSON, ForeignKey, func, Date, Boolean
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector  
from app.db.base import Base
from datetime import datetime

class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    minio_path = Column(String, nullable=False, unique=True)
    file_type = Column(String) 
    upload_date = Column(DateTime, default=datetime.utcnow)
    columns_schema = Column(JSON, nullable=True) # Сделал nullable=True, т.к. для PDF схемы нет
    description = Column(Text, nullable=True)

    # Связь: один файл -> много кусочков текста
    chunks = relationship("DocumentChunk", back_populates="file", cascade="all, delete-orphan")

class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, index=True)
    role = Column(String)
    content = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class DocumentChunk(Base):
    """Хранит кусочки текста и их векторные представления"""
    __tablename__ = "document_chunks"

    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(Integer, ForeignKey("file_metadata.id"))    
    chunk_text = Column(Text, nullable=False)
    page_number = Column(Integer, nullable=True)
    embedding = Column(Vector(768))

    file = relationship("FileMetadata", back_populates="chunks")

class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)  # Например: "Главный экономист"
    role = Column(String, nullable=False)  # Например: "admin", "user"

class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    parent_company_id = Column(Integer, ForeignKey("companies.id"), nullable=True)
    
    children = relationship("Company")

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    middle_name = Column(String, nullable=True)
    email = Column(String, unique=True, index=True)
    birth_date = Column(Date, nullable=True)
    phone_number = Column(String, nullable=True)
    hashed_password = Column(String, nullable=False) 
    photo_url = Column(String, nullable=True)
    telegram_id = Column(BigInteger, unique=True, nullable=True, index=True)

    position_id = Column(Integer, ForeignKey("positions.id"))
    company_id = Column(Integer, ForeignKey("companies.id"))

    position = relationship("Position")
    company = relationship("Company")