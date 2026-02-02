from typing import AsyncGenerator
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from app.core.config import settings

# 1. Base
class Base(DeclarativeBase):
    pass

# 2. Engine
# str() нужен, чтобы Pydantic URL превратился в строку
engine = create_async_engine(str(settings.DATABASE_URL), echo=False)

# 3. Session Maker
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# 4. Init Models
async def init_models():
    async with engine.begin() as conn:
        # Включаем pgvector
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        # Создаем таблицы
        await conn.run_sync(Base.metadata.create_all)

# 5. Get Session (ИСПРАВЛЕНО)
@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Контекстный менеджер для работы с сессией.
    Использование:
    async with get_session() as session:
        ...
    """
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            # Гарантированно закрываем сессию (хотя async_session_maker это делает сам,
            # явно закрыть - хорошая практика)
            await session.close()