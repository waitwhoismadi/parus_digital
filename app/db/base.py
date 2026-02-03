from typing import AsyncGenerator
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from app.core.config import settings

class Base(DeclarativeBase):
    pass

engine = create_async_engine(str(settings.DATABASE_URL), echo=False)

async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_models():
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)

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
            await session.close()