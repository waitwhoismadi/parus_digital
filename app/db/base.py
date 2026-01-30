from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings
from app.db.models import Base

# Создаем движок
engine = create_async_engine(str(settings.DATABASE_URL), echo=False)

# Фабрика сессий
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_models():
    """Создает таблицы в БД, если их нет"""
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all) # Раскомментировать для сброса БД
        await conn.run_sync(Base.metadata.create_all)