from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

# Создаем движок
engine = create_async_engine(str(settings.DATABASE_URL), echo=False)

# Фабрика сессий
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_models():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all) # Раскомментировать для сброса БД
        await conn.run_sync(Base.metadata.create_all)