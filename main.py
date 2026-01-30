import asyncio
import logging
from aiogram import Bot, Dispatcher
from app.core.config import settings
from app.core.logger import setup_logger # Предполагается настройка loguru
from app.bot.handlers import router
from app.bot.middlewares import DbSessionMiddleware
from app.db.base import init_models # Функция create_all для таблиц

async def main():
    # 1. Настройка логгера
    setup_logger()
    
    # 2. Инициализация БД (создание таблиц если нет)
    # В проде лучше использовать Alembic миграции
    await init_models()

    # 3. Инициализация бота
    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()

    # 4. Подключение Middleware и Роутеров
    dp.update.middleware(DbSessionMiddleware())
    dp.include_router(router)

    logging.info("🚀 Parus AI Bot started!")
    
    # 5. Запуск Polling
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")