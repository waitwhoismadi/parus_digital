import asyncio
import logging
from aiogram import Bot, Dispatcher
from app.core.config import settings
from app.bot.handlers import router
from app.db.base import init_models

# !!! ВАЖНО !!!
# Мы должны импортировать модели здесь, чтобы SQLAlchemy "увидела" их 
# и зарегистрировала в метаданных перед созданием таблиц.
import app.db.models 

# Настройка логирования
logging.basicConfig(level=logging.INFO)

async def main():
    # 1. Инициализация моделей БД
    print("🔄 Connecting to DB and checking tables...")
    
    # Эта функция теперь увидит ChatHistory, потому что мы сделали импорт выше
    await init_models()
    
    print("✅ Database tables are ready!")

    # 2. Настройка бота
    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    # Подключаем роутеры
    dp.include_router(router)

    # 3. Запуск polling
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        print("🚀 Parus AI Bot started polling...")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped!")