import asyncio
import logging
from aiogram import Bot, Dispatcher
from app.core.config import settings
from app.bot.handlers import router
from app.db.base import init_models
import app.db.models 

logging.basicConfig(level=logging.INFO)

async def main():
    print("🔄 Connecting to DB and checking tables...")
    
    await init_models()
    
    print("✅ Database tables are ready!")

    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    dp.include_router(router)

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