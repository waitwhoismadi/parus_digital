from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message
from sqlalchemy import select
from app.db.base import async_session_maker
from app.db.models import User

class DbSessionMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        async with async_session_maker() as session:
            data["db_session"] = session
            return await handler(event, data)

class AuthMiddleware(BaseMiddleware):
    """middleware для проверки доступа (Фейс-контроль)"""
    async def __call__(
        self, 
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]], 
        event: TelegramObject, 
        data: Dict[str, Any]
    ) -> Any:
        
        # Проверяем только входящие сообщения (Message)
        # Если это callback (нажатие кнопки) или другое событие — пока пропускаем или дописываем логику
        if not isinstance(event, Message):
            return await handler(event, data)
        
        user_telegram_id = event.from_user.id
        
        # Открываем короткую сессию для проверки наличия юзера
        async with async_session_maker() as session:
            result = await session.execute(
                select(User).where(User.telegram_id == user_telegram_id)
            )
            user = result.scalars().first()

        # Если пользователя нет в таблице users
        if not user:
            await event.answer(
                "⛔ **Доступ запрещен**\n\n"
                "Вас нет в списке сотрудников Parus AI.\n"
                f"Ваш ID: `{user_telegram_id}`\n\n"
                "Отправьте этот ID администратору для добавления.",
                parse_mode="Markdown"
            )
            # ВАЖНО: Мы делаем return, НЕ вызывая handler. 
            # Бот дальше не пойдет, ресурсы ИИ тратиться не будут.
            return

        # Если пользователь найден — кладем его объект в data
        # Теперь в любом хендлере можно получить data['current_user']
        data["current_user"] = user
        
        return await handler(event, data)