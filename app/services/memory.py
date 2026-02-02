from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.models import ChatHistory

class MemoryService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_message(self, user_id: int, role: str, content: str):
        """Сохраняет сообщение в базу"""
        msg = ChatHistory(user_id=user_id, role=role, content=content)
        self.session.add(msg)
        await self.session.commit()

    async def get_recent_history(self, user_id: int, limit: int = 6) -> str:
        """
        Возвращает последние N сообщений в формате текста.
        Пример:
        User: Привет
        Assistant: Привет!
        User: Построй график
        """
        stmt = (
            select(ChatHistory)
            .where(ChatHistory.user_id == user_id)
            .order_by(ChatHistory.id.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        messages = result.scalars().all()
        
        messages = messages[::-1]
        
        history_text = ""
        for msg in messages:
            role_name = "User" if msg.role == "user" else "Assistant"
            history_text += f"{role_name}: {msg.content}\n"
            
        return history_text