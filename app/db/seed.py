from datetime import date
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import User, Position, Company

async def seed_data(session: AsyncSession):
    """Наполняет базу тестовыми данными для новой структуры"""
    
    # Проверяем, есть ли должности (если есть, значит база не пустая)
    result = await session.execute(select(Position))
    if result.scalars().first():
        return

    print("🌱 Seeding database with NEW structure...")

    # 1. Создаем Компании
    comp_main = Company(name="HeadOffice Corp", parent_company_id=None)
    session.add(comp_main)
    await session.commit()
    await session.refresh(comp_main)

    comp_sub = Company(name="Parus Branch Astana", parent_company_id=comp_main.id)
    session.add(comp_sub)
    await session.commit()
    await session.refresh(comp_sub)

    # 2. Создаем Должности
    pos_dir = Position(name="Генеральный директор", role="admin")
    pos_dev = Position(name="Python Разработчик", role="user")
    pos_econ = Position(name="Экономист", role="user")
    
    session.add_all([pos_dir, pos_dev, pos_econ])
    await session.commit()
    await session.refresh(pos_dir)
    await session.refresh(pos_dev)
    await session.refresh(pos_econ)

    # 3. Создаем Пользователей
    user1 = User(
        first_name="Иван", last_name="Иванов", middle_name="Иванович",
        email="ivan@parus.kz", birth_date=date(1990, 5, 20),
        phone_number="+77001112233", hashed_password="secret_hash",
        telegram_id=123456789, # Можно поставить свой ID для тестов
        position_id=pos_dir.id, company_id=comp_main.id
    )

    user2 = User(
        first_name="Алексей", last_name="Смирнов", 
        email="alex@parus.kz", birth_date=date(1995, 8, 15),
        hashed_password="secret_hash",
        position_id=pos_dev.id, company_id=comp_sub.id
    )

    user3 = User(
        first_name="Анна", last_name="Петрова", 
        email="anna@parus.kz", birth_date=date(1998, 1, 10),
        hashed_password="secret_hash",
        position_id=pos_econ.id, company_id=comp_sub.id
    )

    session.add_all([user1, user2, user3])
    await session.commit()
    
    print("✅ Database seeded successfully!")