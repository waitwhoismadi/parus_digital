import io
import base64
from aiogram import Router, F, Bot
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import CommandStart
from aiogram.utils.chat_action import ChatActionSender

from loguru import logger
from app.services.ingestion import IngestionService
from app.services.workflow import app_workflow
from app.services.memory import MemoryService
from app.db.base import async_session_maker # Импортируем фабрику сессий

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я Parus AI — твой ассистент ПЭО.\n\n"
        "📂 Загрузи Excel-файл для анализа.\n"
        "📊 Спроси о данных (например: 'Построй график затрат')."
    )

@router.message(F.document)
async def handle_document(message: Message, bot: Bot):
    """Обработка загрузки файлов"""
    doc = message.document
    
    if not doc.file_name.endswith(('.xlsx', '.csv')):
        await message.answer("❌ Поддерживаются только .xlsx и .csv файлы.")
        return

    status_msg = await message.answer("⏳ Скачиваю и анализирую структуру файла...")
    
    try:
        file_io = await bot.download(doc)
        file_bytes = file_io.read()

        # Создаем сессию только для загрузки
        async with async_session_maker() as session:
            service = IngestionService(session)
            metadata = await service.process_file(file_bytes, doc.file_name)

        await status_msg.edit_text(
            f"✅ *Файл загружен!*\n\n"
            f"📄 Имя: `{metadata.filename}`\n"
            f"📝 Описание: {metadata.description}"
        )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await status_msg.edit_text(f"❌ Ошибка обработки файла: {str(e)}")

@router.message(F.text)
async def handle_text(message: Message):
    """Обработка текста с памятью и графом"""
    user_id = message.from_user.id
    user_text = message.text

    # Показываем статус "печатает..."
    async with ChatActionSender.typing(bot=message.bot, chat_id=message.chat.id):
        
        # 1. Открываем сессию БД
        async with async_session_maker() as session:
            memory = MemoryService(session)
            
            # 2. Сохраняем сообщение пользователя в историю
            await memory.add_message(user_id, "user", user_text)

            try:
                # 3. Запускаем Граф (передаем вопрос и user_id)
                inputs = {
                    "question": user_text, 
                    "user_id": user_id,
                    "session_id": str(user_id)
                }
                
                result = await app_workflow.ainvoke(inputs)
                
                # Достаем результаты
                final_answer = result.get("final_answer", "Не удалось сформировать ответ.")
                plot_b64 = result.get("plot_base64")

                # 4. Сохраняем ответ БОТА в историю
                await memory.add_message(user_id, "assistant", final_answer)

                # 5. Отправляем ответ в Telegram
                if plot_b64:
                    # Если есть график
                    plot_bytes = base64.b64decode(plot_b64)
                    caption = final_answer[:1000] if final_answer else "График"
                    
                    await message.answer_photo(
                        photo=BufferedInputFile(plot_bytes, filename="chart.png"),
                        caption=caption
                    )
                    # Если текст длинный, досылаем остаток
                    if len(final_answer) > 1000:
                        await message.answer(final_answer[1000:])
                else:
                    # Просто текст
                    await message.answer(final_answer)

            except Exception as e:
                logger.error(f"Workflow error: {e}")
                await message.answer(f"⚠️ Произошла ошибка: {str(e)}")