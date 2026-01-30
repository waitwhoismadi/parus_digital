import os
import base64
from aiogram import Router, F, Bot
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import CommandStart
from aiogram.utils.chat_action import ChatActionSender
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
from app.services.ingestion import IngestionService
from app.services.workflow import app_workflow

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я Parus AI — твой ассистент ПЭО.\n\n"
        "📂 *Загрузи Excel-файл* для анализа.\n"
        "📊 *Спроси* о данных (например: 'Построй график затрат').\n"
        "💾 *Спроси* справочную информацию из БД."
    )

@router.message(F.document)
async def handle_document(message: Message, bot: Bot, db_session: AsyncSession):
    """Обработка загрузки файлов (Excel/CSV)"""
    doc = message.document
    
    # Проверка формата
    if not doc.file_name.endswith(('.xlsx', '.csv')):
        await message.answer("❌ Поддерживаются только .xlsx и .csv файлы.")
        return

    status_msg = await message.answer("⏳ Скачиваю и анализирую структуру файла...")
    
    try:
        # Скачиваем файл в память
        file_io = await bot.download(doc)
        file_bytes = file_io.read()

        # Запускаем Ingestion Service
        service = IngestionService(db_session)
        metadata = await service.process_file(file_bytes, doc.file_name)

        await status_msg.edit_text(
            f"✅ *Файл загружен!*\n\n"
            f"📄 Имя: `{metadata.filename}`\n"
            f"📝 Описание содержимого: {metadata.description}\n\n"
            f"Теперь вы можете задавать вопросы по этому файлу."
        )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await status_msg.edit_text(f"❌ Ошибка обработки файла: {str(e)}")

@router.message(F.text)
async def handle_text(message: Message):
    """Обработка текстовых запросов через LangGraph"""
    user_query = message.text
    
    # Используем 'typing', чтобы показать активность
    async with ChatActionSender.typing(bot=message.bot, chat_id=message.chat.id):
        try:
            # Запуск графа (Шаг 4)
            # stream_mode="values" позволяет получать обновления состояния, если нужно
            result = await app_workflow.ainvoke({
                "question": user_query,
                "session_id": str(message.from_user.id)
            })

            final_answer = result.get("final_answer", "Не удалось сформировать ответ.")
            plot_b64 = result.get("plot_base64")

            # Если есть график — декодируем и отправляем
            if plot_b64:
                plot_bytes = base64.b64decode(plot_b64)
                # Отправляем фото с подписью (обрезаем подпись, если длинная)
                caption = final_answer[:1000] if final_answer else "Результат анализа"
                await message.answer_photo(
                    photo=BufferedInputFile(plot_bytes, filename="chart.png"),
                    caption=caption
                )
                # Если ответ был длинный и обрезался, досылаем текстом
                if len(final_answer) > 1000:
                    await message.answer(final_answer[1000:])
            else:
                # Просто текстовый ответ
                await message.answer(final_answer)

        except Exception as e:
            logger.error(f"Workflow error: {e}")
            await message.answer("⚠️ Произошла внутренняя ошибка при обработке запроса.")