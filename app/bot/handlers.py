import io
import base64
from aiogram import Router, F, Bot
from aiogram.types import Message, BufferedInputFile, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart
from aiogram.utils.chat_action import ChatActionSender
from sqlalchemy import select, delete
from loguru import logger

from app.services.ingestion import IngestionService
from app.services.workflow import app_workflow
from app.services.memory import MemoryService
from app.db.base import async_session_maker
from app.db.models import FileMetadata, ChatHistory
from app.bot.middlewares import AuthMiddleware 

router = Router()

# --- КЛАВИАТУРА ---
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Мои файлы"), KeyboardButton(text="🗑 Очистить память")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите действие или напишите вопрос..."
)

# --- ХЕНДЛЕРЫ ---

router.message.middleware(AuthMiddleware())

@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я Parus AI — твой ассистент ПЭО.\n\n"
        "Я умею:\n"
        "1. 📊 Анализировать Excel (графики, расчеты).\n"
        "2. 🧠 Отвечать на вопросы по документам (PDF).\n"
        "3. 💾 Помнить контекст беседы.\n\n"
        "Выбери действие в меню ниже 👇",
        reply_markup=main_kb
    )

@router.message(F.text == "❓ Помощь")
async def cmd_help(message: Message):
    text = (
        "🤖 **Как со мной работать?**\n\n"
        "📂 **Загрузка данных:**\n"
        "Просто перетащи сюда файл `.xlsx`, `.csv` или `.pdf`.\n\n"
        "🗣 **Примеры вопросов:**\n"
        "— *Построй график затрат по месяцам (из Excel)*\n"
        "— *Сделай линию графика красной*\n"
        "— *Какие штрафы предусмотрены в договоре? (из PDF)*\n"
        "— *Сравни план и факт за январь*\n\n"
        "💡 *Совет: Если я запутался, нажми 'Очистить память'.*"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(F.text == "🗑 Очистить память")
async def cmd_clear_history(message: Message):
    user_id = message.from_user.id
    async with async_session_maker() as session:
        # Удаляем записи из таблицы ChatHistory для этого пользователя
        await session.execute(delete(ChatHistory).where(ChatHistory.user_id == user_id))
        await session.commit()
    
    await message.answer("🧹 **Память очищена!**\nЯ забыл контекст предыдущей беседы. Можем начинать с новой темы.")

@router.message(F.text == "📊 Мои файлы")
async def cmd_my_files(message: Message):
    async with async_session_maker() as session:
        # Выбираем последние 10 файлов
        result = await session.execute(
            select(FileMetadata).order_by(FileMetadata.upload_date.desc()).limit(10)
        )
        files = result.scalars().all()

    if not files:
        await message.answer("📂 Вы пока не загрузили ни одного файла.")
        return

    text = "📂 **Последние загруженные файлы:**\n\n"
    for f in files:
        icon = "📕" if f.filename.endswith(".pdf") else "📊"
        text += f"{icon} `{f.filename}`\nFAILED"
        # (Небольшая визуальная фишка: показываем тип файла и дату)
        date_str = f.upload_date.strftime("%d.%m %H:%M")
        text += f"   └ _{date_str}_ | {f.description[:30]}...\n\n"
    
    await message.answer(text, parse_mode="Markdown")

# --- ОБРАБОТКА ФАЙЛОВ ---
@router.message(F.document)
async def handle_document(message: Message, bot: Bot):
    doc = message.document
    if not doc.file_name.endswith(('.xlsx', '.csv', '.pdf')):
        await message.answer("❌ Поддерживаются только файлы: .xlsx, .csv, .pdf")
        return

    status_msg = await message.answer("⏳ Скачиваю и анализирую файл...")
    
    try:
        file_io = await bot.download(doc)
        file_bytes = file_io.read()

        async with async_session_maker() as session:
            service = IngestionService(session)
            metadata = await service.process_file(file_bytes, doc.file_name)

        await status_msg.edit_text(
            f"✅ *Файл загружен!*\n\n"
            f"📄 Имя: `{metadata.filename}`\n"
            f"📝 Статус: {metadata.description}"
        )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await status_msg.edit_text(f"❌ Ошибка обработки файла: {str(e)}")

# --- ОБРАБОТКА ТЕКСТА (AI) ---
# Этот хендлер ловит всё остальное
@router.message(F.text)
async def handle_text(message: Message):
    user_id = message.from_user.id
    user_text = message.text

    async with ChatActionSender.typing(bot=message.bot, chat_id=message.chat.id):
        async with async_session_maker() as session:
            memory = MemoryService(session)
            await memory.add_message(user_id, "user", user_text)

            try:
                inputs = {
                    "question": user_text, 
                    "user_id": user_id,
                    "session_id": str(user_id)
                }
                
                result = await app_workflow.ainvoke(inputs)
                
                final_answer = result.get("final_answer", "Не удалось сформировать ответ.")
                plot_b64 = result.get("plot_base64")

                await memory.add_message(user_id, "assistant", final_answer)

                if plot_b64:
                    plot_bytes = base64.b64decode(plot_b64)
                    caption = final_answer[:1000] if final_answer else "График"
                    await message.answer_photo(
                        photo=BufferedInputFile(plot_bytes, filename="chart.png"),
                        caption=caption
                    )
                    if len(final_answer) > 1000:
                        await message.answer(final_answer[1000:])
                else:
                    await message.answer(final_answer)

            except Exception as e:
                logger.error(f"Workflow error: {e}")
                await message.answer(f"⚠️ Произошла ошибка: {str(e)}")