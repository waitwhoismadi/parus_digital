import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from sqlalchemy import select
from loguru import logger
from langchain_community.chat_models import ChatOllama
from langchain.prompts import PromptTemplate

from app.db.base import async_session_maker
from app.db.models import FileMetadata
from app.services.storage import StorageService
from app.core.config import settings

class AnalystAgent:
    def __init__(self):
        self.storage = StorageService()
        self.llm = ChatOllama(
            model=settings.OLLAMA_MODEL, 
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0 # Для кода важна точность
        )

    async def run_analysis(self, user_id: int, question: str) -> dict:
        """Главный метод: находит файл -> пишет код -> исполняет -> возвращает результат"""
        
        # 1. Получаем последний файл (в идеале добавить фильтр по user_id, если добавите поле в БД)
        async with async_session_maker() as session:
            result = await session.execute(
                select(FileMetadata)
                .where(FileMetadata.file_type.in_(['xlsx', 'csv']))
                .order_by(FileMetadata.upload_date.desc())
                .limit(1)
            )
            file_meta = result.scalars().first()

        if not file_meta:
            return {"answer": "Я не нашел загруженных файлов (Excel/CSV) для анализа."}

        # 2. Скачиваем файл из MinIO
        try:
            file_data = self.storage.get_file(file_meta.minio_path)
            if file_meta.filename.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_data))
            else:
                df = pd.read_excel(io.BytesIO(file_data))
        except Exception as e:
            logger.error(f"Error loading file: {e}")
            return {"answer": "Ошибка при чтении файла данных."}

        # 3. Генерируем Python код
        code = await self._generate_code(df, question)
        
        # 4. Исполняем код
        result = self._execute_code(df, code)
        
        return result

    async def _generate_code(self, df: pd.DataFrame, question: str) -> str:
        """Просит LLM написать код для pandas"""
        columns = list(df.columns)
        dtypes = str(df.dtypes)
        head = str(df.head(3))

        prompt = PromptTemplate.from_template(
            """
            Ты — Python Data Analyst. У тебя есть pandas DataFrame `df`.
            
            Структура данных:
            Колонки: {columns}
            Типы: {dtypes}
            Первые строки:
            {head}

            Задача: {question}

            Напиши Python код, который решает задачу.
            
            ПРАВИЛА:
            1. Код должен использовать переменную `df`.
            2. Если нужен график: 
               - Используй `matplotlib.pyplot` как `plt`.
               - НЕ используй `plt.show()`.
            3. Если нужен текстовый ответ:
               - Сохрани результат в переменную `result_text` (строка).
            4. ВЕРНИ ТОЛЬКО КОД. Без markdown, без ```python.
            """
        )

        chain = prompt | self.llm
        response = await chain.ainvoke({
            "columns": columns, 
            "dtypes": dtypes, 
            "head": head, 
            "question": question
        })
        
        # Чистим ответ от маркдауна
        code = response.content.replace("```python", "").replace("```", "").strip()
        logger.info(f"Generated Analysis Code:\n{code}")
        return code

    def _execute_code(self, df: pd.DataFrame, code: str) -> dict:
        """Безопасное(условно) выполнение кода"""
        
        # Буфер для перехвата графиков
        img_buffer = io.BytesIO()
        
        # Окружение, в котором выполняется код
        local_env = {
            "df": df, 
            "pd": pd, 
            "plt": plt, 
            "result_text": None
        }

        try:
            # Очищаем старые графики
            plt.clf()
            
            # --- EXEC ---
            exec(code, {}, local_env)
            # ------------

            final_answer = local_env.get("result_text", "Готово.")
            plot_base64 = None

            # Если код построил график (текущая фигура не пустая)
            if plt.gcf().get_axes():
                plt.savefig(img_buffer, format='png', bbox_inches='tight')
                img_buffer.seek(0)
                plot_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
                if not final_answer:
                    final_answer = "График построен."

            return {
                "answer": final_answer,
                "plot_base64": plot_base64
            }

        except Exception as e:
            logger.error(f"Code execution failed: {e}")
            return {"answer": f"Ошибка выполнения кода аналитика: {e}"}