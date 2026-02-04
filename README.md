# Parus AI — Интеллектуальный ассистент ПЭО

**Parus AI** — это система класса *Enterprise Assistant*, разработанная для Планово-экономического отдела (ПЭО). Она позволяет анализировать Excel-файлы, строить графики и отвечать на вопросы по бюджету/плану, используя естественный язык.

Система работает полностью **On-premise** (локально), обеспечивая безопасность конфиденциальных данных предприятия.

## ✨ Ключевые возможности

* **📂 Умная загрузка файлов**: Автоматический анализ структуры Excel/CSV файлов и сохранение семантического описания колонок (Metadata Extraction).
* **🐍 Dynamic Python Sandbox**: Генерация и исполнение кода Pandas на лету для ответов на сложные аналитические вопросы ("Посчитай отклонение факта от плана за 1 квартал").
* **📊 Генерация графиков**: Построение диаграмм (Matplotlib) по текстовому запросу и отправка их в чат.
* **❤️‍🩹 Self-Healing Code**: Если нейросеть написала код с ошибкой, система сама анализирует Traceback, исправляет код и перезапускает его.
* **🧠 Локальный LLM**: Работает на базе **Qwen 2.5 (7B)** через Ollama. Данные не уходят в облако.
* **🔀 Умная маршрутизация**: LangGraph определяет тип вопроса (SQL-запрос к справочникам, Аналитика файла или Общий разговор).

---

## 🛠 Технологический стек

* **Язык**: Python 3.11
* **Интерфейс**: Aiogram 3.x (Telegram Bot)
* **LLM Orchestration**: LangChain, LangGraph
* **LLM Engine**: Ollama (Model: Qwen 2.5)
* **Data Processing**: Pandas, Matplotlib, OpenPyXL
* **Storage**: MinIO (S3-compatible) — хранение файлов.
* **PostgreSQL** — хранение метаданных и истории.


* **Infrastructure**: Docker Compose.

---

## 🚀 Установка и Запуск

### Предварительные требования

* Docker & Docker Compose
* Python 3.11+
* PostgreSQL (установленный локально или на сервере)

### Шаг 1. Клонирование и настройка окружения

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-username/parus-ai.git
cd parus-ai

```


2. Создайте виртуальное окружение и установите зависимости:
```bash
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt

```


3. Создайте файл `.env` в корне проекта:
```dotenv
# --- Telegram ---
TELEGRAM_BOT_TOKEN=ваш_токен_от_botfather

# --- Database (PostgreSQL) ---
# Формат: postgresql+asyncpg://user:pass@host:port/dbname
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/parus_db

# --- MinIO (S3) ---
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadminpassword
MINIO_BUCKET_NAME=parus-files
MINIO_SECURE=False

# --- Ollama ---
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=qwen2.5:7b

```



### Шаг 2. Запуск инфраструктуры

1. Запустите Docker-контейнеры (MinIO, Ollama):
```bash
docker-compose up -d

```


2. Загрузите модель в Ollama (выполните один раз):
```bash
docker exec -it parus_ollama ollama run qwen2.5:3b

```


3. Создайте базу данных в PostgreSQL (если еще нет):
```sql
CREATE DATABASE parus_db;

```



### Шаг 3. Запуск приложения

```bash
python main.py

```

*При первом запуске система автоматически создаст необходимые таблицы в базе данных.*

---

## 📖 Как пользоваться

1. **Начало работы**: Откройте бота в Telegram и нажмите `/start`.
2. **Загрузка данных**: Отправьте боту `.xlsx` файл (например, бюджет или отчет).
* *Бот ответит: "✅ Файл загружен" и покажет список колонок.*


3. **Аналитика**:
* 📝 *Текст:* "Какая сумма горной массы за Февраль?"
* 📈 *График:* "Построй график объемов за 1 квартал."
* 🧮 *Сложный расчет:* "Сравни показатели января и марта."



---

## 📂 Структура проекта

```text
parus_ai/
├── app/
│   ├── bot/                 # Хендлеры и Middleware Telegram
│   ├── core/                # Конфигурация (Pydantic) и Логгер
│   ├── db/                  # Модели SQLAlchemy и инициализация БД
│   ├── services/
│   │   ├── analytics.py     # Sandbox: Генерация Python-кода и графиков
│   │   ├── ingestion.py     # Анализ структуры Excel файлов
│   │   ├── storage.py       # Клиент MinIO
│   │   ├── workflow.py      # Граф маршрутизации (LangGraph)
│   │   └── sql_agent.py     # Text-to-SQL (экспериментально)
│   └── schemas/             # Pydantic модели
├── main.py                  # Точка входа
├── docker-compose.yaml      # Инфраструктура
├── requirements.txt         # Зависимости
└── .env                     # Переменные окружения (секреты)

```

---

## 🛡 Безопасность

* Код исполняется в изолированном пространстве `exec()` с ограниченным набором глобальных переменных.
* Пароли и ключи хранятся только в `.env` и не попадают в репозиторий.
* Данные файлов хранятся в вашем локальном контуре (On-premise S3).

---

## 🔮 План развития (Roadmap)

* [x] Поддержка истории диалогов (Context Memory).
* [x] Docker-образ для самого приложения (`app`).
* [ ] Векторный поиск (RAG) для работы с большими текстовыми документами (договоры, приказы).
* [ ] Админ-панель для управления доступами.