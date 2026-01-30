from pydantic_settings import BaseSettings
from pydantic import PostgresDsn

class Settings(BaseSettings):
    # App
    APP_NAME: str = "Parus AI"

    TELEGRAM_BOT_TOKEN: str
    
    # Database (External PostgreSQL)
    DATABASE_URL: PostgresDsn
    
    # MinIO
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET_NAME: str = "parus-files"
    MINIO_SECURE: bool = False # False for local dev without SSL
    
    # LLM
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3"

    class Config:
        env_file = ".env"

settings = Settings()