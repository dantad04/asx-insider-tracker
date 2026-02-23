"""Configuration management using Pydantic Settings"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from environment variables"""

    # Database
    database_url: str
    postgres_user: str
    postgres_password: str
    postgres_db: str

    # Application
    app_env: str = "development"
    app_version: str = "0.1.0"
    debug: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
