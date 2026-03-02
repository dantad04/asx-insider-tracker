"""Configuration management using Pydantic Settings"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration loaded from environment variables"""

    # Database (required)
    database_url: str

    # Database credentials (optional, for reference/Docker Compose)
    postgres_user: str = "asx_tracker"
    postgres_password: str = "postgres"
    postgres_db: str = "asx_tracker"

    # Application
    app_env: str = "development"
    app_version: str = "0.3.0"
    debug: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
