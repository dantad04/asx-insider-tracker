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
    enable_in_app_scheduler: bool = True
    job_trigger_token: str | None = None

    # Cluster Portfolio automation
    cluster_portfolio_enabled: bool = False
    cluster_portfolio_dry_run: bool = False
    cluster_portfolio_email_enabled: bool = False
    cluster_portfolio_email_to: str | None = None

    # SMTP
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from: str | None = None
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False
    smtp_timeout_seconds: int = 15

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
