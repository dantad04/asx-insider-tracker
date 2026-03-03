"""Database configuration and session management"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

# Ensure we use the async driver (Railway provides postgresql://, we need postgresql+asyncpg://)
_db_url = settings.database_url
if _db_url.startswith("postgresql://"):
    _db_url = _db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

# Create async engine for PostgreSQL
engine = create_async_engine(
    _db_url,
    echo=settings.debug,
    future=True,
)

# Create async session factory
async_session = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    future=True,
)

# Base class for all models
Base = declarative_base()


async def get_db() -> AsyncSession:
    """Dependency for getting database session in route handlers"""
    async with async_session() as session:
        yield session


async def init_db() -> None:
    """Initialize database - verify connection (schema managed by Alembic)"""
    async with engine.begin() as conn:
        # Just verify the connection is working
        # Schema is managed by Alembic migrations, not by create_all
        await conn.execute(text("SELECT 1"))


async def close_db() -> None:
    """Close database connection"""
    await engine.dispose()
