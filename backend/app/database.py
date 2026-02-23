"""Database configuration and session management"""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

# Create async engine for PostgreSQL
engine = create_async_engine(
    settings.database_url,
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
    """Initialize database tables (only for development)"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db() -> None:
    """Close database connection"""
    await engine.dispose()
