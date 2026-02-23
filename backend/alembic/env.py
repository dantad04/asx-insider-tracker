import asyncio
import os
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from alembic import context

# Import models for autogenerate
from app.database import Base
from app.models import (
    Company,
    Director,
    DirectorCompany,
    Trade,
    PriceSnapshot,
    Announcement,
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata for autogenerate
target_metadata = Base.metadata

# Load database URL from environment
database_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/asx_db")

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = database_url
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode for async SQLAlchemy.

    In this scenario we need to create an AsyncEngine
    and associate a connection with the context.

    """

    def do_run_migrations(connection: Connection) -> None:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

    # For async migrations, run in an event loop
    async def async_run():
        connectable: AsyncEngine = create_async_engine(
            database_url,
            poolclass=pool.NullPool,
        )

        async with connectable.begin() as connection:
            await connection.run_sync(do_run_migrations)

        await connectable.dispose()

    asyncio.run(async_run())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
