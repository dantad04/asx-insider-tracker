"""Health check endpoint"""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import __version__
from app.config import settings
from app.database import get_db

router = APIRouter()


@router.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Health check endpoint that verifies API and database connectivity.

    Returns:
        Dictionary with status, db connection status, timestamp, and app version
    """
    db_status = "connected"

    try:
        # Simple query to verify database connection
        await db.execute(text("SELECT 1"))
    except Exception:
        db_status = "disconnected"

    return {
        "status": "ok",
        "db": db_status,
        "timestamp": datetime.utcnow().isoformat(),
        "version": __version__,
        "environment": settings.app_env,
    }
