"""Authenticated maintenance job endpoints for external schedulers."""

from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.scheduler import (
    refresh_cluster_portfolio_prices,
    run_daily_cluster_update,
    sync_asxinsider_trades,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

MELBOURNE_TZ = ZoneInfo("Australia/Melbourne")
MAINTENANCE_LOCK_ID = 49251001
SYNC_INTERVAL = timedelta(minutes=5)
PRICE_REFRESH_INTERVAL = timedelta(minutes=15)
DAILY_CLUSTER_TIME = time(hour=18, minute=30)


class JobStepResult(BaseModel):
    status: str
    reason: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None


class MaintenanceResponse(BaseModel):
    status: str
    steps: dict[str, JobStepResult]


def _require_job_token(x_job_token: str | None) -> None:
    if not settings.job_trigger_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JOB_TRIGGER_TOKEN is not configured",
        )
    if x_job_token != settings.job_trigger_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid job trigger token",
        )


async def _try_lock(db: AsyncSession) -> bool:
    result = await db.execute(
        text("SELECT pg_try_advisory_lock(:lock_id)"),
        {"lock_id": MAINTENANCE_LOCK_ID},
    )
    return bool(result.scalar_one())


async def _release_lock(db: AsyncSession) -> None:
    await db.execute(
        text("SELECT pg_advisory_unlock(:lock_id)"),
        {"lock_id": MAINTENANCE_LOCK_ID},
    )


async def _last_success(db: AsyncSession, job_name: str) -> datetime | None:
    result = await db.execute(
        text("SELECT last_success_at FROM app_job_runs WHERE job_name = :job_name"),
        {"job_name": job_name},
    )
    return result.scalar_one_or_none()


async def _record_job(
    db: AsyncSession,
    job_name: str,
    started_at: datetime,
    status_value: str,
    summary: dict[str, Any],
    finished_at: datetime,
) -> None:
    last_success_at = finished_at if status_value == "success" else None
    stmt = text(
        """
        INSERT INTO app_job_runs (
            job_name, last_started_at, last_success_at, last_status, last_summary
        )
        VALUES (
            :job_name,
            :started_at,
            :last_success_at,
            :status_value,
            :summary
        )
        ON CONFLICT (job_name) DO UPDATE SET
            last_started_at = EXCLUDED.last_started_at,
            last_success_at = COALESCE(
                EXCLUDED.last_success_at,
                app_job_runs.last_success_at
            ),
            last_status = EXCLUDED.last_status,
            last_summary = EXCLUDED.last_summary
        """
    ).bindparams(bindparam("summary", type_=JSONB))
    await db.execute(
        stmt,
        {
            "job_name": job_name,
            "started_at": started_at,
            "finished_at": finished_at,
            "last_success_at": last_success_at,
            "status_value": status_value,
            "summary": summary,
        },
    )
    await db.commit()


async def _run_due_job(
    db: AsyncSession,
    job_name: str,
    min_interval: timedelta,
    runner,
) -> JobStepResult:
    now = datetime.now(timezone.utc)
    previous_success = await _last_success(db, job_name)
    if previous_success and now - previous_success < min_interval:
        return JobStepResult(status="skipped", reason="not_due")

    started_at = now
    try:
        await runner()
        finished_at = datetime.now(timezone.utc)
        await _record_job(
            db,
            job_name,
            started_at,
            "success",
            {"finished_at": finished_at.isoformat()},
            finished_at,
        )
        return JobStepResult(
            status="success",
            started_at=started_at,
            finished_at=finished_at,
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        logger.exception("Maintenance job failed: %s", job_name)
        await _record_job(
            db,
            job_name,
            started_at,
            "failed",
            {"error": str(exc), "finished_at": finished_at.isoformat()},
            finished_at,
        )
        return JobStepResult(
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            error=str(exc),
        )


async def _run_daily_if_due(db: AsyncSession) -> JobStepResult:
    melbourne_now = datetime.now(MELBOURNE_TZ)
    if melbourne_now.time() < DAILY_CLUSTER_TIME:
        return JobStepResult(status="skipped", reason="before_daily_window")

    previous_success = await _last_success(db, "daily_cluster_update")
    if previous_success and previous_success.astimezone(MELBOURNE_TZ).date() == melbourne_now.date():
        return JobStepResult(status="skipped", reason="already_ran_today")

    return await _run_due_job(
        db,
        "daily_cluster_update",
        timedelta(hours=1),
        run_daily_cluster_update,
    )


@router.post("/maintenance", response_model=MaintenanceResponse)
async def run_maintenance(
    x_job_token: str | None = Header(default=None, alias="X-Job-Token"),
    db: AsyncSession = Depends(get_db),
) -> MaintenanceResponse:
    """Run due production maintenance tasks from Railway Cron."""
    _require_job_token(x_job_token)

    locked = await _try_lock(db)
    if not locked:
        return MaintenanceResponse(
            status="skipped",
            steps={
                "maintenance": JobStepResult(
                    status="skipped",
                    reason="another_run_in_progress",
                )
            },
        )

    try:
        steps = {
            "sync_trades": await _run_due_job(
                db,
                "sync_trades",
                SYNC_INTERVAL,
                sync_asxinsider_trades,
            ),
            "cluster_portfolio_price_refresh": await _run_due_job(
                db,
                "cluster_portfolio_price_refresh",
                PRICE_REFRESH_INTERVAL,
                refresh_cluster_portfolio_prices,
            ),
            "daily_cluster_update": await _run_daily_if_due(db),
        }
        response_status = "ok" if all(
            step.status in {"success", "skipped"} for step in steps.values()
        ) else "failed"
        return MaintenanceResponse(status=response_status, steps=steps)
    finally:
        try:
            await _release_lock(db)
        except Exception:
            await db.rollback()
            await _release_lock(db)
