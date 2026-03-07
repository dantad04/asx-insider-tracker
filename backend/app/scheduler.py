"""
Background scheduler for syncing ASX insider trades.

Runs sync_from_asxinsider every 2 minutes — fast due to early exit once
the newest already-imported record is hit (only new trades are processed).

Usage: Wire into FastAPI lifespan context in main.py
"""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


async def sync_asxinsider_trades():
    """Sync new trades from asxinsider.com.au (GPT-parsed, newest-first)."""
    logger.info(f"Running asxinsider sync at {datetime.now().isoformat()}")

    try:
        from app.scripts.sync_from_asxinsider import main as sync_main

        stats = await sync_main()
        if stats:
            logger.info(
                f"✓ Sync complete: {stats.get('inserted', 0)} inserted, "
                f"{stats.get('replaced', 0)} replaced, "
                f"{stats.get('upgraded', 0)} upgraded, "
                f"{stats.get('checks', 0)} checks"
            )
        else:
            logger.warning("Sync returned no stats — check ASXINSIDER_URL env var")

    except Exception as e:
        logger.error(f"✗ Sync failed: {e}", exc_info=True)


def setup_scheduler():
    """Initialize and return the APScheduler instance."""
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        sync_asxinsider_trades,
        trigger=IntervalTrigger(minutes=2),
        id="asxinsider_sync",
        name="asxinsider.com.au sync every 2 minutes",
        replace_existing=True,
        coalesce=True,      # skip missed runs instead of stacking
        max_instances=1,    # never run concurrently
    )

    msg = "✓ Scheduler started: asxinsider sync every 2 minutes"
    logger.info(msg)
    print(msg)
    return scheduler
