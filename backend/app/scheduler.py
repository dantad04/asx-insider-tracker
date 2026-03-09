"""
Background scheduler setup (currently minimal).

Syncing is now lazy: triggered on page load if >1 hour since last sync,
plus manual refresh button. No background scheduled jobs.

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
    """Initialize and return the APScheduler instance.

    Currently disabled — syncing now happens on-demand via lazy sync
    (page load if >1 hour since last sync) + manual refresh button.
    """
    scheduler = AsyncIOScheduler()
    # No scheduled jobs — sync is triggered by frontend lazy load
    logger.info("✓ Scheduler started (no jobs — using lazy sync on page load)")
    return scheduler
