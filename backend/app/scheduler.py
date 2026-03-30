"""
Background scheduler — runs every 2 minutes.

Jobs:
  1. sync_asxinsider_trades  — fetch new trades from asxinsider.com.au
  2. run_portfolio_simulation — update live paper trading portfolio
"""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


async def sync_asxinsider_trades():
    """Sync new trades from asxinsider.com.au, then update the live portfolio."""
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

    # Run portfolio update after each sync, regardless of whether sync found new trades
    try:
        from app.scripts.run_portfolio import run as run_portfolio
        await run_portfolio()
    except Exception as e:
        logger.error(f"✗ Portfolio update failed: {e}", exc_info=True)


def setup_scheduler():
    """Initialize the APScheduler with a 2-minute sync+portfolio interval."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        sync_asxinsider_trades,
        trigger=IntervalTrigger(minutes=2),
        id="sync_and_portfolio",
        name="Sync trades + update portfolio",
        replace_existing=True,
    )
    logger.info("✓ Scheduler started — sync + portfolio every 2 minutes")
    return scheduler
