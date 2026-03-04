"""
Background scheduler for ASX announcement scraping and PDF parsing.

Runs daily at 7 PM Sydney time (19:00 AEDT/AEST), shortly after ASX close at 4 PM.

Usage: Wire into FastAPI lifespan context in main.py
"""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


async def sync_asxinsider_trades():
    """Sync trades from asxinsider.com.au (GPT-parsed data)."""
    logger.info("=" * 60)
    logger.info("Starting scheduled asxinsider.com.au sync")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        from app.scripts.sync_asxinsider import main as sync_main

        logger.info("Syncing from asxinsider.com.au (GPT-parsed)...")
        try:
            stats = await sync_main()
            if stats:
                logger.info(
                    f"✓ Sync complete:\n"
                    f"  Inserted: {stats.get('inserted', 0)} new trades\n"
                    f"  Upgraded: {stats.get('upgraded', 0)} seed→GPT\n"
                    f"  Skipped: {stats.get('skipped_already_good', 0)} (already good)\n"
                    f"  Errors: {stats.get('errors', 0)}"
                )
            else:
                logger.warning(
                    "Sync returned no stats. "
                    "Check ASXINSIDER_URL environment variable is set."
                )
        except Exception as e:
            logger.error(f"✗ Sync failed: {e}", exc_info=True)

        logger.info("=" * 60)
        logger.info("✓ Scheduled sync completed")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"✗ Scheduled sync FAILED: {e}", exc_info=True)


def setup_scheduler():
    """Initialize and return the APScheduler instance."""
    scheduler = AsyncIOScheduler()

    # Schedule for 7 PM Sydney time daily (19:00 AEDT/AEST = 08:00 UTC)
    # Using Australia/Sydney timezone to handle DST automatically
    trigger = CronTrigger(
        hour=19,
        minute=0,
        timezone="Australia/Sydney",
        jitter=300  # Randomize up to 5 min to avoid exact times
    )

    scheduler.add_job(
        sync_asxinsider_trades,
        trigger=trigger,
        id="asxinsider_daily_sync",
        name="Daily asxinsider.com.au GPT-parsed trades sync",
        replace_existing=True,
        coalesce=True,  # Only run once if missed
        max_instances=1,  # Never run concurrently
    )

    msg = "✓ Scheduler started: Daily asxinsider sync at 7 PM Sydney time (19:00 AEDT/AEST)"
    logger.info(msg)
    print(msg)  # Ensure it shows in logs
    return scheduler
