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


async def scrape_and_parse_announcements():
    """Fetch new ASX announcements and parse the PDFs."""
    logger.info("=" * 60)
    logger.info("Starting scheduled ASX announcement update")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    try:
        # Import here to avoid circular imports
        from app.scripts.scrape_3y_announcements import main as scrape_main
        from app.scripts.parse_3y_pdfs import main as parse_main
        import argparse

        # Run scraper for today's announcements (recommended, no JavaScript)
        logger.info("Step 1: Scraping today's 3Y announcements from ASX...")
        try:
            # Use --today mode which is recommended and doesn't need Playwright
            scrape_args = argparse.Namespace(
                today=True,  # Scrape today's announcements only
                tickers=None,
                all=False,
                retry_from=None,
            )
            await scrape_main(scrape_args)
            logger.info("✓ Scraping completed")
        except Exception as e:
            logger.error(f"✗ Scraping failed: {e}", exc_info=True)
            # Don't raise - continue to parsing anyway in case PDFs exist

        # Run parser on downloaded PDFs
        logger.info("Step 2: Parsing downloaded PDFs...")
        try:
            await parse_main(limit=None)
            logger.info("✓ Parsing completed")
        except Exception as e:
            logger.error(f"✗ Parsing failed: {e}", exc_info=True)
            # Don't raise - log the error but let scheduler continue


        logger.info("=" * 60)
        logger.info("✓ Scheduled update completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"✗ Scheduled update FAILED: {e}", exc_info=True)


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
        scrape_and_parse_announcements,
        trigger=trigger,
        id="asx_daily_update",
        name="Daily ASX announcement scrape & parse",
        replace_existing=True,
        coalesce=True,  # Only run once if missed
        max_instances=1,  # Never run concurrently
    )

    msg = "✓ Scheduler started: Daily ASX update at 7 PM Sydney time (19:00 AEDT/AEST)"
    logger.info(msg)
    print(msg)  # Ensure it shows in logs
    return scheduler
