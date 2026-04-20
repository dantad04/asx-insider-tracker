"""
Background scheduler for lightweight ongoing maintenance.

Jobs:
  1. sync_asxinsider_trades            — fetch new trades from asxinsider.com.au
  2. refresh_cluster_portfolio_prices  — keep open paper positions freshly priced
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


async def refresh_cluster_portfolio_prices():
    """Refresh stored closes for currently open Cluster Portfolio tickers."""
    logger.info(f"Running Cluster Portfolio price refresh at {datetime.now().isoformat()}")

    try:
        from app.database import async_session
        from app.services.cluster_portfolio import ClusterPortfolioEngine
        from app.services.price_updater import refresh_recent_prices_for_tickers

        engine = ClusterPortfolioEngine()
        async with async_session() as session:
            portfolio = await engine.get_default_portfolio(session)
            if portfolio is None:
                logger.info("Cluster Portfolio price refresh skipped — no portfolio yet")
                return

            positions = await engine.list_positions(session, portfolio.id)
            open_tickers = sorted({position.ticker for position in positions if position.status == "open"})
            if not open_tickers:
                logger.info("Cluster Portfolio price refresh skipped — no open positions")
                return

            refresh_result = await refresh_recent_prices_for_tickers(session, open_tickers)
            await session.commit()
            logger.info(
                "✓ Cluster Portfolio prices refreshed: %s/%s tickers updated (%s failed)",
                refresh_result.get("updated", 0),
                refresh_result.get("attempted", 0),
                refresh_result.get("failed", 0),
            )
            if refresh_result.get("errors"):
                logger.warning(
                    "Cluster Portfolio price refresh issues: %s",
                    refresh_result["errors"],
                )
    except Exception as e:
        logger.error(f"✗ Cluster Portfolio price refresh failed: {e}", exc_info=True)


def setup_scheduler():
    """Initialize the APScheduler with trade sync and portfolio price refresh jobs."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        sync_asxinsider_trades,
        trigger=IntervalTrigger(minutes=2),
        id="sync_and_portfolio",
        name="Sync trades + update portfolio",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_cluster_portfolio_prices,
        trigger=IntervalTrigger(minutes=15),
        id="cluster_portfolio_price_refresh",
        name="Refresh open Cluster Portfolio prices",
        replace_existing=True,
    )
    logger.info(
        "✓ Scheduler started — trade sync every 2 minutes, "
        "Cluster Portfolio price refresh every 15 minutes"
    )
    return scheduler
