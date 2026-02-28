"""
Sync historical stock price data from Yahoo Finance for all ASX tickers.

Usage:
    docker-compose exec backend python -m app.scripts.sync_price_data --all
    docker-compose exec backend python -m app.scripts.sync_price_data --tickers BHP,CBA,NAB
    docker-compose exec backend python -m app.scripts.sync_price_data --limit 10
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta

import yfinance as yf
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.price_snapshot import PriceSnapshot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for Yahoo Finance requests."""

    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / requests_per_second
        self._last_request = 0.0

    def acquire(self):
        """Wait until next request is allowed."""
        now = time.time()
        time_since_last = now - self._last_request

        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)

        self._last_request = time.time()


def format_asx_ticker(ticker: str) -> str:
    """
    Format ticker for yfinance (add .AX suffix for ASX).

    Args:
        ticker: ASX ticker code (e.g., "BHP")

    Returns:
        Formatted ticker (e.g., "BHP.AX")
    """
    ticker = ticker.strip().upper()
    if not ticker.endswith(".AX"):
        ticker = f"{ticker}.AX"
    return ticker


def fetch_price_data(
    ticker: str,
    start_date: datetime.date,
    end_date: datetime.date,
    rate_limiter: RateLimiter,
    max_retries: int = 3
) -> dict | None:
    """
    Fetch daily OHLCV data from Yahoo Finance.

    Args:
        ticker: ASX ticker (will add .AX suffix)
        start_date: Start date for historical data
        end_date: End date for historical data
        rate_limiter: Rate limiter instance
        max_retries: Number of retries on failure

    Returns:
        Dictionary with date as key and OHLCV data as value, or None if failed
    """
    formatted_ticker = format_asx_ticker(ticker)

    # Rate limit
    rate_limiter.acquire()

    for attempt in range(max_retries):
        try:
            logger.debug(f"Fetching price data for {formatted_ticker}")

            # Download data from yfinance
            df = yf.download(
                formatted_ticker,
                start=start_date,
                end=end_date,
                progress=False,
                timeout=10
            )

            if df.empty:
                logger.warning(f"No price data found for {formatted_ticker}")
                return None

            # Convert to dictionary format
            prices = {}
            for idx, row in df.iterrows():
                date_key = idx.date()
                prices[date_key] = {
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"])
                }

            logger.debug(f"Got {len(prices)} price records for {formatted_ticker}")
            return prices

        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(
                    f"Error fetching {formatted_ticker} (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch price data for {formatted_ticker}: {e}")
                return None

    return None


async def sync_ticker_prices(
    ticker: str,
    session: AsyncSession,
    start_date: datetime.date,
    end_date: datetime.date,
    rate_limiter: RateLimiter,
    stats: dict[str, int]
) -> None:
    """
    Sync price data for a single ticker.

    Args:
        ticker: ASX ticker code
        session: Database session
        start_date: Start date for historical data
        end_date: End date for historical data
        rate_limiter: Rate limiter instance
        stats: Statistics dictionary
    """
    logger.debug(f"Syncing prices for {ticker}")

    # Fetch price data
    prices = fetch_price_data(ticker, start_date, end_date, rate_limiter)

    if not prices:
        logger.warning(f"No price data available for {ticker}")
        stats["skipped"] += 1
        return

    # Check which dates already exist
    result = await session.execute(
        select(PriceSnapshot.date).where(
            PriceSnapshot.ticker == ticker
        )
    )
    existing_dates = {row[0] for row in result.all()}

    # Insert new price records
    inserted_count = 0

    for date_key, price_data in prices.items():
        # Skip if already exists
        if date_key in existing_dates:
            continue

        price_snapshot = PriceSnapshot(
            ticker=ticker,
            date=date_key,
            open=price_data["open"],
            high=price_data["high"],
            low=price_data["low"],
            close=price_data["close"],
            volume=price_data["volume"]
        )

        session.add(price_snapshot)
        inserted_count += 1

    if inserted_count > 0:
        logger.info(f"Inserted {inserted_count} price records for {ticker}")
        stats["prices_inserted"] += inserted_count
        stats["tickers_synced"] += 1
    else:
        logger.debug(f"No new price records for {ticker}")
        stats["skipped"] += 1


async def sync_tickers(
    tickers: list[str],
    batch_size: int = 50,
    progress_interval: int = 100
) -> dict[str, int]:
    """
    Sync price data for multiple tickers.

    Args:
        tickers: List of ASX ticker codes
        batch_size: Number of tickers before batch commit
        progress_interval: Log progress every N tickers

    Returns:
        Statistics dictionary
    """
    # Calculate date range (3 years ago to today)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * 3)

    logger.info(f"Fetching price data from {start_date} to {end_date}")

    # Initialize stats
    stats = {
        "tickers_synced": 0,
        "prices_inserted": 0,
        "skipped": 0,
    }

    rate_limiter = RateLimiter(requests_per_second=1.0)

    async with async_session() as session:
        for i, ticker in enumerate(tickers, 1):
            try:
                await sync_ticker_prices(
                    ticker, session, start_date, end_date, rate_limiter, stats
                )

                # Batch commit every N tickers
                if i % batch_size == 0:
                    await session.commit()
                    logger.info(f"Committed batch at {i} tickers")

                # Progress logging
                if i % progress_interval == 0:
                    logger.info(
                        f"Progress: {i}/{len(tickers)} tickers "
                        f"({stats['tickers_synced']} synced, "
                        f"{stats['prices_inserted']} prices inserted)"
                    )

            except Exception as e:
                logger.error(f"Error syncing {ticker}: {e}")
                await session.rollback()
                continue

        # Final commit
        await session.commit()

    return stats


async def get_all_tickers() -> list[str]:
    """Get all company tickers from database."""
    async with async_session() as session:
        result = await session.execute(select(Company.ticker))
        return [row[0] for row in result.all()]


async def main(tickers: list[str] | None = None, limit: int | None = None) -> None:
    """Main entry point."""
    logger.info("Starting price data sync from Yahoo Finance")

    # Determine tickers to process
    if tickers:
        tickers_to_sync = [t.strip().upper() for t in tickers]
        logger.info(f"Processing {len(tickers_to_sync)} specified tickers")
    else:
        tickers_to_sync = await get_all_tickers()
        logger.info(f"Processing all {len(tickers_to_sync)} tickers from database")

    if not tickers_to_sync:
        logger.error("No tickers to process")
        sys.exit(1)

    # Apply limit if specified
    if limit:
        tickers_to_sync = tickers_to_sync[:limit]
        logger.info(f"Limited to {limit} tickers")

    # Sync prices
    start_time = time.time()
    stats = await sync_tickers(tickers_to_sync)
    elapsed_time = time.time() - start_time

    # Print summary
    print("\n" + "=" * 60)
    print("PRICE DATA SYNC SUMMARY")
    print("=" * 60)
    print(f"Tickers synced: {stats['tickers_synced']}/{len(tickers_to_sync)}")
    print(f"Price records inserted: {stats['prices_inserted']:,}")
    print(f"Tickers skipped (no data): {stats['skipped']}")
    print(f"Elapsed time: {elapsed_time:.2f}s")
    print(f"Average time per ticker: {elapsed_time / len(tickers_to_sync):.2f}s")
    print("=" * 60)

    logger.info("Price data sync completed successfully")


if __name__ == "__main__":
    import asyncio

    parser = argparse.ArgumentParser(
        description="Sync historical stock price data from Yahoo Finance"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Sync all tickers from database"
    )
    group.add_argument(
        "--tickers",
        help="Comma-separated list of tickers (e.g., BHP,CBA,NAB)"
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of tickers to process (for testing)"
    )

    args = parser.parse_args()

    tickers_arg = None
    if args.tickers:
        tickers_arg = args.tickers.split(",")

    asyncio.run(main(tickers=tickers_arg, limit=args.limit))
