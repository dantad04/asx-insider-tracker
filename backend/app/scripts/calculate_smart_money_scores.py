"""
Calculate smart money scores for directors based on historical trade performance.

A "smart money score" measures whether a director's on-market purchases
consistently preceded price increases. Directors who buy before rallies score
highly; directors who buy at peaks score low.

Formula per director-company pair:
  For each on_market_buy with price data:
    forward_return_Nd = (close_price_at_trade_date+N - purchase_price) / purchase_price
  smart_money_score_Nd = average(forward_return_Nd) across all scored trades

Usage:
    docker-compose exec backend python -m app.scripts.calculate_smart_money_scores
    docker-compose exec backend python -m app.scripts.calculate_smart_money_scores --min-trades 3
    docker-compose exec backend python -m app.scripts.calculate_smart_money_scores --tickers BHP,CBA
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import logging
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.director_company import DirectorCompany
from app.models.price_snapshot import PriceSnapshot
from app.models.trade import Trade, TradeType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

FORWARD_WINDOWS = [30, 60, 90]  # calendar days


# ---------------------------------------------------------------------------
# Price data cache — keyed by ticker, sorted list of (date, close)
# ---------------------------------------------------------------------------

PriceSeries = list[tuple[date, float]]  # sorted ascending by date


def find_price_at_or_after(series: PriceSeries, target: date, max_delta_days: int = 7) -> float | None:
    """
    Return the closing price on or just after target date.

    Uses binary search on the sorted date list. Accepts up to max_delta_days
    of slippage to account for weekends and public holidays. If the target date
    is beyond the last available date, returns the most recent price.

    Returns None if no price is within max_delta_days.
    """
    if not series:
        return None

    dates = [d for d, _ in series]
    idx = bisect.bisect_left(dates, target)

    # If target is past the end of data, use the last available price
    if idx >= len(series):
        last_date, last_price = series[-1]
        if (target - last_date).days <= 90:  # don't go too stale
            return last_price
        return None

    found_date, found_price = series[idx]

    # Accept if within the tolerance window
    if (found_date - target).days <= max_delta_days:
        return found_price

    return None


def find_price_on_or_before(series: PriceSeries, target: date, max_delta_days: int = 7) -> float | None:
    """
    Return the closing price on or just before target date (for purchase date lookup).
    """
    if not series:
        return None

    dates = [d for d, _ in series]
    idx = bisect.bisect_right(dates, target) - 1

    if idx < 0:
        return None

    found_date, found_price = series[idx]
    if (target - found_date).days <= max_delta_days:
        return found_price

    return None


async def load_price_series(session: AsyncSession, ticker: str) -> PriceSeries:
    """Load all price snapshots for a ticker into a sorted in-memory series."""
    result = await session.execute(
        select(PriceSnapshot.date, PriceSnapshot.close)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.date)
    )
    return [(row.date, float(row.close)) for row in result.all()]


# ---------------------------------------------------------------------------
# Score calculation
# ---------------------------------------------------------------------------

def calculate_forward_returns(
    trades: list[Trade],
    series: PriceSeries,
    min_trades: int,
) -> dict[int, float] | None:
    """
    Calculate average forward returns at each window for a list of buy trades.

    Returns a dict {30: 0.04, 60: 0.07, 90: 0.12} (as fractions, not %)
    or None if fewer than min_trades can be scored.
    """
    window_returns: dict[int, list[float]] = {w: [] for w in FORWARD_WINDOWS}

    today = date.today()

    for trade in trades:
        # Skip trades without a recorded purchase price
        if not trade.price_per_share or float(trade.price_per_share) <= 0:
            continue

        purchase_price = float(trade.price_per_share)
        trade_date = trade.date_of_trade

        # Optionally cross-check purchase price against price_snapshots
        # (use if recorded price seems wrong — skipped for now for speed)

        for window in FORWARD_WINDOWS:
            future_date = trade_date + timedelta(days=window)

            if future_date > today:
                # Trade is recent — use the latest available price
                future_price = find_price_on_or_before(series, today)
            else:
                future_price = find_price_at_or_after(series, future_date)

            if future_price is None:
                continue  # No price data for this window, skip

            ret = (future_price - purchase_price) / purchase_price
            window_returns[window].append(ret)

    # Require min_trades scored at every window for a valid score
    scored_counts = [len(window_returns[w]) for w in FORWARD_WINDOWS]
    if min(scored_counts) < min_trades:
        return None

    return {w: sum(window_returns[w]) / len(window_returns[w]) for w in FORWARD_WINDOWS}


# ---------------------------------------------------------------------------
# Main per-director-company logic
# ---------------------------------------------------------------------------

async def score_director_company(
    dc: DirectorCompany,
    ticker: str,
    director_name: str,
    session: AsyncSession,
    price_cache: dict[str, PriceSeries],
    min_trades: int,
    stats: dict,
) -> None:
    """Calculate and persist smart money scores for one director-company pair."""

    # Fetch all on-market buys for this director+company
    result = await session.execute(
        select(Trade)
        .where(
            Trade.director_id == dc.director_id,
            Trade.company_id == dc.company_id,
            Trade.trade_type == TradeType.ON_MARKET_BUY,
        )
        .order_by(Trade.date_of_trade)
    )
    trades = result.scalars().all()

    buy_count = len(trades)
    dc.trade_count = buy_count  # keep trade_count accurate

    if buy_count < min_trades:
        logger.debug(f"  Skipping {director_name} @ {ticker}: only {buy_count} buy(s)")
        stats["skipped_insufficient_trades"] += 1
        return

    # Load price series (cached per ticker)
    if ticker not in price_cache:
        price_cache[ticker] = await load_price_series(session, ticker)

    series = price_cache[ticker]

    if not series:
        logger.debug(f"  No price data for {ticker} — skipping {director_name}")
        stats["skipped_no_price_data"] += 1
        return

    scores = calculate_forward_returns(trades, series, min_trades)

    if scores is None:
        logger.debug(
            f"  {director_name} @ {ticker}: {buy_count} buys but too few overlap price data"
        )
        stats["skipped_insufficient_price_overlap"] += 1
        return

    dc.smart_money_score_30d = round(scores[30], 6)
    dc.smart_money_score_60d = round(scores[60], 6)
    dc.smart_money_score_90d = round(scores[90], 6)

    stats["scored"] += 1

    sign_30 = "+" if scores[30] >= 0 else ""
    sign_60 = "+" if scores[60] >= 0 else ""
    sign_90 = "+" if scores[90] >= 0 else ""
    logger.info(
        f"  ✓ {director_name:35s} @ {ticker:6s} | {buy_count} buys | "
        f"30d:{sign_30}{scores[30]*100:.1f}%  "
        f"60d:{sign_60}{scores[60]*100:.1f}%  "
        f"90d:{sign_90}{scores[90]*100:.1f}%"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(min_trades: int = 3, tickers: list[str] | None = None) -> None:
    logger.info("Starting smart money score calculation")

    stats = {
        "scored": 0,
        "skipped_insufficient_trades": 0,
        "skipped_no_price_data": 0,
        "skipped_insufficient_price_overlap": 0,
    }

    price_cache: dict[str, PriceSeries] = {}

    async with async_session() as session:
        # Build query for director-company pairs — optionally filtered by ticker
        query = (
            select(DirectorCompany, Company.ticker, Director.full_name)
            .join(Company, Company.id == DirectorCompany.company_id)
            .join(Director, Director.id == DirectorCompany.director_id)
            .order_by(Company.ticker, Director.full_name)
        )

        if tickers:
            upper = [t.upper() for t in tickers]
            query = query.where(Company.ticker.in_(upper))

        result = await session.execute(query)
        rows = result.all()

        logger.info(f"Found {len(rows)} director-company pairs to evaluate")

        for i, (dc, ticker, director_name) in enumerate(rows, 1):
            try:
                await score_director_company(
                    dc, ticker, director_name, session, price_cache, min_trades, stats
                )
            except Exception as e:
                logger.error(f"Error scoring {director_name} @ {ticker}: {e}")
                stats["skipped_insufficient_trades"] += 1
                continue

            # Commit every 200 records
            if i % 200 == 0:
                await session.commit()
                logger.info(f"Progress: {i}/{len(rows)} pairs | {stats['scored']} scored so far")

        await session.commit()

    # ── Summary ──────────────────────────────────────────────────────────────
    total = len(rows)
    print("\n" + "=" * 70)
    print("SMART MONEY SCORE CALCULATION SUMMARY")
    print("=" * 70)
    print(f"Director-company pairs evaluated:      {total:,}")
    print(f"  ✓ Scored (updated):                  {stats['scored']:,}")
    print(f"  – Skipped (< {min_trades} buys):            {stats['skipped_insufficient_trades']:,}")
    print(f"  – Skipped (no price data):            {stats['skipped_no_price_data']:,}")
    print(f"  – Skipped (insufficient overlap):     {stats['skipped_insufficient_price_overlap']:,}")
    print()
    if stats["scored"] > 0:
        print(f"Score coverage: {stats['scored']}/{total} pairs ({100*stats['scored']/total:.1f}%)")
    else:
        print("No scores written — populate price_snapshots first with:")
        print("  docker-compose exec backend python -m app.scripts.sync_price_data --all")
    print("=" * 70)

    logger.info("Smart money score calculation complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate smart money scores for directors based on trade performance"
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=3,
        help="Minimum on-market buys required to score a director (default: 3)",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers to limit calculation (e.g. BHP,CBA)",
    )
    args = parser.parse_args()

    tickers_arg = [t.strip() for t in args.tickers.split(",")] if args.tickers else None

    asyncio.run(main(min_trades=args.min_trades, tickers=tickers_arg))
