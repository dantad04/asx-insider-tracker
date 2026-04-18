"""Price lookup helpers for portfolio calculations."""

from datetime import date, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.price_snapshot import PriceSnapshot

RECENT_FALLBACK_WINDOW_DAYS = 14


async def _get_latest_from_price_snapshots(
    db: AsyncSession,
    ticker: str,
) -> tuple[float | None, date | None]:
    result = await db.execute(
        select(PriceSnapshot.close, PriceSnapshot.date)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.date.desc())
        .limit(1)
    )
    row = result.first()
    if row:
        return float(row.close), row.date
    return None, None


async def _get_latest_from_yf_daily_prices(
    db: AsyncSession,
    ticker: str,
) -> tuple[float | None, date | None]:
    try:
        result = await db.execute(
            text(
                "SELECT close, date "
                "FROM yf_daily_prices "
                "WHERE ticker = :ticker "
                "ORDER BY date DESC "
                "LIMIT 1"
            ),
            {"ticker": ticker},
        )
    except Exception:
        return None, None
    row = result.first()
    if row:
        return float(row.close), row.date
    return None, None


async def _get_forward_close_from_yf_daily_prices(
    db: AsyncSession,
    ticker: str,
    target_date: date,
    max_slippage_days: int,
) -> tuple[float | None, date | None]:
    try:
        result = await db.execute(
            text(
                "SELECT close, date "
                "FROM yf_daily_prices "
                "WHERE ticker = :ticker "
                "  AND date >= :target_date "
                "  AND date <= :max_date "
                "ORDER BY date ASC "
                "LIMIT 1"
            ),
            {
                "ticker": ticker,
                "target_date": target_date,
                "max_date": target_date + timedelta(days=max_slippage_days),
            },
        )
    except Exception:
        return None, None
    row = result.first()
    if row:
        return float(row.close), row.date
    return None, None


async def diagnose_latest_price(
    db: AsyncSession,
    ticker: str,
    recency_window_days: int = RECENT_FALLBACK_WINDOW_DAYS,
) -> dict[str, Any]:
    """
    Diagnose latest-price availability across stored sources.

    Canonical source remains price_snapshots. A recent yf_daily_prices value is
    an acceptable fallback for paper-portfolio use when price_snapshots is empty.
    """
    today = date.today()
    ps_price, ps_date = await _get_latest_from_price_snapshots(db, ticker)
    yf_price, yf_date = await _get_latest_from_yf_daily_prices(db, ticker)

    selected_source = None
    selected_price = None
    selected_date = None
    fallback_used = False
    reason = None

    if ps_price is not None and ps_date is not None:
        selected_source = "price_snapshots"
        selected_price = ps_price
        selected_date = ps_date
        reason = "canonical_source_available"
    elif yf_price is not None and yf_date is not None:
        age_days = (today - yf_date).days
        if age_days <= recency_window_days:
            selected_source = "yf_daily_prices"
            selected_price = yf_price
            selected_date = yf_date
            fallback_used = True
            reason = "used_recent_yf_daily_prices_fallback"
        else:
            reason = (
                f"yf_daily_prices_available_but_stale:{age_days}d>"
                f"{recency_window_days}d_window"
            )
    else:
        reason = "no_recent_price_in_any_stored_source"

    return {
        "ticker": ticker,
        "canonical_source": "price_snapshots",
        "selected_source": selected_source,
        "selected_price": selected_price,
        "selected_price_date": selected_date.isoformat() if selected_date else None,
        "fallback_used": fallback_used,
        "recency_window_days": recency_window_days,
        "checked_sources": {
            "price_snapshots": {
                "price": ps_price,
                "price_date": ps_date.isoformat() if ps_date else None,
                "found": ps_price is not None,
            },
            "yf_daily_prices": {
                "price": yf_price,
                "price_date": yf_date.isoformat() if yf_date else None,
                "found": yf_price is not None,
                "within_recency_window": (
                    (today - yf_date).days <= recency_window_days if yf_date else False
                ),
            },
        },
        "reason": reason,
    }


async def get_close_price(
    db: AsyncSession,
    ticker: str,
    target_date: date,
    max_slippage_days: int = 7,
) -> float | None:
    """
    Return the closing price for ticker on target_date or the next trading day
    within max_slippage_days. Returns None if no price is found.
    """
    result = await db.execute(
        select(PriceSnapshot.close)
        .where(
            PriceSnapshot.ticker == ticker,
            PriceSnapshot.date >= target_date,
            PriceSnapshot.date <= target_date + timedelta(days=max_slippage_days),
        )
        .order_by(PriceSnapshot.date.asc())
        .limit(1)
    )
    row = result.first()
    if row:
        return float(row.close)

    yf_price, _yf_date = await _get_forward_close_from_yf_daily_prices(
        db, ticker, target_date, max_slippage_days
    )
    return yf_price


async def get_close_price_with_date(
    db: AsyncSession,
    ticker: str,
    target_date: date,
    max_slippage_days: int = 7,
) -> tuple[float | None, date | None, str | None]:
    """Return (price, price_date, source) using forward next-trading-day lookup."""
    result = await db.execute(
        select(PriceSnapshot.close, PriceSnapshot.date)
        .where(
            PriceSnapshot.ticker == ticker,
            PriceSnapshot.date >= target_date,
            PriceSnapshot.date <= target_date + timedelta(days=max_slippage_days),
        )
        .order_by(PriceSnapshot.date.asc())
        .limit(1)
    )
    row = result.first()
    if row:
        return float(row.close), row.date, "price_snapshots"

    yf_price, yf_date = await _get_forward_close_from_yf_daily_prices(
        db, ticker, target_date, max_slippage_days
    )
    if yf_price is not None and yf_date is not None:
        return yf_price, yf_date, "yf_daily_prices"
    return None, None, None


async def get_latest_price(
    db: AsyncSession,
    ticker: str,
) -> tuple[float | None, date | None]:
    """Return (close_price, price_date) for the best latest stored price for ticker."""
    diagnostic = await diagnose_latest_price(db, ticker)
    if diagnostic["selected_price"] is None:
        return None, None
    return float(diagnostic["selected_price"]), date.fromisoformat(diagnostic["selected_price_date"])


async def get_latest_price_with_source(
    db: AsyncSession,
    ticker: str,
) -> tuple[float | None, date | None, str | None, bool, str]:
    """Return latest price details including actual source and fallback reasoning."""
    diagnostic = await diagnose_latest_price(db, ticker)
    selected_date = (
        date.fromisoformat(diagnostic["selected_price_date"])
        if diagnostic["selected_price_date"]
        else None
    )
    return (
        float(diagnostic["selected_price"]) if diagnostic["selected_price"] is not None else None,
        selected_date,
        diagnostic["selected_source"],
        bool(diagnostic["fallback_used"]),
        str(diagnostic["reason"]),
    )
