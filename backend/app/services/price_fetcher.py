"""Price lookup helpers for portfolio calculations."""

from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.price_snapshot import PriceSnapshot


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
    return float(row.close) if row else None


async def get_latest_price(
    db: AsyncSession,
    ticker: str,
) -> tuple[float | None, date | None]:
    """Return (close_price, price_date) for the most recent entry for ticker."""
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
