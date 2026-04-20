"""Helpers for refreshing stored market prices outside request handlers."""

from __future__ import annotations

import asyncio
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _yf_history(yf_code: str):
    import yfinance as yf

    return yf.Ticker(yf_code).history(period="5d")


async def refresh_recent_prices_for_tickers(
    db: AsyncSession,
    tickers: Iterable[str],
) -> dict[str, object]:
    """Fetch and upsert recent daily closes for a small ticker set."""
    unique_tickers = sorted({ticker.strip().upper() for ticker in tickers if ticker})
    result: dict[str, object] = {
        "attempted": len(unique_tickers),
        "updated": 0,
        "failed": 0,
        "latest_dates": {},
        "errors": {},
    }

    for ticker in unique_tickers:
        try:
            hist = await asyncio.to_thread(_yf_history, f"{ticker}.AX")
            if hist.empty:
                result["failed"] = int(result["failed"]) + 1
                result["errors"][ticker] = "no_price_data"
                continue

            latest_date = None
            for ts, row in hist.iterrows():
                price_date = ts.date() if hasattr(ts, "date") else ts
                latest_date = price_date
                await db.execute(
                    text(
                        "INSERT INTO yf_daily_prices (ticker, date, close) "
                        "VALUES (:ticker, :price_date, :close) "
                        "ON CONFLICT (ticker, date) "
                        "DO UPDATE SET close = EXCLUDED.close"
                    ),
                    {
                        "ticker": ticker,
                        "price_date": price_date,
                        "close": round(float(row["Close"]), 4),
                    },
                )

            result["updated"] = int(result["updated"]) + 1
            if latest_date is not None:
                result["latest_dates"][ticker] = latest_date.isoformat()
        except Exception as exc:  # pragma: no cover - network variability
            result["failed"] = int(result["failed"]) + 1
            result["errors"][ticker] = str(exc)

    return result
