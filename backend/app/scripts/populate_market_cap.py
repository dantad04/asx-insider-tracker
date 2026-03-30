"""
Populate market_cap for all companies using yfinance.

Usage:
    docker-compose exec backend python -m app.scripts.populate_market_cap
    docker-compose exec backend python -m app.scripts.populate_market_cap --tickers BHP,CBA
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
import sys

import yfinance as yf
from sqlalchemy import select

from app.database import async_session
from app.models.company import Company

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main(tickers: list[str] | None = None):
    async with async_session() as session:
        if tickers:
            result = await session.execute(
                select(Company).where(Company.ticker.in_([t.upper() for t in tickers]))
            )
        else:
            result = await session.execute(select(Company))
        companies = result.scalars().all()

    logger.info(f"Fetching market cap for {len(companies)} companies ...")

    updated = 0
    skipped = 0
    errors = 0

    for i, company in enumerate(companies):
        yf_ticker = f"{company.ticker}.AX"
        try:
            time.sleep(0.5)
            fi = yf.Ticker(yf_ticker).fast_info
            mkt_cap = getattr(fi, "market_cap", None)
            if mkt_cap and mkt_cap > 0:
                async with async_session() as session:
                    result = await session.execute(
                        select(Company).where(Company.id == company.id)
                    )
                    c = result.scalars().first()
                    if c:
                        c.market_cap = int(mkt_cap)
                        await session.commit()
                updated += 1
                if (i + 1) % 50 == 0 or updated % 50 == 0:
                    logger.info(f"  {i+1}/{len(companies)} — {updated} updated, {skipped} skipped, {errors} errors")
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            logger.debug(f"  {company.ticker}: {e}")

    logger.info(f"Done: {updated} updated, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", help="Comma-separated tickers (default: all)")
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    asyncio.run(main(tickers))
