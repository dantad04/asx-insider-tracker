"""
Sync trades from asxinsider.com.au (GPT-parsed) into our database.

The API returns trades sorted newest-first by dateReadable.

Early-exit strategy: after seeing EARLY_EXIT_CONSECUTIVE consecutive
asxinsider_gpt records in a row, we assume everything after is already
imported and stop. This is safe once the DB has been fully synced at
least once — until then the full list is processed.

Strategy per record:
  - No match                → insert as asxinsider_gpt
  - Match source=pdf_parser → delete old, insert new complete record
  - Match source=seed_json  → upgrade in-place with GPT data
  - Match source=asxinsider_gpt → increment consecutive counter;
                                  exit if counter >= EARLY_EXIT_CONSECUTIVE

Usage:
    docker-compose exec backend python -m app.scripts.sync_from_asxinsider

Environment variables:
    ASXINSIDER_URL  - URL serving the asxinsider trades JSON (newest-first)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, date
from typing import Any

import httpx
from sqlalchemy import select, and_, func, delete

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.director_company import DirectorCompany
from app.models.trade import Trade, TradeType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ASXINSIDER_URL = os.getenv("ASXINSIDER_URL", "")

# Stop after this many consecutive already-imported asxinsider_gpt records.
# Low enough to exit quickly on normal syncs, high enough to skip gaps
# left by the old partial sync.
EARLY_EXIT_CONSECUTIVE = 50


# ── Schema mapping ─────────────────────────────────────────────────────────────

def map_trade_type(transaction_type: str, nature: str) -> TradeType:
    t = (transaction_type or "").lower()
    n = (nature or "").lower()
    if "purchase" in t or "on market" in n or "on-market" in n:
        if "purchase" in t or "buy" in t:
            return TradeType.ON_MARKET_BUY
    if "sale" in t or "sell" in t:
        return TradeType.ON_MARKET_SELL
    if "purchase" in t:
        return TradeType.ON_MARKET_BUY
    if "exercise" in t:
        return TradeType.EXERCISE_OPTIONS
    if "conversion" in t or "dividend" in t or "placement" in t:
        return TradeType.OFF_MARKET
    return TradeType.OTHER


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(str(value), fmt).date()
        except ValueError:
            continue
    return None


# ── Database helpers ───────────────────────────────────────────────────────────

async def get_or_create_company(session, ticker: str, name: str) -> Company:
    result = await session.execute(
        select(Company).where(Company.ticker == ticker.upper())
    )
    company = result.scalars().first()
    if company:
        return company
    company = Company(ticker=ticker.upper(), name=(name or ticker.upper())[:255])
    session.add(company)
    await session.flush()
    return company


async def get_or_create_director(session, full_name: str) -> Director:
    result = await session.execute(
        select(Director).where(
            func.lower(Director.full_name) == full_name.lower().strip()
        )
    )
    director = result.scalars().first()
    if director:
        return director
    director = Director(full_name=full_name.strip())
    session.add(director)
    await session.flush()
    return director


async def ensure_director_company(session, director_id: str, company_id: str):
    result = await session.execute(
        select(DirectorCompany).where(
            DirectorCompany.director_id == director_id,
            DirectorCompany.company_id == company_id,
        )
    )
    if not result.scalars().first():
        session.add(DirectorCompany(director_id=director_id, company_id=company_id))
        await session.flush()


async def find_existing_trade(
    session, company_id: str, director_id: str, trade_date: date, quantity: int
) -> Trade | None:
    result = await session.execute(
        select(Trade).where(
            and_(
                Trade.company_id == company_id,
                Trade.director_id == director_id,
                Trade.date_of_trade == trade_date,
                func.abs(Trade.quantity) == abs(quantity),
            )
        )
    )
    return result.scalars().first()


# ── Main ───────────────────────────────────────────────────────────────────────

async def main(url: str | None = None) -> dict:
    target_url = url or ASXINSIDER_URL
    if not target_url:
        logger.error(
            "ASXINSIDER_URL environment variable not set. "
            "e.g. https://asxinsider.com.au/api/trades"
        )
        return {}

    logger.info(f"Fetching trades from {target_url} ...")
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.get(target_url)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch from {target_url}: {e}")
            return {}

    if not isinstance(records, list):
        logger.error(f"Expected a JSON array, got {type(records)}")
        return {}

    logger.info(f"Fetched {len(records)} records (newest-first). Processing ...")

    stats = {
        "inserted": 0,
        "replaced": 0,
        "upgraded": 0,
        "checks": 0,
        "early_exit": False,
    }

    consecutive_existing = 0  # consecutive asxinsider_gpt hits in a row

    async with async_session() as session:
        for i, record in enumerate(records):
            ticker = (record.get("symbol") or "").strip().upper()
            director_name = (record.get("director_name") or "").strip()
            issuer_name = (record.get("issuer_name") or ticker).strip()
            date_of_trade = parse_date(record.get("date_of_change"))
            date_lodged = parse_date(record.get("dateReadable"))
            quantity_raw = record.get("quantity")
            per_share = record.get("per_share")
            total_value = record.get("total_value")
            transaction_type = record.get("transaction_type", "Other")
            nature = record.get("nature_of_change", "")

            if not ticker or not director_name or not date_of_trade or quantity_raw is None:
                continue
            quantity = int(quantity_raw)
            if quantity == 0:
                continue

            stats["checks"] += 1

            price = float(per_share) if per_share else None
            if price is None and total_value and abs(quantity) > 0:
                price = round(abs(float(total_value)) / abs(quantity), 6)

            trade_type = map_trade_type(transaction_type, nature)
            date_lodged = date_lodged or date_of_trade

            try:
                company = await get_or_create_company(session, ticker, issuer_name)
                director = await get_or_create_director(session, director_name)
                await ensure_director_company(session, director.id, company.id)

                existing = await find_existing_trade(
                    session, company.id, director.id, date_of_trade, quantity
                )

                if existing:
                    if existing.source == "asxinsider_gpt":
                        consecutive_existing += 1
                        if consecutive_existing >= EARLY_EXIT_CONSECUTIVE:
                            await session.commit()
                            logger.info(
                                f"Early exit after {stats['checks']} checks "
                                f"({EARLY_EXIT_CONSECUTIVE} consecutive existing records)"
                            )
                            stats["early_exit"] = True
                            break
                        # Don't count this as a processed record — just skip
                        continue

                    elif existing.source == "pdf_parser":
                        # Replace incomplete record with full GPT data
                        await session.execute(
                            delete(Trade).where(Trade.id == existing.id)
                        )
                        session.add(Trade(
                            director_id=director.id,
                            company_id=company.id,
                            date_of_trade=date_of_trade,
                            date_lodged=date_lodged,
                            trade_type=trade_type,
                            quantity=abs(quantity),
                            price_per_share=price,
                            source="asxinsider_gpt",
                        ))
                        stats["replaced"] += 1
                        logger.info(
                            f"Replaced pdf_parser record: {ticker} {director_name} {date_of_trade}"
                        )
                        consecutive_existing = 0

                    elif existing.source == "seed_json":
                        # Upgrade with better GPT data
                        existing.price_per_share = price
                        existing.date_lodged = date_lodged
                        existing.trade_type = trade_type
                        existing.source = "asxinsider_gpt"
                        stats["upgraded"] += 1
                        consecutive_existing = 0

                else:
                    # New record — insert
                    session.add(Trade(
                        director_id=director.id,
                        company_id=company.id,
                        date_of_trade=date_of_trade,
                        date_lodged=date_lodged,
                        trade_type=trade_type,
                        quantity=abs(quantity),
                        price_per_share=price,
                        source="asxinsider_gpt",
                    ))
                    stats["inserted"] += 1
                    consecutive_existing = 0

            except Exception as e:
                logger.warning(f"Error processing record {i} ({ticker}): {e}")
                consecutive_existing = 0
                await session.rollback()
                continue

            # Batch commit every 100 records
            if (i + 1) % 100 == 0:
                await session.commit()
                logger.info(
                    f"  {i + 1} processed — "
                    f"{stats['inserted']} inserted, "
                    f"{stats['replaced']} replaced, "
                    f"{stats['upgraded']} upgraded ..."
                )

        await session.commit()

    logger.info(
        f"Sync complete: {stats['inserted']} new, "
        f"{stats['replaced']} replaced, "
        f"{stats['upgraded']} upgraded, "
        f"{stats['checks']} checks"
        + (" [early exit]" if stats["early_exit"] else " [full scan]")
    )
    return stats


if __name__ == "__main__":
    asyncio.run(main())
