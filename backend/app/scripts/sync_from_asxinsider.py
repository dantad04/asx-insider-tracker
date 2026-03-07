"""
Sync trades from asxinsider.com.au (GPT-parsed) into our database.

The API returns trades sorted newest-first by dateReadable. We exploit this
to stop early as soon as we hit a record already in the DB as asxinsider_gpt,
meaning everything after it is already imported.

Strategy per record:
  - No match        → insert as asxinsider_gpt
  - Match source=pdf_parser → replace with complete asxinsider_gpt record
  - Match source=asxinsider_gpt → STOP (early exit, rest already imported)
  - Match source=seed_json → upgrade with GPT data

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

    logger.info(f"Fetched {len(records)} records (newest-first). Processing until early exit ...")

    stats = {
        "inserted": 0,
        "replaced": 0,
        "upgraded": 0,
        "checks": 0,
    }

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

            # Skip records missing critical fields
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
                        # Early exit — everything after this is already imported
                        await session.commit()
                        logger.info(
                            f"Early exit at record {i}: {ticker} {director_name} "
                            f"{date_of_trade} already in DB as asxinsider_gpt"
                        )
                        break

                    elif existing.source == "pdf_parser":
                        # Replace incomplete record with complete GPT data
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
                            f"Replaced incomplete record for {ticker} {director_name} {date_of_trade}"
                        )

                    elif existing.source == "seed_json":
                        # Upgrade seed record with better GPT data
                        existing.price_per_share = price
                        existing.date_lodged = date_lodged
                        existing.trade_type = trade_type
                        existing.source = "asxinsider_gpt"
                        stats["upgraded"] += 1

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

            except Exception as e:
                logger.warning(f"Error processing record {i} ({ticker}): {e}")
                continue

            # Batch commit every 100 records to avoid long transactions
            if (i + 1) % 100 == 0:
                await session.commit()

        await session.commit()

    logger.info(
        f"Synced {stats['inserted']} new trades, "
        f"replaced {stats['replaced']} incomplete records, "
        f"upgraded {stats['upgraded']} seed records, "
        f"stopped after {stats['checks']} checks"
    )
    return stats


if __name__ == "__main__":
    asyncio.run(main())
