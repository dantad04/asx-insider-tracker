"""
Sync trades from asxinsider.com.au (GPT-parsed) into our database.

Strategy:
  - Fetch their JSON from ASXINSIDER_URL environment variable
  - Deduplicate on (ticker, director_name, date_of_trade, abs(quantity))
  - If match is seed_json   → upgrade record with GPT data (more accurate)
  - If match is pdf_parser  → skip (our verified parse is reliable)
  - If match is asxinsider_gpt → skip (already have it)
  - No match                → insert as source="asxinsider_gpt"

Usage:
    docker-compose exec backend python -m app.scripts.sync_asxinsider

Environment variables:
    ASXINSIDER_URL  - URL that serves the asxinsider JSON
                      e.g. https://asxinsider.com.au/api/trades
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, date
from typing import Any

import httpx
from sqlalchemy import select, and_, func

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
    """Map asxinsider transaction_type to our TradeType enum."""
    t = (transaction_type or "").lower()
    n = (nature or "").lower()

    if "purchase" in t or "on market" in n or "on-market" in n:
        if "purchase" in t or "buy" in t:
            return TradeType.ON_MARKET_BUY
    if "sale" in t or "sell" in t:
        if "on market" in n or "on-market" in n:
            return TradeType.ON_MARKET_SELL
        return TradeType.ON_MARKET_SELL
    if "purchase" in t:
        return TradeType.ON_MARKET_BUY
    if "exercise" in t:
        return TradeType.EXERCISE_OPTIONS
    if "conversion" in t or "dividend" in t or "placement" in t:
        return TradeType.OFF_MARKET
    return TradeType.OTHER


def parse_date(value: Any) -> date | None:
    """Parse a date from various string formats."""
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
    company = result.scalar_one_or_none()
    if not company:
        company = Company(ticker=ticker.upper(), name=name or ticker.upper())
        session.add(company)
        await session.flush()
    return company


async def get_or_create_director(session, full_name: str) -> Director:
    result = await session.execute(
        select(Director).where(
            func.lower(Director.full_name) == full_name.lower().strip()
        )
    )
    director = result.scalar_one_or_none()
    if not director:
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
    if not result.scalar_one_or_none():
        session.add(DirectorCompany(
            director_id=director_id,
            company_id=company_id,
        ))
        await session.flush()


async def find_existing_trade(
    session,
    company_id: str,
    director_id: str,
    trade_date: date,
    quantity: int,
) -> Trade | None:
    """Find a matching trade by the deduplication key."""
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
    return result.scalar_one_or_none()


# ── Per-record processor ───────────────────────────────────────────────────────

async def process_record(session, record: dict, stats: dict) -> None:
    ticker = (record.get("symbol") or "").strip().upper()
    director_name = (record.get("director_name") or "").strip()
    issuer_name = (record.get("issuer_name") or ticker).strip()
    date_of_trade = parse_date(record.get("date_of_change"))
    date_readable = parse_date(record.get("dateReadable"))
    quantity_raw = record.get("quantity")
    per_share = record.get("per_share")
    total_value = record.get("total_value")
    transaction_type = record.get("transaction_type", "Other")
    nature = record.get("nature_of_change", "")

    # Skip records missing critical fields
    if not ticker:
        stats["skipped_no_ticker"] += 1
        return
    if not director_name:
        stats["skipped_no_director"] += 1
        return
    if not date_of_trade:
        stats["skipped_no_date"] += 1
        return
    if quantity_raw is None:
        stats["skipped_no_quantity"] += 1
        return

    quantity = int(quantity_raw)
    if quantity == 0:
        stats["skipped_zero_quantity"] += 1
        return

    # Derive price_per_share if missing
    price = float(per_share) if per_share else None
    if price is None and total_value and abs(quantity) > 0:
        price = round(abs(float(total_value)) / abs(quantity), 6)

    trade_type = map_trade_type(transaction_type, nature)

    # dateReadable from asxinsider is ASX announcement timestamp — valid as date_lodged
    date_lodged = date_readable or date_of_trade

    # Get/create company and director
    company = await get_or_create_company(session, ticker, issuer_name)
    director = await get_or_create_director(session, director_name)
    await ensure_director_company(session, director.id, company.id)

    # Check for existing trade
    existing = await find_existing_trade(
        session, company.id, director.id, date_of_trade, quantity
    )

    if existing:
        if existing.source == "seed_json":
            # Upgrade seed_json record with better GPT data
            existing.price_per_share = price
            existing.date_lodged = date_lodged
            existing.trade_type = trade_type
            existing.source = "asxinsider_gpt"
            stats["upgraded"] += 1
        else:
            # pdf_parser or asxinsider_gpt — keep as is
            stats["skipped_already_good"] += 1
        return

    # New record — insert
    trade = Trade(
        director_id=director.id,
        company_id=company.id,
        date_of_trade=date_of_trade,
        date_lodged=date_lodged,
        trade_type=trade_type,
        quantity=abs(quantity),
        price_per_share=price,
        source="asxinsider_gpt",
    )
    session.add(trade)
    stats["inserted"] += 1


# ── Main ───────────────────────────────────────────────────────────────────────

async def main(url: str | None = None) -> dict:
    target_url = url or ASXINSIDER_URL
    if not target_url:
        logger.error(
            "ASXINSIDER_URL environment variable not set.\n"
            "Set it to the URL serving the asxinsider trades JSON.\n"
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

    logger.info(f"Fetched {len(records)} records from asxinsider")

    stats = {
        "total": len(records),
        "inserted": 0,
        "upgraded": 0,
        "skipped_already_good": 0,
        "skipped_no_ticker": 0,
        "skipped_no_director": 0,
        "skipped_no_date": 0,
        "skipped_no_quantity": 0,
        "skipped_zero_quantity": 0,
        "errors": 0,
    }

    async with async_session() as session:
        async with session.begin():
            for i, record in enumerate(records):
                try:
                    await process_record(session, record, stats)
                except Exception as e:
                    logger.warning(f"Error on record {i}: {e}")
                    stats["errors"] += 1

                # Commit in batches to avoid long transactions
                if (i + 1) % 500 == 0:
                    await session.commit()
                    await session.begin()
                    logger.info(f"  Processed {i + 1}/{len(records)} ...")

    logger.info("=" * 50)
    logger.info("Sync complete:")
    logger.info(f"  Total records:      {stats['total']}")
    logger.info(f"  Inserted (new):     {stats['inserted']}")
    logger.info(f"  Upgraded (seed→gpt):{stats['upgraded']}")
    logger.info(f"  Skipped (good):     {stats['skipped_already_good']}")
    logger.info(f"  Skipped (bad data): "
                f"{stats['skipped_no_ticker'] + stats['skipped_no_director'] + stats['skipped_no_date'] + stats['skipped_no_quantity'] + stats['skipped_zero_quantity']}")
    logger.info(f"  Errors:             {stats['errors']}")
    logger.info("=" * 50)

    return stats


if __name__ == "__main__":
    asyncio.run(main())
