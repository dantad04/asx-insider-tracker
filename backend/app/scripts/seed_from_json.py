"""
Seed historical ASX director trade data from JSON file.

Usage:
    python -m app.scripts.seed_from_json /path/to/data.json
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session, engine
from app.models.company import Company
from app.models.director import Director
from app.models.director_company import DirectorCompany
from app.models.trade import Trade, TradeType


def map_trade_type(transaction_type: str, nature_of_change: str) -> TradeType:
    """
    Map JSON transaction_type and nature_of_change to our TradeType enum.

    Args:
        transaction_type: The transaction type from JSON
        nature_of_change: The nature of change text (check for "on market")

    Returns:
        TradeType enum value
    """
    nature_lower = (nature_of_change or "").lower()
    is_on_market = "on market" in nature_lower

    if transaction_type == "Purchase":
        return TradeType.ON_MARKET_BUY if is_on_market else TradeType.OFF_MARKET
    elif transaction_type == "Sale":
        return TradeType.ON_MARKET_SELL if is_on_market else TradeType.OFF_MARKET
    elif transaction_type == "Exercise":
        return TradeType.EXERCISE_OPTIONS
    else:
        # Conversion, Placement, DividendReinvestment, Other
        return TradeType.OTHER


async def get_or_create_company(
    session: AsyncSession, ticker: str, issuer_name: str
) -> Company:
    """
    Get existing company or create new one by ticker.

    Args:
        session: Database session
        ticker: Company ticker symbol
        issuer_name: Company name

    Returns:
        Company instance
    """
    # Try to find existing company
    result = await session.execute(
        select(Company).where(Company.ticker == ticker)
    )
    company = result.scalar_one_or_none()

    if company is None:
        # Create new company
        company = Company(
            ticker=ticker,
            name=issuer_name,
            sector=None,
            industry_group=None,
            market_cap=None,
            is_asx200=False,
            is_asx300=False,
        )
        session.add(company)

    return company


async def get_or_create_director(
    session: AsyncSession, full_name: str
) -> Director:
    """
    Get existing director or create new one by full_name.

    Args:
        session: Database session
        full_name: Director's full name

    Returns:
        Director instance
    """
    # Try to find existing director
    result = await session.execute(
        select(Director).where(Director.full_name == full_name)
    )
    director = result.scalar_one_or_none()

    if director is None:
        # Create new director
        director = Director(full_name=full_name)
        session.add(director)

    return director


async def get_or_create_director_company(
    session: AsyncSession, director_id: str, company_id: str
) -> DirectorCompany:
    """
    Get existing director-company relationship or create new one.

    Args:
        session: Database session
        director_id: Director UUID
        company_id: Company UUID

    Returns:
        DirectorCompany instance
    """
    # Try to find existing relationship
    result = await session.execute(
        select(DirectorCompany).where(
            DirectorCompany.director_id == director_id,
            DirectorCompany.company_id == company_id,
        )
    )
    director_company = result.scalar_one_or_none()

    if director_company is None:
        # Create new relationship
        director_company = DirectorCompany(
            director_id=director_id,
            company_id=company_id,
            role=None,
            is_active=True,
            trade_count=0,
        )
        session.add(director_company)

    return director_company


async def trade_exists(
    session: AsyncSession,
    director_id: str,
    company_id: str,
    date_of_trade: datetime,
    quantity: int,
    trade_type: TradeType,
) -> bool:
    """
    Check if a trade already exists to avoid duplicates.

    Args:
        session: Database session
        director_id: Director UUID
        company_id: Company UUID
        date_of_trade: Date of the trade
        quantity: Trade quantity
        trade_type: Trade type enum

    Returns:
        True if trade exists, False otherwise
    """
    result = await session.execute(
        select(Trade).where(
            Trade.director_id == director_id,
            Trade.company_id == company_id,
            Trade.date_of_trade == date_of_trade.date(),
            Trade.quantity == quantity,
            Trade.trade_type == trade_type,
        )
    )
    return result.scalar_one_or_none() is not None


async def process_record(
    session: AsyncSession,
    record: dict[str, Any],
    stats: dict[str, int],
) -> None:
    """
    Process a single trade record from JSON.

    Args:
        session: Database session
        record: Trade record from JSON
        stats: Statistics dictionary to update
    """
    # Extract and validate symbol
    symbol = record.get("symbol", "").strip().upper()
    if not symbol:
        stats["null_symbol_skipped"] += 1
        return

    # Extract fields
    issuer_name = record.get("issuer_name", "").strip()
    director_name = record.get("director_name", "Unknown").strip()
    date_of_change_str = record.get("date_of_change")
    transaction_type = record.get("transaction_type", "Other")
    nature_of_change = record.get("nature_of_change", "")
    quantity = record.get("quantity")
    per_share = record.get("per_share")
    date_readable_str = record.get("dateReadable")

    # Validate required fields
    if not issuer_name:
        stats["null_issuer_skipped"] += 1
        return

    if quantity is None or quantity == 0:
        stats["null_quantity_skipped"] += 1
        return

    # Truncate long company names to fit database column (255 chars)
    if len(issuer_name) > 255:
        issuer_name = issuer_name[:252] + "..."

    if len(director_name) > 255:
        director_name = director_name[:252] + "..."

    # Parse dates
    try:
        date_of_trade = datetime.strptime(date_of_change_str, "%Y-%m-%d")
        # Try multiple date formats for date_readable
        if date_readable_str:
            try:
                # Try format: "2025-10-17 11:57:38"
                date_lodged = datetime.strptime(date_readable_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    # Try ISO format with milliseconds: "2025-10-17T11:57:38.123Z"
                    date_lodged = datetime.strptime(date_readable_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    # Fallback to trade date if parsing fails
                    date_lodged = date_of_trade
        else:
            date_lodged = date_of_trade
    except (ValueError, TypeError):
        date_of_trade = datetime.now()
        date_lodged = datetime.now()

    # Map trade type
    trade_type = map_trade_type(transaction_type, nature_of_change)

    # Get or create company
    company = await get_or_create_company(session, symbol, issuer_name)
    if company.id not in [c.id for c in session.new]:
        stats["companies_found"] += 1
    else:
        stats["companies_created"] += 1

    # Flush to get company.id if it's new
    await session.flush()

    # Get or create director
    director = await get_or_create_director(session, director_name)
    if director.id not in [d.id for d in session.new]:
        stats["directors_found"] += 1
    else:
        stats["directors_created"] += 1

    # Flush to get director.id if it's new
    await session.flush()

    # Get or create director-company relationship
    await get_or_create_director_company(session, director.id, company.id)

    # Check for duplicate trade
    if await trade_exists(
        session, director.id, company.id, date_of_trade, quantity, trade_type
    ):
        stats["duplicates_skipped"] += 1
        return

    # Create trade
    trade = Trade(
        director_id=director.id,
        company_id=company.id,
        date_of_trade=date_of_trade.date(),
        date_lodged=date_lodged.date(),
        trade_type=trade_type,
        quantity=quantity,
        price_per_share=float(per_share) if per_share else None,
    )
    session.add(trade)
    stats["trades_inserted"] += 1


async def seed_from_json(file_path: str) -> None:
    """
    Import trade data from JSON file into the database.

    Args:
        file_path: Path to JSON file containing trade records
    """
    print(f"Loading data from {file_path}...")

    # Load JSON file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}")
        sys.exit(1)

    if not isinstance(records, list):
        print("Error: JSON file must contain an array of records")
        sys.exit(1)

    print(f"Found {len(records)} records to process")

    # Initialize stats
    stats = {
        "companies_created": 0,
        "companies_found": 0,
        "directors_created": 0,
        "directors_found": 0,
        "trades_inserted": 0,
        "duplicates_skipped": 0,
        "null_symbol_skipped": 0,
        "null_issuer_skipped": 0,
        "null_quantity_skipped": 0,
        "malformed_skipped": 0,
    }

    # Process records in batches
    batch_size = 500
    total_records = len(records)

    async with async_session() as session:
        for i, record in enumerate(records, start=1):
            # Skip malformed records
            if not isinstance(record, dict):
                stats["malformed_skipped"] += 1
                continue

            try:
                await process_record(session, record, stats)

                # Commit every batch_size records
                if i % batch_size == 0:
                    await session.commit()
                    print(f"Processed {i}/{total_records} records...")

            except Exception as e:
                print(f"Error processing record {i}: {e}")
                await session.rollback()
                continue

        # Final commit for remaining records
        await session.commit()

    # Print summary
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"Total records processed: {total_records}")
    print(f"Companies created: {stats['companies_created']}")
    print(f"Companies found (existing): {stats['companies_found']}")
    print(f"Directors created: {stats['directors_created']}")
    print(f"Directors found (existing): {stats['directors_found']}")
    print(f"Trades inserted: {stats['trades_inserted']}")
    print(f"Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"Null symbol skipped: {stats['null_symbol_skipped']}")
    print(f"Null issuer name skipped: {stats['null_issuer_skipped']}")
    print(f"Null quantity skipped: {stats['null_quantity_skipped']}")
    print(f"Malformed records skipped: {stats['malformed_skipped']}")
    print("=" * 60)
    print("✅ Import completed successfully!")


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Import ASX director trade data from JSON file"
    )
    parser.add_argument(
        "file_path",
        help="Path to JSON file containing trade records",
    )

    args = parser.parse_args()

    # Run async import
    asyncio.run(seed_from_json(args.file_path))


if __name__ == "__main__":
    main()
