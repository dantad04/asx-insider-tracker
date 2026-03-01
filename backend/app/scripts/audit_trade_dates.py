"""
Audit trade dates to identify potential data quality issues.

Detects trades where the date_of_trade might be incorrect:
1. Extreme gaps (90+ business days late) - likely using wrong date field
2. Gaps between 40-90 business days - suspicious, should verify manually
3. Most recent trades with gaps > 10 business days - recent violations to spot-check

Usage:
    docker-compose exec backend python -m app.scripts.audit_trade_dates
    docker-compose exec backend python -m app.scripts.audit_trade_dates --threshold 40
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
from datetime import date
from pathlib import Path

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.trade import Trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("/app/data")


def business_days(start: date, end: date) -> int:
    """Count business days between two dates."""
    if end <= start:
        return 0
    return int(np.busday_count(start.isoformat(), end.isoformat()))


async def audit_trades(threshold_days: int = 40) -> None:
    """
    Audit all trades for potential date issues.

    Flags trades with gaps >= threshold_days business days as potentially incorrect.
    """
    logger.info(f"Starting trade date audit (threshold: {threshold_days} business days)")

    async with async_session() as session:
        result = await session.execute(
            select(
                Trade.id,
                Trade.date_of_trade,
                Trade.date_lodged,
                Trade.quantity,
                Trade.trade_type,
                Company.ticker,
                Company.name.label("company_name"),
                Director.full_name.label("director_name"),
            )
            .join(Company, Company.id == Trade.company_id)
            .join(Director, Director.id == Trade.director_id)
            .where(Trade.date_of_trade != None, Trade.date_lodged != None)
            .order_by(Trade.date_lodged.desc())
        )
        rows = result.all()

    logger.info(f"Auditing {len(rows):,} trades with complete dates")

    suspicious = []
    extreme = []

    for r in rows:
        bd = business_days(r.date_of_trade, r.date_lodged)

        if bd >= 90:
            extreme.append(
                {
                    "trade_id": r.id,
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "director_name": r.director_name,
                    "date_of_trade": r.date_of_trade,
                    "date_lodged": r.date_lodged,
                    "business_days": bd,
                    "severity": "EXTREME",
                }
            )
        elif bd >= threshold_days:
            suspicious.append(
                {
                    "trade_id": r.id,
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "director_name": r.director_name,
                    "date_of_trade": r.date_of_trade,
                    "date_lodged": r.date_lodged,
                    "business_days": bd,
                    "severity": "SUSPICIOUS",
                }
            )

    print("\n" + "=" * 100)
    print("TRADE DATE AUDIT REPORT")
    print("=" * 100)

    print(f"\nTotal trades audited: {len(rows):,}")
    print(f"Extreme violations (90+ bd): {len(extreme)}")
    print(f"Suspicious violations ({threshold_days}-89 bd): {len(suspicious)}")

    if extreme:
        print("\n" + "-" * 100)
        print("EXTREME VIOLATIONS (90+ business days)")
        print("-" * 100)
        print(f"{'Ticker':<8} {'Trade Date':<12} {'Lodged Date':<12} {'Days':<6} Director")
        print("-" * 100)
        for v in sorted(extreme, key=lambda x: -x["business_days"])[:20]:
            print(
                f"{v['ticker']:<8} {str(v['date_of_trade']):<12} {str(v['date_lodged']):<12} "
                f"{v['business_days']:<6} {v['director_name'][:40]}"
            )

    if suspicious:
        print("\n" + "-" * 100)
        print(f"SUSPICIOUS VIOLATIONS ({threshold_days}-89 business days)")
        print("-" * 100)
        print(f"{'Ticker':<8} {'Trade Date':<12} {'Lodged Date':<12} {'Days':<6} Director")
        print("-" * 100)
        for v in sorted(suspicious, key=lambda x: -x["business_days"])[:20]:
            print(
                f"{v['ticker']:<8} {str(v['date_of_trade']):<12} {str(v['date_lodged']):<12} "
                f"{v['business_days']:<6} {v['director_name'][:40]}"
            )

    # Write CSV
    if extreme or suspicious:
        all_flagged = extreme + suspicious
        output_path = OUTPUT_DIR / "audit_flagged_trades.csv"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "severity",
                    "ticker",
                    "company_name",
                    "director_name",
                    "date_of_trade",
                    "date_lodged",
                    "business_days",
                    "trade_id",
                ],
            )
            writer.writeheader()
            for v in sorted(all_flagged, key=lambda x: -x["business_days"]):
                writer.writerow(v)

        logger.info(f"CSV written → {output_path} ({len(all_flagged):,} trades)")

    print("\n" + "=" * 100)
    print("RECOMMENDATIONS:")
    print("-" * 100)
    if extreme:
        print(f"⚠️  {len(extreme)} EXTREME violations found (90+ business days)")
        print("   These likely have incorrect dates - verify against ASX announcements")
        print("   Check if using 'Date of last notice' instead of 'Date of change'")
    if suspicious:
        print(f"⚠️  {len(suspicious)} SUSPICIOUS violations found ({threshold_days}-89 days)")
        print("   Spot-check these by reviewing the original ASX 3Y forms")
    if not extreme and not suspicious:
        print("✅ No suspicious trade dates found")

    print("=" * 100 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit trade dates for potential issues")
    parser.add_argument(
        "--threshold",
        type=int,
        default=40,
        help="Business days threshold for flagging suspicious trades (default: 40)",
    )
    args = parser.parse_args()

    asyncio.run(audit_trades(threshold_days=args.threshold))
