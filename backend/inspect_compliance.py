#!/usr/bin/env python
"""
Inspect compliance violations for debugging.
Shows duplicates and date issues.

Usage:
    docker-compose exec backend python inspect_compliance.py
"""

import asyncio
from datetime import date
from sqlalchemy import select, func
from app.database import async_session
from app.models.trade import Trade
from app.models.company import Company
from app.models.director import Director


async def main():
    async with async_session() as db:
        # Find FRS trades for David Geraghty with date around Feb 2026
        result = await db.execute(
            select(
                Trade.id,
                Trade.date_of_trade,
                Trade.date_lodged,
                Trade.quantity,
                Trade.trade_type,
                Trade.source,
                Company.ticker,
                Director.full_name,
            )
            .join(Company)
            .join(Director)
            .where(
                Company.ticker == "FRS",
                func.lower(Director.full_name).like("%geraghty%"),
                Trade.date_lodged >= date(2026, 2, 20),
                Trade.date_lodged <= date(2026, 3, 5),
            )
            .order_by(Trade.date_lodged.desc(), Trade.date_of_trade.desc())
        )

        rows = result.all()
        print(f"\n✓ Found {len(rows)} FRS/Geraghty trades (Feb 20 - Mar 5)")
        print("=" * 100)

        # Group by (date_lodged, date_of_trade, quantity, trade_type) to find duplicates
        seen = {}
        for r in rows:
            key = (
                str(r.date_lodged),
                str(r.date_of_trade),
                r.quantity,
                r.trade_type,
            )
            if key not in seen:
                seen[key] = []
            seen[key].append(r)

        for key, duplicates in seen.items():
            date_lodged, date_of_trade, qty, trade_type = key
            count = len(duplicates)

            if count > 1:
                print(f"\n⚠️  DUPLICATE: {count} records")
            else:
                print(f"\n✓ Single record")

            print(
                f"  Filing Date: {date_lodged} | Trade Date: {date_of_trade} | "
                f"Qty: {qty} | Type: {trade_type}"
            )

            for dup in duplicates:
                print(
                    f"    ID: {dup.id} | Source: {dup.source} | "
                    f"{dup.ticker} {dup.full_name}"
                )

        print("\n" + "=" * 100)
        print(f"\nTotal unique (date_lodged, date_of_trade, qty, type): {len(seen)}")
        duplicates_count = sum(len(v) - 1 for v in seen.values())
        print(f"Duplicate records: {duplicates_count}")


if __name__ == "__main__":
    asyncio.run(main())
