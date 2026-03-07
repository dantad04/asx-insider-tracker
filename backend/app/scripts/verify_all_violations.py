"""
One-time migration script: verify all existing potential violations in the database.

For every trade where (date_lodged - date_of_trade) >= 7 calendar days:
  1. Look up the original PDF in pending_3y_parses by (ticker, date_lodged ±1 day)
  2. Fetch and re-parse the PDF to extract the true date_of_change
  3. Apply swap fix: if date_of_change < date_of_last_notice, they were extracted
     in the wrong order — use date_of_last_notice as the real trade date
  4. If the verified date differs from the stored date, update the trade record

After this runs once, the /api/compliance/violations endpoint just reads from
the database instantly — no more on-demand PDF fetching per request.

Usage:
    # Docker (local)
    docker-compose exec backend python -m app.scripts.verify_all_violations

    # Dry-run (shows what would change, no DB writes)
    docker-compose exec backend python -m app.scripts.verify_all_violations --dry-run

    # Railway (production)
    railway run python -m app.scripts.verify_all_violations

    # Limit to N trades (for testing)
    docker-compose exec backend python -m app.scripts.verify_all_violations --limit 20
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import sys
from datetime import date, timedelta

import httpx
import numpy as np
import pdfplumber
from sqlalchemy import select, update, and_

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.pending_3y_parse import Pending3YParse, ParseStatus
from app.models.trade import Trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Minimum calendar-day gap to consider a trade as a potential violation.
# 7 calendar days ≈ 5 business days (the ASX deadline), so this catches
# everything that might genuinely be late without too many false positives.
MIN_CAL_DAYS = 7

# Delay between PDF fetches (seconds) — avoids hammering the ASX CDN.
FETCH_DELAY = 0.5

# Commit to DB every N verified records.
BATCH_SIZE = 50


def business_days(start: date, end: date) -> int:
    if end <= start:
        return 0
    return int(np.busday_count(start.isoformat(), end.isoformat()))


async def get_pdf_url(session, ticker: str, date_lodged: date) -> str | None:
    """Look up PDF URL from pending_3y_parses for this filing date ±1 day."""
    result = await session.execute(
        select(Pending3YParse.pdf_url)
        .where(
            Pending3YParse.ticker == ticker,
            Pending3YParse.document_date >= date_lodged - timedelta(days=1),
            Pending3YParse.document_date <= date_lodged + timedelta(days=1),
            Pending3YParse.status == ParseStatus.PARSED,
        )
        .limit(1)
    )
    row = result.first()
    return row[0] if row else None


async def verify_date_from_pdf(
    client: httpx.AsyncClient, pdf_url: str
) -> tuple[date | None, str]:
    """
    Fetch PDF and extract the verified trade date.

    Returns (verified_date, reason) where reason describes what happened.
    verified_date is None if the PDF couldn't be fetched or parsed.
    """
    try:
        response = await client.get(pdf_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        return None, f"fetch_failed: {e}"

    try:
        text = ""
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
    except Exception as e:
        return None, f"pdf_parse_error: {e}"

    from app.scripts.parse_3y_pdfs import parse_date_of_change, parse_date_of_last_notice

    doc = parse_date_of_change(text)
    last = parse_date_of_last_notice(text)

    if doc and last and doc < last:
        # Swap detected: parser confused date_of_change with date_of_last_notice
        return last, f"swap_corrected (doc={doc} last={last})"
    elif doc:
        return doc, "ok"
    else:
        return None, "date_not_found_in_pdf"


async def main(dry_run: bool = False, limit: int | None = None) -> None:
    mode = "[DRY RUN] " if dry_run else ""
    logger.info(f"{mode}Starting verify_all_violations script")

    async with async_session() as session:
        # ── Step 1: find all potential violations ──────────────────────────
        result = await session.execute(
            select(
                Trade.id,
                Trade.date_of_trade,
                Trade.date_lodged,
                Trade.source,
                Company.ticker,
                Director.full_name.label("director_name"),
            )
            .join(Company, Company.id == Trade.company_id)
            .join(Director, Director.id == Trade.director_id)
            .where(
                Trade.date_of_trade != None,
                Trade.date_lodged != None,
                Trade.source.in_(("pdf_parser", "asxinsider_gpt")),
            )
            .order_by(Trade.date_lodged.desc())
        )
        all_trades = result.all()

    # Filter to potential violations by calendar days
    candidates = [
        r for r in all_trades
        if (r.date_lodged - r.date_of_trade).days >= MIN_CAL_DAYS
    ]

    if limit:
        candidates = candidates[:limit]

    total = len(candidates)
    logger.info(f"Found {total} potential violations to verify (>= {MIN_CAL_DAYS} calendar days late)")

    # ── Step 2: verify each against its PDF ───────────────────────────────
    stats = {
        "verified_unchanged": 0,
        "verified_corrected": 0,
        "no_pdf_url": 0,
        "fetch_failed": 0,
        "parse_failed": 0,
        "skipped_nonsensical": 0,
    }

    async with async_session() as session:
        async with httpx.AsyncClient() as client:
            for i, trade in enumerate(candidates, start=1):
                if i % 50 == 0 or i == total:
                    logger.info(f"Progress: {i}/{total} "
                                f"(corrected={stats['verified_corrected']}, "
                                f"no_pdf={stats['no_pdf_url']}, "
                                f"fetch_failed={stats['fetch_failed']})")

                pdf_url = await get_pdf_url(session, trade.ticker, trade.date_lodged)
                if not pdf_url:
                    stats["no_pdf_url"] += 1
                    continue

                await asyncio.sleep(FETCH_DELAY)

                verified_date, reason = await verify_date_from_pdf(client, pdf_url)

                if verified_date is None:
                    if "fetch_failed" in reason:
                        stats["fetch_failed"] += 1
                    else:
                        stats["parse_failed"] += 1
                    logger.debug(f"  {trade.ticker} {trade.director_name}: {reason}")
                    continue

                # Sanity check on verified date
                v_cal = (trade.date_lodged - verified_date).days
                if v_cal < 0 or v_cal > 365:
                    stats["skipped_nonsensical"] += 1
                    logger.warning(
                        f"  {trade.ticker} {trade.director_name}: "
                        f"verified_date={verified_date} vs date_lodged={trade.date_lodged} "
                        f"is nonsensical ({v_cal} days) — skipping"
                    )
                    continue

                if verified_date == trade.date_of_trade:
                    stats["verified_unchanged"] += 1
                    continue

                # Date differs — this trade had a wrong date stored
                bd_before = business_days(trade.date_of_trade, trade.date_lodged)
                bd_after = business_days(verified_date, trade.date_lodged)
                logger.info(
                    f"  {mode}CORRECTING {trade.ticker} {trade.director_name}: "
                    f"{trade.date_of_trade} → {verified_date} "
                    f"({bd_before}bd → {bd_after}bd) [{reason}]"
                )

                if not dry_run:
                    await session.execute(
                        update(Trade)
                        .where(Trade.id == trade.id)
                        .values(date_of_trade=verified_date)
                    )

                stats["verified_corrected"] += 1

                # Batch commit
                if not dry_run and stats["verified_corrected"] % BATCH_SIZE == 0:
                    await session.commit()
                    logger.info(f"  Committed batch of {BATCH_SIZE}")

        if not dry_run:
            await session.commit()

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"{mode}Verification complete:")
    logger.info(f"  Total candidates:      {total}")
    logger.info(f"  Date corrected:        {stats['verified_corrected']}")
    logger.info(f"  Already correct:       {stats['verified_unchanged']}")
    logger.info(f"  No PDF in DB:          {stats['no_pdf_url']}")
    logger.info(f"  PDF fetch failed:      {stats['fetch_failed']}  (URL likely expired)")
    logger.info(f"  PDF parse failed:      {stats['parse_failed']}")
    logger.info(f"  Nonsensical dates:     {stats['skipped_nonsensical']}")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN — no changes written to database.")
    else:
        logger.info("Done. Re-run /api/compliance/violations to see updated results.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify all existing violation dates against original ASX PDFs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to the database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only process the first N candidates (for testing)",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run, limit=args.limit))
