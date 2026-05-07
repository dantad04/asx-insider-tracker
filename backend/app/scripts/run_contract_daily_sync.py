"""Run the daily live contract delta sync and alert generation."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from app.database import async_session
from app.scripts import generate_contract_alerts, sync_contracts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ContractSyncSummary:
    fetched: int
    inserted: int
    updated: int
    skipped_no_abn: int
    suppliers_inserted: int
    window_start: str
    window_end: str


@dataclass(frozen=True)
class AlertSummary:
    evaluated_contracts: int
    defence_candidates: int
    asx_linked_candidates: int
    high_value_candidates: int
    written: int
    duplicates_skipped: int
    since: str


async def _run_delta_sync(dry_run: bool) -> ContractSyncSummary:
    latest = await sync_contracts._default_delta_since()
    since_date = latest.astimezone(timezone.utc).date() - timedelta(days=1)
    windows = [
        (
            sync_contracts._start_of_day(since_date),
            datetime.now(timezone.utc),
        )
    ]
    date_type = "contractLastModified"
    stats = sync_contracts.SyncStats()
    parsed_rows: list[dict] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": sync_contracts.USER_AGENT},
        timeout=60,
        follow_redirects=True,
    ) as client:
        for since, until in windows:
            releases = await sync_contracts.fetch_releases(client, date_type, since, until)
            stats.fetched += len(releases)
            for release in releases:
                row = sync_contracts.parse_release(release)
                if row is None:
                    stats.skipped_no_abn += 1
                    continue
                parsed_rows.append(row)

    stats.inserted, stats.updated, stats.suppliers_new = await sync_contracts.write_rows(
        parsed_rows,
        dry_run,
    )

    return ContractSyncSummary(
        fetched=stats.fetched,
        inserted=stats.inserted,
        updated=stats.updated,
        skipped_no_abn=stats.skipped_no_abn,
        suppliers_inserted=stats.suppliers_new,
        window_start=sync_contracts._format_ocds_datetime(windows[0][0]),
        window_end=sync_contracts._format_ocds_datetime(windows[-1][1]),
    )


async def _run_alert_generation(dry_run: bool) -> AlertSummary:
    since = datetime.now(timezone.utc).date() - timedelta(days=30)
    async with async_session() as db:
        rows = await generate_contract_alerts._load_contracts(db, since)
        alert_rows, counts = generate_contract_alerts._build_alert_rows(rows)

        if dry_run:
            written = 0
            duplicates = await generate_contract_alerts._count_duplicates(db, alert_rows)
        else:
            written = await generate_contract_alerts._insert_alerts(db, alert_rows)
            duplicates = len(alert_rows) - written

    return AlertSummary(
        evaluated_contracts=len(rows),
        defence_candidates=counts["defence"],
        asx_linked_candidates=counts["asx_linked"],
        high_value_candidates=counts["high_value"],
        written=written,
        duplicates_skipped=duplicates,
        since=since.isoformat(),
    )


async def run(args: argparse.Namespace) -> int:
    started = datetime.now(timezone.utc)
    started_monotonic = time.monotonic()

    logger.info("daily contract sync started")
    sync_summary = await _run_delta_sync(args.dry_run)
    logger.info("daily contract delta sync completed")
    alert_summary = await _run_alert_generation(args.dry_run)
    logger.info("daily contract alert generation completed")

    finished = datetime.now(timezone.utc)
    duration = time.monotonic() - started_monotonic

    print(f"started_at: {started.isoformat()}")
    print(f"finished_at: {finished.isoformat()}")
    print(f"duration_seconds: {duration:.1f}")
    print(f"dry_run: {args.dry_run}")
    print(f"sync_window_start: {sync_summary.window_start}")
    print(f"sync_window_end: {sync_summary.window_end}")
    print(f"contracts_fetched: {sync_summary.fetched}")
    print(f"contracts_inserted: {sync_summary.inserted}")
    print(f"contracts_updated: {sync_summary.updated}")
    print(f"contracts_skipped_no_abn: {sync_summary.skipped_no_abn}")
    print(f"suppliers_inserted: {sync_summary.suppliers_inserted}")
    print(f"alerts_since: {alert_summary.since}")
    print(f"alert_contracts_evaluated: {alert_summary.evaluated_contracts}")
    print(f"alert_candidates_defence: {alert_summary.defence_candidates}")
    print(f"alert_candidates_asx_linked: {alert_summary.asx_linked_candidates}")
    print(f"alert_candidates_high_value: {alert_summary.high_value_candidates}")
    print(f"alerts_written: {alert_summary.written}")
    print(f"alert_duplicates_skipped: {alert_summary.duplicates_skipped}")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run daily live contract sync")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and evaluate without writes")
    parser.add_argument("--verbose", action="store_true", help="Enable INFO logging")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        raise SystemExit(asyncio.run(run(args)))
    except Exception:
        logger.exception("daily contract sync failed")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
