"""Sync AusTender OCDS contract notices into Postgres.

This is a data-foundation script only. It reads the official AusTender OCDS
API, parses contracts[] from each release, and upserts contract rows plus
unmapped supplier shell rows. It does not generate alerts or ticker mappings.

Usage:
  python -m app.scripts.sync_contracts --mode bootstrap --since 2024-11-01 --until 2025-05-06 --verbose
  python -m app.scripts.sync_contracts --mode delta --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as dt_time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.database import async_session
from app.models.contracts import Contract, ContractSupplier

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tenders.gov.au/ocds"
USER_AGENT = "asx-insider-tracker/contracts-sync (+https://asx-insider.up.railway.app)"
BOOTSTRAP_CHUNK_DAYS = 30
RETRY_DELAYS_SECONDS = (1, 2, 4, 8)
WRITE_BATCH_SIZE = 500


@dataclass
class SyncStats:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    skipped_no_abn: int = 0
    suppliers_new: int = 0


def _parse_cli_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def _start_of_day(value: date) -> datetime:
    return datetime.combine(value, dt_time.min, tzinfo=UTC)


def _end_of_day(value: date) -> datetime:
    return datetime.combine(value, dt_time(23, 59, 59), tzinfo=UTC)


def _format_ocds_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            try:
                parsed = datetime.combine(date.fromisoformat(raw[:10]), dt_time.min)
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: Any) -> date | None:
    parsed = _parse_datetime(value)
    return parsed.date() if parsed else None


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _roles_include_supplier(roles: Any) -> bool:
    if roles is None:
        return False
    if isinstance(roles, str):
        roles = [roles]
    if not isinstance(roles, list):
        return False
    return any("supplier" in str(role).lower() for role in roles)


def _supplier_from_parties(parties: Any) -> tuple[str | None, str | None]:
    if not isinstance(parties, list):
        return None, None
    for party in parties:
        if not isinstance(party, dict) or not _roles_include_supplier(party.get("roles")):
            continue
        identifier = party.get("identifier") if isinstance(party.get("identifier"), dict) else {}
        supplier_abn = identifier.get("id")
        additional_identifiers = party.get("additionalIdentifiers")
        if not supplier_abn and isinstance(additional_identifiers, list):
            for extra_identifier in additional_identifiers:
                if not isinstance(extra_identifier, dict):
                    continue
                if str(extra_identifier.get("scheme", "")).upper() == "AU-ABN":
                    supplier_abn = extra_identifier.get("id")
                    break
        supplier_name = party.get("name")
        if supplier_abn:
            return str(supplier_abn).strip(), str(supplier_name or "").strip() or None
    return None, None


def _first_contract(release: dict[str, Any]) -> dict[str, Any]:
    contracts = release.get("contracts")
    if isinstance(contracts, list) and contracts and isinstance(contracts[0], dict):
        return contracts[0]
    return {}


def _parent_cn_id(release: dict[str, Any], is_amendment: bool) -> str | None:
    if not is_amendment:
        return None
    related_processes = release.get("relatedProcesses")
    if not isinstance(related_processes, list):
        return None
    for process in related_processes:
        if not isinstance(process, dict):
            continue
        identifier = process.get("identifier") or process.get("id")
        if identifier:
            return str(identifier).strip()
    return None


def _contract_unspsc(contract: dict[str, Any]) -> str | None:
    items = contract.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        classification = item.get("classification")
        if isinstance(classification, dict) and classification.get("id"):
            return str(classification["id"]).strip()
    return None


def parse_release(release: dict[str, Any]) -> dict[str, Any] | None:
    contract = _first_contract(release)
    tender = release.get("tender") if isinstance(release.get("tender"), dict) else {}
    buyer = release.get("buyer") if isinstance(release.get("buyer"), dict) else {}
    value = contract.get("value") if isinstance(contract.get("value"), dict) else {}
    period = contract.get("period") if isinstance(contract.get("period"), dict) else {}
    classification = (
        tender.get("classification") if isinstance(tender.get("classification"), dict) else {}
    )
    supplier_abn, supplier_name = _supplier_from_parties(release.get("parties"))
    if not supplier_abn:
        return None

    cn_id = contract.get("id") or release.get("id")
    if not cn_id:
        return None

    currency = value.get("currency")
    if currency and str(currency).upper() != "AUD":
        logger.warning("contract %s has non-AUD currency: %s", cn_id, currency)

    release_date = _parse_datetime(release.get("date"))
    if release_date is None:
        logger.warning("contract %s missing parseable release date; skipping", cn_id)
        return None

    tags = release.get("tag")
    if isinstance(tags, str):
        tags = [tags]
    if not isinstance(tags, list):
        tags = []
    is_amendment = "contractAmendment" in tags

    return {
        "cn_id": str(cn_id).strip(),
        "publish_date": _parse_date(contract.get("dateSigned")) or release_date.date(),
        "last_modified": release_date,
        "start_date": _parse_date(period.get("startDate")),
        "end_date": _parse_date(period.get("endDate")),
        "value_aud": _parse_decimal(value.get("amount")),
        "agency": str(buyer.get("name") or "").strip(),
        "description": (
            str(tender.get("title") or contract.get("description") or contract.get("title") or "").strip()
            or None
        ),
        "category_unspsc": str(classification.get("id") or _contract_unspsc(contract) or "").strip() or None,
        "procurement_method": str(tender.get("procurementMethod") or "").strip() or None,
        "supplier_abn": supplier_abn,
        "supplier_name_raw": supplier_name or supplier_abn,
        "is_amendment": is_amendment,
        "parent_cn_id": _parent_cn_id(release, is_amendment),
        "raw_payload": release,
    }


def _chunk_windows(since: date, until: date) -> list[tuple[datetime, datetime]]:
    end = _end_of_day(until)
    cursor = _start_of_day(since)
    windows = []
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=BOOTSTRAP_CHUNK_DAYS) - timedelta(seconds=1), end)
        windows.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(seconds=1)
    return windows


async def _request_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    attempts = len(RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        response = await client.get(url)
        if response.status_code == 429 or response.status_code >= 500:
            if attempt < len(RETRY_DELAYS_SECONDS):
                delay = RETRY_DELAYS_SECONDS[attempt]
                logger.info(
                    "retrying AusTender request after status %s in %ss",
                    response.status_code,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("AusTender OCDS response was not a JSON object")
        return payload
    raise RuntimeError(f"AusTender request failed after {attempts} attempts: {url}")


async def fetch_releases(
    client: httpx.AsyncClient,
    date_type: str,
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    since_iso = _format_ocds_datetime(since)
    until_iso = _format_ocds_datetime(until)
    url = f"{BASE_URL}/findByDates/{date_type}/{since_iso}/{until_iso}"
    releases: list[dict[str, Any]] = []

    while url:
        payload = await _request_json(client, url)
        page_releases = payload.get("releases", [])
        if isinstance(page_releases, list):
            releases.extend(release for release in page_releases if isinstance(release, dict))

        links = payload.get("links") if isinstance(payload.get("links"), dict) else {}
        next_url = links.get("next")
        url = urljoin(url, next_url) if next_url else ""

    logger.info("fetching %s %s → %s: got %s releases", date_type, since_iso, until_iso, len(releases))
    return releases


async def _default_delta_since() -> datetime:
    async with async_session() as session:
        result = await session.execute(select(func.max(Contract.last_modified)))
        latest = result.scalar_one_or_none()
    if latest is None:
        return datetime.now(UTC) - timedelta(days=7)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)
    return latest.astimezone(UTC)


async def build_windows(args: argparse.Namespace) -> tuple[str, list[tuple[datetime, datetime]]]:
    if args.mode == "bootstrap":
        if args.since is None or args.until is None:
            raise ValueError("--mode bootstrap requires --since and --until")
        if args.since > args.until:
            raise ValueError("--since must be before or equal to --until")
        return "contractPublished", _chunk_windows(args.since, args.until)

    since = _start_of_day(args.since) if args.since else await _default_delta_since()
    until = _end_of_day(args.until) if args.until else datetime.now(UTC)
    if since > until:
        raise ValueError("delta since must be before or equal to until")
    return "contractLastModified", [(since, until)]


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        cn_id = row["cn_id"]
        existing = deduped.get(cn_id)
        if existing is None:
            deduped[cn_id] = row
            continue

        if row["last_modified"] < existing["last_modified"]:
            deduped[cn_id] = {
                **row,
                "last_modified": existing["last_modified"],
                "value_aud": existing["value_aud"],
                "raw_payload": existing["raw_payload"],
            }
            continue

        if row["last_modified"] > existing["last_modified"]:
            existing["last_modified"] = row["last_modified"]
            existing["value_aud"] = row["value_aud"]
            existing["raw_payload"] = row["raw_payload"]
    return list(deduped.values())


def _batches(rows: list[dict[str, Any]], size: int = WRITE_BATCH_SIZE):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


async def _existing_contract_ids(session, cn_ids: list[str]) -> set[str]:
    existing: set[str] = set()
    for batch in _batches([{"cn_id": cn_id} for cn_id in cn_ids], 1000):
        result = await session.execute(
            select(Contract.cn_id).where(Contract.cn_id.in_([row["cn_id"] for row in batch]))
        )
        existing.update(result.scalars().all())
    return existing


async def _existing_supplier_abns(session, supplier_abns: list[str]) -> set[str]:
    existing: set[str] = set()
    for batch in _batches([{"supplier_abn": abn} for abn in supplier_abns], 1000):
        result = await session.execute(
            select(ContractSupplier.supplier_abn).where(
                ContractSupplier.supplier_abn.in_([row["supplier_abn"] for row in batch])
            )
        )
        existing.update(result.scalars().all())
    return existing


async def write_rows(rows: list[dict[str, Any]], dry_run: bool) -> tuple[int, int, int]:
    if not rows:
        return 0, 0, 0

    rows = _dedupe_rows(rows)
    cn_ids = [row["cn_id"] for row in rows]
    supplier_abns = sorted({row["supplier_abn"] for row in rows})

    async with async_session() as session:
        existing_contracts = await _existing_contract_ids(session, cn_ids)
        existing_suppliers = await _existing_supplier_abns(session, supplier_abns)

        inserted = sum(1 for cn_id in cn_ids if cn_id not in existing_contracts)
        updated = len(rows) - inserted
        suppliers_new = sum(1 for abn in supplier_abns if abn not in existing_suppliers)

        if dry_run:
            return inserted, updated, suppliers_new

        for batch in _batches(rows):
            contract_insert = insert(Contract.__table__).values(batch)
            await session.execute(
                contract_insert.on_conflict_do_update(
                    index_elements=[Contract.__table__.c.cn_id],
                    set_={
                        "last_modified": contract_insert.excluded.last_modified,
                        "value_aud": contract_insert.excluded.value_aud,
                        "raw_payload": contract_insert.excluded.raw_payload,
                        "updated_at": func.now(),
                    },
                )
            )

        supplier_rows = [
            {
                "supplier_abn": row["supplier_abn"],
                "supplier_name": row["supplier_name_raw"],
                "confidence": "unmapped",
            }
            for row in rows
            if row["supplier_abn"] not in existing_suppliers
        ]
        supplier_rows = list({row["supplier_abn"]: row for row in supplier_rows}.values())
        for batch in _batches(supplier_rows):
            supplier_insert = insert(ContractSupplier.__table__).values(batch)
            await session.execute(
                supplier_insert.on_conflict_do_nothing(
                    index_elements=[ContractSupplier.__table__.c.supplier_abn]
                )
            )

        await session.commit()

    return inserted, updated, suppliers_new


async def run(args: argparse.Namespace) -> None:
    start_time = time.monotonic()
    date_type, windows = await build_windows(args)
    stats = SyncStats()
    parsed_rows: list[dict[str, Any]] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=60,
        follow_redirects=True,
    ) as client:
        for since, until in windows:
            releases = await fetch_releases(client, date_type, since, until)
            stats.fetched += len(releases)
            for release in releases:
                row = parse_release(release)
                if row is None:
                    stats.skipped_no_abn += 1
                    continue
                parsed_rows.append(row)

    stats.inserted, stats.updated, stats.suppliers_new = await write_rows(parsed_rows, args.dry_run)

    window_start = _format_ocds_datetime(windows[0][0])
    window_end = _format_ocds_datetime(windows[-1][1])
    duration = time.monotonic() - start_time

    if args.dry_run:
        logger.info("dry run: no database writes performed")

    print(
        f"contracts:    {stats.fetched} fetched, {stats.inserted} inserted, "
        f"{stats.updated} updated, {stats.skipped_no_abn} skipped (no ABN)"
    )
    print(f"suppliers:    {stats.suppliers_new} new unmapped")
    print(f"window:       {window_start} → {window_end}")
    print(f"duration:     {duration:.1f}s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync AusTender OCDS contracts")
    parser.add_argument("--mode", choices=("bootstrap", "delta"), required=True)
    parser.add_argument("--since", type=_parse_cli_date, help="Start date, YYYY-MM-DD")
    parser.add_argument("--until", type=_parse_cli_date, help="End date, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse without writes")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose INFO logging")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(run(args))
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
