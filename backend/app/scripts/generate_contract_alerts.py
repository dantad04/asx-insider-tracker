"""Generate live contract alerts from ingested AusTender contracts."""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.contracts import Contract, ContractAlert, ContractSupplier

logger = logging.getLogger(__name__)

AGENCY_KEYWORDS = [
    "Defence",
    "ADF",
    "ARMY",
    "NAVY",
    "RAAF",
    "Australian Signals Directorate",
    "ASD",
    "ASIS",
    "DIO",
    "DST Group",
    "CASG",
    "Joint",
    "Home Affairs",
]

DESCRIPTION_KEYWORDS = [
    "combat",
    "weapons",
    "munitions",
    "explosive",
    "surveillance",
    "intelligence",
    "classified",
    "military",
    "submarine",
    "frigate",
    "armoured",
    "armored",
    "drone",
    "UAV",
    "missile",
    "radar",
    "cyber security",
    "sovereign capability",
]

UNSPSC_PREFIXES = ("14", "58", "95")
HIGH_VALUE_THRESHOLD = Decimal("50000000")
VERY_HIGH_VALUE_THRESHOLD = Decimal("200000000")
ASX_LINKED_HIGH_VALUE_THRESHOLD = Decimal("10000000")


@dataclass(frozen=True)
class ContractRow:
    contract: Contract
    supplier: ContractSupplier | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate contract alerts")
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Only evaluate contracts with publish_date >= YYYY-MM-DD",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute alerts without writing rows")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _contains_keyword(value: str | None, keywords: list[str]) -> str | None:
    if not value:
        return None

    haystack = value.lower()
    for keyword in keywords:
        if keyword.lower() in haystack:
            return keyword
    return None


def _defence_reason(contract: Contract) -> str | None:
    agency_reason = _contains_keyword(contract.agency, AGENCY_KEYWORDS)
    if agency_reason:
        return agency_reason

    description_reason = _contains_keyword(contract.description, DESCRIPTION_KEYWORDS)
    if description_reason:
        return description_reason

    category_unspsc = contract.category_unspsc or ""
    for prefix in UNSPSC_PREFIXES:
        if category_unspsc.startswith(prefix):
            return f"UNSPSC {prefix}"

    return None


def _is_greater_than(value: Decimal | None, threshold: Decimal) -> bool:
    return value is not None and value > threshold


def _priority_for_value(
    value: Decimal | None,
    threshold: Decimal,
) -> str:
    return "high" if _is_greater_than(value, threshold) else "medium"


async def _load_contracts(db: AsyncSession, since: date) -> list[ContractRow]:
    result = await db.execute(
        select(Contract, ContractSupplier)
        .outerjoin(ContractSupplier, ContractSupplier.supplier_abn == Contract.supplier_abn)
        .where(Contract.publish_date >= since)
        .order_by(Contract.publish_date.asc(), Contract.cn_id.asc())
    )
    return [ContractRow(contract=row[0], supplier=row[1]) for row in result.all()]


def _build_alert_rows(rows: list[ContractRow]) -> tuple[list[dict], dict[str, int]]:
    detected_at = datetime.now(timezone.utc)
    alert_rows: list[dict] = []
    counts = {"defence": 0, "asx_linked": 0, "high_value": 0}

    for row in rows:
        contract = row.contract
        supplier = row.supplier
        parent_ticker = supplier.parent_ticker if supplier and supplier.parent_ticker else None
        supplier_name = (
            supplier.supplier_name
            if supplier and supplier.supplier_name
            else contract.supplier_name_raw
        )
        defence_reason = _defence_reason(contract)
        is_defence_contract = defence_reason is not None

        def append_alert(alert_type: str, priority: str) -> None:
            counts[alert_type] += 1
            alert_rows.append(
                {
                    "id": str(uuid4()),
                    "cn_id": contract.cn_id,
                    "ticker": parent_ticker,
                    "supplier_abn": contract.supplier_abn,
                    "supplier_name": supplier_name,
                    "alert_type": alert_type,
                    "priority": priority,
                    "detected_at": detected_at,
                    "contract_value_aud": contract.value_aud,
                    "is_defence_contract": alert_type == "defence" or is_defence_contract,
                    "defence_reason": defence_reason,
                    "notified": False,
                }
            )

        if is_defence_contract:
            append_alert(
                "defence",
                _priority_for_value(contract.value_aud, HIGH_VALUE_THRESHOLD),
            )

        if parent_ticker:
            append_alert(
                "asx_linked",
                _priority_for_value(contract.value_aud, ASX_LINKED_HIGH_VALUE_THRESHOLD),
            )

        if _is_greater_than(contract.value_aud, HIGH_VALUE_THRESHOLD):
            append_alert(
                "high_value",
                _priority_for_value(contract.value_aud, VERY_HIGH_VALUE_THRESHOLD),
            )

    return alert_rows, counts


async def _count_duplicates(db: AsyncSession, alert_rows: list[dict]) -> int:
    if not alert_rows:
        return 0

    cn_ids = {row["cn_id"] for row in alert_rows}
    result = await db.execute(
        select(ContractAlert.cn_id, ContractAlert.alert_type).where(ContractAlert.cn_id.in_(cn_ids))
    )
    existing = {(cn_id, alert_type) for cn_id, alert_type in result.all()}
    return sum((row["cn_id"], row["alert_type"]) in existing for row in alert_rows)


async def _insert_alerts(db: AsyncSession, alert_rows: list[dict]) -> int:
    if not alert_rows:
        return 0

    stmt = (
        insert(ContractAlert)
        .values(alert_rows)
        .on_conflict_do_nothing(index_elements=["cn_id", "alert_type"])
        .returning(ContractAlert.id)
    )
    result = await db.execute(stmt)
    inserted = len(result.scalars().all())
    await db.commit()
    return inserted


async def run(args: argparse.Namespace) -> None:
    since = args.since or (datetime.now(timezone.utc).date() - timedelta(days=30))

    async with async_session() as db:
        rows = await _load_contracts(db, since)
        logger.info("evaluating %d contracts since %s", len(rows), since)

        alert_rows, counts = _build_alert_rows(rows)
        duplicates = await _count_duplicates(db, alert_rows)

        if args.dry_run:
            written = 0
        else:
            written = await _insert_alerts(db, alert_rows)
            duplicates = len(alert_rows) - written

    logger.info("  defence:    %d alerts", counts["defence"])
    logger.info("  asx_linked: %d alerts", counts["asx_linked"])
    logger.info("  high_value: %d alerts", counts["high_value"])
    logger.info("  total: %d written, %d duplicates skipped", written, duplicates)


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
