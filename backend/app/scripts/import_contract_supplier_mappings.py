"""Import verified contract supplier ABN to ASX ticker mappings."""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.database import async_session
from app.models.contracts import ContractSupplier

logger = logging.getLogger(__name__)

INCLUDE_TRUE_VALUES = {"1", "true", "t", "y", "yes"}
IMPORT_NOTE = "Imported from contract supplier seed"


@dataclass(frozen=True)
class MappingRow:
    supplier_abn: str
    parent_ticker: str
    supplier_name: str | None
    notes: str | None


@dataclass
class ImportStats:
    rows_read: int = 0
    rows_eligible: int = 0
    skipped_missing_abn: int = 0
    skipped_missing_ticker: int = 0
    skipped_include_not_yes: int = 0
    duplicate_abns_collapsed: int = 0
    no_include_filter_present: bool = False


def _normalise_header(value: str) -> str:
    return value.strip().lower()


def _normalise_abn(value: str | None) -> str:
    return "".join(re.findall(r"\d+", value or ""))


def _normalise_ticker(value: str | None) -> str:
    ticker = (value or "").strip().upper()
    if ticker.endswith(".AX"):
        ticker = ticker[:-3]
    return re.sub(r"[^A-Z0-9]", "", ticker)


def _clean_text(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None


def _resolve_path(path_arg: str) -> Path:
    path = Path(path_arg).expanduser()
    candidates = [path]
    if not path.is_absolute():
        here = Path(__file__).resolve()
        candidates.extend(
            [
                Path.cwd() / path,
                here.parents[2] / path,
                here.parents[3] / path,
            ]
        )
        if path.parts and path.parts[0] == "backend":
            stripped = Path(*path.parts[1:])
            candidates.extend(
                [
                    here.parents[2] / stripped,
                    here.parents[3] / stripped,
                ]
            )

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(f"seed file not found: {path_arg}")


def _append_import_note(existing_notes: str | None, row_notes: str | None) -> str:
    notes: list[str] = []
    for value in (existing_notes, row_notes, IMPORT_NOTE):
        cleaned = _clean_text(value)
        if cleaned and cleaned not in notes:
            notes.append(cleaned)
    return " | ".join(notes)


def _row_value(row: dict[str, str], *names: str) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None and value.strip():
            return value
    return None


def _read_seed(path: Path) -> tuple[list[MappingRow], ImportStats, list[str]]:
    stats = ImportStats()
    mappings_by_abn: dict[str, MappingRow] = {}

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("seed CSV has no header row")

        headers = [_normalise_header(header) for header in reader.fieldnames]
        include_present = "include" in headers
        stats.no_include_filter_present = not include_present

        for raw_row in reader:
            stats.rows_read += 1
            row = {_normalise_header(key): value for key, value in raw_row.items() if key}

            if include_present:
                include_value = (row.get("include") or "").strip().lower()
                if include_value not in INCLUDE_TRUE_VALUES:
                    stats.skipped_include_not_yes += 1
                    continue

            stats.rows_eligible += 1
            supplier_abn = _normalise_abn(_row_value(row, "abn", "supplier_abn"))
            parent_ticker = _normalise_ticker(_row_value(row, "ticker", "parent_ticker"))
            supplier_name = _clean_text(
                _row_value(row, "supplier_name_raw", "supplier_name")
            )
            notes = _clean_text(row.get("notes"))

            if not supplier_abn:
                stats.skipped_missing_abn += 1
                continue
            if not parent_ticker:
                stats.skipped_missing_ticker += 1
                continue

            if supplier_abn in mappings_by_abn:
                stats.duplicate_abns_collapsed += 1

            mappings_by_abn[supplier_abn] = MappingRow(
                supplier_abn=supplier_abn,
                parent_ticker=parent_ticker,
                supplier_name=supplier_name,
                notes=notes,
            )

    return list(mappings_by_abn.values()), stats, headers


async def _load_existing(
    supplier_abns: list[str],
) -> dict[str, ContractSupplier]:
    if not supplier_abns:
        return {}

    async with async_session() as session:
        result = await session.execute(
            select(ContractSupplier).where(ContractSupplier.supplier_abn.in_(supplier_abns))
        )
        return {supplier.supplier_abn: supplier for supplier in result.scalars()}


async def _mapped_supplier_count() -> int:
    async with async_session() as session:
        result = await session.execute(
            select(func.count()).select_from(ContractSupplier).where(
                ContractSupplier.parent_ticker.is_not(None)
            )
        )
        return int(result.scalar_one())


async def run(args: argparse.Namespace) -> None:
    path = _resolve_path(args.path)
    mappings, stats, headers = _read_seed(path)
    existing = await _load_existing([mapping.supplier_abn for mapping in mappings])
    would_update = sum(1 for mapping in mappings if mapping.supplier_abn in existing)
    would_insert = len(mappings) - would_update
    tickers = sorted({mapping.parent_ticker for mapping in mappings})

    if args.verbose:
        logger.info("seed path: %s", path)
        logger.info("headers: %s", ", ".join(headers))
        if stats.no_include_filter_present:
            logger.info("include filter: not present, all rows considered eligible")
        logger.info("distinct tickers: %s", ", ".join(tickers))

    if args.dry_run:
        print(f"rows read: {stats.rows_read}")
        print(f"rows eligible: {stats.rows_eligible}")
        print(f"rows skipped missing ABN: {stats.skipped_missing_abn}")
        print(f"rows skipped missing ticker: {stats.skipped_missing_ticker}")
        print(f"rows skipped include not yes: {stats.skipped_include_not_yes}")
        print(f"duplicate ABNs collapsed: {stats.duplicate_abns_collapsed}")
        print(f"existing suppliers that would be updated: {would_update}")
        print(f"new suppliers that would be inserted: {would_insert}")
        print(f"distinct tickers included: {', '.join(tickers)}")
        return

    rows = []
    for mapping in mappings:
        existing_supplier = existing.get(mapping.supplier_abn)
        supplier_name = (
            mapping.supplier_name
            or (existing_supplier.supplier_name if existing_supplier else None)
            or mapping.supplier_abn
        )
        existing_notes = existing_supplier.notes if existing_supplier else None
        rows.append(
            {
                "supplier_abn": mapping.supplier_abn,
                "supplier_name": supplier_name,
                "parent_ticker": mapping.parent_ticker,
                "confidence": "verified",
                "notes": _append_import_note(existing_notes, mapping.notes),
            }
        )

    if rows:
        async with async_session() as session:
            stmt = insert(ContractSupplier.__table__).values(rows)
            await session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[ContractSupplier.__table__.c.supplier_abn],
                    set_={
                        "supplier_name": stmt.excluded.supplier_name,
                        "parent_ticker": stmt.excluded.parent_ticker,
                        "confidence": stmt.excluded.confidence,
                        "notes": stmt.excluded.notes,
                        "updated_at": func.now(),
                    },
                )
            )
            await session.commit()

    mapped_after = await _mapped_supplier_count()
    skipped_total = (
        stats.skipped_missing_abn
        + stats.skipped_missing_ticker
        + stats.skipped_include_not_yes
    )

    print(f"rows read: {stats.rows_read}")
    print(f"suppliers inserted: {would_insert}")
    print(f"suppliers updated: {would_update}")
    print(f"suppliers skipped: {skipped_total}")
    print(f"mapped supplier count after import: {mapped_after}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import verified contract supplier ABN to ASX ticker mappings"
    )
    parser.add_argument("--path", required=True, help="Path to supplier mapping seed CSV")
    parser.add_argument("--dry-run", action="store_true", help="Inspect import without writes")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
