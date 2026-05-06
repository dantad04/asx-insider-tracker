"""Contract feed API endpoints."""

from __future__ import annotations

import logging
import math
from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/contracts", tags=["contracts"])

_ALERT_TYPES = {"defence", "asx_linked", "high_value"}
_PRIORITIES = {"high", "medium"}


class LiveContract(BaseModel):
    cn_id: str
    publish_date: date
    last_modified: datetime
    value_aud: float | None
    agency: str
    description: str | None
    category_unspsc: str | None
    supplier_name: str
    supplier_abn: str
    parent_ticker: str | None
    is_defence_contract: bool
    defence_reason: str | None
    is_amendment: bool
    procurement_method: str | None


class LiveContractsResponse(BaseModel):
    total: int
    contracts: list[LiveContract]


class ContractAlertItem(BaseModel):
    id: str
    detected_at: datetime
    cn_id: str
    ticker: str | None
    supplier_name: str
    contract_value_aud: float | None
    alert_type: str
    priority: str
    agency: str
    description: str | None
    is_defence_contract: bool
    defence_reason: str | None


class ContractAlertsResponse(BaseModel):
    total: int
    alerts: list[ContractAlertItem]


class ContractStatusResponse(BaseModel):
    contract_count: int
    alert_count: int
    high_priority_alert_count: int
    defence_contract_count: int
    asx_linked_count: int
    mapped_supplier_count: int
    unmapped_supplier_count: int
    last_successful_sync_at: datetime | None
    last_error: str | None


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _normalise_ticker(ticker: str | None) -> str | None:
    if not ticker:
        return None
    return ticker.strip().upper()


def _where_sql(clauses: list[str]) -> str:
    return "WHERE " + " AND ".join(clauses) if clauses else ""


def _validate_choice(name: str, value: str | None, allowed: set[str]) -> str | None:
    if value is None:
        return None
    normalised = value.strip().lower()
    if normalised not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be one of: {', '.join(sorted(allowed))}",
        )
    return normalised


def _build_live_filters(
    ticker: str | None,
    defence_only: bool,
    min_value: float | None,
    agency: str | None,
) -> tuple[list[str], dict]:
    clauses: list[str] = []
    params: dict = {}

    normalised_ticker = _normalise_ticker(ticker)
    if normalised_ticker:
        clauses.append("s.parent_ticker = :ticker")
        params["ticker"] = normalised_ticker

    if defence_only:
        clauses.append("da.cn_id IS NOT NULL")

    if min_value is not None:
        clauses.append("c.value_aud >= :min_value")
        params["min_value"] = Decimal(str(min_value))

    if agency:
        clauses.append("c.agency ILIKE :agency")
        params["agency"] = f"%{agency.strip()}%"

    return clauses, params


@router.get("/live", response_model=LiveContractsResponse)
async def list_live_contracts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    ticker: str | None = Query(None),
    defence_only: bool = Query(False),
    min_value: float | None = Query(None, ge=0),
    agency: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    clauses, params = _build_live_filters(ticker, defence_only, min_value, agency)
    where_sql = _where_sql(clauses)
    base_cte = """
        WITH defence_alerts AS (
            SELECT DISTINCT ON (cn_id)
                   cn_id,
                   defence_reason
            FROM contract_alerts
            WHERE is_defence_contract = TRUE
            ORDER BY cn_id, detected_at DESC
        )
    """

    total_sql = text(f"""
        {base_cte}
        SELECT COUNT(*)
        FROM contracts c
        LEFT JOIN contract_suppliers s ON s.supplier_abn = c.supplier_abn
        LEFT JOIN defence_alerts da ON da.cn_id = c.cn_id
        {where_sql}
    """)
    total = (await db.execute(total_sql, params)).scalar_one()

    rows_sql = text(f"""
        {base_cte}
        SELECT
            c.cn_id,
            c.publish_date,
            c.last_modified,
            c.value_aud,
            c.agency,
            c.description,
            c.category_unspsc,
            COALESCE(s.supplier_name, c.supplier_name_raw) AS supplier_name,
            c.supplier_abn,
            s.parent_ticker,
            (da.cn_id IS NOT NULL) AS is_defence_contract,
            da.defence_reason,
            c.is_amendment,
            c.procurement_method
        FROM contracts c
        LEFT JOIN contract_suppliers s ON s.supplier_abn = c.supplier_abn
        LEFT JOIN defence_alerts da ON da.cn_id = c.cn_id
        {where_sql}
        ORDER BY c.publish_date DESC, c.last_modified DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = (
        await db.execute(
            rows_sql,
            {
                **params,
                "limit": limit,
                "offset": offset,
            },
        )
    ).all()

    contracts = [
        LiveContract(
            cn_id=row.cn_id,
            publish_date=row.publish_date,
            last_modified=row.last_modified,
            value_aud=_float_or_none(row.value_aud),
            agency=row.agency,
            description=row.description,
            category_unspsc=row.category_unspsc,
            supplier_name=row.supplier_name,
            supplier_abn=row.supplier_abn,
            parent_ticker=row.parent_ticker,
            is_defence_contract=row.is_defence_contract,
            defence_reason=row.defence_reason,
            is_amendment=row.is_amendment,
            procurement_method=row.procurement_method,
        )
        for row in rows
    ]

    return LiveContractsResponse(total=total, contracts=contracts)


@router.get("/alerts", response_model=ContractAlertsResponse)
async def list_contract_alerts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    alert_type: str | None = Query(None),
    ticker: str | None = Query(None),
    priority: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    alert_type = _validate_choice("alert_type", alert_type, _ALERT_TYPES)
    priority = _validate_choice("priority", priority, _PRIORITIES)

    clauses: list[str] = []
    params: dict = {}

    if alert_type:
        clauses.append("a.alert_type = :alert_type")
        params["alert_type"] = alert_type

    normalised_ticker = _normalise_ticker(ticker)
    if normalised_ticker:
        clauses.append("a.ticker = :ticker")
        params["ticker"] = normalised_ticker

    if priority:
        clauses.append("a.priority = :priority")
        params["priority"] = priority

    where_sql = _where_sql(clauses)

    total_sql = text(f"""
        SELECT COUNT(*)
        FROM contract_alerts a
        JOIN contracts c ON c.cn_id = a.cn_id
        {where_sql}
    """)
    total = (await db.execute(total_sql, params)).scalar_one()

    rows_sql = text(f"""
        SELECT
            a.id,
            a.detected_at,
            a.cn_id,
            a.ticker,
            a.supplier_name,
            a.contract_value_aud,
            a.alert_type,
            a.priority,
            c.agency,
            c.description,
            a.is_defence_contract,
            a.defence_reason
        FROM contract_alerts a
        JOIN contracts c ON c.cn_id = a.cn_id
        {where_sql}
        ORDER BY a.detected_at DESC, a.cn_id DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = (
        await db.execute(
            rows_sql,
            {
                **params,
                "limit": limit,
                "offset": offset,
            },
        )
    ).all()

    alerts = [
        ContractAlertItem(
            id=str(row.id),
            detected_at=row.detected_at,
            cn_id=row.cn_id,
            ticker=row.ticker,
            supplier_name=row.supplier_name,
            contract_value_aud=_float_or_none(row.contract_value_aud),
            alert_type=row.alert_type,
            priority=row.priority,
            agency=row.agency,
            description=row.description,
            is_defence_contract=row.is_defence_contract,
            defence_reason=row.defence_reason,
        )
        for row in rows
    ]

    return ContractAlertsResponse(total=total, alerts=alerts)


async def _read_sync_state(
    db: AsyncSession,
    fallback_last_successful_sync_at: datetime | None,
) -> tuple[datetime | None, str | None]:
    state_table = (
        await db.execute(text("SELECT to_regclass('public.contract_sync_state')"))
    ).scalar_one_or_none()
    if not state_table:
        return fallback_last_successful_sync_at, None

    try:
        row = (
            await db.execute(
                text("""
                    SELECT last_successful_sync_at, last_error
                    FROM contract_sync_state
                    ORDER BY COALESCE(updated_at, last_successful_sync_at) DESC NULLS LAST
                    LIMIT 1
                """)
            )
        ).first()
    except Exception as exc:
        await db.rollback()
        logger.warning("failed to read contract_sync_state: %s", exc)
        return fallback_last_successful_sync_at, None

    if row is None:
        return fallback_last_successful_sync_at, None

    return row.last_successful_sync_at or fallback_last_successful_sync_at, row.last_error


@router.get("/status", response_model=ContractStatusResponse)
async def contract_status(db: AsyncSession = Depends(get_db)):
    row = (
        await db.execute(
            text("""
                SELECT
                    (SELECT COUNT(*) FROM contracts) AS contract_count,
                    (SELECT COUNT(*) FROM contract_alerts) AS alert_count,
                    (
                        SELECT COUNT(*)
                        FROM contract_alerts
                        WHERE priority = 'high'
                    ) AS high_priority_alert_count,
                    (
                        SELECT COUNT(DISTINCT cn_id)
                        FROM contract_alerts
                        WHERE is_defence_contract = TRUE OR alert_type = 'defence'
                    ) AS defence_contract_count,
                    (
                        SELECT COUNT(DISTINCT cn_id)
                        FROM contract_alerts
                        WHERE alert_type = 'asx_linked'
                    ) AS asx_linked_count,
                    (
                        SELECT COUNT(*)
                        FROM contract_suppliers
                        WHERE parent_ticker IS NOT NULL
                    ) AS mapped_supplier_count,
                    (
                        SELECT COUNT(*)
                        FROM contract_suppliers
                        WHERE parent_ticker IS NULL
                    ) AS unmapped_supplier_count,
                    (SELECT MAX(updated_at) FROM contracts) AS fallback_last_successful_sync_at
            """)
        )
    ).one()

    last_successful_sync_at, last_error = await _read_sync_state(
        db,
        row.fallback_last_successful_sync_at,
    )

    return ContractStatusResponse(
        contract_count=row.contract_count,
        alert_count=row.alert_count,
        high_priority_alert_count=row.high_priority_alert_count,
        defence_contract_count=row.defence_contract_count,
        asx_linked_count=row.asx_linked_count,
        mapped_supplier_count=row.mapped_supplier_count,
        unmapped_supplier_count=row.unmapped_supplier_count,
        last_successful_sync_at=last_successful_sync_at,
        last_error=last_error,
    )
