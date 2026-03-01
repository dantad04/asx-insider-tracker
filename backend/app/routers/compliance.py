"""
Compliance API router — ASX Appendix 3Y late-filing endpoints.

Endpoints:
  GET /api/compliance/late-filings        All late filings (paginated, filterable)
  GET /api/compliance/repeat-offenders    Directors with 3+ violations in past year
  GET /api/compliance/company/{ticker}    Per-company compliance history
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.company import Company
from app.models.director import Director
from app.models.trade import Trade

router = APIRouter(prefix="/api/compliance", tags=["compliance"])

VERIFIED_CUTOFF = date(2026, 1, 1)
ARTEFACT_THRESHOLD_DAYS = 365


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def business_days(start: date, end: date) -> int:
    if end <= start:
        return 0
    return int(np.busday_count(start.isoformat(), end.isoformat()))


def severity(bdays: int) -> str:
    """ASX Listing Rule 3.19A.2: 5 business days to lodge."""
    if bdays <= 5:
        return "compliant"
    elif bdays <= 10:
        return "minor"
    elif bdays <= 20:
        return "moderate"
    return "severe"


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class LateFilingItem(BaseModel):
    trade_id: str
    ticker: str
    company_name: str
    director_name: str
    date_of_trade: date
    date_lodged: date
    calendar_days: int
    business_days: int
    severity: str
    trade_type: str
    verified: bool


class RepeatOffender(BaseModel):
    director_name: str
    violations_past_year: int
    tickers: list[str]


class CompanyComplianceItem(BaseModel):
    ticker: str
    company_name: str
    total_trades: int
    late_filings: int
    compliance_rate_pct: float
    minor: int
    moderate: int
    severe: int
    violations: list[LateFilingItem]


# ---------------------------------------------------------------------------
# Helpers that load and classify trade rows
# ---------------------------------------------------------------------------

async def _load_violations(
    db: AsyncSession,
    ticker: str | None = None,
    severity_filter: str | None = None,
    verified_only: bool = False,
    days: int | None = None,
) -> list[LateFilingItem]:
    query = (
        select(
            Trade.id,
            Trade.date_of_trade,
            Trade.date_lodged,
            Trade.trade_type,
            Company.ticker,
            Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.date_of_trade != None, Trade.date_lodged != None)
    )

    if ticker:
        query = query.where(Company.ticker == ticker.upper())
    if verified_only:
        query = query.where(Trade.date_lodged >= VERIFIED_CUTOFF)
    if days:
        cutoff = date.today() - timedelta(days=days)
        query = query.where(Trade.date_lodged >= cutoff)

    result = await db.execute(query.order_by(Trade.date_lodged.desc()))
    rows = result.all()

    items: list[LateFilingItem] = []
    for r in rows:
        cal = (r.date_lodged - r.date_of_trade).days
        if cal < 0 or cal > ARTEFACT_THRESHOLD_DAYS:
            continue

        bd = business_days(r.date_of_trade, r.date_lodged)
        sev = severity(bd)

        if sev == "compliant":
            continue  # only violations
        if severity_filter and sev != severity_filter:
            continue

        items.append(LateFilingItem(
            trade_id=r.id,
            ticker=r.ticker,
            company_name=r.company_name,
            director_name=r.director_name,
            date_of_trade=r.date_of_trade,
            date_lodged=r.date_lodged,
            calendar_days=cal,
            business_days=bd,
            severity=sev,
            trade_type=r.trade_type,
            verified=r.date_lodged >= VERIFIED_CUTOFF,
        ))

    return items


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/late-filings", response_model=list[LateFilingItem])
async def get_late_filings(
    severity: Literal["minor", "moderate", "severe"] | None = Query(
        None, description="Filter by severity"
    ),
    verified_only: bool = Query(False, description="Only include verified 3Y scraper data"),
    days: int | None = Query(None, description="Only filings within last N days"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
):
    """
    Return all late Appendix 3Y filings, ordered by date lodged descending.

    - **severity**: `minor` (6-10 bd), `moderate` (11-20 bd), `severe` (21+ bd)
    - **verified_only**: restrict to trades sourced from the 3Y PDF scraper (high-confidence dates)
    - **days**: only show filings lodged within the last N days
    """
    items = await _load_violations(
        db,
        severity_filter=severity,
        verified_only=verified_only,
        days=days,
    )
    return items[offset: offset + limit]


@router.get("/repeat-offenders", response_model=list[RepeatOffender])
async def get_repeat_offenders(
    min_violations: int = Query(3, ge=2, description="Minimum violations in past year"),
    verified_only: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """
    Directors with ≥ min_violations late filings in the past 365 days.
    """
    items = await _load_violations(db, verified_only=verified_only, days=365)

    counts: dict[str, dict] = {}
    for item in items:
        key = item.director_name
        if key not in counts:
            counts[key] = {"director_name": key, "violations_past_year": 0, "tickers": set()}
        counts[key]["violations_past_year"] += 1
        counts[key]["tickers"].add(item.ticker)

    result = [
        RepeatOffender(
            director_name=v["director_name"],
            violations_past_year=v["violations_past_year"],
            tickers=sorted(v["tickers"]),
        )
        for v in counts.values()
        if v["violations_past_year"] >= min_violations
    ]
    return sorted(result, key=lambda x: -x.violations_past_year)


@router.get("/company/{ticker}", response_model=CompanyComplianceItem)
async def get_company_compliance(
    ticker: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Full compliance history for a specific company ticker.
    Includes compliance rate, severity breakdown, and all late filings.
    """
    # Check company exists
    company_result = await db.execute(
        select(Company).where(Company.ticker == ticker.upper())
    )
    company = company_result.scalar_one_or_none()
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found")

    # Total trade count (for compliance rate denominator)
    all_trades_result = await db.execute(
        select(Trade)
        .join(Company, Company.id == Trade.company_id)
        .where(Company.ticker == ticker.upper())
    )
    all_trades = all_trades_result.scalars().all()
    total = len(all_trades)

    violations = await _load_violations(db, ticker=ticker)

    minor = sum(1 for v in violations if v.severity == "minor")
    moderate = sum(1 for v in violations if v.severity == "moderate")
    severe = sum(1 for v in violations if v.severity == "severe")
    late = len(violations)

    return CompanyComplianceItem(
        ticker=ticker.upper(),
        company_name=company.name,
        total_trades=total,
        late_filings=late,
        compliance_rate_pct=round(100 * (total - late) / max(total, 1), 1),
        minor=minor,
        moderate=moderate,
        severe=severe,
        violations=violations,
    )
