"""
Cluster API endpoints.

GET /api/clusters          — paginated list with filters, sort, join to cluster_returns
GET /api/clusters/stats    — aggregate stats bar (total, status counts, median alpha, hit rate)
GET /api/clusters/{id}     — full cluster detail with all 4 return variants + underlying trades
"""

from __future__ import annotations

import logging
import math
import statistics
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


# ── Response models ───────────────────────────────────────────────────────────

class ReturnVariant(BaseModel):
    absolute_return: float | None
    alpha_vs_asx200: float | None
    alpha_vs_sector: float | None


class ClusterSummary(BaseModel):
    cluster_id: int
    ticker: str
    sector: str | None
    industry: str | None
    market_cap_bucket: str | None
    start_date: date
    end_date: date
    max_disclosure_date: date | None
    director_count: int
    total_value_aud: float
    pct_market_cap: float | None
    status: str
    returns: ReturnVariant | None


class ClusterListResponse(BaseModel):
    clusters: list[ClusterSummary]
    total: int
    page: int
    per_page: int


class TradeDetail(BaseModel):
    director_name: str
    date_of_trade: date
    date_lodged: date | None
    value_aud: float | None
    number_of_securities: int


class ClusterDetail(BaseModel):
    cluster_id: int
    ticker: str
    sector: str | None
    industry: str | None
    market_cap_bucket: str | None
    market_cap_aud: float | None
    start_date: date
    end_date: date
    max_disclosure_date: date | None
    director_count: int
    total_value_aud: float
    pct_market_cap: float | None
    status: str
    returns: dict[str, ReturnVariant]
    trades: list[TradeDetail]


class ClusterStats(BaseModel):
    total_clusters: int
    active_count: int
    maturing_count: int
    historical_count: int
    median_90d_alpha: float | None
    hit_rate_90d: float | None
    sample_size_90d: int
    best_sector: str | None
    filter_applied: dict[str, Any]


# ── Helpers ───────────────────────────────────────────────────────────────────

# Whitelisted sort column mapping → prevents SQL injection from user input
_SORT_COLS = {
    "alpha_vs_asx200": "cr.alpha_vs_asx200",
    "absolute_return":  "cr.absolute_return",
    "start_date":       "c.start_date",
    "director_count":   "c.director_count",
    "pct_market_cap":   "c.pct_market_cap",
}

_ALL_STATUSES = ["active", "maturing", "historical"]


def _float_or_none(v) -> float | None:
    if v is None:
        return None
    f = float(v)
    # PostgreSQL can store NaN/Infinity in NUMERIC columns; treat as missing
    return f if math.isfinite(f) else None


def _build_filters(
    sector: str | None,
    industry: str | None,
    statuses: list[str],
) -> tuple[str, dict]:
    """Return (extra WHERE clauses, params dict)."""
    clauses: list[str] = []
    params: dict = {}
    if sector:
        clauses.append("c.sector = :sector")
        params["sector"] = sector
    if industry:
        clauses.append("c.industry = :industry")
        params["industry"] = industry
    if set(statuses) != set(_ALL_STATUSES):
        # Use expanded IN clause — avoids asyncpg array type-inference issues with ANY()
        if len(statuses) == 1:
            clauses.append("c.status = :status_0")
            params["status_0"] = statuses[0]
        else:
            for i, s in enumerate(statuses):
                params[f"status_{i}"] = s
            placeholders = ",".join(f":status_{i}" for i in range(len(statuses)))
            clauses.append(f"c.status IN ({placeholders})")
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


# ── GET /api/clusters ─────────────────────────────────────────────────────────

@router.get("", response_model=ClusterListResponse)
async def list_clusters(
    status: str | None = Query(None, description="Comma-separated: active,maturing,historical"),
    sector: str | None = Query(None),
    industry: str | None = Query(None),
    horizon: int = Query(90, description="90 or 180"),
    entry_basis: str = Query("disclosure_date", description="trade_date or disclosure_date"),
    sort: str = Query("start_date", description="Field to sort by"),
    order: str = Query("desc", description="asc or desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    statuses = (
        [s.strip() for s in status.split(",") if s.strip() in _ALL_STATUSES]
        if status else _ALL_STATUSES
    )
    if not statuses:
        statuses = _ALL_STATUSES

    if horizon not in (90, 180):
        horizon = 90
    if entry_basis not in ("trade_date", "disclosure_date"):
        entry_basis = "disclosure_date"
    sort_col   = _SORT_COLS.get(sort, _SORT_COLS["start_date"])
    order_dir  = "ASC" if order == "asc" else "DESC"
    extra_where, filter_params = _build_filters(sector, industry, statuses)
    offset = (page - 1) * per_page

    # Count query
    count_sql = text(f"""
        SELECT COUNT(*) FROM clusters c
        WHERE 1=1{extra_where}
    """)
    total = (await db.execute(count_sql, filter_params)).scalar_one()

    # List query
    list_params = {
        **filter_params,
        "horizon": horizon,
        "entry_basis": entry_basis,
        "per_page": per_page,
        "offset": offset,
    }
    list_sql = text(f"""
        SELECT
            c.cluster_id, c.ticker, c.sector, c.industry, c.market_cap_bucket,
            c.start_date, c.end_date, c.max_disclosure_date, c.director_count,
            c.total_value_aud, c.pct_market_cap, c.status,
            cr.absolute_return, cr.alpha_vs_asx200, cr.alpha_vs_sector
        FROM clusters c
        LEFT JOIN cluster_returns cr
               ON cr.cluster_id  = c.cluster_id
              AND cr.horizon_days = :horizon
              AND cr.entry_basis  = :entry_basis
        WHERE 1=1{extra_where}
        ORDER BY {sort_col} {order_dir} NULLS LAST, c.cluster_id DESC
        LIMIT :per_page OFFSET :offset
    """)

    rows = (await db.execute(list_sql, list_params)).all()

    clusters: list[ClusterSummary] = []
    for r in rows:
        abs_ret   = _float_or_none(r.absolute_return)
        alpha_asx = _float_or_none(r.alpha_vs_asx200)
        alpha_sec = _float_or_none(r.alpha_vs_sector)
        has_return = any(v is not None for v in [abs_ret, alpha_asx, alpha_sec])
        clusters.append(ClusterSummary(
            cluster_id=r.cluster_id,
            ticker=r.ticker,
            sector=r.sector,
            industry=r.industry,
            market_cap_bucket=r.market_cap_bucket,
            start_date=r.start_date,
            end_date=r.end_date,
            max_disclosure_date=r.max_disclosure_date,
            director_count=r.director_count,
            total_value_aud=float(r.total_value_aud),
            pct_market_cap=_float_or_none(r.pct_market_cap),
            status=r.status,
            returns=ReturnVariant(
                absolute_return=abs_ret,
                alpha_vs_asx200=alpha_asx,
                alpha_vs_sector=alpha_sec,
            ) if has_return else None,
        ))

    return ClusterListResponse(clusters=clusters, total=total, page=page, per_page=per_page)


# ── GET /api/clusters/stats  (must be before /{cluster_id}) ──────────────────

@router.get("/stats", response_model=ClusterStats)
async def cluster_stats(
    sector: str | None = Query(None),
    industry: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    extra_where, filter_params = _build_filters(sector, industry, _ALL_STATUSES)

    # Status counts
    count_sql = text(f"""
        SELECT
            COUNT(*)                                               AS total,
            SUM(CASE WHEN status = 'active'     THEN 1 ELSE 0 END) AS active_count,
            SUM(CASE WHEN status = 'maturing'   THEN 1 ELSE 0 END) AS maturing_count,
            SUM(CASE WHEN status = 'historical' THEN 1 ELSE 0 END) AS historical_count
        FROM clusters c
        WHERE 1=1{extra_where}
    """)
    cr = (await db.execute(count_sql, filter_params)).one()

    # 90d disclosure alpha values for median + hit rate
    alpha_sql = text(f"""
        SELECT cr.alpha_vs_asx200
        FROM cluster_returns cr
        JOIN clusters c ON c.cluster_id = cr.cluster_id
        WHERE cr.horizon_days = 90
          AND cr.entry_basis  = 'disclosure_date'
          AND cr.alpha_vs_asx200 IS NOT NULL
          {('AND ' + ' AND '.join(
              f"c.{k} = :{k}" for k in ['sector', 'industry'] if filter_params.get(k)
           )) if any(filter_params.get(k) for k in ['sector', 'industry']) else ''}
    """)
    alpha_rows = (await db.execute(alpha_sql, filter_params)).all()
    alpha_vals = [float(r.alpha_vs_asx200) for r in alpha_rows]

    median_alpha = statistics.median(alpha_vals) if alpha_vals else None
    hit_rate = (sum(1 for v in alpha_vals if v > 0) / len(alpha_vals)) if alpha_vals else None
    sample_size = len(alpha_vals)

    # Best sector: highest median 90d alpha, ≥5 data points, from all data (global)
    sector_sql = text("""
        SELECT c.sector,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cr.alpha_vs_asx200) AS med_alpha,
               COUNT(*) AS n
        FROM cluster_returns cr
        JOIN clusters c ON c.cluster_id = cr.cluster_id
        WHERE cr.horizon_days = 90
          AND cr.entry_basis  = 'disclosure_date'
          AND cr.alpha_vs_asx200 IS NOT NULL
          AND c.sector IS NOT NULL
        GROUP BY c.sector
        HAVING COUNT(*) >= 5
        ORDER BY med_alpha DESC
        LIMIT 1
    """)
    best_row = (await db.execute(sector_sql)).first()
    best_sector = best_row.sector if best_row else None

    return ClusterStats(
        total_clusters=cr.total,
        active_count=cr.active_count or 0,
        maturing_count=cr.maturing_count or 0,
        historical_count=cr.historical_count or 0,
        median_90d_alpha=median_alpha,
        hit_rate_90d=hit_rate,
        sample_size_90d=sample_size,
        best_sector=best_sector,
        filter_applied={k: v for k, v in {"sector": sector, "industry": industry}.items() if v},
    )


# ── GET /api/clusters/filters ────────────────────────────────────────────────

@router.get("/filters")
async def cluster_filters(db: AsyncSession = Depends(get_db)):
    """Return distinct non-null sectors and industries for dropdown population."""
    sectors_sql = text("""
        SELECT DISTINCT sector FROM clusters WHERE sector IS NOT NULL ORDER BY sector
    """)
    industries_sql = text("""
        SELECT DISTINCT sector, industry
        FROM clusters
        WHERE sector IS NOT NULL AND industry IS NOT NULL
        ORDER BY sector, industry
    """)
    sectors = [r[0] for r in (await db.execute(sectors_sql)).all()]
    ind_rows = (await db.execute(industries_sql)).all()

    industries_by_sector: dict[str, list[str]] = {}
    for sector, industry in ind_rows:
        industries_by_sector.setdefault(sector, []).append(industry)

    return {
        "available_sectors": sectors,
        "industries_by_sector": industries_by_sector,
    }


# ── GET /api/clusters/{cluster_id} ───────────────────────────────────────────

@router.get("/{cluster_id}", response_model=ClusterDetail)
async def get_cluster(
    cluster_id: int,
    db: AsyncSession = Depends(get_db),
):
    # Cluster + all return variants
    cluster_sql = text("""
        SELECT
            c.cluster_id, c.ticker, c.sector, c.industry, c.market_cap_bucket,
            c.market_cap_aud, c.start_date, c.end_date, c.max_disclosure_date,
            c.director_count, c.total_value_aud, c.pct_market_cap, c.status,
            cr.horizon_days, cr.entry_basis,
            cr.absolute_return, cr.alpha_vs_asx200, cr.alpha_vs_sector
        FROM clusters c
        LEFT JOIN cluster_returns cr ON cr.cluster_id = c.cluster_id
        WHERE c.cluster_id = :cluster_id
        ORDER BY cr.horizon_days, cr.entry_basis
    """)
    rows = (await db.execute(cluster_sql, {"cluster_id": cluster_id})).all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    # Build base from first row (all rows share cluster metadata)
    first = rows[0]
    returns_map: dict[str, ReturnVariant] = {}
    for r in rows:
        if r.horizon_days is not None and r.entry_basis is not None:
            key = f"{r.horizon_days}d_{r.entry_basis}"
            returns_map[key] = ReturnVariant(
                absolute_return=_float_or_none(r.absolute_return),
                alpha_vs_asx200=_float_or_none(r.alpha_vs_asx200),
                alpha_vs_sector=_float_or_none(r.alpha_vs_sector),
            )

    # Underlying trades via junction
    trades_sql = text("""
        SELECT
            d.full_name                              AS director_name,
            t.date_of_trade,
            t.date_lodged,
            ABS(t.quantity)                          AS number_of_securities,
            CASE WHEN t.price_per_share IS NOT NULL
                 THEN ABS(t.quantity)::numeric * t.price_per_share
                 ELSE NULL END                       AS value_aud
        FROM cluster_trades ct
        JOIN trades     t ON t.id = ct.trade_id
        JOIN directors  d ON d.id = t.director_id
        WHERE ct.cluster_id = :cluster_id
        ORDER BY t.date_of_trade
    """)
    trade_rows = (await db.execute(trades_sql, {"cluster_id": cluster_id})).all()

    trades = [
        TradeDetail(
            director_name=tr.director_name,
            date_of_trade=tr.date_of_trade,
            date_lodged=tr.date_lodged,
            value_aud=_float_or_none(tr.value_aud),
            number_of_securities=tr.number_of_securities,
        )
        for tr in trade_rows
    ]

    return ClusterDetail(
        cluster_id=first.cluster_id,
        ticker=first.ticker,
        sector=first.sector,
        industry=first.industry,
        market_cap_bucket=first.market_cap_bucket,
        market_cap_aud=_float_or_none(first.market_cap_aud),
        start_date=first.start_date,
        end_date=first.end_date,
        max_disclosure_date=first.max_disclosure_date,
        director_count=first.director_count,
        total_value_aud=float(first.total_value_aud),
        pct_market_cap=_float_or_none(first.pct_market_cap),
        status=first.status,
        returns=returns_map,
        trades=trades,
    )
