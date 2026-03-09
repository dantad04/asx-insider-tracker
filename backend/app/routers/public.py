"""
Public API endpoints for the ASX Insider Tracker web interface.

Endpoints:
  GET /api/trades/all          All 12,998 trades as JSON
  GET /api/stats               Summary statistics (trades, directors, companies, etc.)
  GET /api/compliance/latest   Recent late filings
"""

from __future__ import annotations

import io
import logging
import math
import time as _time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import httpx
import numpy as np
import pdfplumber
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.company import Company
from app.models.director import Director
from app.models.pending_3y_parse import Pending3YParse, ParseStatus
from app.models.price_snapshot import PriceSnapshot
from app.models.trade import Trade, TradeType

logger = logging.getLogger(__name__)

# Cache: pdf_url -> verified date_of_change (or None if verification failed).
# Persists for the life of the process; avoids re-fetching the same PDF.
_pdf_verification_cache: dict[str, date | None] = {}

# Cache: smart money tickers (result, computed_at). TTL = 10 minutes.
_smart_money_cache: tuple[list, float] | None = None
_SMART_MONEY_TTL = 600

router = APIRouter(prefix="/api", tags=["public"])


# ─────────────────────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────────────────────


class TradeResponse(BaseModel):
    id: str
    director_id: str
    ticker: str
    company_name: str
    director_name: str
    date_of_trade: date
    date_lodged: date | None
    quantity: int | None
    price_per_share: float | None
    trade_type: str
    days_to_report: int | None

    class Config:
        from_attributes = True




COMPLIANCE_WINDOW = 5  # ASX Listing Rule 3.19A.2: 5 business days to lodge


class ComplianceViolationResponse(BaseModel):
    trade_id: str
    ticker: str
    company_name: str
    director_name: str
    date_of_trade: date
    date_lodged: date
    days_late: int    # business days PAST the 5-day deadline (business_days_total - 5)
    severity: str
    verified: bool = False  # True = confirmed against ASX PDF; False = asxinsider estimate only


class DirectorSummary(BaseModel):
    id: str
    name: str
    trade_count: int
    buy_value: float
    sell_value: float


class CompanyProfileResponse(BaseModel):
    ticker: str
    name: str
    sector: str | None
    directors: list[DirectorSummary]
    trades: list[TradeResponse]
    violations: list[ComplianceViolationResponse]


class CompanySummary(BaseModel):
    ticker: str
    name: str
    trade_count: int


class DirectorProfileResponse(BaseModel):
    id: str
    full_name: str
    companies: list[CompanySummary]
    trades: list[TradeResponse]
    total_buy_value: float
    total_sell_value: float
    violations: list[ComplianceViolationResponse]


class StatsResponse(BaseModel):
    total_trades: int
    total_directors: int
    total_companies: int
    late_filings: int
    most_active_director: str | None
    most_active_director_trades: int
    most_traded_company: str | None
    most_traded_company_trades: int
    compliance_rate_pct: float


class SmartMoneyTicker(BaseModel):
    ticker: str
    company_name: str
    director_count: int
    directors: list[str]
    latest_trade_date: date


class SmartMoneyV2Item(BaseModel):
    director_id: str
    director_name: str
    ticker: str
    company_name: str
    score: float | None
    score_all_time: float | None
    score_label: str | None
    total_scored_trades: int
    last_accurate_trade: date | None
    recency_tag: str | None
    latest_trade_date: date


class SectorHeatmapCompany(BaseModel):
    ticker: str
    company_name: str
    buy_count: int
    sell_count: int
    trade_count: int


class SectorHeatmap(BaseModel):
    sector: str
    buy_count: int
    sell_count: int
    buy_value: float
    sell_value: float
    net_sentiment: str  # "bullish", "bearish", "neutral"
    trade_count: int
    top_companies: list[SectorHeatmapCompany]


# ─────────────────────────────────────────────────────────────────────────
# Smart money v2 scoring helpers
# ─────────────────────────────────────────────────────────────────────────

_HALF_LIFE_DAYS = 547  # ~18 months
_K = math.log(2) / _HALF_LIFE_DAYS


def _decay_weight(trade_date: date, today: date) -> float:
    return math.exp(-_K * max(0, (today - trade_date).days))


def _score_label(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 8.0:
        return "Strong"
    if score >= 6.0:
        return "Good"
    if score >= 4.0:
        return "Neutral"
    if score >= 2.0:
        return "Weak"
    return "Poor"


def _recency_str(d: date, ref: date) -> str:
    days = (ref - d).days
    if days < 30:
        return f"{days} day{'s' if days != 1 else ''} ago"
    if days < 365:
        months = round(days / 30)
        return f"{months} month{'s' if months != 1 else ''} ago"
    years = round(days / 365, 1)
    return f"{years} year{'s' if years != 1.0 else ''} ago"


# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────


def business_days(start: date, end: date) -> int:
    """Count business days between two dates."""
    if end <= start:
        return 0
    return int(np.busday_count(start.isoformat(), end.isoformat()))


def classify_severity(bdays: int) -> str:
    """
    Classify severity per ASX Listing Rule 3.19A.2.
    Directors have 5 business days to lodge an Appendix 3Y.
    """
    if bdays <= 5:
        return "compliant"
    elif bdays <= 10:
        return "minor"
    elif bdays <= 20:
        return "moderate"
    return "severe"


# ─────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────


FEED_TRADE_TYPES = (TradeType.ON_MARKET_BUY, TradeType.ON_MARKET_SELL, TradeType.OFF_MARKET)


@router.get("/trades/all", response_model=list[TradeResponse])
async def get_all_trades(db: AsyncSession = Depends(get_db)):
    """
    Return on-market buy and sell trades only.
    Off-market transfers, option exercises and other types are excluded
    from the trade feed (but still tracked for compliance purposes).
    """
    result = await db.execute(
        select(
            Trade.id,
            Trade.director_id,
            Trade.date_of_trade,
            Trade.date_lodged,
            Trade.quantity,
            Trade.price_per_share,
            Trade.trade_type,
            Company.ticker,
            Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.trade_type.in_(FEED_TRADE_TYPES))
        .order_by(Trade.date_lodged.desc())
    )
    rows = result.all()

    return [_build_trade_response(r) for r in rows]


def _build_trade_response(r) -> TradeResponse:
    days_to_report = None
    if r.date_of_trade and r.date_lodged:
        days_to_report = (r.date_lodged - r.date_of_trade).days
    return TradeResponse(
        id=r.id,
        director_id=r.director_id,
        ticker=r.ticker,
        company_name=r.company_name,
        director_name=r.director_name,
        date_of_trade=r.date_of_trade,
        date_lodged=r.date_lodged,
        quantity=abs(r.quantity) if r.quantity is not None else None,
        price_per_share=float(r.price_per_share) if r.price_per_share else None,
        trade_type=r.trade_type,
        days_to_report=days_to_report,
    )


def _build_violations(rows) -> list[ComplianceViolationResponse]:
    violations = []
    for r in rows:
        if not r.date_of_trade or not r.date_lodged:
            continue
        # Only use verified sources — pdf_parser and asxinsider_gpt both have
        # reliable date_lodged values (real ASX announcement timestamps).
        # seed_json is excluded (dateReadable = data export timestamp, not filing date).
        if r.source not in ("pdf_parser", "asxinsider_gpt"):
            continue
        cal = (r.date_lodged - r.date_of_trade).days
        if cal < 0 or cal > 365:
            continue
        bd = business_days(r.date_of_trade, r.date_lodged)
        sev = classify_severity(bd)
        if sev == "compliant":
            continue
        violations.append(ComplianceViolationResponse(
            trade_id=r.id,
            ticker=r.ticker,
            company_name=r.company_name,
            director_name=r.director_name,
            date_of_trade=r.date_of_trade,
            date_lodged=r.date_lodged,
            days_late=bd - COMPLIANCE_WINDOW,
            severity=sev,
        ))
    return violations


@router.get("/stats", response_model=StatsResponse)
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Return summary statistics for the dashboard.
    """
    # Total counts — on-market buys and sells only for the trade feed
    total_trades_result = await db.execute(
        select(func.count(Trade.id)).where(Trade.trade_type.in_(FEED_TRADE_TYPES))
    )
    total_trades = total_trades_result.scalar() or 0

    total_directors_result = await db.execute(select(func.count(func.distinct(Director.id))))
    total_directors = total_directors_result.scalar() or 0

    total_companies_result = await db.execute(select(func.count(func.distinct(Company.id))))
    total_companies = total_companies_result.scalar() or 0

    # Late filings (violations) - PDF-parsed trades only (verified ASX filing dates)
    violations_result = await db.execute(
        select(
            Trade.id,
            Trade.date_of_trade,
            Trade.date_lodged,
        )
        .where(
            Trade.date_of_trade != None,
            Trade.date_lodged != None,
            Trade.source.in_(("pdf_parser", "asxinsider_gpt")),
        )
    )
    violations_data = violations_result.all()

    late_count = 0
    for v in violations_data:
        bd = business_days(v.date_of_trade, v.date_lodged)
        if classify_severity(bd) != "compliant":
            late_count += 1

    # Compliance rate — verified sources only
    verified_trades_result = await db.execute(
        select(func.count(Trade.id)).where(
            Trade.source.in_(("pdf_parser", "asxinsider_gpt"))
        )
    )
    verified_total = verified_trades_result.scalar() or 1

    compliance_rate = 100 * (verified_total - late_count) / max(verified_total, 1)

    # Most active director (on-market trades only)
    director_result = await db.execute(
        select(Director.full_name, func.count(Trade.id).label("trade_count"))
        .join(Trade, Trade.director_id == Director.id)
        .where(Trade.trade_type.in_(FEED_TRADE_TYPES))
        .group_by(Director.id)
        .order_by(func.count(Trade.id).desc())
        .limit(1)
    )
    director_row = director_result.first()
    most_active_director = director_row[0] if director_row else None
    most_active_director_trades = director_row[1] if director_row else 0

    # Most traded company (on-market trades only)
    company_result = await db.execute(
        select(Company.ticker, func.count(Trade.id).label("trade_count"))
        .join(Trade, Trade.company_id == Company.id)
        .where(Trade.trade_type.in_(FEED_TRADE_TYPES))
        .group_by(Company.id)
        .order_by(func.count(Trade.id).desc())
        .limit(1)
    )
    company_row = company_result.first()
    most_traded_company = company_row[0] if company_row else None
    most_traded_company_trades = company_row[1] if company_row else 0

    return StatsResponse(
        total_trades=total_trades,
        total_directors=total_directors,
        total_companies=total_companies,
        late_filings=late_count,
        most_active_director=most_active_director,
        most_active_director_trades=most_active_director_trades,
        most_traded_company=most_traded_company,
        most_traded_company_trades=most_traded_company_trades,
        compliance_rate_pct=round(compliance_rate, 1),
    )


# ─────────────────────────────────────────────────────────────────────────
# Smart Money endpoint
# ─────────────────────────────────────────────────────────────────────────


@router.get("/trades/smart-money", response_model=list[SmartMoneyTicker])
async def get_smart_money_tickers(db: AsyncSession = Depends(get_db)):
    """
    Return tickers where 2+ distinct directors bought on the open market
    within any rolling 7-day window in the last 90 days.
    Result is cached for 10 minutes.
    """
    global _smart_money_cache

    if _smart_money_cache is not None:
        cached_result, cached_at = _smart_money_cache
        if _time.monotonic() - cached_at < _SMART_MONEY_TTL:
            return cached_result

    cutoff = date.today() - timedelta(days=90)

    result = await db.execute(
        select(
            Trade.director_id,
            Trade.date_of_trade,
            Company.ticker,
            Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(
            Trade.trade_type == TradeType.ON_MARKET_BUY,
            Trade.date_of_trade >= cutoff,
        )
        .order_by(Company.ticker, Trade.date_of_trade)
    )
    rows = result.all()

    ticker_rows: dict[str, list] = defaultdict(list)
    for r in rows:
        ticker_rows[r.ticker].append(r)

    smart_money: list[SmartMoneyTicker] = []

    for ticker, trades in ticker_rows.items():
        trades_sorted = sorted(trades, key=lambda r: r.date_of_trade)
        company_name = trades_sorted[0].company_name

        best_window_dirs: dict[str, str] = {}
        latest_date = trades_sorted[-1].date_of_trade

        for i, anchor in enumerate(trades_sorted):
            window_end = anchor.date_of_trade + timedelta(days=6)
            window_dirs: dict[str, str] = {}
            for r in trades_sorted[i:]:
                if r.date_of_trade > window_end:
                    break
                window_dirs[str(r.director_id)] = r.director_name
            if len(window_dirs) >= 2 and len(window_dirs) > len(best_window_dirs):
                best_window_dirs = window_dirs

        if best_window_dirs:
            smart_money.append(SmartMoneyTicker(
                ticker=ticker,
                company_name=company_name,
                director_count=len(best_window_dirs),
                directors=list(best_window_dirs.values()),
                latest_trade_date=latest_date,
            ))

    smart_money.sort(key=lambda s: s.latest_trade_date, reverse=True)
    _smart_money_cache = (smart_money, _time.monotonic())
    return smart_money


# ─────────────────────────────────────────────────────────────────────────
# Smart Money v2 endpoint — price-accuracy scored
# ─────────────────────────────────────────────────────────────────────────


@router.get("/trades/smart-money/v2", response_model=list[SmartMoneyV2Item])
async def get_smart_money_v2(
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    db: AsyncSession = Depends(get_db),
):
    """
    Score each director-company pair by trade accuracy:
    - Buy trade = accurate if price rose within 90 days
    - Sell trade = accurate if price fell within 90 days
    - Weighted by recency (exponential decay, 18-month half-life)
    - Score 0–10; optional date-range filter returns period + all-time score
    Requires price data in price_snapshots table.
    """
    today = date.today()
    three_years_ago = today - timedelta(days=365 * 3)

    trade_result = await db.execute(
        select(
            Trade.director_id,
            Trade.date_of_trade,
            Trade.price_per_share,
            Trade.trade_type,
            Company.ticker,
            Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(
            Trade.trade_type.in_([TradeType.ON_MARKET_BUY, TradeType.ON_MARKET_SELL]),
            Trade.price_per_share.is_not(None),
            Trade.quantity.is_not(None),
            Trade.date_of_trade >= three_years_ago,
        )
        .order_by(Trade.date_of_trade)
    )
    all_trades = trade_result.all()

    if not all_trades:
        return []

    tickers = list({r.ticker for r in all_trades})
    price_result = await db.execute(
        select(PriceSnapshot.ticker, PriceSnapshot.date, PriceSnapshot.close)
        .where(PriceSnapshot.ticker.in_(tickers))
        .order_by(PriceSnapshot.ticker, PriceSnapshot.date)
    )

    prices: dict[str, list] = defaultdict(list)
    for p in price_result.all():
        prices[p.ticker].append((p.date, float(p.close)))

    def get_future_close(ticker: str, trade_date: date) -> float | None:
        target = trade_date + timedelta(days=90)
        best: float | None = None
        best_gap = 999
        for d, c in prices.get(ticker, []):
            gap = abs((d - target).days)
            if gap <= 10 and gap < best_gap:
                best, best_gap = c, gap
        return best

    def compute_score(trade_list) -> tuple[float | None, int, date | None]:
        weighted_acc = 0.0
        weighted_total = 0.0
        last_acc: date | None = None
        scored = 0
        for t in trade_list:
            future = get_future_close(t.ticker, t.date_of_trade)
            if future is None:
                continue
            w = _decay_weight(t.date_of_trade, today)
            is_buy = t.trade_type == TradeType.ON_MARKET_BUY
            accurate = (is_buy and future > float(t.price_per_share)) or \
                       (not is_buy and future < float(t.price_per_share))
            weighted_total += w
            if accurate:
                weighted_acc += w
                if last_acc is None or t.date_of_trade > last_acc:
                    last_acc = t.date_of_trade
            scored += 1
        if weighted_total == 0:
            return None, scored, last_acc
        return round(weighted_acc / weighted_total * 10, 1), scored, last_acc

    groups: dict[tuple, list] = defaultdict(list)
    for r in all_trades:
        groups[(str(r.director_id), r.ticker)].append(r)

    results = []
    for (director_id, ticker), group in groups.items():
        rep = group[0]

        score_all, count_all, last_acc_all = compute_score(group)

        if from_date or to_date:
            filtered = [
                t for t in group
                if (from_date is None or t.date_of_trade >= from_date)
                and (to_date is None or t.date_of_trade <= to_date)
            ]
            score_period, count_period, last_acc_period = compute_score(filtered)
        else:
            score_period, count_period, last_acc_period = score_all, count_all, last_acc_all

        if count_all == 0:
            continue

        display_score = score_period if score_period is not None else score_all
        last_acc = last_acc_period or last_acc_all

        results.append(SmartMoneyV2Item(
            director_id=director_id,
            director_name=rep.director_name,
            ticker=rep.ticker,
            company_name=rep.company_name,
            score=score_period,
            score_all_time=score_all,
            score_label=_score_label(display_score),
            total_scored_trades=count_period if (from_date or to_date) else count_all,
            last_accurate_trade=last_acc,
            recency_tag=_recency_str(last_acc, today) if last_acc else None,
            latest_trade_date=max(t.date_of_trade for t in group),
        ))

    results.sort(key=lambda r: (r.score or -1), reverse=True)
    return results


# ─────────────────────────────────────────────────────────────────────────
# Sector heatmap endpoint
# ─────────────────────────────────────────────────────────────────────────


@router.get("/stats/sector-heatmap", response_model=list[SectorHeatmap])
async def get_sector_heatmap(db: AsyncSession = Depends(get_db)):
    """
    Return sector-level buy/sell activity for the last 90 days.
    Groups on-market trades by company sector and calculates sentiment.
    """
    cutoff = date.today() - timedelta(days=90)

    buy_count_expr = func.count(
        case((Trade.trade_type == TradeType.ON_MARKET_BUY, Trade.id), else_=None)
    )
    sell_count_expr = func.count(
        case((Trade.trade_type == TradeType.ON_MARKET_SELL, Trade.id), else_=None)
    )
    buy_value_expr = func.coalesce(
        func.sum(
            case(
                (
                    Trade.trade_type == TradeType.ON_MARKET_BUY,
                    func.abs(Trade.quantity) * Trade.price_per_share,
                ),
                else_=None,
            )
        ),
        0,
    )
    sell_value_expr = func.coalesce(
        func.sum(
            case(
                (
                    Trade.trade_type == TradeType.ON_MARKET_SELL,
                    func.abs(Trade.quantity) * Trade.price_per_share,
                ),
                else_=None,
            )
        ),
        0,
    )

    _base_where = [
        Trade.trade_type.in_((TradeType.ON_MARKET_BUY, TradeType.ON_MARKET_SELL)),
        Trade.date_of_trade >= cutoff,
        Company.sector.is_not(None),
        Trade.price_per_share.is_not(None),
        Trade.quantity.is_not(None),
    ]

    result = await db.execute(
        select(
            Company.sector,
            func.count(Trade.id).label("trade_count"),
            buy_count_expr.label("buy_count"),
            sell_count_expr.label("sell_count"),
            buy_value_expr.label("buy_value"),
            sell_value_expr.label("sell_value"),
        )
        .join(Company, Company.id == Trade.company_id)
        .where(*_base_where)
        .group_by(Company.sector)
        .order_by(func.count(Trade.id).desc())
    )
    rows = result.all()

    # Company-level breakdown for modal detail
    company_result = await db.execute(
        select(
            Company.sector,
            Company.ticker,
            Company.name.label("company_name"),
            func.count(Trade.id).label("trade_count"),
            buy_count_expr.label("buy_count"),
            sell_count_expr.label("sell_count"),
        )
        .join(Company, Company.id == Trade.company_id)
        .where(*_base_where)
        .group_by(Company.sector, Company.ticker, Company.name)
        .order_by(Company.sector, func.count(Trade.id).desc())
    )
    sector_companies: dict[str, list[SectorHeatmapCompany]] = defaultdict(list)
    for cr in company_result.all():
        sector_companies[cr.sector].append(SectorHeatmapCompany(
            ticker=cr.ticker,
            company_name=cr.company_name,
            buy_count=cr.buy_count or 0,
            sell_count=cr.sell_count or 0,
            trade_count=cr.trade_count or 0,
        ))

    output = []
    for r in rows:
        buy_v = float(r.buy_value or 0)
        sell_v = float(r.sell_value or 0)
        if buy_v > sell_v * 1.5:
            sentiment = "bullish"
        elif sell_v > buy_v * 1.5:
            sentiment = "bearish"
        else:
            sentiment = "neutral"
        output.append(SectorHeatmap(
            sector=r.sector,
            buy_count=r.buy_count or 0,
            sell_count=r.sell_count or 0,
            buy_value=round(buy_v, 2),
            sell_value=round(sell_v, 2),
            net_sentiment=sentiment,
            trade_count=r.trade_count or 0,
            top_companies=sector_companies.get(r.sector, [])[:5],
        ))

    return output


async def _get_pdf_url_for_filing(
    db: AsyncSession, ticker: str, date_lodged: date
) -> str | None:
    """Look up the PDF URL from pending_3y_parses for a given filing date ±1 day."""
    result = await db.execute(
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


async def _verify_trade_date_from_pdf(pdf_url: str) -> date | None:
    """
    Fetch PDF and extract the true date_of_change, with date-swap correction.

    Appendix 3Y fields:
      date_of_change     = the actual trade date (should be NEWER)
      date_of_last_notice = the previous filing date (should be OLDER)

    If date_of_change < date_of_last_notice the parser got confused and
    extracted them in the wrong order — swap and return the correct one.

    Returns None if the URL is expired/unreachable or dates can't be parsed.
    Results are cached in _pdf_verification_cache.
    """
    if pdf_url in _pdf_verification_cache:
        return _pdf_verification_cache[pdf_url]

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(pdf_url)
            response.raise_for_status()

        text = ""
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""

        from app.scripts.parse_3y_pdfs import parse_date_of_change, parse_date_of_last_notice
        doc = parse_date_of_change(text)
        last = parse_date_of_last_notice(text)

        if doc and last:
            if doc < last:
                # Parser swapped the two dates — correct it
                logger.warning(
                    f"Date swap detected in PDF: date_of_change={doc} < "
                    f"date_of_last_notice={last}. Using {last} as trade date."
                )
                verified_date = last
            else:
                verified_date = doc
        else:
            verified_date = doc  # None if not found

        _pdf_verification_cache[pdf_url] = verified_date
        logger.info(f"PDF verified: {pdf_url} → trade_date={verified_date}")
        return verified_date

    except Exception as e:
        logger.warning(f"PDF verification failed ({pdf_url}): {e}")
        _pdf_verification_cache[pdf_url] = None
        return None


@router.get("/compliance/violations", response_model=list[ComplianceViolationResponse])
async def get_compliance_violations(db: AsyncSession = Depends(get_db)):
    """
    Return verified late filings. All sources treated equally.

    Pass 1 — flag potential violations:
      Group trades by filing (ticker, director_id, date_lodged).
      Use the latest date_of_trade across ALL sources as the initial trade date.
      Flag anything that looks MINOR, MODERATE, or SEVERE.

    Pass 2 — verify MODERATE/SEVERE violations against the original PDF:
      Fetch PDF from pending_3y_parses.pdf_url (if available).
      Extract BOTH date_of_change and date_of_last_notice.
      Sanity check: date_of_change must be AFTER date_of_last_notice.
        If not → parser swapped them; use date_of_last_notice as the real trade date.
      Recalculate with verified date:
        - Still violating → keep, verified=True, use corrected date
        - Actually compliant → discard (false alarm logged)
        - PDF unavailable → keep, verified=False (unverified estimate)

    MINOR violations skip Pass 2 (small gaps are very unlikely to be date errors).
    """
    result = await db.execute(
        select(
            Trade.id,
            Trade.date_of_trade,
            Trade.date_lodged,
            Trade.source,
            Company.ticker,
            Company.name.label("company_name"),
            Director.id.label("director_id"),
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
    rows = result.all()

    # ── Pass 1: group by filing, flag potential violations ──────────────────
    filings: dict[tuple, list] = defaultdict(list)
    for r in rows:
        filings[(r.ticker, r.director_id, str(r.date_lodged))].append(r)

    violations = []

    for (ticker, director_id, _), records in filings.items():
        rep = records[0]
        date_lodged = rep.date_lodged

        # Use the latest date_of_trade from any source as the initial estimate
        valid = [r for r in records if (date_lodged - r.date_of_trade).days in range(0, 366)]
        if not valid:
            continue
        best = max(valid, key=lambda r: r.date_of_trade)
        trade_date = best.date_of_trade

        bd = business_days(trade_date, date_lodged)
        severity = classify_severity(bd)
        if severity == "compliant":
            continue

        # ── Pass 2: verify MODERATE/SEVERE against original PDF ────────────
        if severity in ("moderate", "severe"):
            pdf_url = await _get_pdf_url_for_filing(db, ticker, date_lodged)
            if pdf_url:
                verified_date = await _verify_trade_date_from_pdf(pdf_url)
                if verified_date is not None:
                    v_cal = (date_lodged - verified_date).days
                    if v_cal < 0 or v_cal > 365:
                        continue  # Nonsensical verified date — skip
                    bd = business_days(verified_date, date_lodged)
                    severity = classify_severity(bd)
                    if severity == "compliant":
                        logger.info(
                            f"False violation discarded: {ticker} {rep.director_name} "
                            f"stored={trade_date} verified={verified_date}"
                        )
                        continue
                    violations.append(ComplianceViolationResponse(
                        trade_id=best.id,
                        ticker=rep.ticker,
                        company_name=rep.company_name,
                        director_name=rep.director_name,
                        date_of_trade=verified_date,
                        date_lodged=date_lodged,
                        days_late=bd - COMPLIANCE_WINDOW,
                        severity=severity,
                        verified=True,
                    ))
                    continue
            # PDF unavailable — include as unverified, only if SEVERE
            # (MODERATE unverified is too noisy without confirmation)
            if severity == "severe":
                violations.append(ComplianceViolationResponse(
                    trade_id=best.id,
                    ticker=rep.ticker,
                    company_name=rep.company_name,
                    director_name=rep.director_name,
                    date_of_trade=trade_date,
                    date_lodged=date_lodged,
                    days_late=bd - COMPLIANCE_WINDOW,
                    severity=severity,
                    verified=False,
                ))
        else:
            # MINOR — include as-is, small gaps are genuine
            violations.append(ComplianceViolationResponse(
                trade_id=best.id,
                ticker=rep.ticker,
                company_name=rep.company_name,
                director_name=rep.director_name,
                date_of_trade=trade_date,
                date_lodged=date_lodged,
                days_late=bd - COMPLIANCE_WINDOW,
                severity=severity,
                verified=False,
            ))

    violations.sort(key=lambda v: v.date_lodged, reverse=True)
    return violations


# ─────────────────────────────────────────────────────────────────────────
# Company profile
# ─────────────────────────────────────────────────────────────────────────


@router.get("/company/{ticker}", response_model=CompanyProfileResponse)
async def get_company_profile(ticker: str, db: AsyncSession = Depends(get_db)):
    """Full profile for a company: directors, all trades, compliance violations."""
    from fastapi import HTTPException

    ticker = ticker.upper()

    company_result = await db.execute(select(Company).where(Company.ticker == ticker))
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found")

    # All FEED trades for this company
    rows = (await db.execute(
        select(
            Trade.id, Trade.director_id, Trade.date_of_trade, Trade.date_lodged,
            Trade.quantity, Trade.price_per_share, Trade.trade_type,
            Company.ticker, Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Company.ticker == ticker, Trade.trade_type.in_(FEED_TRADE_TYPES))
        .order_by(Trade.date_of_trade.desc())
    )).all()

    trades = [_build_trade_response(r) for r in rows]

    # Director summaries
    director_map: dict[str, dict] = {}
    for t in trades:
        if t.director_id not in director_map:
            director_map[t.director_id] = {
                "id": t.director_id, "name": t.director_name,
                "trade_count": 0, "buy_value": 0.0, "sell_value": 0.0,
            }
        d = director_map[t.director_id]
        d["trade_count"] += 1
        val = abs(t.quantity or 0) * abs(t.price_per_share or 0)
        if t.trade_type == "on_market_buy":
            d["buy_value"] += val
        elif t.trade_type == "on_market_sell":
            d["sell_value"] += val

    directors = [
        DirectorSummary(**v)
        for v in sorted(director_map.values(), key=lambda x: -x["trade_count"])
    ]

    # Violations — all trade types for this company
    all_rows = (await db.execute(
        select(
            Trade.id, Trade.date_of_trade, Trade.date_lodged, Trade.source,
            Company.ticker, Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Company.ticker == ticker)
        .order_by(Trade.date_lodged.desc())
    )).all()

    return CompanyProfileResponse(
        ticker=company.ticker,
        name=company.name,
        sector=company.sector,
        directors=directors,
        trades=trades,
        violations=_build_violations(all_rows),
    )


# ─────────────────────────────────────────────────────────────────────────
# Director profile
# ─────────────────────────────────────────────────────────────────────────


@router.get("/director/{director_id}", response_model=DirectorProfileResponse)
async def get_director_profile(director_id: str, db: AsyncSession = Depends(get_db)):
    """Full profile for a director: companies, all trades, buy/sell totals, violations."""
    from fastapi import HTTPException

    director_result = await db.execute(select(Director).where(Director.id == director_id))
    director = director_result.scalar_one_or_none()
    if not director:
        raise HTTPException(status_code=404, detail="Director not found")

    # All FEED trades for this director
    rows = (await db.execute(
        select(
            Trade.id, Trade.director_id, Trade.date_of_trade, Trade.date_lodged,
            Trade.quantity, Trade.price_per_share, Trade.trade_type,
            Company.ticker, Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.director_id == director_id, Trade.trade_type.in_(FEED_TRADE_TYPES))
        .order_by(Trade.date_of_trade.desc())
    )).all()

    trades = [_build_trade_response(r) for r in rows]

    # Company summaries
    company_map: dict[str, dict] = {}
    for t in trades:
        if t.ticker not in company_map:
            company_map[t.ticker] = {"ticker": t.ticker, "name": t.company_name, "trade_count": 0}
        company_map[t.ticker]["trade_count"] += 1

    companies = [
        CompanySummary(**v)
        for v in sorted(company_map.values(), key=lambda x: -x["trade_count"])
    ]

    # Buy/sell totals (use abs to handle negative quantities from seed data)
    total_buy = sum(
        abs(t.quantity or 0) * abs(t.price_per_share or 0)
        for t in trades if t.trade_type == "on_market_buy"
    )
    total_sell = sum(
        abs(t.quantity or 0) * abs(t.price_per_share or 0)
        for t in trades if t.trade_type == "on_market_sell"
    )

    # Violations — all trade types for this director
    all_rows = (await db.execute(
        select(
            Trade.id, Trade.date_of_trade, Trade.date_lodged, Trade.source,
            Company.ticker, Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.director_id == director_id)
        .order_by(Trade.date_lodged.desc())
    )).all()

    return DirectorProfileResponse(
        id=director.id,
        full_name=director.full_name,
        companies=companies,
        trades=trades,
        total_buy_value=round(total_buy, 2),
        total_sell_value=round(total_sell, 2),
        violations=_build_violations(all_rows),
    )


# ─────────────────────────────────────────────────────────────────────────
# Manual sync trigger
# ─────────────────────────────────────────────────────────────────────────


class SyncResponse(BaseModel):
    status: str
    message: str
    inserted: int = 0
    upgraded: int = 0
    skipped: int = 0
    errors: int = 0


@router.post("/admin/sync", response_model=SyncResponse)
async def trigger_sync():
    """
    Manually trigger a sync from asxinsider.com.au.
    Returns sync statistics.
    """
    import logging
    from app.scripts.sync_asxinsider import main as sync_main

    logger = logging.getLogger(__name__)
    logger.info("Manual sync triggered via API")

    global _smart_money_cache
    _smart_money_cache = None  # Invalidate so next request recomputes

    try:
        stats = await sync_main()
        if stats:
            return SyncResponse(
                status="success",
                message=f"Sync completed: {stats.get('inserted', 0)} inserted, {stats.get('upgraded', 0)} upgraded",
                inserted=stats.get("inserted", 0),
                upgraded=stats.get("upgraded", 0),
                skipped=stats.get("skipped_already_good", 0),
                errors=stats.get("errors", 0),
            )
        else:
            return SyncResponse(
                status="error",
                message="Sync returned no stats. Check ASXINSIDER_URL environment variable.",
            )
    except Exception as e:
        logger.error(f"Manual sync failed: {e}", exc_info=True)
        return SyncResponse(
            status="error",
            message=f"Sync failed: {str(e)}",
        )


# ─────────────────────────────────────────────────────────────────────────
# Verify all violations (one-time migration)
# ─────────────────────────────────────────────────────────────────────────


class VerifyViolationsResponse(BaseModel):
    status: str
    message: str


@router.post("/admin/verify-violations", response_model=VerifyViolationsResponse)
async def trigger_verify_violations():
    """
    Manually trigger the verify_all_violations migration script.

    This one-time script:
    - Finds all trades with (date_lodged - date_of_trade) >= 7 calendar days
    - Fetches original PDFs and extracts verified dates
    - Updates Trade.date_of_trade where verified date differs
    - Batch commits every 50 records

    Warning: This is a long-running operation (fetches ~200+ PDFs).
    """
    import asyncio
    from app.scripts.verify_all_violations import main as verify_main

    logger.info("Verify all violations triggered via API")

    try:
        await verify_main(dry_run=False, limit=None)
        return VerifyViolationsResponse(
            status="success",
            message="Verification complete. Check logs for summary.",
        )
    except Exception as e:
        logger.error(f"Verify violations failed: {e}", exc_info=True)
        return VerifyViolationsResponse(
            status="error",
            message=f"Verification failed: {str(e)}",
        )
