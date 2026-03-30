"""
Portfolio API endpoints for the Smart Money Portfolio.

GET  /api/portfolio/summary     Overall P&L, cash, positions count
GET  /api/portfolio/positions   All positions (open + closed) with current value
GET  /api/portfolio/performance Equity curve data points for charting
POST /api/portfolio/run         Trigger a live portfolio update
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import date, timedelta

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.company import Company
from app.models.director import Director
from app.models.portfolio import PortfolioPosition
from app.models.price_snapshot import PriceSnapshot
from app.models.trade import Trade, TradeType
from app.services.price_fetcher import get_latest_price

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])

STARTING_CAPITAL = 100_000.0
POSITION_SIZE = 10_000.0
MAX_POSITIONS = 10


# ─────────────────────────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────────────────────────


class PositionDetail(BaseModel):
    id: str
    ticker: str
    company_name: str
    signal_date: date
    signal_directors: list[str]
    entry_date: date
    entry_price: float
    quantity: int
    cost_basis: float
    exit_date: date | None
    exit_price: float | None
    exit_reason: str | None
    status: str
    pnl_aud: float | None
    pnl_pct: float | None
    # Live fields (computed)
    current_price: float | None
    current_price_date: date | None
    current_value: float | None
    unrealised_pnl_aud: float | None
    unrealised_pnl_pct: float | None
    days_held: int | None
    exit_due_date: date | None  # entry_date + 90 days


class PortfolioSummary(BaseModel):
    starting_capital: float
    portfolio_start_date: date | None
    total_positions: int
    open_positions: int
    closed_positions: int
    realised_pnl_aud: float
    realised_pnl_pct: float
    unrealised_pnl_aud: float
    open_cost_basis: float
    current_open_value: float
    total_portfolio_value: float
    total_return_aud: float
    total_return_pct: float
    cash_available: float
    win_rate_pct: float | None  # % of closed positions that were profitable
    avg_win_aud: float | None
    avg_loss_aud: float | None
    best_trade: dict | None
    worst_trade: dict | None


class EquityPoint(BaseModel):
    date: date
    portfolio_value: float
    realised_pnl: float
    open_positions: int


class RunResponse(BaseModel):
    status: str
    positions: int
    open: int
    closed: int


class InvestNowPosition(BaseModel):
    ticker: str
    company_name: str
    directors: list[str]
    original_signal_date: date   # when the smart money signal actually fired
    entry_price: float
    quantity: int
    cost_basis: float


class InvestNowResponse(BaseModel):
    positions_opened: int
    total_invested: float
    positions: list[InvestNowPosition]
    skipped: list[str]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _parse_directors(raw: str) -> list[str]:
    try:
        return json.loads(raw)
    except Exception:
        return [raw]


async def _enrich_position(db: AsyncSession, pos: PortfolioPosition) -> PositionDetail:
    """Add live price data to a position row."""
    current_price, current_price_date = None, None
    current_value = None
    unrealised_pnl_aud = None
    unrealised_pnl_pct = None

    if pos.status == "open":
        current_price, current_price_date = await get_latest_price(db, pos.ticker)
        if current_price:
            current_value = round(current_price * pos.quantity, 2)
            unrealised_pnl_aud = round(
                (current_price - float(pos.entry_price)) * pos.quantity, 2
            )
            unrealised_pnl_pct = round(
                (current_price - float(pos.entry_price)) / float(pos.entry_price) * 100, 4
            )
    elif pos.status == "closed" and pos.exit_price:
        current_price = float(pos.exit_price)
        current_price_date = pos.exit_date
        current_value = round(float(pos.exit_price) * pos.quantity, 2)

    days_held = None
    if pos.status == "open":
        days_held = (date.today() - pos.entry_date).days
    elif pos.exit_date:
        days_held = (pos.exit_date - pos.entry_date).days

    return PositionDetail(
        id=pos.id,
        ticker=pos.ticker,
        company_name=pos.company_name,
        signal_date=pos.signal_date,
        signal_directors=_parse_directors(pos.signal_directors),
        entry_date=pos.entry_date,
        entry_price=float(pos.entry_price),
        quantity=pos.quantity,
        cost_basis=float(pos.cost_basis),
        exit_date=pos.exit_date,
        exit_price=float(pos.exit_price) if pos.exit_price else None,
        exit_reason=pos.exit_reason,
        status=pos.status,
        pnl_aud=float(pos.pnl_aud) if pos.pnl_aud is not None else None,
        pnl_pct=float(pos.pnl_pct) if pos.pnl_pct is not None else None,
        current_price=current_price,
        current_price_date=current_price_date,
        current_value=current_value,
        unrealised_pnl_aud=unrealised_pnl_aud,
        unrealised_pnl_pct=unrealised_pnl_pct,
        days_held=days_held,
        exit_due_date=pos.entry_date + timedelta(days=90),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/positions", response_model=list[PositionDetail])
async def get_positions(db: AsyncSession = Depends(get_db)):
    """Return all portfolio positions (open first, then closed by exit_date desc)."""
    result = await db.execute(
        select(PortfolioPosition).order_by(
            PortfolioPosition.status.asc(),  # "closed" < "open" alphabetically → open first
            PortfolioPosition.entry_date.desc(),
        )
    )
    positions = result.scalars().all()
    # "open" > "closed" alphabetically, so sort open first manually
    positions = sorted(
        positions,
        key=lambda p: (0 if p.status == "open" else 1, -(p.entry_date.toordinal())),
    )
    return [await _enrich_position(db, p) for p in positions]


@router.get("/summary", response_model=PortfolioSummary)
async def get_summary(db: AsyncSession = Depends(get_db)):
    """Return portfolio summary statistics."""
    result = await db.execute(select(PortfolioPosition))
    positions = result.scalars().all()

    # Determine live portfolio start date (earliest signal on record)
    start_result = await db.execute(select(func.min(PortfolioPosition.signal_date)))
    portfolio_start_date: date | None = start_result.scalar()

    if not positions:
        return PortfolioSummary(
            starting_capital=STARTING_CAPITAL,
            portfolio_start_date=portfolio_start_date,
            total_positions=0,
            open_positions=0,
            closed_positions=0,
            realised_pnl_aud=0.0,
            realised_pnl_pct=0.0,
            unrealised_pnl_aud=0.0,
            open_cost_basis=0.0,
            current_open_value=0.0,
            total_portfolio_value=STARTING_CAPITAL,
            total_return_aud=0.0,
            total_return_pct=0.0,
            cash_available=STARTING_CAPITAL,
            win_rate_pct=None,
            avg_win_aud=None,
            avg_loss_aud=None,
            best_trade=None,
            worst_trade=None,
        )

    open_pos = [p for p in positions if p.status == "open"]
    closed_pos = [p for p in positions if p.status == "closed"]

    # Realised P&L from closed positions
    realised_pnl = sum(float(p.pnl_aud or 0) for p in closed_pos)
    total_closed_cost = sum(float(p.cost_basis) for p in closed_pos)

    # Open positions: current value from latest prices
    open_cost_basis = sum(float(p.cost_basis) for p in open_pos)
    current_open_value = 0.0
    unrealised_pnl = 0.0

    for p in open_pos:
        price, _ = await get_latest_price(db, p.ticker)
        if price:
            val = price * p.quantity
            current_open_value += val
            unrealised_pnl += val - float(p.cost_basis)
        else:
            current_open_value += float(p.cost_basis)  # assume flat if no price

    # Cash = starting - all money deployed + money returned from closed trades
    total_deployed = open_cost_basis + total_closed_cost
    total_returned = sum(
        float(p.exit_price or 0) * p.quantity for p in closed_pos if p.exit_price
    )
    cash_available = STARTING_CAPITAL - total_deployed + total_returned

    total_portfolio_value = cash_available + current_open_value
    total_return_aud = total_portfolio_value - STARTING_CAPITAL
    total_return_pct = round(total_return_aud / STARTING_CAPITAL * 100, 4)

    # Win rate
    wins = [p for p in closed_pos if (p.pnl_aud or 0) > 0]
    losses = [p for p in closed_pos if (p.pnl_aud or 0) <= 0]
    win_rate = round(len(wins) / len(closed_pos) * 100, 2) if closed_pos else None
    avg_win = round(sum(float(p.pnl_aud) for p in wins) / len(wins), 2) if wins else None
    avg_loss = round(sum(float(p.pnl_aud) for p in losses) / len(losses), 2) if losses else None

    # Best / worst
    best = max(closed_pos, key=lambda p: float(p.pnl_aud or 0), default=None)
    worst = min(closed_pos, key=lambda p: float(p.pnl_aud or 0), default=None)

    def _pos_summary(p: PortfolioPosition | None) -> dict | None:
        if not p:
            return None
        return {
            "ticker": p.ticker,
            "company_name": p.company_name,
            "pnl_aud": float(p.pnl_aud or 0),
            "pnl_pct": float(p.pnl_pct or 0),
            "entry_date": p.entry_date.isoformat(),
            "exit_date": p.exit_date.isoformat() if p.exit_date else None,
        }

    return PortfolioSummary(
        starting_capital=STARTING_CAPITAL,
        portfolio_start_date=portfolio_start_date,
        total_positions=len(positions),
        open_positions=len(open_pos),
        closed_positions=len(closed_pos),
        realised_pnl_aud=round(realised_pnl, 2),
        realised_pnl_pct=round(realised_pnl / STARTING_CAPITAL * 100, 4) if STARTING_CAPITAL else 0,
        unrealised_pnl_aud=round(unrealised_pnl, 2),
        open_cost_basis=round(open_cost_basis, 2),
        current_open_value=round(current_open_value, 2),
        total_portfolio_value=round(total_portfolio_value, 2),
        total_return_aud=round(total_return_aud, 2),
        total_return_pct=total_return_pct,
        cash_available=round(cash_available, 2),
        win_rate_pct=win_rate,
        avg_win_aud=avg_win,
        avg_loss_aud=avg_loss,
        best_trade=_pos_summary(best),
        worst_trade=_pos_summary(worst),
    )


@router.get("/performance", response_model=list[EquityPoint])
async def get_performance(db: AsyncSession = Depends(get_db)):
    """
    Return an equity curve: for each date a position closed, calculate
    the cumulative portfolio value (starting_capital + all realised P&L
    up to that date).
    """
    result = await db.execute(
        select(PortfolioPosition)
        .where(PortfolioPosition.status == "closed")
        .order_by(PortfolioPosition.exit_date)
    )
    closed = result.scalars().all()

    if not closed:
        return [EquityPoint(
            date=date.today(),
            portfolio_value=STARTING_CAPITAL,
            realised_pnl=0.0,
            open_positions=0,
        )]

    # Group by exit_date
    by_date: dict[date, list] = defaultdict(list)
    for p in closed:
        if p.exit_date:
            by_date[p.exit_date].append(p)

    # Build cumulative curve
    curve: list[EquityPoint] = [
        EquityPoint(
            date=min(by_date.keys()) - timedelta(days=1),
            portfolio_value=STARTING_CAPITAL,
            realised_pnl=0.0,
            open_positions=0,
        )
    ]
    cumulative_pnl = 0.0

    for exit_date in sorted(by_date.keys()):
        day_pnl = sum(float(p.pnl_aud or 0) for p in by_date[exit_date])
        cumulative_pnl += day_pnl
        # Count how many positions were open on that date
        open_count = sum(
            1 for p in closed
            if p.entry_date <= exit_date and (p.exit_date is None or p.exit_date > exit_date)
        )
        curve.append(EquityPoint(
            date=exit_date,
            portfolio_value=round(STARTING_CAPITAL + cumulative_pnl, 2),
            realised_pnl=round(cumulative_pnl, 2),
            open_positions=open_count,
        ))

    return curve


@router.post("/run", response_model=RunResponse)
async def run_portfolio():
    """Trigger a live portfolio update (process new signals, close expired positions)."""
    from app.scripts.run_portfolio import run as _run
    try:
        stats = await _run()
        return RunResponse(
            status="ok",
            positions=stats.get("positions", 0),
            open=stats.get("open", 0),
            closed=stats.get("closed", 0),
        )
    except Exception as exc:
        logger.exception("Portfolio run failed")
        return RunResponse(status=f"error: {exc}", positions=0, open=0, closed=0)


@router.post("/invest-now", response_model=InvestNowResponse)
async def invest_now(db: AsyncSession = Depends(get_db)):
    """
    Seed the portfolio with up to 5 positions (~$50,000) based on the most
    recent Smart Money signals from the last 90 days.

    Uses today's latest price from price_snapshots as entry price.
    Respects existing portfolio limits (max 10 positions, $10k cash reserve).
    Idempotent: running twice on the same day for the same ticker is a no-op.
    """
    today = date.today()
    cutoff = today - timedelta(days=90)
    INVEST_BUDGET = 50_000.0
    max_new = int(INVEST_BUDGET / POSITION_SIZE)   # 5 positions

    # Load already-held tickers so we don't double-buy
    held_result = await db.execute(
        select(PortfolioPosition.ticker).where(PortfolioPosition.status == "open")
    )
    held_tickers: set[str] = {r[0] for r in held_result.all()}
    held_count = len(held_tickers)

    # Query recent on-market buys
    trade_result = await db.execute(
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
    rows = trade_result.all()

    # Detect smart money signals (same 7-day / 2-director algorithm)
    ticker_rows: dict[str, list] = defaultdict(list)
    for r in rows:
        ticker_rows[r.ticker].append(r)

    candidates = []
    for ticker, trades in ticker_rows.items():
        if ticker in held_tickers:
            continue
        trades_sorted = sorted(trades, key=lambda r: r.date_of_trade)
        company_name = trades_sorted[0].company_name
        latest_date = trades_sorted[-1].date_of_trade

        best_dirs: dict[str, str] = {}
        best_signal_date: date | None = None

        for i, anchor in enumerate(trades_sorted):
            window_end = anchor.date_of_trade + timedelta(days=6)
            window_dirs: dict[str, str] = {}
            for r in trades_sorted[i:]:
                if r.date_of_trade > window_end:
                    break
                window_dirs[str(r.director_id)] = r.director_name
            if len(window_dirs) >= 2 and len(window_dirs) >= len(best_dirs):
                best_dirs = window_dirs
                best_signal_date = max(
                    r.date_of_trade for r in trades_sorted[i:]
                    if r.date_of_trade <= window_end
                )

        if best_dirs and best_signal_date:
            candidates.append({
                "ticker": ticker,
                "company_name": company_name,
                "directors": list(best_dirs.values()),
                "original_signal_date": best_signal_date,
                "latest_trade_date": latest_date,
            })

    # Sort by most recent activity first
    candidates.sort(key=lambda c: c["latest_trade_date"], reverse=True)

    opened: list[InvestNowPosition] = []
    skipped: list[str] = []

    for sig in candidates:
        if len(opened) >= max_new:
            break
        if held_count + len(opened) >= MAX_POSITIONS:
            skipped.append(f"{sig['ticker']} (portfolio full)")
            continue

        price, _price_date = await get_latest_price(db, sig["ticker"])
        if not price:
            skipped.append(f"{sig['ticker']} (no price data)")
            continue

        quantity = max(1, int(POSITION_SIZE // price))
        cost_basis = round(quantity * price, 2)

        # signal_date = today so it stays inside the live window
        stmt = pg_insert(PortfolioPosition).values(
            id=str(uuid.uuid4()),
            ticker=sig["ticker"],
            company_name=sig["company_name"],
            signal_date=today,
            signal_directors=json.dumps(sig["directors"]),
            entry_date=today,
            entry_price=price,
            quantity=quantity,
            cost_basis=cost_basis,
            exit_date=None,
            exit_price=None,
            exit_reason=None,
            status="open",
            pnl_aud=None,
            pnl_pct=None,
        ).on_conflict_do_nothing()  # idempotent: same ticker+today → skip
        await db.execute(stmt)

        opened.append(InvestNowPosition(
            ticker=sig["ticker"],
            company_name=sig["company_name"],
            directors=sig["directors"],
            original_signal_date=sig["original_signal_date"],
            entry_price=price,
            quantity=quantity,
            cost_basis=cost_basis,
        ))
        held_tickers.add(sig["ticker"])
        logger.info(
            f"  INVEST {sig['ticker']:6s} @ ${price:.4f} × {quantity:,d} = ${cost_basis:,.2f}"
            f"  (signal fired {sig['original_signal_date']})"
        )

    await db.commit()

    total = round(sum(p.cost_basis for p in opened), 2)
    logger.info(f"invest-now: {len(opened)} positions opened, ${total:,.2f} invested")
    return InvestNowResponse(
        positions_opened=len(opened),
        total_invested=total,
        positions=opened,
        skipped=skipped,
    )
