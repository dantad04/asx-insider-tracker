"""API endpoints for the Cluster Portfolio paper portfolio."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.cluster_portfolio import (
    ClusterPortfolioEvent,
    ClusterPortfolioPosition,
)
from app.services.cluster_portfolio import (
    DEFAULT_STARTING_CASH,
    ClusterPortfolioEngine,
)
from app.services.cluster_portfolio_notifier import NotificationAttempt
from app.services.cluster_portfolio_runner import run_cluster_portfolio_cycle
from app.services.price_fetcher import get_latest_price_with_source

router = APIRouter(prefix="/api/cluster-portfolio", tags=["cluster-portfolio"])

engine = ClusterPortfolioEngine()


class ClusterPortfolioSummaryResponse(BaseModel):
    portfolio_exists: bool
    portfolio_id: int | None
    name: str | None
    strategy_key: str | None
    start_date: date | None
    is_active: bool | None
    starting_cash: float
    current_cash: float
    deployed_capital: float
    open_positions_count: int
    closed_positions_count: int
    open_positions_value: float | None
    total_portfolio_value: float | None
    valuation_complete: bool
    missing_price_tickers: list[str]


class ClusterPortfolioPositionResponse(BaseModel):
    id: int
    portfolio_id: int
    entry_cluster_id: int | None
    ticker: str
    sector: str | None
    industry: str | None
    source_status: str
    cluster_start_date: date
    cluster_end_date: date
    buy_date: date
    planned_exit_date: date
    sell_date: date | None
    entry_price: float
    entry_price_date: date
    exit_price: float | None
    exit_price_date: date | None
    quantity: float
    allocated_aud: float
    status: str
    buy_reason: str
    sell_reason: str | None
    current_price: float | None
    current_price_date: date | None
    current_price_source: str | None
    current_value: float | None
    unrealised_pnl_aud: float | None
    unrealised_pnl_pct: float | None


class ClusterPortfolioPositionsEnvelope(BaseModel):
    portfolio_exists: bool
    positions: list[ClusterPortfolioPositionResponse]


class ClusterPortfolioEventResponse(BaseModel):
    id: int
    portfolio_id: int
    position_id: int | None
    event_type: str
    event_time: datetime
    payload_json: dict[str, Any] | None


class ClusterPortfolioEventsEnvelope(BaseModel):
    portfolio_exists: bool
    events: list[ClusterPortfolioEventResponse]


class ClusterPortfolioHistoryPoint(BaseModel):
    date: date
    value_aud: float
    label: str
    event_type: str


class ClusterPortfolioHistoryResponse(BaseModel):
    portfolio_exists: bool
    methodology: str
    points: list[ClusterPortfolioHistoryPoint]


class ClusterPortfolioRunRequest(BaseModel):
    dry_run: bool = False
    send_emails: bool = False
    email_test_replay_latest: bool = False


class ClusterPortfolioActionResponse(BaseModel):
    event_type: str
    ticker: str
    cluster_id: int | None
    reason: str
    allocated_aud: float
    buy_date: date | None = None
    planned_exit_date: date | None = None
    sell_date: date | None = None
    entry_price: float | None = None
    entry_price_date: date | None = None
    exit_price: float | None = None
    exit_price_date: date | None = None
    quantity: float | None = None


class ClusterPortfolioRunResponse(BaseModel):
    dry_run: bool
    portfolio_id: int
    portfolio_name: str
    portfolio_created: bool
    portfolio_reused: bool
    starting_cash: float
    current_cash: float
    deployed_capital: float
    buys_opened: list[ClusterPortfolioActionResponse]
    sells_closed: list[ClusterPortfolioActionResponse]
    skips_logged: list[ClusterPortfolioActionResponse]
    open_positions_count: int
    closed_positions_count: int
    open_positions_value: float | None
    total_portfolio_value: float | None
    valuation_complete: bool
    missing_price_tickers: list[str]
    notifications: list[dict[str, Any]]


def _serialize_notification(attempt: NotificationAttempt) -> dict[str, Any]:
    return {
        "trade_event_type": attempt.trade_event_type,
        "ticker": attempt.ticker,
        "position_id": attempt.position_id,
        "status": attempt.status,
        "replayed": attempt.replayed,
        "error": attempt.error,
    }


def _serialize_action(action: Any) -> ClusterPortfolioActionResponse:
    return ClusterPortfolioActionResponse(
        event_type=action.event_type,
        ticker=action.ticker,
        cluster_id=action.cluster_id,
        reason=action.reason,
        allocated_aud=action.allocated_aud,
        buy_date=action.buy_date,
        planned_exit_date=action.planned_exit_date,
        sell_date=action.sell_date,
        entry_price=action.entry_price,
        entry_price_date=action.entry_price_date,
        exit_price=action.exit_price,
        exit_price_date=action.exit_price_date,
        quantity=action.quantity,
    )


async def _serialize_position(
    db: AsyncSession,
    position: ClusterPortfolioPosition,
) -> ClusterPortfolioPositionResponse:
    current_price = None
    current_price_date = None
    current_price_source = None
    current_value = None
    unrealised_pnl_aud = None
    unrealised_pnl_pct = None

    if position.status == "open":
        current_price, current_price_date, current_price_source, _fallback_used, _reason = (
            await get_latest_price_with_source(db, position.ticker)
        )
        if current_price is not None:
            current_value = round(current_price * float(position.quantity), 2)
            unrealised_pnl_aud = round(
                current_value - float(position.allocated_aud),
                2,
            )
            if float(position.allocated_aud):
                unrealised_pnl_pct = round(
                    unrealised_pnl_aud / float(position.allocated_aud) * 100,
                    4,
                )

    return ClusterPortfolioPositionResponse(
        id=position.id,
        portfolio_id=position.portfolio_id,
        entry_cluster_id=position.entry_cluster_id,
        ticker=position.ticker,
        sector=position.sector,
        industry=position.industry,
        source_status=position.source_status,
        cluster_start_date=position.cluster_start_date,
        cluster_end_date=position.cluster_end_date,
        buy_date=position.buy_date,
        planned_exit_date=position.planned_exit_date,
        sell_date=position.sell_date,
        entry_price=float(position.entry_price),
        entry_price_date=position.entry_price_date,
        exit_price=float(position.exit_price) if position.exit_price is not None else None,
        exit_price_date=position.exit_price_date,
        quantity=float(position.quantity),
        allocated_aud=float(position.allocated_aud),
        status=position.status,
        buy_reason=position.buy_reason,
        sell_reason=position.sell_reason,
        current_price=current_price,
        current_price_date=current_price_date,
        current_price_source=current_price_source,
        current_value=current_value,
        unrealised_pnl_aud=unrealised_pnl_aud,
        unrealised_pnl_pct=unrealised_pnl_pct,
    )


def _serialize_event(event: ClusterPortfolioEvent) -> ClusterPortfolioEventResponse:
    return ClusterPortfolioEventResponse(
        id=event.id,
        portfolio_id=event.portfolio_id,
        position_id=event.position_id,
        event_type=event.event_type,
        event_time=event.event_time,
        payload_json=event.payload_json,
    )


@router.get("/summary", response_model=ClusterPortfolioSummaryResponse)
async def get_cluster_portfolio_summary(
    db: AsyncSession = Depends(get_db),
):
    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        return ClusterPortfolioSummaryResponse(
            portfolio_exists=False,
            portfolio_id=None,
            name=None,
            strategy_key=None,
            start_date=None,
            is_active=None,
            starting_cash=DEFAULT_STARTING_CASH,
            current_cash=DEFAULT_STARTING_CASH,
            deployed_capital=0.0,
            open_positions_count=0,
            closed_positions_count=0,
            open_positions_value=0.0,
            total_portfolio_value=DEFAULT_STARTING_CASH,
            valuation_complete=True,
            missing_price_tickers=[],
        )

    valuation = await engine.get_summary(db, portfolio)
    return ClusterPortfolioSummaryResponse(
        portfolio_exists=True,
        portfolio_id=portfolio.id,
        name=portfolio.name,
        strategy_key=portfolio.strategy_key,
        start_date=portfolio.start_date,
        is_active=portfolio.is_active,
        starting_cash=float(portfolio.starting_cash),
        current_cash=float(portfolio.current_cash),
        deployed_capital=valuation.deployed_capital,
        open_positions_count=valuation.open_positions_count,
        closed_positions_count=valuation.closed_positions_count,
        open_positions_value=valuation.open_positions_value,
        total_portfolio_value=valuation.total_portfolio_value,
        valuation_complete=valuation.valuation_complete,
        missing_price_tickers=valuation.missing_price_tickers,
    )


@router.get("/positions", response_model=ClusterPortfolioPositionsEnvelope)
async def get_cluster_portfolio_positions(
    db: AsyncSession = Depends(get_db),
):
    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        return ClusterPortfolioPositionsEnvelope(portfolio_exists=False, positions=[])

    positions = await engine.list_positions(db, portfolio.id)
    serialized_positions = [await _serialize_position(db, position) for position in positions]
    return ClusterPortfolioPositionsEnvelope(
        portfolio_exists=True,
        positions=serialized_positions,
    )


@router.get("/events", response_model=ClusterPortfolioEventsEnvelope)
async def get_cluster_portfolio_events(
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        return ClusterPortfolioEventsEnvelope(portfolio_exists=False, events=[])

    events = await engine.list_events(db, portfolio.id, limit=limit)
    return ClusterPortfolioEventsEnvelope(
        portfolio_exists=True,
        events=[_serialize_event(event) for event in events],
    )


@router.get("/history", response_model=ClusterPortfolioHistoryResponse)
async def get_cluster_portfolio_history(
    db: AsyncSession = Depends(get_db),
):
    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        return ClusterPortfolioHistoryResponse(
            portfolio_exists=False,
            methodology="transaction_based_estimate",
            points=[],
        )

    valuation = await engine.get_summary(db, portfolio)
    positions = await engine.list_positions(db, portfolio.id)

    points: list[ClusterPortfolioHistoryPoint] = [
        ClusterPortfolioHistoryPoint(
            date=portfolio.start_date,
            value_aud=float(portfolio.starting_cash),
            label="Portfolio initialized",
            event_type="portfolio_init",
        )
    ]

    for position in sorted(positions, key=lambda item: (item.buy_date, item.id)):
        points.append(
            ClusterPortfolioHistoryPoint(
                date=position.buy_date,
                value_aud=float(portfolio.starting_cash),
                label=f"Bought {position.ticker}",
                event_type="buy",
            )
        )

    realized_value = float(portfolio.starting_cash)
    for position in sorted(
        [item for item in positions if item.status == "closed" and item.sell_date],
        key=lambda item: (item.sell_date, item.id),
    ):
        if position.exit_price is not None:
            realized_value = round(
                realized_value
                + (float(position.exit_price) - float(position.entry_price))
                * float(position.quantity),
                2,
            )
        points.append(
            ClusterPortfolioHistoryPoint(
                date=position.sell_date,
                value_aud=realized_value,
                label=f"Closed {position.ticker}",
                event_type="sell",
            )
        )

    current_point_date = portfolio.start_date
    for position in positions:
        if position.status == "closed" and position.sell_date:
            current_point_date = max(current_point_date, position.sell_date)
            continue
        if position.status != "open":
            continue
        current_price, current_price_date, _source, _fallback_used, _reason = (
            await get_latest_price_with_source(db, position.ticker)
        )
        if current_price is not None and current_price_date is not None:
            current_point_date = max(current_point_date, current_price_date)

    current_total_value = valuation.total_portfolio_value
    if current_total_value is not None:
        points.append(
            ClusterPortfolioHistoryPoint(
                date=current_point_date,
                value_aud=current_total_value,
                label="Current valuation",
                event_type="valuation",
            )
        )

    return ClusterPortfolioHistoryResponse(
        portfolio_exists=True,
        methodology="transaction_based_estimate",
        points=points,
    )


@router.post("/run", response_model=ClusterPortfolioRunResponse)
async def run_cluster_portfolio(
    request: ClusterPortfolioRunRequest,
    db: AsyncSession = Depends(get_db),
):
    existing_portfolio = await engine.get_default_portfolio(db)
    base_summary = (
        await engine.get_summary(db, existing_portfolio)
        if existing_portfolio is not None
        else None
    )
    outcome = await run_cluster_portfolio_cycle(
        db,
        dry_run=request.dry_run,
        send_emails=request.send_emails,
        email_test_replay_latest=request.email_test_replay_latest,
    )
    result = outcome.cycle_result

    if request.dry_run:
        base_open_count = base_summary.open_positions_count if base_summary else 0
        base_closed_count = base_summary.closed_positions_count if base_summary else 0
        base_deployed_capital = base_summary.deployed_capital if base_summary else 0.0
        deployed_capital = round(
            base_deployed_capital
            - sum(action.allocated_aud for action in result.sells_closed)
            + sum(action.allocated_aud for action in result.buys_opened),
            2,
        )
        return ClusterPortfolioRunResponse(
            dry_run=True,
            portfolio_id=result.portfolio_id,
            portfolio_name=result.portfolio_name,
            portfolio_created=result.created_default_portfolio,
            portfolio_reused=not result.created_default_portfolio,
            starting_cash=result.starting_cash,
            current_cash=result.current_cash,
            deployed_capital=deployed_capital,
            buys_opened=[_serialize_action(action) for action in result.buys_opened],
            sells_closed=[_serialize_action(action) for action in result.sells_closed],
            skips_logged=[_serialize_action(action) for action in result.skips_logged],
            open_positions_count=base_open_count - len(result.sells_closed) + len(result.buys_opened),
            closed_positions_count=base_closed_count + len(result.sells_closed),
            open_positions_value=None,
            total_portfolio_value=None,
            valuation_complete=False,
            missing_price_tickers=[],
            notifications=[_serialize_notification(item) for item in outcome.notifications],
        )
    valuation = outcome.valuation
    if valuation is None:
        raise RuntimeError("Cluster Portfolio valuation missing after successful run")

    return ClusterPortfolioRunResponse(
        dry_run=False,
        portfolio_id=result.portfolio_id,
        portfolio_name=result.portfolio_name,
        portfolio_created=result.created_default_portfolio,
        portfolio_reused=not result.created_default_portfolio,
        starting_cash=float(valuation.portfolio.starting_cash),
        current_cash=float(valuation.portfolio.current_cash),
        deployed_capital=valuation.deployed_capital,
        buys_opened=[_serialize_action(action) for action in result.buys_opened],
        sells_closed=[_serialize_action(action) for action in result.sells_closed],
        skips_logged=[_serialize_action(action) for action in result.skips_logged],
        open_positions_count=valuation.open_positions_count,
        closed_positions_count=valuation.closed_positions_count,
        open_positions_value=valuation.open_positions_value,
        total_portfolio_value=valuation.total_portfolio_value,
        valuation_complete=valuation.valuation_complete,
        missing_price_tickers=valuation.missing_price_tickers,
        notifications=[_serialize_notification(item) for item in outcome.notifications],
    )
