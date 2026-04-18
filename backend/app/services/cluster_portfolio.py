"""Cluster Portfolio engine for the rules-based paper portfolio."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import case, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.cluster_portfolio import (
    ClusterPortfolio,
    ClusterPortfolioEvent,
    ClusterPortfolioPosition,
)
from app.services.price_fetcher import get_close_price_with_date, get_latest_price_with_source

DEFAULT_PORTFOLIO_NAME = "Basic Materials Cluster Portfolio"
DEFAULT_STRATEGY_KEY = "basic-materials-active-clusters-v1"
DEFAULT_STARTING_CASH = 100_000.0
DEFAULT_ALLOCATION_AUD = 10_000.0
UPGRADED_ALLOCATION_AUD = 15_000.0
UPGRADED_DIRECTOR_COUNT = 3
MAX_CONCURRENT_POSITIONS = 10
EXIT_HOLD_DAYS = 90
TARGET_SECTOR = "Basic Materials"
BUYABLE_SOURCE_STATUS = "active"

EVENT_BUY = "buy"
EVENT_SELL = "sell"
EVENT_SKIP = "skip"
EVENT_EMAIL_SENT = "email_sent"
EVENT_PORTFOLIO_INITIALIZED = "portfolio_init"

SKIP_ALREADY_OPEN_TICKER = "already_open_ticker"
SKIP_ALREADY_SEEN_CLUSTER = "already_seen_cluster"
SKIP_INSUFFICIENT_CASH = "insufficient_cash"
SKIP_MISSING_ENTRY_PRICE = "missing_entry_price"
SKIP_NOT_ACTIVE = "not_active"
SKIP_MAX_POSITIONS_REACHED = "max_positions_reached"
SKIP_ALLOCATION_TOO_SMALL = "allocation_too_small"
SKIP_MISSING_EXIT_PRICE = "missing_exit_price"


@dataclass(frozen=True)
class ClusterCandidate:
    """Candidate cluster snapshot sourced from the existing cluster tables."""

    cluster_id: int
    ticker: str
    sector: str | None
    industry: str | None
    start_date: date
    end_date: date
    director_count: int
    total_value_aud: float
    pct_market_cap: float | None
    status: str


@dataclass
class PortfolioAction:
    """Structured action emitted by a portfolio cycle."""

    event_type: str
    position_id: int | None
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


@dataclass
class PortfolioCycleResult:
    """Return object for one paper-portfolio cycle."""

    portfolio_id: int
    portfolio_name: str
    created_default_portfolio: bool
    starting_cash: float
    current_cash: float
    buys_opened: list[PortfolioAction]
    sells_closed: list[PortfolioAction]
    skips_logged: list[PortfolioAction]


@dataclass(frozen=True)
class PortfolioValuation:
    """Computed summary state for API responses."""

    portfolio: ClusterPortfolio
    deployed_capital: float
    open_positions_count: int
    closed_positions_count: int
    open_positions_value: float | None
    total_portfolio_value: float | None
    valuation_complete: bool
    missing_price_tickers: list[str]


class ClusterPortfolioEngine:
    """Paper portfolio engine driven by newly detected cluster buys."""

    async def ensure_default_portfolio(
        self,
        db: AsyncSession,
        today: date | None = None,
        dry_run: bool = False,
    ) -> tuple[ClusterPortfolio, bool]:
        today = today or date.today()
        result = await db.execute(
            select(ClusterPortfolio).where(
                ClusterPortfolio.strategy_key == DEFAULT_STRATEGY_KEY
            )
        )
        portfolio = result.scalar_one_or_none()
        if portfolio:
            return portfolio, False

        if dry_run:
            portfolio = ClusterPortfolio(
                id=0,
                name=DEFAULT_PORTFOLIO_NAME,
                strategy_key=DEFAULT_STRATEGY_KEY,
                start_date=today,
                starting_cash=DEFAULT_STARTING_CASH,
                current_cash=DEFAULT_STARTING_CASH,
                is_active=True,
            )
            return portfolio, True

        portfolio = ClusterPortfolio(
            name=DEFAULT_PORTFOLIO_NAME,
            strategy_key=DEFAULT_STRATEGY_KEY,
            start_date=today,
            starting_cash=DEFAULT_STARTING_CASH,
            current_cash=DEFAULT_STARTING_CASH,
            is_active=True,
        )
        db.add(portfolio)
        await db.flush()
        await self._record_event(
            db,
            portfolio_id=portfolio.id,
            position_id=None,
            event_type=EVENT_PORTFOLIO_INITIALIZED,
            payload={
                "name": portfolio.name,
                "strategy_key": portfolio.strategy_key,
                "start_date": today.isoformat(),
                "starting_cash": DEFAULT_STARTING_CASH,
            },
        )
        return portfolio, True

    async def list_new_candidate_clusters(
        self,
        db: AsyncSession,
        portfolio_id: int,
    ) -> list[ClusterCandidate]:
        """Return unseen Basic Materials clusters still in active/maturing states."""
        cluster_sql = text(
            """
            SELECT
                c.cluster_id,
                c.ticker,
                c.sector,
                c.industry,
                c.start_date,
                c.end_date,
                c.director_count,
                c.total_value_aud,
                c.pct_market_cap,
                c.status
            FROM clusters c
            WHERE c.sector = :sector
              AND c.status IN ('active', 'maturing')
            ORDER BY c.start_date ASC, c.cluster_id ASC
            """
        )
        rows = (await db.execute(
            cluster_sql,
            {"sector": TARGET_SECTOR},
        )).mappings().all()

        return [
            ClusterCandidate(
                cluster_id=row["cluster_id"],
                ticker=row["ticker"],
                sector=row["sector"],
                industry=row["industry"],
                start_date=row["start_date"],
                end_date=row["end_date"],
                director_count=row["director_count"],
                total_value_aud=float(row["total_value_aud"]),
                pct_market_cap=(
                    float(row["pct_market_cap"])
                    if row["pct_market_cap"] is not None
                    else None
                ),
                status=row["status"],
            )
            for row in rows
        ]

    def determine_allocation(self, cluster: ClusterCandidate) -> float:
        """Return the target allocation for a qualifying cluster."""
        if cluster.director_count >= UPGRADED_DIRECTOR_COUNT:
            return UPGRADED_ALLOCATION_AUD
        return DEFAULT_ALLOCATION_AUD

    async def run_cycle(
        self,
        db: AsyncSession,
        today: date | None = None,
        dry_run: bool = False,
    ) -> PortfolioCycleResult:
        """Execute one cluster-portfolio cycle without any UI concerns."""
        today = today or date.today()
        portfolio, created = await self.ensure_default_portfolio(
            db, today=today, dry_run=dry_run
        )

        sells_closed = await self.close_due_positions(
            db, portfolio=portfolio, today=today, dry_run=dry_run
        )
        candidates = await self.list_new_candidate_clusters(db, portfolio_id=portfolio.id)
        buys_opened, skips_logged = await self.process_new_entries(
            db, portfolio=portfolio, candidates=candidates, today=today, dry_run=dry_run
        )

        if not dry_run:
            await db.flush()

        return PortfolioCycleResult(
            portfolio_id=portfolio.id,
            portfolio_name=portfolio.name,
            created_default_portfolio=created,
            starting_cash=float(portfolio.starting_cash),
            current_cash=float(portfolio.current_cash),
            buys_opened=buys_opened,
            sells_closed=sells_closed,
            skips_logged=skips_logged,
        )

    async def get_default_portfolio(
        self,
        db: AsyncSession,
    ) -> ClusterPortfolio | None:
        """Return the default Cluster Portfolio if it exists."""
        result = await db.execute(
            select(ClusterPortfolio).where(
                ClusterPortfolio.strategy_key == DEFAULT_STRATEGY_KEY
            )
        )
        return result.scalar_one_or_none()

    async def list_positions(
        self,
        db: AsyncSession,
        portfolio_id: int,
    ) -> list[ClusterPortfolioPosition]:
        """Return portfolio positions with open rows first."""
        result = await db.execute(
            select(ClusterPortfolioPosition)
            .where(ClusterPortfolioPosition.portfolio_id == portfolio_id)
            .order_by(
                case(
                    (ClusterPortfolioPosition.status == "open", 0),
                    else_=1,
                ),
                ClusterPortfolioPosition.buy_date.desc(),
                ClusterPortfolioPosition.id.desc(),
            )
        )
        return list(result.scalars().all())

    async def list_events(
        self,
        db: AsyncSession,
        portfolio_id: int,
        limit: int = 100,
    ) -> list[ClusterPortfolioEvent]:
        """Return recent event log rows in reverse chronological order."""
        result = await db.execute(
            select(ClusterPortfolioEvent)
            .where(ClusterPortfolioEvent.portfolio_id == portfolio_id)
            .order_by(
                ClusterPortfolioEvent.event_time.desc(),
                ClusterPortfolioEvent.id.desc(),
            )
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_summary(
        self,
        db: AsyncSession,
        portfolio: ClusterPortfolio,
    ) -> PortfolioValuation:
        """Compute portfolio cash and valuation summary using stored prices."""
        result = await db.execute(
            select(ClusterPortfolioPosition).where(
                ClusterPortfolioPosition.portfolio_id == portfolio.id
            )
        )
        positions = list(result.scalars().all())
        open_positions = [position for position in positions if position.status == "open"]
        closed_positions = [
            position for position in positions if position.status == "closed"
        ]

        deployed_capital = round(
            sum(float(position.allocated_aud or 0) for position in open_positions),
            2,
        )
        open_value = 0.0
        valuation_complete = True
        missing_price_tickers: list[str] = []

        for position in open_positions:
            latest_price, _price_date, _source, _fallback_used, _reason = (
                await get_latest_price_with_source(db, position.ticker)
            )
            if latest_price is None:
                valuation_complete = False
                missing_price_tickers.append(position.ticker)
                continue
            open_value += latest_price * float(position.quantity or 0)

        open_positions_value = round(open_value, 2) if open_positions else 0.0
        total_portfolio_value = (
            round(float(portfolio.current_cash) + open_positions_value, 2)
            if valuation_complete
            else None
        )

        return PortfolioValuation(
            portfolio=portfolio,
            deployed_capital=deployed_capital,
            open_positions_count=len(open_positions),
            closed_positions_count=len(closed_positions),
            open_positions_value=open_positions_value if valuation_complete else None,
            total_portfolio_value=total_portfolio_value,
            valuation_complete=valuation_complete,
            missing_price_tickers=missing_price_tickers,
        )

    async def process_new_entries(
        self,
        db: AsyncSession,
        portfolio: ClusterPortfolio,
        candidates: list[ClusterCandidate],
        today: date,
        dry_run: bool = False,
    ) -> tuple[list[PortfolioAction], list[PortfolioAction]]:
        """Process newly detected Basic Materials clusters into buys or skips."""
        actions_buy: list[PortfolioAction] = []
        actions_skip: list[PortfolioAction] = []

        open_tickers = await self._load_open_tickers(db, portfolio.id)
        seen_keys = await self._load_seen_cluster_keys(db, portfolio.id)
        open_count = len(open_tickers)
        available_cash = float(portfolio.current_cash)

        for cluster in candidates:
            allocation = self.determine_allocation(cluster)
            key = self._cluster_key(cluster)

            if key in seen_keys:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_ALREADY_SEEN_CLUSTER, dry_run
                    )
                )
                continue

            if cluster.status != BUYABLE_SOURCE_STATUS:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_NOT_ACTIVE, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            if cluster.ticker in open_tickers:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_ALREADY_OPEN_TICKER, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            if open_count >= MAX_CONCURRENT_POSITIONS:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_MAX_POSITIONS_REACHED, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            entry_price, entry_price_date, entry_price_source, fallback_used, fallback_reason = (
                await get_latest_price_with_source(db, cluster.ticker)
            )
            if not entry_price:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_MISSING_ENTRY_PRICE, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            quantity = int(allocation // entry_price)
            if quantity < 1:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_ALLOCATION_TOO_SMALL, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            actual_allocation = round(quantity * entry_price, 2)
            if available_cash < actual_allocation:
                actions_skip.append(
                    await self._skip_cluster(
                        db, portfolio, cluster, allocation, SKIP_INSUFFICIENT_CASH, dry_run
                    )
                )
                seen_keys.add(key)
                continue

            planned_exit = today + timedelta(days=EXIT_HOLD_DAYS)
            reason = (
                "rules-based active Basic Materials cluster entry"
                if allocation == DEFAULT_ALLOCATION_AUD
                else "rules-based active Basic Materials cluster entry (upgraded allocation)"
            )

            action = PortfolioAction(
                event_type=EVENT_BUY,
                position_id=None,
                ticker=cluster.ticker,
                cluster_id=cluster.cluster_id,
                reason=reason,
                allocated_aud=actual_allocation,
                buy_date=today,
                planned_exit_date=planned_exit,
                entry_price=entry_price,
                entry_price_date=entry_price_date,
                quantity=float(quantity),
            )
            actions_buy.append(action)

            if not dry_run:
                position = ClusterPortfolioPosition(
                    portfolio_id=portfolio.id,
                    entry_cluster_id=cluster.cluster_id,
                    cluster_start_date=cluster.start_date,
                    cluster_end_date=cluster.end_date,
                    ticker=cluster.ticker,
                    sector=cluster.sector,
                    industry=cluster.industry,
                    source_status=cluster.status,
                    buy_date=today,
                    planned_exit_date=planned_exit,
                    sell_date=None,
                    entry_price=entry_price,
                    entry_price_date=entry_price_date,
                    exit_price=None,
                    exit_price_date=None,
                    quantity=float(quantity),
                    allocated_aud=actual_allocation,
                    status="open",
                    buy_reason=reason,
                    sell_reason=None,
                )
                db.add(position)
                await db.flush()
                action.position_id = position.id
                portfolio.current_cash = round(available_cash - actual_allocation, 2)
                await self._record_event(
                    db,
                    portfolio_id=portfolio.id,
                    position_id=position.id,
                    event_type=EVENT_BUY,
                    payload=self._build_event_payload(
                        cluster,
                        action,
                        target_allocation=allocation,
                        extra_payload={
                            "entry_price_source": entry_price_source,
                            "entry_price_fallback_used": fallback_used,
                            "entry_price_resolution_reason": fallback_reason,
                        },
                    ),
                )
            else:
                portfolio.current_cash = round(available_cash - actual_allocation, 2)

            open_tickers.add(cluster.ticker)
            seen_keys.add(key)
            open_count += 1
            available_cash = round(available_cash - actual_allocation, 2)

        return actions_buy, actions_skip

    async def close_due_positions(
        self,
        db: AsyncSession,
        portfolio: ClusterPortfolio,
        today: date,
        dry_run: bool = False,
    ) -> list[PortfolioAction]:
        """Close open positions once their planned exit date has passed."""
        result = await db.execute(
            select(ClusterPortfolioPosition)
            .where(
                ClusterPortfolioPosition.portfolio_id == portfolio.id,
                ClusterPortfolioPosition.status == "open",
                ClusterPortfolioPosition.planned_exit_date.is_not(None),
                ClusterPortfolioPosition.planned_exit_date <= today,
            )
            .order_by(ClusterPortfolioPosition.planned_exit_date.asc())
        )
        positions = result.scalars().all()
        actions: list[PortfolioAction] = []

        for position in positions:
            exit_price, sell_date, exit_price_source = await get_close_price_with_date(
                db,
                ticker=position.ticker,
                target_date=position.planned_exit_date,
            )
            if not exit_price or not sell_date:
                if not dry_run:
                    await self._record_event(
                        db,
                        portfolio_id=portfolio.id,
                        position_id=position.id,
                        event_type=EVENT_SKIP,
                        payload={
                            "reason": SKIP_MISSING_EXIT_PRICE,
                            "ticker": position.ticker,
                            "entry_cluster_id": position.entry_cluster_id,
                            "cluster_start_date": position.cluster_start_date.isoformat(),
                            "cluster_end_date": position.cluster_end_date.isoformat(),
                            "planned_exit_date": (
                                position.planned_exit_date.isoformat()
                                if position.planned_exit_date else None
                            ),
                        },
                    )
                continue

            action = PortfolioAction(
                event_type=EVENT_SELL,
                position_id=position.id,
                ticker=position.ticker,
                cluster_id=position.entry_cluster_id,
                reason="planned_exit_90d",
                allocated_aud=float(position.allocated_aud),
                buy_date=position.buy_date,
                planned_exit_date=position.planned_exit_date,
                sell_date=sell_date,
                entry_price=float(position.entry_price) if position.entry_price is not None else None,
                entry_price_date=position.entry_price_date,
                exit_price=exit_price,
                exit_price_date=sell_date,
                quantity=float(position.quantity) if position.quantity is not None else None,
            )
            actions.append(action)

            if not dry_run:
                buy_context = await self._load_buy_event_context(
                    db,
                    portfolio_id=portfolio.id,
                    position_id=position.id,
                )
                position.sell_date = sell_date
                position.exit_price = exit_price
                position.exit_price_date = sell_date
                position.status = "closed"
                position.sell_reason = "planned_exit_90d"
                proceeds = round(exit_price * float(position.quantity or 0), 2)
                portfolio.current_cash = round(float(portfolio.current_cash) + proceeds, 2)
                await self._record_event(
                    db,
                    portfolio_id=portfolio.id,
                    position_id=position.id,
                    event_type=EVENT_SELL,
                    payload={
                        "ticker": position.ticker,
                        "entry_cluster_id": position.entry_cluster_id,
                        "cluster_start_date": position.cluster_start_date.isoformat(),
                        "cluster_end_date": position.cluster_end_date.isoformat(),
                        "industry": position.industry,
                        "sector": buy_context.get("sector", position.sector),
                        "director_count": buy_context.get("director_count"),
                        "total_value_aud": buy_context.get("total_value_aud"),
                        "pct_market_cap": buy_context.get("pct_market_cap"),
                        "buy_date": position.buy_date.isoformat() if position.buy_date else None,
                        "planned_exit_date": (
                            position.planned_exit_date.isoformat()
                            if position.planned_exit_date else None
                        ),
                        "sell_date": sell_date.isoformat(),
                        "entry_price": float(position.entry_price) if position.entry_price is not None else None,
                        "entry_price_date": (
                            position.entry_price_date.isoformat()
                            if position.entry_price_date else None
                        ),
                        "exit_price": exit_price,
                        "exit_price_date": sell_date.isoformat(),
                        "exit_price_source": exit_price_source,
                        "quantity": float(position.quantity) if position.quantity is not None else None,
                        "reason": "planned_exit_90d",
                        "buy_reason": buy_context.get("reason"),
                    },
                )
            else:
                proceeds = round(exit_price * float(position.quantity or 0), 2)
                portfolio.current_cash = round(float(portfolio.current_cash) + proceeds, 2)

        return actions

    async def _skip_cluster(
        self,
        db: AsyncSession,
        portfolio: ClusterPortfolio,
        cluster: ClusterCandidate,
        allocation: float,
        reason: str,
        dry_run: bool,
    ) -> PortfolioAction:
        action = PortfolioAction(
            event_type=EVENT_SKIP,
            position_id=None,
            ticker=cluster.ticker,
            cluster_id=cluster.cluster_id,
            reason=reason,
            allocated_aud=allocation,
        )
        if dry_run:
            return action

        await self._record_event(
            db,
            portfolio_id=portfolio.id,
            position_id=None,
            event_type=EVENT_SKIP,
            payload=self._build_event_payload(cluster, action, target_allocation=allocation),
        )
        return action

    async def _record_event(
        self,
        db: AsyncSession,
        portfolio_id: int,
        position_id: int | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        event = ClusterPortfolioEvent(
            portfolio_id=portfolio_id,
            position_id=position_id,
            event_type=event_type,
            payload_json=payload,
        )
        db.add(event)

    async def _load_open_tickers(self, db: AsyncSession, portfolio_id: int) -> set[str]:
        result = await db.execute(
            select(ClusterPortfolioPosition.ticker).where(
                ClusterPortfolioPosition.portfolio_id == portfolio_id,
                ClusterPortfolioPosition.status == "open",
            )
        )
        return {row[0] for row in result.all()}

    async def _load_seen_cluster_keys(
        self,
        db: AsyncSession,
        portfolio_id: int,
    ) -> set[tuple[str, date, date]]:
        position_result = await db.execute(
            select(
                ClusterPortfolioPosition.ticker,
                ClusterPortfolioPosition.cluster_start_date,
                ClusterPortfolioPosition.cluster_end_date,
            ).where(ClusterPortfolioPosition.portfolio_id == portfolio_id)
        )
        keys = {(row[0], row[1], row[2]) for row in position_result.all()}

        event_sql = text(
            """
            SELECT
                payload_json->>'ticker' AS ticker,
                payload_json->>'cluster_start_date' AS cluster_start_date,
                payload_json->>'cluster_end_date' AS cluster_end_date
            FROM cluster_portfolio_events
            WHERE portfolio_id = :portfolio_id
              AND event_type IN ('buy', 'skip')
              AND payload_json ? 'cluster_start_date'
              AND payload_json ? 'cluster_end_date'
            """
        )
        event_rows = (await db.execute(event_sql, {"portfolio_id": portfolio_id})).mappings().all()
        for row in event_rows:
            if row["ticker"] and row["cluster_start_date"] and row["cluster_end_date"]:
                keys.add((
                    row["ticker"],
                    date.fromisoformat(row["cluster_start_date"]),
                    date.fromisoformat(row["cluster_end_date"]),
                ))
        return keys

    async def _load_buy_event_context(
        self,
        db: AsyncSession,
        portfolio_id: int,
        position_id: int,
    ) -> dict[str, Any]:
        result = await db.execute(
            select(ClusterPortfolioEvent.payload_json)
            .where(
                ClusterPortfolioEvent.portfolio_id == portfolio_id,
                ClusterPortfolioEvent.position_id == position_id,
                ClusterPortfolioEvent.event_type == EVENT_BUY,
            )
            .order_by(ClusterPortfolioEvent.id.desc())
            .limit(1)
        )
        payload = result.scalar_one_or_none()
        return payload or {}

    def _cluster_key(self, cluster: ClusterCandidate) -> tuple[str, date, date]:
        return (cluster.ticker, cluster.start_date, cluster.end_date)

    def _build_event_payload(
        self,
        cluster: ClusterCandidate,
        action: PortfolioAction,
        target_allocation: float,
        extra_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "ticker": cluster.ticker,
            "cluster_id": cluster.cluster_id,
            "sector": cluster.sector,
            "industry": cluster.industry,
            "source_status": cluster.status,
            "director_count": cluster.director_count,
            "total_value_aud": cluster.total_value_aud,
            "pct_market_cap": cluster.pct_market_cap,
            "cluster_start_date": cluster.start_date.isoformat(),
            "cluster_end_date": cluster.end_date.isoformat(),
            "reason": action.reason,
            "target_allocation_aud": target_allocation,
            "allocated_aud": action.allocated_aud,
        }
        if action.buy_date:
            payload["buy_date"] = action.buy_date.isoformat()
        if action.planned_exit_date:
            payload["planned_exit_date"] = action.planned_exit_date.isoformat()
        if action.sell_date:
            payload["sell_date"] = action.sell_date.isoformat()
        if action.entry_price is not None:
            payload["entry_price"] = action.entry_price
        if action.entry_price_date is not None:
            payload["entry_price_date"] = action.entry_price_date.isoformat()
        if action.exit_price is not None:
            payload["exit_price"] = action.exit_price
        if action.exit_price_date is not None:
            payload["exit_price_date"] = action.exit_price_date.isoformat()
        if action.quantity is not None:
            payload["quantity"] = action.quantity
        if extra_payload:
            payload.update(extra_payload)
        return payload

    def summarize(self, result: PortfolioCycleResult) -> dict[str, Any]:
        """Serialize a cycle result for dry runs or future API responses."""
        return {
            "portfolio_id": result.portfolio_id,
            "portfolio_name": result.portfolio_name,
            "created_default_portfolio": result.created_default_portfolio,
            "starting_cash": result.starting_cash,
            "current_cash": result.current_cash,
            "buys_opened": [self._serialize_action(action) for action in result.buys_opened],
            "sells_closed": [self._serialize_action(action) for action in result.sells_closed],
            "skips_logged": [self._serialize_action(action) for action in result.skips_logged],
        }

    def _serialize_action(self, action: PortfolioAction) -> dict[str, Any]:
        """Convert a portfolio action into JSON-safe primitives."""
        payload = asdict(action)
        for field_name in (
            "buy_date",
            "planned_exit_date",
            "sell_date",
            "entry_price_date",
            "exit_price_date",
        ):
            if payload[field_name] is not None:
                payload[field_name] = payload[field_name].isoformat()
        return payload
