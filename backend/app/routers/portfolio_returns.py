"""Return-series API for the cluster portfolio chart."""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.cluster_portfolio import ClusterPortfolioPosition
from app.services.cluster_portfolio import DEFAULT_STARTING_CASH, ClusterPortfolioEngine

router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])

engine = ClusterPortfolioEngine()

RangeKey = Literal["1M", "3M", "6M", "1Y", "all"]
BENCHMARK_TICKER = "STW"


class PortfolioReturnPoint(BaseModel):
    date: date
    portfolio_return_pct: float
    benchmark_return_pct: float | None
    portfolio_value_aud: float
    benchmark_value: float | None


class PortfolioReturnsResponse(BaseModel):
    portfolio_exists: bool
    range: RangeKey
    benchmark_label: str
    benchmark_ticker: str
    benchmark_available: bool
    points: list[PortfolioReturnPoint]


def _range_start(range_key: RangeKey, portfolio_start: date | None) -> date:
    today = date.today()
    if range_key == "1M":
        return today - timedelta(days=31)
    if range_key == "3M":
        return today - timedelta(days=93)
    if range_key == "6M":
        return today - timedelta(days=186)
    if range_key == "1Y":
        return today - timedelta(days=366)
    return portfolio_start or today - timedelta(days=366)


async def _load_price_history(
    db: AsyncSession,
    tickers: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, list[tuple[date, float]]]:
    if not tickers:
        return {}

    sql = text(
        """
        SELECT DISTINCT ON (ticker, date)
               ticker, date, close
        FROM (
            SELECT ticker, date, close, 1 AS source_rank
            FROM price_snapshots
            WHERE ticker = ANY(:tickers)
              AND date <= :end_date
            UNION ALL
            SELECT ticker, date, close, 2 AS source_rank
            FROM yf_daily_prices
            WHERE ticker = ANY(:tickers)
              AND date <= :end_date
        ) prices
        WHERE date >= :lookback_date
          AND close IS NOT NULL
        ORDER BY ticker, date, source_rank
        """
    )
    rows = (
        await db.execute(
            sql,
            {
                "tickers": tickers,
                "lookback_date": start_date - timedelta(days=10),
                "end_date": end_date,
            },
        )
    ).mappings().all()

    by_ticker: dict[str, list[tuple[date, float]]] = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append((row["date"], float(row["close"])))
    return by_ticker


def _price_on_or_before(series: list[tuple[date, float]], target: date) -> float | None:
    if not series:
        return None
    dates = [item[0] for item in series]
    idx = bisect_right(dates, target) - 1
    if idx < 0:
        return None
    return series[idx][1]


def _portfolio_value_on_date(
    positions: list[ClusterPortfolioPosition],
    prices: dict[str, list[tuple[date, float]]],
    target: date,
    starting_cash: float,
) -> float:
    cash = starting_cash
    holdings_value = 0.0

    for position in positions:
        buy_date = position.buy_date
        sell_date = position.sell_date
        if target < buy_date:
            continue

        allocated = float(position.allocated_aud)
        quantity = float(position.quantity)
        entry_price = float(position.entry_price)
        cash -= allocated

        if sell_date and target >= sell_date:
            exit_price = float(position.exit_price) if position.exit_price is not None else entry_price
            cash += exit_price * quantity
            continue

        latest_price = _price_on_or_before(prices.get(position.ticker, []), target)
        holdings_value += (latest_price if latest_price is not None else entry_price) * quantity

    return round(cash + holdings_value, 2)


@router.get("/returns", response_model=PortfolioReturnsResponse)
async def get_portfolio_returns(
    range: RangeKey = Query("6M", description="1M, 3M, 6M, 1Y, or all"),
    db: AsyncSession = Depends(get_db),
):
    """Return cumulative cluster-portfolio returns versus a stored ASX 200 proxy."""
    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        return PortfolioReturnsResponse(
            portfolio_exists=False,
            range=range,
            benchmark_label="ASX 200 benchmark",
            benchmark_ticker=BENCHMARK_TICKER,
            benchmark_available=False,
            points=[],
        )

    start_date = _range_start(range, portfolio.start_date)
    end_date = date.today()
    positions = await engine.list_positions(db, portfolio.id)
    tickers = sorted({position.ticker for position in positions} | {BENCHMARK_TICKER})
    prices = await _load_price_history(db, tickers, start_date, end_date)

    chart_dates = {
        item_date
        for ticker_series in prices.values()
        for item_date, _close in ticker_series
        if start_date <= item_date <= end_date
    }
    chart_dates.add(start_date)
    chart_dates.add(end_date)
    for position in positions:
        if start_date <= position.buy_date <= end_date:
            chart_dates.add(position.buy_date)
        if position.sell_date and start_date <= position.sell_date <= end_date:
            chart_dates.add(position.sell_date)

    ordered_dates = sorted(chart_dates)
    starting_cash = float(portfolio.starting_cash or DEFAULT_STARTING_CASH)

    benchmark_series = prices.get(BENCHMARK_TICKER, [])
    benchmark_base = _price_on_or_before(benchmark_series, ordered_dates[0]) if ordered_dates else None
    if benchmark_base is None:
        for point_date, close in benchmark_series:
            if point_date >= start_date:
                benchmark_base = close
                break

    points: list[PortfolioReturnPoint] = []
    for point_date in ordered_dates:
        portfolio_value = _portfolio_value_on_date(
            positions=positions,
            prices=prices,
            target=point_date,
            starting_cash=starting_cash,
        )
        benchmark_value = _price_on_or_before(benchmark_series, point_date)
        benchmark_return = (
            round((benchmark_value - benchmark_base) / benchmark_base * 100, 4)
            if benchmark_value is not None and benchmark_base
            else None
        )
        points.append(
            PortfolioReturnPoint(
                date=point_date,
                portfolio_return_pct=round((portfolio_value - starting_cash) / starting_cash * 100, 4),
                benchmark_return_pct=benchmark_return,
                portfolio_value_aud=portfolio_value,
                benchmark_value=benchmark_value,
            )
        )

    return PortfolioReturnsResponse(
        portfolio_exists=True,
        range=range,
        benchmark_label="ASX 200 benchmark",
        benchmark_ticker=BENCHMARK_TICKER,
        benchmark_available=benchmark_base is not None,
        points=points,
    )
