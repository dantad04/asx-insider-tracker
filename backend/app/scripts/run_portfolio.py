"""
Smart Money Portfolio — live paper trading simulation.

Strategy:
  Entry : 2+ directors buy on-market within a 7-day window.
          Buy $10,000 AUD at the closing price on the LAST director's buy date.
  Exit  : Hold 90 days, OR exit immediately if 2+ directors SELL within 7 days.
  Params: $100,000 starting capital, max 10 open positions, $10,000 per trade,
          keep at least $10,000 cash reserve.

Live mode:
  - Only processes trades from PORTFOLIO_LIVE_START onwards.
  - On each run, loads existing open positions from the DB and processes their
    exits before looking for new entry signals.
  - Safe to call every 2 minutes — ON CONFLICT upsert prevents duplicates.

Run:
  docker-compose exec backend python -m app.scripts.run_portfolio
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import uuid
from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.portfolio import PortfolioPosition
from app.models.price_snapshot import PriceSnapshot
from app.models.trade import Trade, TradeType

logger = logging.getLogger(__name__)

# Date when live paper trading began.
# Positions with signal_date before this are treated as legacy backtest data
# and deleted on first run.
PORTFOLIO_LIVE_START = date(2026, 3, 15)

STARTING_CAPITAL = 100_000.0
POSITION_SIZE = 10_000.0
MAX_POSITIONS = 10
CASH_RESERVE = 10_000.0
HOLD_DAYS = 90
SIGNAL_WINDOW_DAYS = 7
MIN_DIRECTORS = 2


# ─────────────────────────────────────────────────────────────────────────────
# Price helpers
# ─────────────────────────────────────────────────────────────────────────────


def _lookup_price(
    prices: list[tuple[date, float]],
    target: date,
    direction: str = "forward",
    max_slippage: int = 7,
) -> float | None:
    """Binary-search a sorted (date, close) list with trading-day slippage."""
    if not prices:
        return None

    lo, hi = 0, len(prices) - 1
    best_idx = None

    if direction == "forward":
        limit = target + timedelta(days=max_slippage)
        while lo <= hi:
            mid = (lo + hi) // 2
            if prices[mid][0] < target:
                lo = mid + 1
            elif prices[mid][0] > limit:
                hi = mid - 1
            else:
                best_idx = mid
                hi = mid - 1  # prefer earliest
    else:
        limit = target - timedelta(days=max_slippage)
        while lo <= hi:
            mid = (lo + hi) // 2
            if prices[mid][0] > target:
                hi = mid - 1
            elif prices[mid][0] < limit:
                lo = mid + 1
            else:
                best_idx = mid
                lo = mid + 1  # prefer latest

    return float(prices[best_idx][1]) if best_idx is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# Signal detection
# ─────────────────────────────────────────────────────────────────────────────


def _detect_buy_signals(ticker_buy_trades: dict[str, list]) -> list[dict]:
    """Return sorted list of (ticker, signal_date, directors) buy-signal dicts."""
    signals = []
    for ticker, trades in ticker_buy_trades.items():
        trades_sorted = sorted(trades, key=lambda t: t["date"])
        seen_windows: set[date] = set()

        for i, anchor in enumerate(trades_sorted):
            window_end = anchor["date"] + timedelta(days=SIGNAL_WINDOW_DAYS - 1)
            window_trades = [t for t in trades_sorted[i:] if t["date"] <= window_end]
            dirs = {t["director_id"]: t["director_name"] for t in window_trades}

            if len(dirs) >= MIN_DIRECTORS:
                signal_date = max(t["date"] for t in window_trades)
                if signal_date not in seen_windows:
                    seen_windows.add(signal_date)
                    signals.append({
                        "ticker": ticker,
                        "company_name": trades_sorted[0]["company_name"],
                        "signal_date": signal_date,
                        "directors": list(dirs.values()),
                    })

    signals.sort(key=lambda s: s["signal_date"])
    return signals


def _detect_sell_signals(ticker_sell_trades: dict[str, list]) -> dict[str, list[date]]:
    """Return {ticker: [sell_signal_date, ...]} for 7-day windows of 2+ director sells."""
    sell_signals: dict[str, list[date]] = defaultdict(list)

    for ticker, trades in ticker_sell_trades.items():
        trades_sorted = sorted(trades, key=lambda t: t["date"])
        seen: set[date] = set()

        for i, anchor in enumerate(trades_sorted):
            window_end = anchor["date"] + timedelta(days=SIGNAL_WINDOW_DAYS - 1)
            window = [t for t in trades_sorted[i:] if t["date"] <= window_end]
            dirs = {t["director_id"] for t in window}
            if len(dirs) >= MIN_DIRECTORS:
                sig_date = max(t["date"] for t in window)
                if sig_date not in seen:
                    seen.add(sig_date)
                    sell_signals[ticker].append(sig_date)

    return dict(sell_signals)


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio simulation
# ─────────────────────────────────────────────────────────────────────────────


def _pos_to_dict(p: PortfolioPosition) -> dict:
    """Convert a DB row to a simulation-compatible dict."""
    return {
        "ticker": p.ticker,
        "company_name": p.company_name,
        "signal_date": p.signal_date,
        "signal_directors": p.signal_directors,
        "entry_date": p.entry_date,
        "entry_price": float(p.entry_price),
        "quantity": p.quantity,
        "cost_basis": float(p.cost_basis),
        "exit_date": None,
        "exit_price": None,
        "exit_reason": None,
        "status": "open",
        "pnl_aud": None,
        "pnl_pct": None,
    }


def _simulate(
    buy_signals: list[dict],
    sell_signals: dict[str, list[date]],
    price_map: dict[str, list[tuple[date, float]]],
    today: date,
    initial_open: list[dict] | None = None,
    initial_cash: float = STARTING_CAPITAL,
) -> list[dict]:
    """
    Simulate portfolio entries and exits.

    initial_open: positions already held (loaded from DB) — exits are processed
                  for these before any new buy signals are evaluated.
    initial_cash: cash balance reconstructed from existing DB positions.

    Returns all positions that were touched: pre-loaded ones (updated if closed)
    plus any newly opened ones.
    """
    # Start with positions that already exist in the DB
    open_positions: list[dict] = list(initial_open or [])
    touched: list[dict] = list(initial_open or [])   # track what to upsert
    cash = initial_cash

    # First, sweep exits for all pre-loaded positions up to today
    if open_positions:
        proceeds = _process_exits(open_positions, sell_signals, price_map, today, today)
        cash += proceeds

    # Then process new buy signals chronologically
    for sig in buy_signals:
        ticker = sig["ticker"]
        signal_date = sig["signal_date"]

        # Process exits that should have fired by this signal_date
        proceeds = _process_exits(open_positions, sell_signals, price_map, signal_date, today)
        cash += proceeds

        # Skip if ticker already held
        if any(p["ticker"] == ticker and p["status"] == "open" for p in open_positions):
            continue

        # Check capacity and cash reserve
        if len(open_positions) >= MAX_POSITIONS:
            continue
        if cash - POSITION_SIZE < CASH_RESERVE:
            continue

        # Entry price: close on signal_date (or next trading day)
        prices = price_map.get(ticker, [])
        entry_price = _lookup_price(prices, signal_date, direction="forward")
        if not entry_price:
            logger.debug(f"No price for {ticker} on/after {signal_date} — skipping")
            continue

        quantity = max(1, int(POSITION_SIZE // entry_price))
        cost_basis = round(quantity * entry_price, 2)

        position = {
            "ticker": ticker,
            "company_name": sig["company_name"],
            "signal_date": signal_date,
            "signal_directors": json.dumps(sig["directors"]),
            "entry_date": signal_date,
            "entry_price": entry_price,
            "quantity": quantity,
            "cost_basis": cost_basis,
            "exit_date": None,
            "exit_price": None,
            "exit_reason": None,
            "status": "open",
            "pnl_aud": None,
            "pnl_pct": None,
        }

        cash -= cost_basis
        open_positions.append(position)
        touched.append(position)

        logger.info(
            f"  BUY  {ticker:6s} @ ${entry_price:.4f} × {quantity:,d} = ${cost_basis:,.2f}"
            f"  [{signal_date}]  cash=${cash:,.0f}  dirs={sig['directors']}"
        )

    # Final exit sweep
    _process_exits(open_positions, sell_signals, price_map, today, today)

    return touched


def _process_exits(
    open_positions: list[dict],
    sell_signals: dict[str, list[date]],
    price_map: dict[str, list[tuple[date, float]]],
    as_of: date,
    today: date,
) -> float:
    """Close positions that hit their exit condition on or before as_of.
    Returns total exit proceeds for cash tracking."""
    to_close = []
    total_proceeds = 0.0

    for pos in open_positions:
        if pos["status"] != "open":
            continue

        ticker = pos["ticker"]
        entry_date = pos["entry_date"]
        exit_deadline = entry_date + timedelta(days=HOLD_DAYS)
        prices = price_map.get(ticker, [])

        ticker_sells = sell_signals.get(ticker, [])
        triggered_sell = next(
            (d for d in sorted(ticker_sells) if entry_date < d <= as_of),
            None,
        )

        if triggered_sell:
            exit_date = triggered_sell
            exit_reason = "sell_signal"
        elif exit_deadline <= as_of:
            exit_date = exit_deadline
            exit_reason = "90_days"
        else:
            continue

        exit_price = _lookup_price(prices, exit_date, direction="forward")
        if not exit_price:
            exit_price = _lookup_price(prices, exit_date, direction="backward")
        if not exit_price:
            continue

        pnl_aud = round((exit_price - pos["entry_price"]) * pos["quantity"], 2)
        pnl_pct = round((exit_price - pos["entry_price"]) / pos["entry_price"] * 100, 4)
        proceeds = round(exit_price * pos["quantity"], 2)
        total_proceeds += proceeds

        pos.update({
            "exit_date": exit_date,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "status": "closed",
            "pnl_aud": pnl_aud,
            "pnl_pct": pnl_pct,
        })

        logger.info(
            f"  SELL {ticker:6s} @ ${exit_price:.4f}  [{exit_date}]  "
            f"P&L ${pnl_aud:+,.2f} ({pnl_pct:+.2f}%)  reason={exit_reason}"
        )
        to_close.append(pos)

    for pos in to_close:
        open_positions.remove(pos)

    return total_proceeds


# ─────────────────────────────────────────────────────────────────────────────
# Main async runner
# ─────────────────────────────────────────────────────────────────────────────


async def run() -> dict:
    async with async_session() as db:
        today = date.today()

        # ── One-time cleanup: remove legacy backtest positions ─────────────────
        deleted_result = await db.execute(
            delete(PortfolioPosition).where(
                PortfolioPosition.signal_date < PORTFOLIO_LIVE_START
            )
        )
        if deleted_result.rowcount:
            logger.info(
                f"Cleaned up {deleted_result.rowcount} legacy backtest positions "
                f"(signal_date < {PORTFOLIO_LIVE_START})"
            )
            await db.commit()

        # ── Determine live portfolio start date ────────────────────────────────
        earliest_result = await db.execute(
            select(func.min(PortfolioPosition.signal_date))
        )
        earliest_signal = earliest_result.scalar()
        portfolio_start_date = earliest_signal or PORTFOLIO_LIVE_START

        logger.info(
            f"Live portfolio: start={portfolio_start_date}, today={today}"
        )

        # ── Load existing open positions from DB ───────────────────────────────
        open_result = await db.execute(
            select(PortfolioPosition).where(PortfolioPosition.status == "open")
        )
        existing_open_db = open_result.scalars().all()

        # ── Compute cash from ALL existing DB positions ────────────────────────
        all_result = await db.execute(select(PortfolioPosition))
        all_pos_db = all_result.scalars().all()

        initial_cash = STARTING_CAPITAL
        for p in all_pos_db:
            initial_cash -= float(p.cost_basis)
            if p.status == "closed" and p.exit_price:
                initial_cash += float(p.exit_price) * p.quantity

        existing_open_dicts = [_pos_to_dict(p) for p in existing_open_db]
        existing_tickers = {p["ticker"] for p in existing_open_dicts}

        logger.info(
            f"  {len(existing_open_dicts)} existing open positions, "
            f"initial cash=${initial_cash:,.0f}"
        )

        # ── Load trades from portfolio_start_date onwards ──────────────────────
        logger.info(f"Loading trades from {portfolio_start_date} onwards...")
        result = await db.execute(
            select(
                Trade.director_id,
                Trade.date_of_trade,
                Trade.trade_type,
                Company.ticker,
                Company.name.label("company_name"),
                Director.full_name.label("director_name"),
            )
            .join(Company, Company.id == Trade.company_id)
            .join(Director, Director.id == Trade.director_id)
            .where(
                Trade.trade_type.in_([TradeType.ON_MARKET_BUY, TradeType.ON_MARKET_SELL]),
                Trade.date_of_trade >= portfolio_start_date,
            )
            .order_by(Trade.date_of_trade)
        )
        rows = result.all()
        logger.info(f"  {len(rows):,d} trades in live window")

        ticker_buys: dict[str, list] = defaultdict(list)
        ticker_sells: dict[str, list] = defaultdict(list)
        for r in rows:
            entry = {
                "director_id": str(r.director_id),
                "director_name": r.director_name,
                "date": r.date_of_trade,
                "company_name": r.company_name,
            }
            if r.trade_type == TradeType.ON_MARKET_BUY:
                ticker_buys[r.ticker].append(entry)
            else:
                ticker_sells[r.ticker].append(entry)

        buy_signals = _detect_buy_signals(ticker_buys)
        sell_signals = _detect_sell_signals(ticker_sells)
        logger.info(
            f"  {len(buy_signals)} buy signals, "
            f"{sum(len(v) for v in sell_signals.values())} sell signal dates"
        )

        # ── Load prices for all relevant tickers ───────────────────────────────
        all_tickers = (
            {s["ticker"] for s in buy_signals} | existing_tickers
        )
        if not all_tickers:
            logger.info("Nothing to process.")
            return {"positions": 0, "open": 0, "closed": 0,
                    "start_date": str(portfolio_start_date)}

        price_result = await db.execute(
            select(PriceSnapshot.ticker, PriceSnapshot.date, PriceSnapshot.close)
            .where(PriceSnapshot.ticker.in_(list(all_tickers)))
            .order_by(PriceSnapshot.ticker, PriceSnapshot.date)
        )
        price_map: dict[str, list[tuple[date, float]]] = defaultdict(list)
        for pr in price_result.all():
            price_map[pr.ticker].append((pr.date, float(pr.close)))
        logger.info(f"  {len(price_map)} tickers with price data")

        # ── Run simulation ─────────────────────────────────────────────────────
        touched = _simulate(
            buy_signals, sell_signals, price_map, today,
            initial_open=existing_open_dicts,
            initial_cash=initial_cash,
        )

        if not touched:
            logger.info("Nothing changed.")
            return {"positions": 0, "open": 0, "closed": 0,
                    "start_date": str(portfolio_start_date)}

        # ── Upsert all touched positions (new entries + updated exits) ─────────
        for pos in touched:
            stmt = pg_insert(PortfolioPosition).values(
                id=str(uuid.uuid4()),
                ticker=pos["ticker"],
                company_name=pos["company_name"],
                signal_date=pos["signal_date"],
                signal_directors=pos["signal_directors"],
                entry_date=pos["entry_date"],
                entry_price=pos["entry_price"],
                quantity=pos["quantity"],
                cost_basis=pos["cost_basis"],
                exit_date=pos["exit_date"],
                exit_price=pos["exit_price"],
                exit_reason=pos["exit_reason"],
                status=pos["status"],
                pnl_aud=pos["pnl_aud"],
                pnl_pct=pos["pnl_pct"],
            ).on_conflict_do_update(
                constraint="uq_portfolio_ticker_signal",
                set_={
                    "exit_date": pos["exit_date"],
                    "exit_price": pos["exit_price"],
                    "exit_reason": pos["exit_reason"],
                    "status": pos["status"],
                    "pnl_aud": pos["pnl_aud"],
                    "pnl_pct": pos["pnl_pct"],
                },
            )
            await db.execute(stmt)

        await db.commit()

        open_count = sum(1 for p in touched if p["status"] == "open")
        closed_count = sum(1 for p in touched if p["status"] == "closed")
        logger.info(
            f"Done. {len(touched)} touched: {open_count} open, {closed_count} closed."
        )
        return {
            "positions": len(touched),
            "open": open_count,
            "closed": closed_count,
            "start_date": str(portfolio_start_date),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(run())
