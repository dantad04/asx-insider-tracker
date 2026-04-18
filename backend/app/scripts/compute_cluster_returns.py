"""
Cluster Return Computation (Phase 2 — writes to cluster_returns)

Pure analysis — no external API calls. Reads clusters + yf_daily_prices,
writes cluster_returns via INSERT ... ON CONFLICT DO UPDATE so re-running
overwrites prior values (important: new price data can make a
previously-skipped cluster computable).

Per cluster, up to 4 rows are produced:
  (90d, trade_date), (90d, disclosure_date), (180d, trade_date), (180d, disclosure_date)

Entry bases (match cluster_backtest.py exactly):
  trade_date      : close on cluster.end_date (forward-snap within 7 days)
  disclosure_date : close on next trading day AFTER cluster.max_disclosure_date

Exit: entry_date + horizon_days (calendar), forward-snap within 7 days then
backward-snap as fallback. Skip if the horizon has not elapsed, if entry is
unavailable, or if exit is unavailable.

Sector benchmark: equal-weight return of all on-market-buy tickers in the
same resolved sector, excluding the cluster's own ticker. Requires ≥3
contributing tickers (copied verbatim from cluster_backtest.py).

Usage:
  docker-compose exec backend python -m app.scripts.compute_cluster_returns
  docker-compose exec backend python -m app.scripts.compute_cluster_returns --cluster-ids 12,34,56
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import text

from app.database import async_session
from app.models.company import Company
from app.models.trade import Trade, TradeType
from sqlalchemy import select

logger = logging.getLogger(__name__)

PriceSeries = list[tuple[date, float]]

PRICE_LOOKUP_TOLERANCE = 7  # calendar days, matches cluster_backtest.py
SECTOR_MIN_PEERS = 3        # matches cluster_backtest.py _sector_return


# ── Price helpers (copied verbatim from cluster_backtest.py) ─────────────────

def _price_at(
    series: PriceSeries, target: date, direction: str = "forward",
) -> float | None:
    if not series:
        return None
    dates = [d for d, _ in series]
    if direction == "forward":
        idx = bisect.bisect_left(dates, target)
        if idx < len(series) and (series[idx][0] - target).days <= PRICE_LOOKUP_TOLERANCE:
            return series[idx][1]
    else:
        idx = bisect.bisect_right(dates, target) - 1
        if idx >= 0 and (target - series[idx][0]).days <= PRICE_LOOKUP_TOLERANCE:
            return series[idx][1]
    return None


def _next_trading_day(calendar: list[date], after: date) -> date | None:
    idx = bisect.bisect_right(calendar, after)
    return calendar[idx] if idx < len(calendar) else None


def _compute_return(
    series: PriceSeries, entry_date: date, horizon_days: int,
) -> tuple[float | None, float | None]:
    """Return (entry_price, return_fraction). Matches cluster_backtest.py."""
    entry_price = _price_at(series, entry_date, "forward")
    if not entry_price:
        return None, None
    exit_date = entry_date + timedelta(days=horizon_days)
    exit_price = _price_at(series, exit_date, "forward")
    if exit_price is None:
        exit_price = _price_at(series, exit_date, "backward")
    if exit_price is None:
        return entry_price, None
    return entry_price, (exit_price / entry_price) - 1


def _sector_return(
    sector_prices: dict[str, dict[str, PriceSeries]],
    sector: str,
    exclude_ticker: str,
    entry_date: date,
    horizon: int,
) -> float | None:
    """Equal-weight sector return, excluding the cluster ticker.
    NULL if <3 peer tickers contribute returns."""
    tickers_data = sector_prices.get(sector, {})
    returns: list[float] = []
    for t, series in tickers_data.items():
        if t == exclude_ticker:
            continue
        _, ret = _compute_return(series, entry_date, horizon)
        if ret is not None:
            returns.append(ret)
    if len(returns) < SECTOR_MIN_PEERS:
        return None
    return sum(returns) / len(returns)


# ── Data loading ─────────────────────────────────────────────────────────────

async def _load_trading_calendar(session) -> list[date]:
    result = await session.execute(
        text("SELECT DISTINCT date FROM yf_daily_prices "
             "WHERE ticker = 'STW' ORDER BY date")
    )
    return [r.date for r in result.all()]


async def _load_clusters(session, cluster_ids: list[int] | None):
    if cluster_ids:
        result = await session.execute(
            text(
                "SELECT cluster_id, ticker, sector, start_date, end_date, "
                "max_disclosure_date FROM clusters "
                "WHERE cluster_id = ANY(:ids) ORDER BY cluster_id"
            ),
            {"ids": cluster_ids},
        )
    else:
        result = await session.execute(
            text(
                "SELECT cluster_id, ticker, sector, start_date, end_date, "
                "max_disclosure_date FROM clusters ORDER BY cluster_id"
            )
        )
    return [dict(r._mapping) for r in result.all()]


async def _load_prices(session, tickers: set[str]) -> dict[str, PriceSeries]:
    if not tickers:
        return {}
    result = await session.execute(
        text("SELECT ticker, date, close FROM yf_daily_prices "
             "WHERE ticker = ANY(:tickers) ORDER BY ticker, date"),
        {"tickers": list(tickers)},
    )
    price_map: dict[str, PriceSeries] = defaultdict(list)
    for r in result.all():
        price_map[r.ticker].append((r.date, float(r.close)))
    return dict(price_map)


async def _load_benchmark(session) -> PriceSeries:
    result = await session.execute(
        text("SELECT date, close FROM yf_daily_prices "
             "WHERE ticker = 'STW' ORDER BY date")
    )
    return [(r.date, float(r.close)) for r in result.all()]


async def _load_sector_universe(session) -> dict[str, str]:
    """
    Build ticker → resolved-sector map for all on-market-buy tickers,
    matching the resolution chain used by detect_clusters.py and
    cluster_backtest.py. Tickers whose sector stays 'Other' are returned
    with sector=None (they won't contribute to any sector average).
    """
    # Base: companies.sector for every on-market-buy ticker
    result = await session.execute(
        select(Company.ticker, Company.sector)
        .join(Trade, Trade.company_id == Company.id)
        .where(Trade.trade_type == TradeType.ON_MARKET_BUY)
        .distinct()
    )
    base: dict[str, str] = {r.ticker: (r.sector or "Other") for r in result.all()}

    # Override 1: asx200_sectors.json (wins over yf when 'Other')
    asx200_path = Path("/app/data/asx200_sectors.json")
    if not asx200_path.exists():
        asx200_path = Path(__file__).resolve().parents[3] / "data" / "asx200_sectors.json"
    asx200_map: dict[str, str] = {}
    if asx200_path.exists():
        try:
            entries = json.loads(asx200_path.read_text())
            asx200_map = {e["ticker"]: e["sector"] for e in entries}
        except Exception:
            pass

    # Override 2: yf_ticker_sectors
    yf_map: dict[str, str] = {}
    try:
        result = await session.execute(text("SELECT ticker, sector FROM yf_ticker_sectors"))
        yf_map = {r.ticker: r.sector for r in result.all()}
    except Exception:
        pass

    resolved: dict[str, str | None] = {}
    for ticker, sec in base.items():
        if sec != "Other":
            resolved[ticker] = sec
        elif ticker in asx200_map:
            resolved[ticker] = asx200_map[ticker]
        elif ticker in yf_map:
            resolved[ticker] = yf_map[ticker]
        else:
            resolved[ticker] = None  # truly unknown → excluded from sector bench
    return resolved


# ── Upsert ───────────────────────────────────────────────────────────────────

UPSERT_SQL = text("""
    INSERT INTO cluster_returns (
        cluster_id, horizon_days, entry_basis,
        absolute_return, alpha_vs_asx200, alpha_vs_sector, computed_at
    ) VALUES (
        :cluster_id, :horizon_days, :entry_basis,
        :absolute_return, :alpha_vs_asx200, :alpha_vs_sector, NOW()
    )
    ON CONFLICT (cluster_id, horizon_days, entry_basis) DO UPDATE SET
        absolute_return = EXCLUDED.absolute_return,
        alpha_vs_asx200 = EXCLUDED.alpha_vs_asx200,
        alpha_vs_sector = EXCLUDED.alpha_vs_sector,
        computed_at     = NOW()
""")


# ── Main ─────────────────────────────────────────────────────────────────────

HORIZONS = [90, 180]
BASES = ["trade_date", "disclosure_date"]


async def run(cluster_ids: list[int] | None = None) -> None:
    async with async_session() as session:
        logger.info("Loading trading calendar ...")
        calendar = await _load_trading_calendar(session)
        if not calendar:
            print("ERROR: yf_daily_prices has no STW data. "
                  "Run fetch_prices_local.py first.", file=sys.stderr)
            return
        logger.info(f"  {len(calendar)} trading days "
                    f"({calendar[0]} to {calendar[-1]})")

        logger.info("Loading clusters ...")
        clusters = await _load_clusters(session, cluster_ids)
        logger.info(f"  {len(clusters)} clusters to process")
        if not clusters:
            return

        logger.info("Resolving sector universe ...")
        ticker_sector = await _load_sector_universe(session)
        sector_to_tickers: dict[str, set[str]] = defaultdict(set)
        for ticker, sec in ticker_sector.items():
            if sec:
                sector_to_tickers[sec].add(ticker)

        cluster_sectors = {c["sector"] for c in clusters if c["sector"]}
        # Tickers we need prices for: every cluster ticker + every peer in those sectors
        needed: set[str] = {c["ticker"] for c in clusters}
        for sec in cluster_sectors:
            needed |= sector_to_tickers.get(sec, set())

        logger.info(f"Loading prices for {len(needed)} tickers "
                    f"(cluster tickers + sector peers) ...")
        price_map = await _load_prices(session, needed)
        logger.info(f"  {len(price_map)}/{len(needed)} tickers have price data")

        asx200_series = await _load_benchmark(session)
        logger.info(f"  STW benchmark: {len(asx200_series)} prices")

        # sector → { ticker → series }
        sector_prices: dict[str, dict[str, PriceSeries]] = {}
        for sec in cluster_sectors:
            sector_prices[sec] = {
                t: price_map[t] for t in sector_to_tickers.get(sec, set())
                if t in price_map
            }

        today = date.today()

        # Aggregate counters for the skip-reason log
        skips = {
            "horizon_not_elapsed": 0,
            "no_entry_price": 0,
            "no_exit_price": 0,
            "no_disclosure_date": 0,
        }
        rows_upserted = 0
        rows_by_basis: dict[tuple[int, str], int] = defaultdict(int)
        params: list[dict] = []

        for c in clusters:
            ticker = c["ticker"]
            sector = c["sector"]  # already NULL when 'Other'
            series = price_map.get(ticker, [])

            entry_dates: dict[str, date | None] = {
                "trade_date": c["end_date"],
                "disclosure_date": (
                    _next_trading_day(calendar, c["max_disclosure_date"])
                    if c["max_disclosure_date"] else None
                ),
            }

            for basis in BASES:
                entry = entry_dates[basis]
                if entry is None:
                    # Only counts disclosure; trade_date is always set.
                    skips["no_disclosure_date"] += len(HORIZONS)
                    continue

                for h in HORIZONS:
                    exit_cal = entry + timedelta(days=h)
                    if exit_cal > today:
                        skips["horizon_not_elapsed"] += 1
                        continue

                    entry_price, abs_ret = _compute_return(series, entry, h)
                    if entry_price is None:
                        skips["no_entry_price"] += 1
                        continue
                    if abs_ret is None:
                        skips["no_exit_price"] += 1
                        continue

                    _, bench_ret = _compute_return(asx200_series, entry, h)
                    alpha_asx = (
                        abs_ret - bench_ret
                        if bench_ret is not None else None
                    )

                    if sector:
                        sec_ret = _sector_return(
                            sector_prices, sector, ticker, entry, h,
                        )
                        alpha_sec = (
                            abs_ret - sec_ret if sec_ret is not None else None
                        )
                    else:
                        alpha_sec = None

                    params.append({
                        "cluster_id": c["cluster_id"],
                        "horizon_days": h,
                        "entry_basis": basis,
                        "absolute_return": abs_ret,
                        "alpha_vs_asx200": alpha_asx,
                        "alpha_vs_sector": alpha_sec,
                    })
                    rows_by_basis[(h, basis)] += 1

        if params:
            logger.info(f"Upserting {len(params)} cluster_returns rows ...")
            try:
                await session.execute(UPSERT_SQL, params)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            rows_upserted = len(params)

        logger.info(f"  Upserted {rows_upserted} rows")
        for (h, basis), n in sorted(rows_by_basis.items()):
            logger.info(f"    {h}d / {basis}: {n}")
        logger.info("Skip summary:")
        for reason, n in skips.items():
            if n:
                logger.info(f"    {reason}: {n}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute absolute + alpha returns for every cluster × "
                    "entry-basis × horizon and upsert into cluster_returns.",
    )
    parser.add_argument(
        "--cluster-ids", type=str, default=None,
        help="Comma-separated cluster_ids to recompute (default: all).",
    )
    args = parser.parse_args()

    cluster_ids: list[int] | None = None
    if args.cluster_ids:
        cluster_ids = [int(x.strip()) for x in args.cluster_ids.split(",") if x.strip()]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    asyncio.run(run(cluster_ids=cluster_ids))


if __name__ == "__main__":
    main()
