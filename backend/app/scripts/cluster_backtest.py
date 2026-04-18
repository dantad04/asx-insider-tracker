"""
Cluster Buy Backtest Engine

Identifies historical "cluster buy" events and backtests their forward returns.

Definitions:
  Cluster = 2+ distinct directors making on-market purchases in the same
            ASX-listed entity within a rolling 20 trading-day window.
  Filters: on-market purchases only. Exclude option exercises, DRP, rights
           issues, off-market transfers. Min $20k AUD per director trade.
           Min aggregate cluster value >= 0.05% of market cap.

Entry points:
  Trade-date     : close on cluster end date
  Disclosure-date: close on max(date_lodged) + 1 trading day

Horizons: 90 and 180 calendar days.
Benchmarks: ASX 200 (STW.AX ETF proxy) and equal-weight sector.

Usage:
  docker-compose exec backend python -m app.scripts.cluster_backtest
  docker-compose exec backend python -m app.scripts.cluster_backtest --csv /app/data/cluster_backtest.csv
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import io
import json
import logging
import statistics
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select, text

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.price_snapshot import PriceSnapshot
from app.models.trade import Trade, TradeType

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

CLUSTER_WINDOW_TRADING_DAYS = 20
MIN_DIRECTORS = 2
MIN_TRADE_VALUE_AUD = 20_000
MIN_PCT_MARKET_CAP = 0.0005  # 0.05%
RETURN_HORIZONS = [90, 180]  # calendar days
PRICE_LOOKUP_TOLERANCE = 7   # max calendar-day slippage for price lookups


# ── Market-cap buckets ───────────────────────────────────────────────────────

def _cap_bucket(mc: int | None) -> str:
    if mc is None:
        return "unknown"
    if mc < 50_000_000:
        return "nano <$50m"
    if mc < 300_000_000:
        return "micro $50-300m"
    if mc < 2_000_000_000:
        return "small $300m-2b"
    return "mid+ $2b+"


# ── Price helpers ────────────────────────────────────────────────────────────

PriceSeries = list[tuple[date, float]]  # sorted ascending by date


def _price_at(
    series: PriceSeries, target: date, direction: str = "forward",
) -> float | None:
    """Binary-search for close price on/after (forward) or on/before (backward)."""
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


def _advance_trading_days(calendar: list[date], from_date: date, n: int) -> date:
    """Return the calendar date that is *n* trading days after *from_date*."""
    idx = bisect.bisect_left(calendar, from_date)
    target = idx + n
    if target >= len(calendar):
        overshoot = target - len(calendar) + 1
        return calendar[-1] + timedelta(days=int(overshoot * 1.4) + 1)
    return calendar[target]


def _next_trading_day(calendar: list[date], after: date) -> date | None:
    """Return the first trading day strictly after *after*."""
    idx = bisect.bisect_right(calendar, after)
    return calendar[idx] if idx < len(calendar) else None


# ── Data loading (all from database — no external API calls) ─────────────────

async def _load_trading_calendar(session) -> list[date]:
    """Build trading-day calendar from STW dates in yf_daily_prices,
    falling back to price_snapshots if yf_daily_prices is empty."""
    # Prefer yf_daily_prices (populated by fetch_prices_local.py)
    try:
        result = await session.execute(
            text("SELECT DISTINCT date FROM yf_daily_prices "
                 "WHERE ticker = 'STW' ORDER BY date")
        )
        dates = [r.date for r in result.all()]
        if dates:
            return dates
    except Exception:
        pass  # table may not exist yet

    # Fallback: price_snapshots
    result = await session.execute(
        select(PriceSnapshot.date).distinct().order_by(PriceSnapshot.date)
    )
    return [r.date for r in result.all()]


async def _load_on_market_buys(session):
    """Load all on-market buy trades joined with company + director info."""
    result = await session.execute(
        select(
            Trade.id,
            Trade.director_id,
            Trade.company_id,
            Trade.date_of_trade,
            Trade.date_lodged,
            Trade.quantity,
            Trade.price_per_share,
            Company.ticker,
            Company.name.label("company_name"),
            Company.sector,
            Company.market_cap,
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.trade_type == TradeType.ON_MARKET_BUY)
        .order_by(Trade.date_of_trade)
    )
    rows = result.all()

    trades: list[dict] = []
    companies: dict[str, dict] = {}

    for r in rows:
        price = float(r.price_per_share) if r.price_per_share else None
        qty = abs(r.quantity) if r.quantity else 0
        value = price * qty if (price and qty) else None

        companies[r.ticker] = {
            "name": r.company_name,
            "sector": r.sector or "Other",
            "market_cap": r.market_cap,
        }

        trades.append({
            "director_id": str(r.director_id),
            "director_name": r.director_name,
            "ticker": r.ticker,
            "date_of_trade": r.date_of_trade,
            "date_lodged": r.date_lodged,
            "quantity": qty,
            "price": price,
            "value_aud": value,
        })

    return trades, companies


async def _load_prices(session, tickers: set[str]) -> dict[str, PriceSeries]:
    """Load prices from yf_daily_prices, falling back to price_snapshots."""
    if not tickers:
        return {}

    price_map: dict[str, PriceSeries] = defaultdict(list)

    # Primary: yf_daily_prices (populated by fetch_prices_local.py)
    try:
        result = await session.execute(
            text("SELECT ticker, date, close FROM yf_daily_prices "
                 "WHERE ticker = ANY(:tickers) ORDER BY ticker, date"),
            {"tickers": list(tickers)},
        )
        for r in result.all():
            price_map[r.ticker].append((r.date, float(r.close)))
    except Exception:
        pass  # table may not exist

    yf_count = len(price_map)

    # Fallback: price_snapshots for tickers not found above
    missing = tickers - set(price_map.keys())
    if missing:
        result = await session.execute(
            select(PriceSnapshot.ticker, PriceSnapshot.date, PriceSnapshot.close)
            .where(PriceSnapshot.ticker.in_(list(missing)))
            .order_by(PriceSnapshot.ticker, PriceSnapshot.date)
        )
        for r in result.all():
            price_map[r.ticker].append((r.date, float(r.close)))

    ps_count = len(price_map) - yf_count
    logger.info(f"  Prices loaded: {yf_count} from yf_daily_prices, "
                f"{ps_count} from price_snapshots")

    return dict(price_map)


async def _load_benchmark(session) -> PriceSeries:
    """Load STW (ASX 200 proxy) from yf_daily_prices."""
    try:
        result = await session.execute(
            text("SELECT date, close FROM yf_daily_prices "
                 "WHERE ticker = 'STW' ORDER BY date")
        )
        series = [(r.date, float(r.close)) for r in result.all()]
        if series:
            logger.info(f"  ASX 200 benchmark (STW): {len(series)} daily prices")
            return series
    except Exception:
        pass
    logger.warning("  No STW data in yf_daily_prices — "
                   "run fetch_prices_local.py first. ASX 200 alpha unavailable.")
    return []


async def _load_yf_sectors(session) -> dict[str, str]:
    """Load sector overrides from yf_ticker_sectors table."""
    try:
        result = await session.execute(
            text("SELECT ticker, sector FROM yf_ticker_sectors")
        )
        mapping = {r.ticker: r.sector for r in result.all()}
        if mapping:
            logger.info(f"  Loaded {len(mapping)} sectors from yf_ticker_sectors")
        return mapping
    except Exception:
        return {}


async def _load_yf_market_caps(session) -> dict[str, int]:
    """Load market-cap overrides from yf_company_info table (populated by
    fetch_prices_local.py)."""
    try:
        result = await session.execute(
            text("SELECT ticker, market_cap FROM yf_company_info "
                 "WHERE market_cap IS NOT NULL")
        )
        mapping = {r.ticker: int(r.market_cap) for r in result.all()}
        if mapping:
            logger.info(f"  Loaded {len(mapping)} market caps from yf_company_info")
        return mapping
    except Exception:
        return {}


def _resolve_market_caps(
    companies: dict[str, dict],
    yf_market_caps: dict[str, int],
) -> None:
    """Backfill missing/zero market caps from yf_company_info. Mutates in place."""
    updated = 0
    for ticker, info in companies.items():
        current = info.get("market_cap")
        if current and current > 0:
            continue
        yf_mc = yf_market_caps.get(ticker)
        if yf_mc:
            info["market_cap"] = yf_mc
            updated += 1
    if updated:
        logger.info(f"  Market-cap overrides: {updated} from yf_company_info")


def _resolve_sectors(
    companies: dict[str, dict],
    yf_sectors: dict[str, str],
) -> None:
    """
    Backfill 'Other' sectors using asx200_sectors.json and yf_ticker_sectors.
    Mutates *companies* in place.  No external API calls.
    """
    # 1. asx200_sectors.json
    asx200_path = Path("/app/data/asx200_sectors.json")
    if not asx200_path.exists():
        asx200_path = (
            Path(__file__).resolve().parents[3] / "data" / "asx200_sectors.json"
        )

    asx200_map: dict[str, str] = {}
    if asx200_path.exists():
        try:
            entries = json.loads(asx200_path.read_text())
            asx200_map = {e["ticker"]: e["sector"] for e in entries}
        except Exception:
            pass

    updated_json = 0
    updated_yf = 0
    for ticker, info in companies.items():
        if info["sector"] != "Other":
            continue
        if ticker in asx200_map:
            info["sector"] = asx200_map[ticker]
            updated_json += 1
        elif ticker in yf_sectors:
            info["sector"] = yf_sectors[ticker]
            updated_yf += 1

    if updated_json or updated_yf:
        logger.info(f"  Sector overrides: {updated_json} from asx200_sectors.json, "
                    f"{updated_yf} from yf_ticker_sectors")


# ── Cluster detection ────────────────────────────────────────────────────────

def _qualify_trades(
    trades: list[dict],
    price_map: dict[str, PriceSeries],
) -> list[dict]:
    """
    Return trades whose AUD value >= MIN_TRADE_VALUE_AUD.

    For trades missing price_per_share, attempt to infer value from the
    close price in price_snapshots on the trade date.
    """
    qualified: list[dict] = []
    inferred = 0

    for t in trades:
        value = t["value_aud"]

        # Try to infer value from price_snapshots when price_per_share is NULL
        if value is None and t["quantity"] > 0:
            series = price_map.get(t["ticker"], [])
            p = _price_at(series, t["date_of_trade"], "backward")
            if p is None:
                p = _price_at(series, t["date_of_trade"], "forward")
            if p:
                value = p * t["quantity"]
                t = {**t, "value_aud": value, "price": p}
                inferred += 1

        if value is not None and value >= MIN_TRADE_VALUE_AUD:
            qualified.append(t)

    if inferred:
        logger.info(f"  Inferred value for {inferred} trades from price_snapshots")
    return qualified


def _detect_clusters(
    qualified_trades: list[dict],
    companies: dict[str, dict],
    calendar: list[date],
) -> list[dict]:
    """
    Detect cluster-buy events using a greedy, forward-scanning algorithm.

    For each ticker (sorted by trade date):
      1. Anchor on the earliest unclaimed trade.
      2. Set window = anchor + 20 trading days.
      3. Collect all subsequent unclaimed trades within the window.
      4. If a NEW director appears, extend window from that trade's date.
      5. If 2+ distinct directors found, emit cluster; claim all trades.
      6. Apply the 0.05 % market-cap filter (skip if market_cap unknown).
    """
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in qualified_trades:
        by_ticker[t["ticker"]].append(t)

    clusters: list[dict] = []

    for ticker in sorted(by_ticker):
        ticker_trades = sorted(by_ticker[ticker], key=lambda t: t["date_of_trade"])
        claimed: set[int] = set()

        for i in range(len(ticker_trades)):
            if i in claimed:
                continue

            anchor = ticker_trades[i]
            window_end = _advance_trading_days(
                calendar, anchor["date_of_trade"], CLUSTER_WINDOW_TRADING_DAYS,
            )

            cluster_trades = [anchor]
            cluster_indices = {i}
            directors: set[str] = {anchor["director_id"]}

            j = i + 1
            while j < len(ticker_trades):
                if j in claimed:
                    j += 1
                    continue
                t = ticker_trades[j]
                if t["date_of_trade"] > window_end:
                    break

                cluster_trades.append(t)
                cluster_indices.add(j)

                if t["director_id"] not in directors:
                    directors.add(t["director_id"])
                    # Extend window when a new director enters
                    new_end = _advance_trading_days(
                        calendar, t["date_of_trade"], CLUSTER_WINDOW_TRADING_DAYS,
                    )
                    if new_end > window_end:
                        window_end = new_end

                j += 1

            if len(directors) < MIN_DIRECTORS:
                continue

            # Mark trades as consumed
            for idx in cluster_indices:
                claimed.add(idx)

            total_value = sum(t["value_aud"] for t in cluster_trades)
            company = companies.get(ticker, {})
            mc = company.get("market_cap")
            pct_mc = (total_value / mc) if mc else None

            # Apply market-cap filter (pass through if market_cap unknown)
            if pct_mc is not None and pct_mc < MIN_PCT_MARKET_CAP:
                continue

            disclosure_dates = [
                t["date_lodged"] for t in cluster_trades if t.get("date_lodged")
            ]

            clusters.append({
                "ticker": ticker,
                "sector": company.get("sector", "Other"),
                "market_cap": mc,
                "cap_bucket": _cap_bucket(mc),
                "start_date": min(t["date_of_trade"] for t in cluster_trades),
                "end_date": max(t["date_of_trade"] for t in cluster_trades),
                "director_count": len(directors),
                "total_value_aud": round(total_value, 2),
                "pct_market_cap": pct_mc,
                "max_disclosure_date": max(disclosure_dates) if disclosure_dates else None,
            })

    # Sort chronologically and assign sequential IDs
    clusters.sort(key=lambda c: c["start_date"])
    for i, c in enumerate(clusters, 1):
        c["cluster_id"] = i

    return clusters


# ── Return computation ───────────────────────────────────────────────────────

def _compute_return(
    series: PriceSeries, entry_date: date, horizon_days: int,
) -> tuple[float | None, float | None]:
    """Return (entry_price, return_fraction) or (None, None)."""
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
    """Equal-weight sector return, excluding the cluster ticker."""
    tickers_data = sector_prices.get(sector, {})
    returns: list[float] = []
    for t, series in tickers_data.items():
        if t == exclude_ticker:
            continue
        _, ret = _compute_return(series, entry_date, horizon)
        if ret is not None:
            returns.append(ret)
    if len(returns) < 3:
        return None
    return sum(returns) / len(returns)


def _compute_all_returns(
    clusters: list[dict],
    price_map: dict[str, PriceSeries],
    asx200_series: PriceSeries,
    sector_prices: dict[str, dict[str, PriceSeries]],
    calendar: list[date],
) -> list[dict]:
    """Compute absolute returns + alpha for every cluster × entry-method × horizon."""
    results: list[dict] = []

    for c in clusters:
        series = price_map.get(c["ticker"], [])

        row: dict = {
            "cluster_id": c["cluster_id"],
            "ticker": c["ticker"],
            "sector": c["sector"],
            "cap_bucket": c["cap_bucket"],
            "start_date": c["start_date"],
            "end_date": c["end_date"],
            "director_count": c["director_count"],
            "total_value_aud": c["total_value_aud"],
            "pct_market_cap": c["pct_market_cap"],
        }

        # ── Trade-date entry (close on cluster end date) ─────────────────
        trade_entry = c["end_date"]
        for h in RETURN_HORIZONS:
            _, ret = _compute_return(series, trade_entry, h)
            row[f"return_{h}d_trade"] = ret

            _, bench = _compute_return(asx200_series, trade_entry, h)
            row[f"alpha_asx200_{h}d_trade"] = (
                ret - bench if ret is not None and bench is not None else None
            )

            sec = _sector_return(sector_prices, c["sector"], c["ticker"], trade_entry, h)
            row[f"alpha_sector_{h}d_trade"] = (
                ret - sec if ret is not None and sec is not None else None
            )

        # ── Disclosure-date entry (close on max disclosure + 1 td) ───────
        disc = c["max_disclosure_date"]
        disc_entry = _next_trading_day(calendar, disc) if disc else None

        for h in RETURN_HORIZONS:
            if disc_entry:
                _, ret = _compute_return(series, disc_entry, h)
            else:
                ret = None
            row[f"return_{h}d_disclosure"] = ret

            if disc_entry:
                _, bench = _compute_return(asx200_series, disc_entry, h)
            else:
                bench = None
            row[f"alpha_asx200_{h}d_disclosure"] = (
                ret - bench if ret is not None and bench is not None else None
            )

            if disc_entry:
                sec = _sector_return(
                    sector_prices, c["sector"], c["ticker"], disc_entry, h,
                )
            else:
                sec = None
            row[f"alpha_sector_{h}d_disclosure"] = (
                ret - sec if ret is not None and sec is not None else None
            )

        results.append(row)

    return results


# ── CSV output ───────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    "cluster_id", "ticker", "sector", "start_date", "end_date",
    "director_count", "total_value_aud", "pct_market_cap",
    "return_90d_trade", "return_180d_trade",
    "return_90d_disclosure", "return_180d_disclosure",
    "alpha_asx200_90d_trade", "alpha_asx200_180d_trade",
    "alpha_asx200_90d_disclosure", "alpha_asx200_180d_disclosure",
    "alpha_sector_90d_trade", "alpha_sector_180d_trade",
    "alpha_sector_90d_disclosure", "alpha_sector_180d_disclosure",
]


def _write_csv(results: list[dict], path: str | None) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()

    for r in results:
        row: dict = {}
        for col in CSV_COLUMNS:
            val = r.get(col)
            if isinstance(val, float):
                row[col] = f"{val:.6f}"
            elif val is None:
                row[col] = ""
            else:
                row[col] = val
        writer.writerow(row)

    text = buf.getvalue()
    if path:
        Path(path).write_text(text)
        logger.info(f"CSV written to {path}")
    else:
        sys.stdout.write(text)


# ── Stats output ─────────────────────────────────────────────────────────────

def _print_stats(results: list[dict]) -> None:
    total = len(results)

    print("\n" + "=" * 80, file=sys.stderr)
    print("CLUSTER BUY BACKTEST — SUMMARY STATISTICS", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"\nTotal clusters identified: {total}", file=sys.stderr)

    if total == 0:
        print("No clusters found. Check data filters.", file=sys.stderr)
        return

    # Filter params recap
    print(
        f"  Window: {CLUSTER_WINDOW_TRADING_DAYS} trading days  |  "
        f"Min directors: {MIN_DIRECTORS}  |  "
        f"Min trade value: ${MIN_TRADE_VALUE_AUD:,}  |  "
        f"Min % mkt cap: {MIN_PCT_MARKET_CAP*100:.2f}%",
        file=sys.stderr,
    )

    _print_metric(results, "return_90d_trade",
                  "90-day Absolute Return (trade-date)")
    _print_metric(results, "return_180d_trade",
                  "180-day Absolute Return (trade-date)")
    _print_metric(results, "alpha_asx200_90d_trade",
                  "90-day Alpha vs ASX 200 (trade-date)")
    _print_metric(results, "alpha_asx200_180d_trade",
                  "180-day Alpha vs ASX 200 (trade-date)")

    # ── By sector ────────────────────────────────────────────────────────
    print("\n" + "-" * 80, file=sys.stderr)
    print("BY SECTOR (90d alpha vs ASX 200, trade-date entry)", file=sys.stderr)
    print("-" * 80, file=sys.stderr)

    by_sector: dict[str, list] = defaultdict(list)
    for r in results:
        by_sector[r["sector"]].append(r)
    for sector in sorted(by_sector):
        _print_group(by_sector[sector], "alpha_asx200_90d_trade", sector)

    # ── By market-cap bucket ─────────────────────────────────────────────
    print("\n" + "-" * 80, file=sys.stderr)
    print("BY MARKET CAP BUCKET (90d alpha vs ASX 200, trade-date entry)", file=sys.stderr)
    print("-" * 80, file=sys.stderr)

    by_cap: dict[str, list] = defaultdict(list)
    for r in results:
        by_cap[r.get("cap_bucket", "unknown")].append(r)
    for bucket in ["nano <$50m", "micro $50-300m", "small $300m-2b",
                    "mid+ $2b+", "unknown"]:
        if bucket in by_cap:
            _print_group(by_cap[bucket], "alpha_asx200_90d_trade", bucket)

    print("\n" + "=" * 80, file=sys.stderr)


def _print_metric(results: list[dict], key: str, label: str) -> None:
    values = [r[key] for r in results if r.get(key) is not None]
    if not values:
        print(f"\n  {label}: insufficient data", file=sys.stderr)
        return
    print(f"\n  {label}  (n={len(values)}/{len(results)})", file=sys.stderr)
    print(f"    Mean:     {statistics.mean(values)*100:+.2f}%", file=sys.stderr)
    print(f"    Median:   {statistics.median(values)*100:+.2f}%", file=sys.stderr)
    if len(values) >= 4:
        q = statistics.quantiles(values, n=4)
        print(f"    IQR:      [{q[0]*100:+.2f}%, {q[2]*100:+.2f}%]", file=sys.stderr)
    pos = sum(1 for v in values if v > 0)
    print(f"    Positive: {pos}/{len(values)} ({100*pos/len(values):.1f}%)", file=sys.stderr)


def _print_group(group: list[dict], key: str, label: str) -> None:
    values = [r[key] for r in group if r.get(key) is not None]
    if not values:
        print(f"  {label:28s}  n={len(group):3d}  (no return data)", file=sys.stderr)
        return
    med = statistics.median(values) * 100
    mean = statistics.mean(values) * 100
    pos = sum(1 for v in values if v > 0)
    pct_pos = 100 * pos / len(values)
    iqr_str = ""
    if len(values) >= 4:
        q = statistics.quantiles(values, n=4)
        iqr_str = f"  IQR [{q[0]*100:+.1f}%, {q[2]*100:+.1f}%]"
    print(
        f"  {label:28s}  n={len(values):3d}  mean={mean:+6.1f}%  "
        f"med={med:+6.1f}%  pos={pct_pos:4.0f}%{iqr_str}",
        file=sys.stderr,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

async def run(
    csv_path: str | None = None,
    sector_filter: list[str] | None = None,
) -> list[dict]:
    async with async_session() as session:

        # 1. Trading calendar
        logger.info("Loading trading calendar ...")
        calendar = await _load_trading_calendar(session)
        if not calendar:
            print("ERROR: No price data found. Run fetch_prices_local.py "
                  "or sync_price_data first.", file=sys.stderr)
            return []
        logger.info(f"  {len(calendar)} trading days  "
                     f"({calendar[0]} to {calendar[-1]})")

        # 2. All on-market buy trades + company metadata
        logger.info("Loading on-market buy trades ...")
        trades, companies = await _load_on_market_buys(session)
        logger.info(f"  {len(trades):,} on-market buy trades across "
                     f"{len(companies):,} companies")

        # 3. Price data (yf_daily_prices → price_snapshots fallback)
        trade_tickers = {t["ticker"] for t in trades}
        logger.info(f"Loading prices for {len(trade_tickers):,} tickers ...")
        price_map = await _load_prices(session, trade_tickers)
        logger.info(f"  {len(price_map):,}/{len(trade_tickers):,} tickers "
                     "have price data")

        # 4. Resolve sectors (asx200_sectors.json + yf_ticker_sectors)
        logger.info("Resolving sectors ...")
        yf_sectors = await _load_yf_sectors(session)
        _resolve_sectors(companies, yf_sectors)

        # 4b. Resolve market caps (yf_company_info backfill)
        logger.info("Resolving market caps ...")
        yf_market_caps = await _load_yf_market_caps(session)
        _resolve_market_caps(companies, yf_market_caps)

        # 5. Qualify trades by $20k value
        logger.info("Qualifying trades (>= $20k) ...")
        qualified = _qualify_trades(trades, price_map)
        logger.info(f"  {len(qualified):,} qualifying trades")

        # 6. Detect clusters
        logger.info("Detecting clusters ...")
        clusters = _detect_clusters(qualified, companies, calendar)
        logger.info(f"  {len(clusters)} clusters found")
        other_count = sum(1 for c in clusters if c["sector"] == "Other")
        if other_count:
            logger.info(f"  ({other_count} clusters still sector 'Other')")

        unknown_mc = sum(1 for c in clusters if c["cap_bucket"] == "unknown")
        if unknown_mc:
            logger.info(f"  ({unknown_mc}/{len(clusters)} clusters have unknown market cap)")

        # 6b. Apply sector filter if provided
        if sector_filter:
            want = {s.strip().lower() for s in sector_filter}
            before = len(clusters)
            clusters = [c for c in clusters if c["sector"].lower() in want]
            logger.info(
                f"  Sector filter {sorted(want)}: {len(clusters)}/{before} clusters retained"
            )

        if not clusters:
            print("No clusters found matching the criteria.", file=sys.stderr)
            return []

        # 7. ASX 200 benchmark (STW from yf_daily_prices)
        logger.info("Loading ASX 200 benchmark (STW) ...")
        asx200_series = await _load_benchmark(session)

        # 8. Sector price data for equal-weight sector benchmarks
        cluster_sectors = {c["sector"] for c in clusters}
        sector_tickers_map: dict[str, set[str]] = defaultdict(set)
        for ticker, info in companies.items():
            s = info.get("sector", "Other")
            if s in cluster_sectors:
                sector_tickers_map[s].add(ticker)

        sector_missing = set()
        for tset in sector_tickers_map.values():
            sector_missing |= tset
        sector_missing -= set(price_map.keys())
        if sector_missing:
            logger.info(f"Loading {len(sector_missing)} extra sector-ticker prices ...")
            extra = await _load_prices(session, sector_missing)
            price_map.update(extra)

        sector_prices: dict[str, dict[str, PriceSeries]] = {}
        for sector, tset in sector_tickers_map.items():
            sector_prices[sector] = {
                t: price_map[t] for t in tset if t in price_map
            }

        # 9. Compute returns and alphas
        logger.info("Computing returns and alphas ...")
        results = _compute_all_returns(
            clusters, price_map, asx200_series, sector_prices, calendar,
        )

    # 10. Output
    _write_csv(results, csv_path)
    _print_stats(results)

    return results


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cluster Buy Backtest Engine",
    )
    parser.add_argument(
        "--csv",
        metavar="PATH",
        help="Write CSV results to PATH (default: stdout)",
    )
    parser.add_argument(
        "--sectors",
        metavar="LIST",
        help='Comma-separated list of sectors to include, e.g. '
             '"Basic Materials,Energy,Financial Services,Industrials"',
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    sector_filter: list[str] | None = None
    if args.sectors:
        sector_filter = [s.strip() for s in args.sectors.split(",") if s.strip()]

    asyncio.run(run(csv_path=args.csv, sector_filter=sector_filter))


if __name__ == "__main__":
    main()
