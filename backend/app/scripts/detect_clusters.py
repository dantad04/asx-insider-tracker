"""
Cluster Detection (Phase 2 — writes to clusters + cluster_trades)

Splits out the detection half of cluster_backtest.py. Detection logic is
copied verbatim — do not modify. Returns are computed separately by
compute_cluster_returns.py.

Resolution chain per cluster row:
  sector         : companies.sector → asx200_sectors.json → yf_ticker_sectors
                   → NULL (matches cluster_backtest.py precedence)
  industry       : yf_company_info.industry → NULL
  market_cap_aud : companies.market_cap → yf_company_info.market_cap → NULL
  market_cap_bucket : derived from resolved market_cap_aud

Idempotency: for each detected cluster, DELETE by (ticker, start_date,
end_date) then INSERT. ON DELETE CASCADE wipes cluster_trades + cluster_returns
children.  Re-running with unchanged source data produces zero net change.

Usage:
  docker-compose exec backend python -m app.scripts.detect_clusters
  docker-compose exec backend python -m app.scripts.detect_clusters --lookback-days 30
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

from sqlalchemy import select, text

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.trade import Trade, TradeType

logger = logging.getLogger(__name__)

# ── Constants (copied verbatim from cluster_backtest.py) ─────────────────────

CLUSTER_WINDOW_TRADING_DAYS = 20
MIN_DIRECTORS = 2
MIN_TRADE_VALUE_AUD = 20_000
MIN_PCT_MARKET_CAP = 0.0005  # 0.05%
PRICE_LOOKUP_TOLERANCE = 7   # max calendar-day slippage for price lookups

# Status thresholds (trading days for active/maturing, calendar days for
# historical to match the 90d return horizon convention).
STATUS_ACTIVE_MAX_TD = 20
STATUS_MATURING_MAX_TD = 90
STATUS_HISTORICAL_CAL_DAYS = 90

PriceSeries = list[tuple[date, float]]


# ── Market-cap buckets (copied verbatim from cluster_backtest.py) ────────────

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


def _advance_trading_days(calendar: list[date], from_date: date, n: int) -> date:
    idx = bisect.bisect_left(calendar, from_date)
    target = idx + n
    if target >= len(calendar):
        overshoot = target - len(calendar) + 1
        return calendar[-1] + timedelta(days=int(overshoot * 1.4) + 1)
    return calendar[target]


# ── Data loading (mirrors cluster_backtest.py) ───────────────────────────────

async def _load_trading_calendar(session) -> list[date]:
    try:
        result = await session.execute(
            text("SELECT DISTINCT date FROM yf_daily_prices "
                 "WHERE ticker = 'STW' ORDER BY date")
        )
        dates = [r.date for r in result.all()]
        if dates:
            return dates
    except Exception:
        pass
    from app.models.price_snapshot import PriceSnapshot
    result = await session.execute(
        select(PriceSnapshot.date).distinct().order_by(PriceSnapshot.date)
    )
    return [r.date for r in result.all()]


async def _load_on_market_buys(session):
    """Load on-market buy trades. Includes Trade.id so we can build the
    cluster_trades junction after detection."""
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
            "trade_id": str(r.id),
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
    if not tickers:
        return {}
    price_map: dict[str, PriceSeries] = defaultdict(list)
    try:
        result = await session.execute(
            text("SELECT ticker, date, close FROM yf_daily_prices "
                 "WHERE ticker = ANY(:tickers) ORDER BY ticker, date"),
            {"tickers": list(tickers)},
        )
        for r in result.all():
            price_map[r.ticker].append((r.date, float(r.close)))
    except Exception:
        pass

    missing = tickers - set(price_map.keys())
    if missing:
        from app.models.price_snapshot import PriceSnapshot
        result = await session.execute(
            select(PriceSnapshot.ticker, PriceSnapshot.date, PriceSnapshot.close)
            .where(PriceSnapshot.ticker.in_(list(missing)))
            .order_by(PriceSnapshot.ticker, PriceSnapshot.date)
        )
        for r in result.all():
            price_map[r.ticker].append((r.date, float(r.close)))
    return dict(price_map)


async def _load_yf_sectors(session) -> dict[str, str]:
    try:
        result = await session.execute(
            text("SELECT ticker, sector FROM yf_ticker_sectors")
        )
        return {r.ticker: r.sector for r in result.all()}
    except Exception:
        return {}


async def _load_yf_market_caps(session) -> dict[str, int]:
    try:
        result = await session.execute(
            text("SELECT ticker, market_cap FROM yf_company_info "
                 "WHERE market_cap IS NOT NULL")
        )
        return {r.ticker: int(r.market_cap) for r in result.all()}
    except Exception:
        return {}


async def _load_yf_industries(session) -> dict[str, str]:
    try:
        result = await session.execute(
            text("SELECT ticker, industry FROM yf_company_info "
                 "WHERE industry IS NOT NULL")
        )
        return {r.ticker: r.industry for r in result.all()}
    except Exception:
        return {}


def _load_asx200_sectors() -> dict[str, str]:
    asx200_path = Path("/app/data/asx200_sectors.json")
    if not asx200_path.exists():
        asx200_path = Path(__file__).resolve().parents[3] / "data" / "asx200_sectors.json"
    if not asx200_path.exists():
        return {}
    try:
        entries = json.loads(asx200_path.read_text())
        return {e["ticker"]: e["sector"] for e in entries}
    except Exception:
        return {}


def _resolve_market_caps(companies: dict[str, dict], yf_market_caps: dict[str, int]) -> None:
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
    asx200_map: dict[str, str],
    yf_sectors: dict[str, str],
) -> None:
    """Same precedence as cluster_backtest.py: companies.sector (if not 'Other')
    → asx200_sectors.json → yf_ticker_sectors."""
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


# ── Trade qualification (copied verbatim from cluster_backtest.py) ───────────

def _qualify_trades(
    trades: list[dict],
    price_map: dict[str, PriceSeries],
) -> list[dict]:
    qualified: list[dict] = []
    inferred = 0

    for t in trades:
        value = t["value_aud"]

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


# ── Cluster detection (copied VERBATIM from cluster_backtest.py) ─────────────

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
                    new_end = _advance_trading_days(
                        calendar, t["date_of_trade"], CLUSTER_WINDOW_TRADING_DAYS,
                    )
                    if new_end > window_end:
                        window_end = new_end

                j += 1

            if len(directors) < MIN_DIRECTORS:
                continue

            for idx in cluster_indices:
                claimed.add(idx)

            total_value = sum(t["value_aud"] for t in cluster_trades)
            company = companies.get(ticker, {})
            mc = company.get("market_cap")
            pct_mc = (total_value / mc) if mc else None

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
                "trade_ids": [t["trade_id"] for t in cluster_trades],
            })

    clusters.sort(key=lambda c: c["start_date"])
    for i, c in enumerate(clusters, 1):
        c["cluster_id"] = i

    return clusters


# ── Status computation (trading-day aware) ───────────────────────────────────

def _trading_days_since(calendar: list[date], ref: date, today: date) -> int:
    start_idx = bisect.bisect_right(calendar, ref)
    end_idx = bisect.bisect_right(calendar, today)
    return max(0, end_idx - start_idx)


def _compute_status(end_date: date, calendar: list[date], today: date) -> str:
    cal_days = (today - end_date).days
    if cal_days >= STATUS_HISTORICAL_CAL_DAYS:
        return "historical"
    td_since = _trading_days_since(calendar, end_date, today)
    if td_since <= STATUS_ACTIVE_MAX_TD:
        return "active"
    return "maturing"


# ── Persistence (idempotent per-cluster delete-then-insert) ──────────────────

async def _write_clusters(
    session,
    detected: list[dict],
    industries: dict[str, str],
    calendar: list[date],
    today: date,
) -> tuple[int, int, int]:
    """
    For each detected cluster:
      DELETE FROM clusters WHERE ticker=? AND start_date=? AND end_date=?
      INSERT new row, capture SERIAL cluster_id
      bulk INSERT cluster_trades for every trade_id in the cluster.

    Returns (inserted_clusters, inserted_trade_links, deleted_existing).
    """
    inserted_clusters = 0
    inserted_trade_links = 0
    deleted_existing = 0

    for c in detected:
        # Normalise final column values for storage.
        sector = c["sector"] if c["sector"] != "Other" else None
        industry = industries.get(c["ticker"])
        market_cap_aud = c["market_cap"]
        bucket = c["cap_bucket"]
        status = _compute_status(c["end_date"], calendar, today)

        del_result = await session.execute(
            text(
                "DELETE FROM clusters "
                "WHERE ticker = :ticker "
                "  AND start_date = :start_date "
                "  AND end_date = :end_date"
            ),
            {
                "ticker": c["ticker"],
                "start_date": c["start_date"],
                "end_date": c["end_date"],
            },
        )
        if del_result.rowcount:
            deleted_existing += del_result.rowcount

        ins = await session.execute(
            text(
                "INSERT INTO clusters ("
                "  ticker, sector, industry, market_cap_bucket, market_cap_aud, "
                "  start_date, end_date, max_disclosure_date, director_count, "
                "  total_value_aud, pct_market_cap, status"
                ") VALUES ("
                "  :ticker, :sector, :industry, :bucket, :market_cap_aud, "
                "  :start_date, :end_date, :max_disclosure_date, :director_count, "
                "  :total_value_aud, :pct_market_cap, :status"
                ") RETURNING cluster_id"
            ),
            {
                "ticker": c["ticker"],
                "sector": sector,
                "industry": industry,
                "bucket": bucket,
                "market_cap_aud": market_cap_aud,
                "start_date": c["start_date"],
                "end_date": c["end_date"],
                "max_disclosure_date": c["max_disclosure_date"],
                "director_count": c["director_count"],
                "total_value_aud": c["total_value_aud"],
                "pct_market_cap": c["pct_market_cap"],
                "status": status,
            },
        )
        new_id = ins.scalar_one()
        inserted_clusters += 1

        # Unique per cluster (deduped in case detection produced a dup).
        unique_trade_ids = list({tid for tid in c["trade_ids"]})
        if unique_trade_ids:
            await session.execute(
                text(
                    "INSERT INTO cluster_trades (cluster_id, trade_id) "
                    "VALUES (:cluster_id, :trade_id)"
                ),
                [
                    {"cluster_id": new_id, "trade_id": tid}
                    for tid in unique_trade_ids
                ],
            )
            inserted_trade_links += len(unique_trade_ids)

    return inserted_clusters, inserted_trade_links, deleted_existing


# ── Main ─────────────────────────────────────────────────────────────────────

async def run(lookback_days: int | None = None) -> None:
    async with async_session() as session:
        logger.info("Loading trading calendar ...")
        calendar = await _load_trading_calendar(session)
        if not calendar:
            print("ERROR: No price data found. Run fetch_prices_local.py first.",
                  file=sys.stderr)
            return
        logger.info(f"  {len(calendar)} trading days "
                    f"({calendar[0]} to {calendar[-1]})")

        logger.info("Loading on-market buy trades ...")
        trades, companies = await _load_on_market_buys(session)
        logger.info(f"  {len(trades):,} on-market buy trades across "
                    f"{len(companies):,} companies")

        trade_tickers = {t["ticker"] for t in trades}
        logger.info(f"Loading prices for {len(trade_tickers):,} tickers ...")
        price_map = await _load_prices(session, trade_tickers)
        logger.info(f"  {len(price_map):,}/{len(trade_tickers):,} tickers have price data")

        logger.info("Resolving sectors + market caps + industries ...")
        yf_sectors = await _load_yf_sectors(session)
        asx200_map = _load_asx200_sectors()
        _resolve_sectors(companies, asx200_map, yf_sectors)

        yf_market_caps = await _load_yf_market_caps(session)
        _resolve_market_caps(companies, yf_market_caps)

        industries = await _load_yf_industries(session)
        logger.info(f"  Loaded {len(industries)} industries from yf_company_info")

        logger.info("Qualifying trades (>= $20k) ...")
        qualified = _qualify_trades(trades, price_map)
        logger.info(f"  {len(qualified):,} qualifying trades")

        logger.info("Detecting clusters ...")
        detected = _detect_clusters(qualified, companies, calendar)
        logger.info(f"  {len(detected)} clusters detected")

        today = date.today()

        if lookback_days is not None:
            cutoff = today - timedelta(days=lookback_days)
            before = len(detected)
            detected = [c for c in detected if c["start_date"] >= cutoff]
            logger.info(f"  --lookback-days {lookback_days}: "
                        f"{len(detected)}/{before} clusters in window "
                        f"(start_date >= {cutoff})")

        if not detected:
            logger.info("Nothing to persist.")
            return

        logger.info("Persisting clusters + cluster_trades (per-cluster "
                    "delete-then-insert, single transaction) ...")
        try:
            ins_c, ins_t, del_existing = await _write_clusters(
                session, detected, industries, calendar, today,
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise

        logger.info(f"  Deleted {del_existing} existing cluster rows "
                    f"(cascaded to children)")
        logger.info(f"  Inserted {ins_c} clusters")
        logger.info(f"  Inserted {ins_t} cluster_trades rows")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect director-trade clusters and write to clusters + cluster_trades.",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=None,
        help="Only re-detect clusters whose start_date >= today - N days "
             "(default: all history).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    asyncio.run(run(lookback_days=args.lookback_days))


if __name__ == "__main__":
    main()
