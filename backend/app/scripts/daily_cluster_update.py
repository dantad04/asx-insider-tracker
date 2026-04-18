"""
Daily Cluster Update Pipeline

Runs once daily after ASX market close (~5:30 PM AEST).
Four steps in sequence; each step is independent — a failure in one
step logs the error but does not block the others.

  1. Price update   — fetch recent closes via yfinance into yf_daily_prices
  2. Cluster detect  — re-detect clusters in a lookback window
  3. Return compute  — compute/recompute returns for all eligible clusters
  4. Status refresh  — update clusters.status where it has drifted

Usage:
  docker-compose exec backend python -m app.scripts.daily_cluster_update
  docker-compose exec backend python -m app.scripts.daily_cluster_update --skip-prices
  docker-compose exec backend python -m app.scripts.daily_cluster_update --lookback-days 30
  docker-compose exec backend python -m app.scripts.daily_cluster_update --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date

from sqlalchemy import text

from app.database import async_session
from app.scripts.detect_clusters import (
    run as detect_run,
    _compute_status,
    _load_trading_calendar,
)
from app.scripts.compute_cluster_returns import run as compute_run

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 90
PRICE_BATCH_SIZE = 10
PRICE_BATCH_DELAY = 2  # seconds between batches


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _count(session, table: str) -> int:
    r = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return r.scalar_one()


# ── Step 1: Price Update ────────────────────────────────────────────────────

def _yf_history(yf_code: str):
    """Sync helper — called inside asyncio.to_thread."""
    import yfinance as yf
    return yf.Ticker(yf_code).history(period="5d")


async def _step_prices(dry_run: bool) -> dict:
    result = {"attempted": 0, "updated": 0, "failed": 0, "skipped": False}

    try:
        import yfinance  # noqa: F401 — just testing availability
    except ImportError:
        logger.warning("  yfinance not available — skipping price update")
        result["skipped"] = True
        return result

    async with async_session() as session:
        r = await session.execute(
            text("SELECT DISTINCT ticker FROM yf_daily_prices")
        )
        tickers = [row.ticker for row in r.all()]
    if "STW" not in tickers:
        tickers.append("STW")

    result["attempted"] = len(tickers)

    if dry_run:
        logger.info(f"  [DRY RUN] Would fetch prices for {len(tickers)} tickers")
        return result

    total_batches = (len(tickers) + PRICE_BATCH_SIZE - 1) // PRICE_BATCH_SIZE

    async with async_session() as session:
        for batch_start in range(0, len(tickers), PRICE_BATCH_SIZE):
            batch = tickers[batch_start:batch_start + PRICE_BATCH_SIZE]

            for ticker in batch:
                fetched = False
                for attempt in range(2):
                    try:
                        hist = await asyncio.to_thread(_yf_history, f"{ticker}.AX")
                        if hist.empty:
                            break
                        for ts, row in hist.iterrows():
                            d = ts.date() if hasattr(ts, "date") else ts
                            await session.execute(
                                text(
                                    "INSERT INTO yf_daily_prices (ticker, date, close) "
                                    "VALUES (:t, :d, :c) "
                                    "ON CONFLICT (ticker, date) "
                                    "DO UPDATE SET close = EXCLUDED.close"
                                ),
                                {"t": ticker, "d": d, "c": round(float(row["Close"]), 4)},
                            )
                        fetched = True
                        result["updated"] += 1
                        break
                    except Exception as e:
                        if attempt == 0:
                            await asyncio.sleep(1)
                        else:
                            logger.debug(f"    FAIL {ticker}: {e}")

                if not fetched:
                    result["failed"] += 1

            try:
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.warning(f"  Batch commit failed: {e}")

            batch_num = batch_start // PRICE_BATCH_SIZE + 1
            if batch_num % 10 == 0 or batch_num == total_batches:
                logger.info(
                    f"  Prices: batch {batch_num}/{total_batches} "
                    f"({result['updated']} ok, {result['failed']} failed)"
                )

            await asyncio.sleep(PRICE_BATCH_DELAY)

    return result


# ── Step 2: Cluster Detection ───────────────────────────────────────────────

async def _step_detect(lookback_days: int, dry_run: bool) -> dict:
    async with async_session() as session:
        before_c = await _count(session, "clusters")
        before_t = await _count(session, "cluster_trades")

    if dry_run:
        logger.info(
            f"  [DRY RUN] Would re-detect clusters (lookback={lookback_days}d). "
            f"Current: {before_c} clusters, {before_t} cluster_trades"
        )
        return {
            "clusters_before": before_c,
            "clusters_after": before_c,
            "new_clusters": 0,
            "dry_run": True,
        }

    await detect_run(lookback_days=lookback_days)

    async with async_session() as session:
        after_c = await _count(session, "clusters")
        after_t = await _count(session, "cluster_trades")

    return {
        "clusters_before": before_c,
        "clusters_after": after_c,
        "new_clusters": after_c - before_c,
        "trades_before": before_t,
        "trades_after": after_t,
    }


# ── Step 3: Return Computation ──────────────────────────────────────────────

async def _step_returns(dry_run: bool) -> dict:
    async with async_session() as session:
        before = await _count(session, "cluster_returns")

    if dry_run:
        logger.info(
            f"  [DRY RUN] Would compute returns. "
            f"Current: {before} cluster_returns rows"
        )
        return {
            "returns_before": before,
            "returns_after": before,
            "new_returns": 0,
            "dry_run": True,
        }

    await compute_run()

    async with async_session() as session:
        after = await _count(session, "cluster_returns")

    return {
        "returns_before": before,
        "returns_after": after,
        "new_returns": after - before,
    }


# ── Step 4: Status Refresh ──────────────────────────────────────────────────

async def _step_status_refresh(dry_run: bool) -> dict:
    transitions: dict[str, int] = {}
    total_changed = 0

    async with async_session() as session:
        calendar = await _load_trading_calendar(session)
        if not calendar:
            logger.warning("  No trading calendar — skipping status refresh")
            return {"changed": 0, "transitions": {}}

        today = date.today()

        result = await session.execute(
            text("SELECT cluster_id, end_date, status FROM clusters")
        )
        rows = result.all()

        updates: list[tuple[int, str]] = []
        for row in rows:
            new_status = _compute_status(row.end_date, calendar, today)
            if new_status != row.status:
                key = f"{row.status}→{new_status}"
                transitions[key] = transitions.get(key, 0) + 1
                total_changed += 1
                updates.append((row.cluster_id, new_status))

        if dry_run:
            logger.info(f"  [DRY RUN] {total_changed} clusters would change status")
            for t, n in transitions.items():
                logger.info(f"    {t}: {n}")
            return {"changed": total_changed, "transitions": transitions, "dry_run": True}

        if updates:
            for cid, new_status in updates:
                await session.execute(
                    text(
                        "UPDATE clusters SET status = :status, updated_at = NOW() "
                        "WHERE cluster_id = :cid"
                    ),
                    {"status": new_status, "cid": cid},
                )
            await session.commit()

    return {"changed": total_changed, "transitions": transitions}


# ── Orchestrator ─────────────────────────────────────────────────────────────

async def run(
    skip_prices: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    dry_run: bool = False,
) -> dict:
    summary: dict = {}
    mode = "[DRY RUN] " if dry_run else ""

    if skip_prices:
        logger.info(f"{mode}STEP 1 — Price update: SKIPPED (--skip-prices)")
    else:
        logger.info(f"{mode}STEP 1 — Price update")
        try:
            summary["prices"] = await _step_prices(dry_run)
        except Exception as e:
            logger.error(f"  STEP 1 FAILED: {e}")
            summary["prices"] = {"error": str(e)}

    logger.info(f"{mode}STEP 2 — Cluster detection (lookback={lookback_days}d)")
    try:
        summary["detect"] = await _step_detect(lookback_days, dry_run)
    except Exception as e:
        logger.error(f"  STEP 2 FAILED: {e}")
        summary["detect"] = {"error": str(e)}

    logger.info(f"{mode}STEP 3 — Return computation")
    try:
        summary["returns"] = await _step_returns(dry_run)
    except Exception as e:
        logger.error(f"  STEP 3 FAILED: {e}")
        summary["returns"] = {"error": str(e)}

    logger.info(f"{mode}STEP 4 — Status refresh")
    try:
        summary["status"] = await _step_status_refresh(dry_run)
    except Exception as e:
        logger.error(f"  STEP 4 FAILED: {e}")
        summary["status"] = {"error": str(e)}

    _print_summary(summary, dry_run)
    return summary


# ── Summary ──────────────────────────────────────────────────────────────────

def _print_summary(summary: dict, dry_run: bool) -> None:
    mode = "[DRY RUN] " if dry_run else ""
    print(f"\n{'='*60}", file=sys.stderr)
    print(f"{mode}DAILY CLUSTER UPDATE — SUMMARY", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    p = summary.get("prices")
    if p is None:
        print("  Prices: skipped (--skip-prices)", file=sys.stderr)
    elif p.get("skipped"):
        print("  Prices: skipped (yfinance unavailable)", file=sys.stderr)
    elif p.get("error"):
        print(f"  Prices: FAILED — {p['error']}", file=sys.stderr)
    else:
        print(
            f"  Prices: {p.get('updated', 0)}/{p.get('attempted', 0)} "
            f"tickers updated ({p.get('failed', 0)} failed)",
            file=sys.stderr,
        )

    d = summary.get("detect", {})
    if d.get("error"):
        print(f"  Detection: FAILED — {d['error']}", file=sys.stderr)
    else:
        print(
            f"  Detection: {d.get('clusters_after', '?')} total clusters "
            f"({d.get('new_clusters', 0):+d} net change)",
            file=sys.stderr,
        )

    r = summary.get("returns", {})
    if r.get("error"):
        print(f"  Returns: FAILED — {r['error']}", file=sys.stderr)
    else:
        print(
            f"  Returns: {r.get('returns_after', '?')} total rows "
            f"({r.get('new_returns', 0):+d} net change)",
            file=sys.stderr,
        )

    s = summary.get("status", {})
    if s.get("error"):
        print(f"  Status: FAILED — {s['error']}", file=sys.stderr)
    else:
        changed = s.get("changed", 0)
        if changed:
            parts = [f"{n} {t}" for t, n in s.get("transitions", {}).items()]
            print(
                f"  Status: {changed} clusters updated ({', '.join(parts)})",
                file=sys.stderr,
            )
        else:
            print("  Status: no changes", file=sys.stderr)

    print(f"{'='*60}\n", file=sys.stderr)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily cluster update pipeline: price update → "
                    "cluster detection → return computation → status refresh.",
    )
    parser.add_argument(
        "--skip-prices", action="store_true",
        help="Skip step 1 (price update).",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"Detection lookback window in days (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Detect and compute but don't write to database.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    try:
        asyncio.run(run(
            skip_prices=args.skip_prices,
            lookback_days=args.lookback_days,
            dry_run=args.dry_run,
        ))
    except Exception:
        logger.exception("Daily cluster update failed with unhandled exception")
        sys.exit(1)


if __name__ == "__main__":
    main()
