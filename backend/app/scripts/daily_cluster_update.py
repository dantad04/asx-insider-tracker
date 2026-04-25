"""
Daily Cluster Update Pipeline

Runs once daily after ASX market close (~5:30 PM AEST).
Five steps in sequence; each step is independent — a failure in one
step logs the error but does not block the others.

  1. Price update   — fetch recent closes via yfinance into yf_daily_prices
  2. Cluster detect  — re-detect clusters in a lookback window
  3. Return compute  — compute/recompute returns for all eligible clusters
  4. Status refresh  — update clusters.status where it has drifted
  5. Cluster Portfolio — run one rules-based paper portfolio cycle

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
from datetime import date, timedelta

from sqlalchemy import text

from app.config import settings
from app.database import async_session
from app.scripts.detect_clusters import (
    run as detect_run,
    _compute_status,
    _load_trading_calendar,
)
from app.scripts.compute_cluster_returns import run as compute_run
from app.services.cluster_portfolio_runner import run_cluster_portfolio_cycle

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 90
PRICE_BATCH_SIZE = 10
PRICE_BATCH_DELAY = 2  # seconds between batches
PRICE_BOOTSTRAP_STW_MIN_DAYS = 120
PRICE_RECENT_BUY_LOOKBACK_DAYS = 180
PRICE_ABORT_AFTER_INITIAL_FAILURES = 50


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _count(session, table: str) -> int:
    r = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return r.scalar_one()


# ── Step 1: Price Update ────────────────────────────────────────────────────

def _yf_history(yf_code: str, period: str = "5d"):
    """Sync helper — called inside asyncio.to_thread."""
    import yfinance as yf
    return yf.Ticker(yf_code).history(period=period)


async def _price_update_tickers(session) -> tuple[list[str], bool]:
    """Return the stored and recent tickers needed by the cluster pipeline.

    Fresh production databases start with no yf_daily_prices rows. Including
    recent on-market buy tickers lets the cluster detector bootstrap enough
    price coverage to create active/maturing clusters instead of only STW.
    """
    since = date.today() - timedelta(days=PRICE_RECENT_BUY_LOOKBACK_DAYS)

    stw_count = (
        await session.execute(
            text("SELECT COUNT(*) FROM yf_daily_prices WHERE ticker = 'STW'")
        )
    ).scalar_one()
    bootstrap_history = stw_count < PRICE_BOOTSTRAP_STW_MIN_DAYS

    result = await session.execute(
        text(
            """
            SELECT DISTINCT ticker
            FROM (
                SELECT ticker FROM yf_daily_prices
                UNION ALL
                SELECT 'STW' AS ticker
                UNION ALL
                SELECT DISTINCT c.ticker
                FROM trades t
                JOIN companies c ON c.id = t.company_id
                WHERE t.date_of_trade >= :since
                  AND (
                    UPPER(t.trade_type) IN ('ON_MARKET_BUY', 'ONMARKETBUY')
                    OR LOWER(t.trade_type) = 'on_market_buy'
                  )
                UNION ALL
                SELECT ticker
                FROM clusters
                WHERE status IN ('active', 'maturing')
                UNION ALL
                SELECT ticker
                FROM cluster_portfolio_positions
                WHERE status = 'open'
            ) tickers
            WHERE ticker IS NOT NULL AND ticker <> ''
            ORDER BY ticker
            """
        ),
        {"since": since},
    )
    tickers = [row.ticker for row in result.all()]
    if "STW" in tickers:
        tickers = ["STW"] + [ticker for ticker in tickers if ticker != "STW"]
    return tickers, bootstrap_history


async def _step_prices(dry_run: bool) -> dict:
    result = {"attempted": 0, "updated": 0, "failed": 0, "skipped": False}

    try:
        import yfinance  # noqa: F401 — just testing availability
    except ImportError:
        logger.warning("  yfinance not available — skipping price update")
        result["skipped"] = True
        return result

    async with async_session() as session:
        tickers, bootstrap_history = await _price_update_tickers(session)

    result["attempted"] = len(tickers)
    result["period"] = "3y" if bootstrap_history else "5d"

    if dry_run:
        logger.info(
            f"  [DRY RUN] Would fetch {result['period']} prices for "
            f"{len(tickers)} tickers"
        )
        return result

    total_batches = (len(tickers) + PRICE_BATCH_SIZE - 1) // PRICE_BATCH_SIZE

    async with async_session() as session:
        for batch_start in range(0, len(tickers), PRICE_BATCH_SIZE):
            batch = tickers[batch_start:batch_start + PRICE_BATCH_SIZE]

            for ticker in batch:
                fetched = False
                for attempt in range(2):
                    try:
                        hist = await asyncio.to_thread(
                            _yf_history,
                            f"{ticker}.AX",
                            str(result["period"]),
                        )
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

                if (
                    result["updated"] == 0
                    and result["failed"] >= PRICE_ABORT_AFTER_INITIAL_FAILURES
                ):
                    result["aborted"] = True
                    logger.warning(
                        "  Price update aborted after %s initial failures and "
                        "0 successful tickers. Cluster detection will continue "
                        "using stored prices or its calendar fallback.",
                        result["failed"],
                    )
                    return result

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


# ── Step 5: Cluster Portfolio ────────────────────────────────────────────────

async def _step_cluster_portfolio(
    dry_run: bool,
    email_test_replay_latest: bool = False,
) -> dict:
    if not settings.cluster_portfolio_enabled:
        logger.info("  Cluster Portfolio disabled by CLUSTER_PORTFOLIO_ENABLED")
        return {"skipped": True, "reason": "disabled"}

    portfolio_dry_run = dry_run or settings.cluster_portfolio_dry_run

    async with async_session() as session:
        outcome = await run_cluster_portfolio_cycle(
            session,
            dry_run=portfolio_dry_run,
            send_emails=settings.cluster_portfolio_email_enabled,
            email_test_replay_latest=email_test_replay_latest,
        )

    result = outcome.cycle_result
    valuation = outcome.valuation
    notifications = outcome.notifications

    summary = {
        "skipped": False,
        "dry_run": portfolio_dry_run,
        "portfolio_created": result.created_default_portfolio,
        "portfolio_reused": not result.created_default_portfolio,
        "buys_opened": len(result.buys_opened),
        "sells_closed": len(result.sells_closed),
        "skips_logged": len(result.skips_logged),
        "starting_cash": result.starting_cash,
        "current_cash": (
            result.current_cash if portfolio_dry_run else float(valuation.portfolio.current_cash)
        ),
        "deployed_capital": (
            round(result.starting_cash - result.current_cash, 2)
            if portfolio_dry_run
            else valuation.deployed_capital
        ),
        "emails_sent": sum(1 for item in notifications if item.status == "sent"),
        "emails_failed": sum(1 for item in notifications if item.status == "failed"),
        "emails_disabled": sum(1 for item in notifications if item.status == "disabled"),
    }

    logger.info(
        "  Cluster Portfolio: %s, buys=%s sells=%s skips=%s cash=%s -> %s emails(sent=%s failed=%s)",
        "created" if result.created_default_portfolio else "reused",
        summary["buys_opened"],
        summary["sells_closed"],
        summary["skips_logged"],
        summary["starting_cash"],
        summary["current_cash"],
        summary["emails_sent"],
        summary["emails_failed"],
    )
    return summary


# ── Orchestrator ─────────────────────────────────────────────────────────────

async def run(
    skip_prices: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    dry_run: bool = False,
    cluster_portfolio_email_test_replay_latest: bool = False,
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

    logger.info(f"{mode}STEP 5 — Cluster Portfolio")
    try:
        summary["cluster_portfolio"] = await _step_cluster_portfolio(
            dry_run,
            email_test_replay_latest=cluster_portfolio_email_test_replay_latest,
        )
    except Exception as e:
        logger.error(f"  STEP 5 FAILED: {e}", exc_info=True)
        summary["cluster_portfolio"] = {"error": str(e)}

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
    elif p.get("aborted"):
        print(
            f"  Prices: aborted after {p.get('failed', 0)} failed tickers "
            f"({p.get('updated', 0)}/{p.get('attempted', 0)} updated)",
            file=sys.stderr,
        )
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

    cp = summary.get("cluster_portfolio", {})
    if cp.get("skipped"):
        print(
            f"  Cluster Portfolio: skipped ({cp.get('reason', 'unknown')})",
            file=sys.stderr,
        )
    elif cp.get("error"):
        print(f"  Cluster Portfolio: FAILED — {cp['error']}", file=sys.stderr)
    else:
        print(
            "  Cluster Portfolio: "
            f"{'created' if cp.get('portfolio_created') else 'reused'}, "
            f"{cp.get('buys_opened', 0)} buys, "
            f"{cp.get('sells_closed', 0)} sells, "
            f"{cp.get('skips_logged', 0)} skips, "
            f"cash {cp.get('starting_cash')} → {cp.get('current_cash')}, "
            f"emails {cp.get('emails_sent', 0)} sent / {cp.get('emails_failed', 0)} failed",
            file=sys.stderr,
        )

    print(f"{'='*60}\n", file=sys.stderr)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily cluster update pipeline: price update → "
                    "cluster detection → return computation → status refresh "
                    "→ cluster portfolio.",
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
