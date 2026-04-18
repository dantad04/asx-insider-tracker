#!/usr/bin/env python3
"""
Fetch daily close prices + GICS sectors from yfinance into PostgreSQL.

Run on your LOCAL Mac (outside Docker) while Docker Postgres is up.
yfinance works on Mac but fails inside Docker — this script bridges the gap.

Creates three tables:
  yf_daily_prices  (ticker, date, close)                      — 3 yr daily closes, .AX suffix
  yf_ticker_sectors (ticker, sector)                          — GICS sector per ticker
  yf_company_info  (ticker, market_cap, industry, fetched_at) — AUD market cap + yfinance industry

pip install yfinance psycopg2-binary python-dotenv

Usage:
  cd asx-insider-tracker
  python backend/app/scripts/fetch_prices_local.py
  python backend/app/scripts/fetch_prices_local.py --sectors-only       # skip prices
  python backend/app/scripts/fetch_prices_local.py --market-cap-only    # skip prices + sectors
  python backend/app/scripts/fetch_prices_local.py --industry-only      # backfill industry only
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg2
import yfinance as yf
from dotenv import load_dotenv

# ── Database connection ──────────────────────────────────────────────────────

def _build_dsn() -> str:
    """Read DATABASE_URL from .env, convert to psycopg2 format (localhost)."""
    env_path = Path(__file__).resolve().parents[3] / ".env"
    load_dotenv(env_path)
    url = os.getenv("DATABASE_URL", "")
    if not url:
        sys.exit("DATABASE_URL not found in .env")
    # asyncpg driver → psycopg2;  Docker service name 'db' → localhost
    dsn = url.replace("postgresql+asyncpg://", "postgresql://")
    dsn = dsn.replace("@db:", "@localhost:")
    return dsn


# ── Schema ───────────────────────────────────────────────────────────────────

DDL_PRICES = """
CREATE TABLE IF NOT EXISTS yf_daily_prices (
    ticker  VARCHAR(20) NOT NULL,
    date    DATE        NOT NULL,
    close   NUMERIC(12, 4) NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_yfdp_ticker ON yf_daily_prices (ticker);
"""

DDL_SECTORS = """
CREATE TABLE IF NOT EXISTS yf_ticker_sectors (
    ticker  VARCHAR(20) PRIMARY KEY,
    sector  VARCHAR(100) NOT NULL
);
"""

DDL_COMPANY_INFO = """
CREATE TABLE IF NOT EXISTS yf_company_info (
    ticker      VARCHAR(20) PRIMARY KEY,
    market_cap  BIGINT,
    industry    VARCHAR(100),
    fetched_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE yf_company_info ADD COLUMN IF NOT EXISTS industry VARCHAR(100);
"""


# ── Price fetching ───────────────────────────────────────────────────────────

def fetch_prices(conn, tickers: list[str], label: str = "") -> tuple[int, list[str]]:
    """
    Fetch 3-year daily closes for *tickers* (bare ASX codes).
    Skips tickers already present in yf_daily_prices.
    Returns (success_count, failed_list).
    """
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT ticker FROM yf_daily_prices")
    existing = {row[0] for row in cur.fetchall()}

    to_fetch = [t for t in tickers if t not in existing]
    already = len(tickers) - len(to_fetch)
    if already:
        print(f"  Skipping {already} tickers already in yf_daily_prices")
    if not to_fetch:
        print("  Nothing to fetch — all tickers present.")
        return 0, []

    success = 0
    failed: list[str] = []

    for i, ticker in enumerate(to_fetch, 1):
        yf_code = f"{ticker}.AX"
        try:
            hist = yf.Ticker(yf_code).history(period="3y")
            if hist.empty:
                failed.append(ticker)
            else:
                rows = []
                for ts, row in hist.iterrows():
                    d = ts.date() if hasattr(ts, "date") else ts
                    rows.append((ticker, d, float(row["Close"])))
                if rows:
                    args = ",".join(
                        cur.mogrify("(%s,%s,%s)", r).decode() for r in rows
                    )
                    cur.execute(
                        f"INSERT INTO yf_daily_prices (ticker,date,close) "
                        f"VALUES {args} ON CONFLICT DO NOTHING"
                    )
                    conn.commit()
                    success += 1
        except Exception as e:
            failed.append(ticker)
            if i <= 5 or i % 50 == 0:
                print(f"    FAIL {ticker}: {e}")

        if i % 10 == 0 or i == len(to_fetch):
            print(f"  {label}Fetched {i}/{len(to_fetch)} "
                  f"({success} ok, {len(failed)} failed)")

        time.sleep(1)

    cur.close()
    return success, failed


# ── Sector fetching ──────────────────────────────────────────────────────────

def fetch_sectors(conn, tickers: list[str]) -> tuple[int, int]:
    """
    Fetch GICS sector for *tickers* via yf.Ticker().info.
    Skips tickers already in yf_ticker_sectors.
    Returns (updated, skipped).
    """
    cur = conn.cursor()

    cur.execute("SELECT ticker FROM yf_ticker_sectors")
    existing = {row[0] for row in cur.fetchall()}

    to_fetch = [t for t in tickers if t not in existing]
    already = len(tickers) - len(to_fetch)
    if already:
        print(f"  Skipping {already} tickers already in yf_ticker_sectors")
    if not to_fetch:
        print("  Nothing to fetch — all sectors present.")
        return 0, 0

    updated = 0
    no_sector = 0

    for i, ticker in enumerate(to_fetch, 1):
        try:
            info = yf.Ticker(f"{ticker}.AX").info
            sector = info.get("sector")
            if sector:
                cur.execute(
                    "INSERT INTO yf_ticker_sectors (ticker, sector) "
                    "VALUES (%s, %s) ON CONFLICT (ticker) DO UPDATE SET sector = %s",
                    (ticker, sector, sector),
                )
                conn.commit()
                updated += 1
            else:
                no_sector += 1
        except Exception:
            no_sector += 1

        if i % 25 == 0 or i == len(to_fetch):
            print(f"  Sectors: {i}/{len(to_fetch)} "
                  f"({updated} found, {no_sector} unavailable)")

        time.sleep(1)

    cur.close()
    return updated, no_sector


# ── Company-info fetching (market cap + industry) ───────────────────────────

def fetch_company_info(conn, tickers: list[str]) -> tuple[int, int]:
    """
    Fetch marketCap (AUD) + industry for *tickers* via yf.Ticker().info.
    Skips tickers already present in yf_company_info with BOTH market_cap AND
    industry populated. Returns (updated, unavailable) where updated counts
    rows for which at least one of market_cap / industry was resolved.
    """
    cur = conn.cursor()

    cur.execute(
        "SELECT ticker FROM yf_company_info "
        "WHERE market_cap IS NOT NULL AND industry IS NOT NULL"
    )
    existing = {row[0] for row in cur.fetchall()}

    to_fetch = [t for t in tickers if t not in existing]
    already = len(tickers) - len(to_fetch)
    if already:
        print(f"  Skipping {already} tickers already fully populated")
    if not to_fetch:
        print("  Nothing to fetch — all company info present.")
        return 0, 0

    updated = 0
    unavailable = 0

    for i, ticker in enumerate(to_fetch, 1):
        mc: int | None = None
        industry: str | None = None
        try:
            info = yf.Ticker(f"{ticker}.AX").info
            raw = info.get("marketCap")
            if isinstance(raw, (int, float)) and raw > 0:
                mc = int(raw)
            ind = info.get("industry")
            if isinstance(ind, str) and ind.strip():
                industry = ind.strip()[:100]
        except Exception:
            mc = None
            industry = None

        try:
            cur.execute(
                "INSERT INTO yf_company_info (ticker, market_cap, industry, fetched_at) "
                "VALUES (%s, %s, %s, NOW()) "
                "ON CONFLICT (ticker) DO UPDATE "
                "SET market_cap = COALESCE(EXCLUDED.market_cap, yf_company_info.market_cap), "
                "    industry   = COALESCE(EXCLUDED.industry,   yf_company_info.industry), "
                "    fetched_at = NOW()",
                (ticker, mc, industry),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            if i <= 5:
                print(f"    DB FAIL {ticker}: {e}")

        if mc is not None or industry is not None:
            updated += 1
        else:
            unavailable += 1

        if i % 25 == 0 or i == len(to_fetch):
            print(f"  Company info: {i}/{len(to_fetch)} "
                  f"({updated} found, {unavailable} unavailable)")

        time.sleep(1)

    cur.close()
    return updated, unavailable


def fetch_industries_only(conn) -> tuple[int, int]:
    """
    Backfill industry for rows in yf_company_info where industry IS NULL.
    Reads tickers directly from yf_company_info (not from trades) so we
    only re-hit tickers we've already qualified. Returns (updated, unavailable).
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT ticker FROM yf_company_info WHERE industry IS NULL ORDER BY ticker"
    )
    to_fetch = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT COUNT(*) FROM yf_company_info")
    total = cur.fetchone()[0]
    print(f"  yf_company_info: {total} rows total, "
          f"{len(to_fetch)} missing industry")

    if not to_fetch:
        print("  Nothing to backfill — all rows have industry.")
        cur.close()
        return 0, 0

    updated = 0
    unavailable = 0

    for i, ticker in enumerate(to_fetch, 1):
        industry: str | None = None
        try:
            info = yf.Ticker(f"{ticker}.AX").info
            ind = info.get("industry")
            if isinstance(ind, str) and ind.strip():
                industry = ind.strip()[:100]
        except Exception:
            industry = None

        try:
            cur.execute(
                "UPDATE yf_company_info SET industry = %s, fetched_at = NOW() "
                "WHERE ticker = %s",
                (industry, ticker),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            if i <= 5:
                print(f"    DB FAIL {ticker}: {e}")

        if industry is not None:
            updated += 1
        else:
            unavailable += 1

        if i % 25 == 0 or i == len(to_fetch):
            print(f"  Industry backfill: {i}/{len(to_fetch)} "
                  f"({updated} found, {unavailable} unavailable)")

        time.sleep(1)

    cur.close()
    return updated, unavailable


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch yfinance prices + sectors into PostgreSQL",
    )
    parser.add_argument(
        "--sectors-only", action="store_true",
        help="Skip price fetching, only fetch sectors + market caps",
    )
    parser.add_argument(
        "--market-cap-only", action="store_true",
        help="Skip price + sector fetching, only fetch company info (market cap + industry)",
    )
    parser.add_argument(
        "--industry-only", action="store_true",
        help="Backfill industry column for existing yf_company_info rows "
             "where industry IS NULL. Does not touch prices, sectors, or market caps.",
    )
    args = parser.parse_args()

    dsn = _build_dsn()
    print(f"Connecting to database ...")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    # Create tables (+ ALTER for additive columns)
    cur = conn.cursor()
    cur.execute(DDL_PRICES)
    cur.execute(DDL_SECTORS)
    cur.execute(DDL_COMPANY_INFO)
    conn.commit()
    cur.close()
    print("Tables yf_daily_prices, yf_ticker_sectors, yf_company_info ready.")

    # Fast path: industry-only backfill runs against rows already in
    # yf_company_info and exits without touching trades/prices/sectors.
    if args.industry_only:
        print("\n=== INDUSTRY BACKFILL ===")
        updated, unavailable = fetch_industries_only(conn)
        print(f"Industry backfill done: {updated} resolved, "
              f"{unavailable} unavailable.")

        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM yf_company_info WHERE industry IS NOT NULL"
        )
        with_industry = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM yf_company_info")
        total = cur.fetchone()[0]
        pct = (100.0 * with_industry / total) if total else 0
        print(f"yf_company_info: {with_industry}/{total} rows have industry "
              f"({pct:.1f}%).")
        cur.close()

        conn.close()
        print("\nDone.")
        return

    # Get unique tickers from on-market buy trades.
    # Note: Trade.trade_type is stored as the enum NAME ('ON_MARKET_BUY'),
    # not the value ('on_market_buy'), because the SQLAlchemy model declares
    # it as SQLEnum(TradeType, native_enum=False) which serializes by name.
    cur = conn.cursor()

    # Diagnostic: show what trade_type values actually exist
    cur.execute("SELECT trade_type, COUNT(*) FROM trades GROUP BY trade_type "
                "ORDER BY COUNT(*) DESC")
    print("\nTrade-type distribution in database:")
    for tt, cnt in cur.fetchall():
        print(f"  {tt!r:35s}  {cnt:,}")

    query = """
        SELECT DISTINCT c.ticker
        FROM trades t
        JOIN companies c ON c.id = t.company_id
        WHERE UPPER(t.trade_type) IN ('ON_MARKET_BUY', 'ONMARKETBUY')
           OR LOWER(t.trade_type) = 'on_market_buy'
        ORDER BY c.ticker
    """
    print(f"\nExecuting:\n{query}")
    cur.execute(query)
    trade_tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    print(f"Found {len(trade_tickers)} unique tickers in on-market buy trades.")
    if trade_tickers:
        print(f"  First 10: {trade_tickers[:10]}")

    # ── Prices ───────────────────────────────────────────────────────────
    if not args.sectors_only and not args.market_cap_only:
        print("\n=== PRICE FETCH ===")

        # Trade tickers
        ok, fails = fetch_prices(conn, trade_tickers, label="trades: ")
        print(f"  Trade tickers done: {ok} fetched, {len(fails)} failed.")

        # STW (ASX 200 benchmark proxy)
        print("\nFetching STW.AX (ASX 200 benchmark) ...")
        ok_b, fails_b = fetch_prices(conn, ["STW"], label="benchmark: ")
        if fails_b:
            print("  WARNING: STW.AX fetch failed!")

        # Summary
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT ticker), COUNT(*) FROM yf_daily_prices"
        )
        n_tickers, n_rows = cur.fetchone()
        cur.close()
        print(f"\nyf_daily_prices: {n_tickers} tickers, {n_rows:,} rows total.")

    # ── Sectors ──────────────────────────────────────────────────────────
    if not args.market_cap_only:
        print("\n=== SECTOR FETCH ===")
        updated, no_sec = fetch_sectors(conn, trade_tickers)
        print(f"Sectors done: {updated} resolved, {no_sec} unavailable.")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM yf_ticker_sectors")
        total_sectors = cur.fetchone()[0]
        cur.close()
        print(f"yf_ticker_sectors: {total_sectors} tickers with sector data.")

    # ── Company info (market cap + industry) ─────────────────────────────
    print("\n=== COMPANY INFO FETCH (market cap + industry) ===")
    ci_updated, ci_unavailable = fetch_company_info(conn, trade_tickers)
    print(f"Company info done: {ci_updated} resolved, {ci_unavailable} unavailable.")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM yf_company_info WHERE market_cap IS NOT NULL")
    total_mc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM yf_company_info WHERE industry IS NOT NULL")
    total_ind = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM yf_company_info")
    total_rows = cur.fetchone()[0]
    cur.close()
    print(f"yf_company_info: {total_rows} rows — "
          f"{total_mc} with market cap, {total_ind} with industry.")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
