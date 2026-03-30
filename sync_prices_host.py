"""
Standalone host-side price sync script.
Fetches ASX price data from yfinance and inserts into Docker postgres.

Usage:
    python3 sync_prices_host.py --tickers BHP,CBA,WES
    python3 sync_prices_host.py --top 100   # top N most-traded tickers
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
import yfinance as yf

DB = dict(host="localhost", port=5432, dbname="asx_db", user="asx_user", password="asx_password")
YEARS = 3


def get_top_tickers(n: int) -> list[str]:
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.ticker, COUNT(*) as cnt
        FROM trades t
        JOIN companies c ON c.id = t.company_id
        WHERE t.trade_type IN ('ON_MARKET_BUY','ON_MARKET_SELL')
          AND t.price_per_share IS NOT NULL
          AND t.date_of_trade >= NOW() - INTERVAL '3 years'
        GROUP BY c.ticker
        ORDER BY cnt DESC
        LIMIT %s
    """, (n,))
    tickers = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return tickers


def sync_ticker(ticker: str, conn) -> int:
    yf_ticker = ticker.upper() + ".AX"
    start = date.today() - timedelta(days=YEARS * 365)
    try:
        df = yf.download(yf_ticker, start=start, end=date.today(), progress=False, auto_adjust=True)
    except Exception as e:
        print(f"  ✗ {ticker}: download error: {e}")
        return 0

    if df is None or df.empty:
        print(f"  ✗ {ticker}: no data")
        return 0

    rows = []
    for idx, row in df.iterrows():
        try:
            d = idx.date() if hasattr(idx, "date") else idx

            def get_val(col):
                try:
                    v = row[col]
                    if hasattr(v, 'item'):
                        v = v.item()
                    return float(v) if v is not None else None
                except Exception:
                    return None

            close_val = get_val("Close")
            if close_val is None:
                continue
            rows.append((
                str(uuid.uuid4()),
                ticker,
                d,
                get_val("Open") or close_val,
                get_val("High") or close_val,
                get_val("Low") or close_val,
                close_val,
                int(get_val("Volume") or 0),
            ))
        except Exception:
            continue

    if not rows:
        print(f"  ✗ {ticker}: no parseable rows")
        return 0

    cur = conn.cursor()
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO price_snapshots (id, ticker, date, open, high, low, close, volume) "
        "VALUES %s "
        "ON CONFLICT (ticker, date) DO UPDATE SET "
        "open=EXCLUDED.open, high=EXCLUDED.high, "
        "low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume",
        rows,
    )
    conn.commit()
    cur.close()
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", help="Comma-separated list")
    parser.add_argument("--top", type=int, default=0, help="Top N most-traded tickers")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.top:
        print(f"Fetching top {args.top} tickers from DB...")
        tickers = get_top_tickers(args.top)
        print(f"Got: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
    else:
        print("Specify --tickers or --top N")
        sys.exit(1)

    conn = psycopg2.connect(**DB)
    total = 0
    for i, ticker in enumerate(tickers, 1):
        rows = sync_ticker(ticker, conn)
        if rows:
            print(f"  ✓ {ticker}: {rows} rows  ({i}/{len(tickers)})")
        total += rows
        time.sleep(0.4)  # ~2.5 req/s, stay under limits

    conn.close()
    print(f"\nDone: {total} rows inserted across {len(tickers)} tickers")


if __name__ == "__main__":
    main()
