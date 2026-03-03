#!/bin/bash
set -e

echo "=== ASX Insider Tracker Startup ==="

# Run migrations
echo "Running database migrations..."
alembic upgrade head
echo "✓ Migrations complete"

# Seed data if database is empty
echo "Checking if database needs seeding..."
TRADE_COUNT=$(python -c "
import asyncio
import os

async def count():
    import asyncpg
    url = os.getenv('DATABASE_URL', '').replace('postgresql+asyncpg://', 'postgresql://')
    try:
        conn = await asyncpg.connect(url)
        count = await conn.fetchval('SELECT COUNT(*) FROM trades')
        await conn.close()
        print(count)
    except Exception:
        print(0)

asyncio.run(count())
" 2>/dev/null || echo "0")

if [ "$TRADE_COUNT" -eq "0" ] 2>/dev/null; then
    echo "Database is empty, seeding historical data..."
    python -m app.scripts.seed_from_json /app/data/asx_director_transactions.json
    echo "✓ Seed data imported"
else
    echo "✓ Database already has $TRADE_COUNT trades, skipping seed"
fi

# Backfill PDF-parsed data if none exists yet
echo "Checking if PDF-parsed trades exist..."
PDF_COUNT=$(python -c "
import asyncio
import os

async def count():
    import asyncpg
    url = os.getenv('DATABASE_URL', '').replace('postgresql+asyncpg://', 'postgresql://')
    try:
        conn = await asyncpg.connect(url)
        count = await conn.fetchval(\"SELECT COUNT(*) FROM trades WHERE source = 'pdf_parser'\")
        await conn.close()
        print(count)
    except Exception:
        print(0)

asyncio.run(count())
" 2>/dev/null || echo "0")

if [ "$PDF_COUNT" -eq "0" ] 2>/dev/null; then
    echo "No PDF-parsed trades found. Running full 3Y backfill (this may take ~45 minutes)..."
    python -m app.scripts.scrape_3y_announcements --all 2>&1 | tail -5
    echo "✓ Scraping complete"
    python -m app.scripts.parse_3y_pdfs 2>&1 | tail -5
    echo "✓ PDF parsing complete"
else
    echo "✓ Already have $PDF_COUNT PDF-parsed trades, skipping backfill"
fi

# Start the application
echo "Starting server on port ${PORT:-8000}..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
