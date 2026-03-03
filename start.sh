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
import sys

async def count():
    import asyncpg
    url = os.getenv('DATABASE_URL', '')
    # Strip asyncpg prefix for raw asyncpg connection
    url = url.replace('postgresql+asyncpg://', 'postgresql://')
    try:
        conn = await asyncpg.connect(url)
        count = await conn.fetchval('SELECT COUNT(*) FROM trades')
        await conn.close()
        print(count)
    except Exception as e:
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

# Start the application
echo "Starting server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
