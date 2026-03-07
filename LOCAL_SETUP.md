# Local Development Setup

## Prerequisites
- Docker Desktop (running)
- Git
- curl (for testing)

## Quick Start

### 1. Start the stack
```bash
cd /Users/dantadmore/ASX2/asx-insider-tracker
docker-compose up
```

Wait for the output to show:
```
asx_tracker_api | Uvicorn running on http://0.0.0.0:8000
```

**The web interface is at: http://localhost:8000**

### 2. Verify it's working

**Check database health:**
```bash
curl http://localhost:8000/api/health
```

Expected response:
```json
{"db": "connected", "status": "healthy"}
```

**Check trade count:**
```bash
curl http://localhost:8000/api/stats | jq '.total_trades'
```

Should show a number (e.g., `13533`)

**Open in browser:**
```
http://localhost:8000
```

You should see the trade feed dashboard sorted by newest trades first.

---

## Common Issues & Fixes

### Issue: "Cannot connect to db"
**Cause:** Database hasn't finished initializing (healthcheck takes 30-50s)

**Fix:** Wait longer. Watch logs for:
```
asx_tracker_db  | database system is ready to accept connections
```

### Issue: "Table does not exist"
**Cause:** Migrations didn't run

**Fix:** Check the backend logs for:
```
✓ Database migrations completed
```

If missing, migrations failed. Check the logs for the SQL error.

### Issue: Port 8000 already in use
**Cause:** Another service is using port 8000

**Fix:** Stop the other service or change the port in docker-compose.yml:
```yaml
ports:
  - "8001:8000"  # Use 8001 instead
```
Then access at http://localhost:8001

### Issue: "ASXINSIDER_URL not set"
**Cause:** .env file not loaded

**Fix:** Verify .env exists and has:
```
ASXINSIDER_URL=https://www.asxinsider.com.au/api/trades
```

The docker-compose.yml should have `env_file: .env` on both services.

---

## Testing the Sync Manually

The sync runs every 2 minutes automatically. To trigger it manually:

### Via API (Refresh Now button)
Click the green **"Refresh Now"** button in the Trade Feed header.

### Via CLI
```bash
docker-compose exec backend python -m app.scripts.sync_from_asxinsider
```

Watch the output:
```
Fetched 15179 records (newest-first). Processing ...
  100 processed — 15 inserted, 2 replaced, 83 upgraded ...
  200 processed — 28 inserted, 4 replaced, 165 upgraded ...
...
Early exit after 1025 checks (50 consecutive existing records)
Sync complete: 28 new, 4 replaced, 1050 upgraded [early exit]
```

---

## Testing Changes Locally Before Pushing

### 1. Make a code change
```bash
# Edit a file in backend/app/static/index.html or similar
```

### 2. Restart the backend (hot reload)
```bash
# Ctrl+C to stop docker-compose, then:
docker-compose up
```

The backend has `--reload` flag, so Python changes auto-reload.

### 3. Clear browser cache
Press `Ctrl+Shift+Delete` (or Cmd+Shift+Delete on Mac) to clear cache, then refresh.

### 4. Verify the change
Test in the web interface at http://localhost:8000

### 5. If it works, commit and push
```bash
git add <files>
git commit -m "Your message"
git push
```

---

## Checking Logs

### Backend logs
```bash
docker-compose logs -f backend
```

### Database logs
```bash
docker-compose logs -f db
```

### Both (follow in real-time)
```bash
docker-compose logs -f
```

### Last 50 lines
```bash
docker-compose logs backend --tail 50
```

---

## Stopping & Cleaning Up

### Stop containers (keep data)
```bash
docker-compose stop
```

### Stop and remove containers (keep data)
```bash
docker-compose down
```

### Full clean (WARNING: deletes database)
```bash
docker-compose down -v
```

This removes the `postgres_data` volume. Next start will initialize a fresh database.

---

## Database Access

### Connect directly to Postgres (advanced)
```bash
docker-compose exec db psql -U asx_user -d asx_db
```

Then in psql:
```sql
\dt                    -- list tables
SELECT COUNT(*) FROM trades;  -- count trades
\q                     -- exit
```

---

## Environment Variables

The `.env` file controls:
- `DATABASE_URL` - PostgreSQL connection string
- `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` - DB credentials
- `APP_ENV` - "development" or "production"
- `APP_VERSION` - version string

For Railway, these are set in the Railway dashboard (no .env file needed).

---

## Next Steps

1. **Make a local change** to test the workflow
2. **Verify it works** at http://localhost:8000
3. **Commit and push** to deploy to Railway
4. **Check Railway dashboard** for the new deployment

