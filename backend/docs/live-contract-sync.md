# Live Contract Sync

This runbook covers the daily AusTender contract maintenance path for production.

## What It Does

The daily runner performs two idempotent steps:

1. Fetches AusTender OCDS releases changed from one day before the latest stored contract `last_modified` date.
2. Generates contract alerts for the recent contract window, skipping alerts that already exist under the `(cn_id, alert_type)` uniqueness rule.

It does not run the historical bootstrap, clear existing data, change supplier mappings, or alter schemas.

## Manual Production Run

```bash
railway ssh --service asx-insider-tracker --environment production -- python -m app.scripts.run_contract_daily_sync --verbose
```

For a no-write check:

```bash
railway ssh --service asx-insider-tracker --environment production -- python -m app.scripts.run_contract_daily_sync --dry-run --verbose
```

## Status Check

```bash
curl -sS https://asx-insider.up.railway.app/api/contracts/status
```

Useful success signals:

- `contract_count` stays the same or increases.
- `alert_count` stays the same or increases.
- `last_successful_sync_at` remains current.
- `last_error` is null.

## Fallback Direct Commands

If the combined runner fails, run the two underlying commands separately and keep the output:

```bash
railway ssh --service asx-insider-tracker --environment production -- python -m app.scripts.sync_contracts --mode delta --verbose
railway ssh --service asx-insider-tracker --environment production -- python -m app.scripts.generate_contract_alerts --verbose
```

Both commands are safe to rerun. Contract sync upserts by `cn_id`; alert generation skips duplicates.

## Schedule

Recommended cadence: daily, early morning Melbourne time, after AusTender has had time to publish prior-day updates.

Recommended command:

```bash
python -m app.scripts.run_contract_daily_sync --verbose
```

As of this runbook, the Railway CLI in this project exposes Railway Functions cron management, and the project already has an `asx-maintenance-cron` function for trade/cluster maintenance. It does not expose a safe service-level cron command that runs the Python module inside the deployed `asx-insider-tracker` container.

Configure the daily contract job in the Railway UI if a service cron/job option is available:

- Project: `AsxInsider`
- Environment: `production`
- Service: `asx-insider-tracker`
- Command: `python -m app.scripts.run_contract_daily_sync --verbose`
- Schedule: daily, early morning Australia/Melbourne time

Do not add an infinite loop or in-process scheduler to the web server for this job.

## Failure Handling

1. Check the latest output from the scheduled job or manual command.
2. Run the dry-run command to confirm AusTender and database access.
3. If delta sync failed, rerun the combined daily command. It resumes with a one-day overlap from stored contract timestamps and upserts by `cn_id`.
4. If alert generation failed after sync succeeded, run the fallback alert command. It skips duplicates.
5. Check `/api/contracts/status` and the `/contracts` page after recovery.

Do not rerun the historical bootstrap unless intentionally backfilling a specific older date range.
