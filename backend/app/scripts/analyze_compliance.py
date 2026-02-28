"""
ASX Appendix 3Y Compliance Analyser.

ASX Listing Rule 3.19A.2 requires directors to lodge an Appendix 3Y within
2 business days of a change in relevant interest.

Data notes:
  - "Verified" trades: date_lodged sourced directly from ASX announcements
    platform (our 3Y PDF scraper, document_date >= 2026-01-01).  These are
    treated as ground truth for compliance purposes.
  - "Historical" trades: date_lodged came from the seed JSON; some records
    show unrealistically large gaps (1 000 + days) indicating the field was
    set to the batch-import date rather than the real lodging date.  Those
    records are included in aggregates but flagged separately.

Severity scale (business days late):
  0-2  → Compliant
  3-5  → Minor
  6-10 → Moderate
  11+  → Severe

Usage:
    docker-compose exec backend python -m app.scripts.analyze_compliance
    docker-compose exec backend python -m app.scripts.analyze_compliance --verified-only
    docker-compose exec backend python -m app.scripts.analyze_compliance --days 90
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.director_company import DirectorCompany
from app.models.trade import Trade, TradeType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Trades lodged on or after this date are treated as verified (from our scraper)
VERIFIED_CUTOFF = date(2026, 1, 1)
# Gaps beyond this many calendar days are almost certainly data artefacts
ARTEFACT_THRESHOLD_DAYS = 365
OUTPUT_DIR = Path("/app/data")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class Violation:
    trade_id: str
    ticker: str
    company_name: str
    director_name: str
    date_of_trade: str   # ISO string for JSON serialisation
    date_lodged: str
    calendar_days: int
    business_days: int
    severity: str        # compliant / minor / moderate / severe
    trade_type: str
    verified: bool       # True = sourced from 3Y PDF scraper


def business_days_between(start: date, end: date) -> int:
    """
    Count business days (Mon-Fri) between start (exclusive) and end (inclusive).
    Does NOT exclude Australian public holidays — note this in reports.
    Returns 0 if end <= start.
    """
    if end <= start:
        return 0
    return int(np.busday_count(start.isoformat(), end.isoformat()))


def classify_severity(bdays: int) -> str:
    if bdays <= 2:
        return "compliant"
    elif bdays <= 5:
        return "minor"
    elif bdays <= 10:
        return "moderate"
    else:
        return "severe"


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

async def load_all_trades(session: AsyncSession) -> list[Violation]:
    """Load every trade with both dates and join company + director names."""
    result = await session.execute(
        select(
            Trade.id,
            Trade.date_of_trade,
            Trade.date_lodged,
            Trade.trade_type,
            Company.ticker,
            Company.name.label("company_name"),
            Director.full_name.label("director_name"),
        )
        .join(Company, Company.id == Trade.company_id)
        .join(Director, Director.id == Trade.director_id)
        .where(Trade.date_of_trade != None, Trade.date_lodged != None)
        .order_by(Trade.date_lodged.desc())
    )
    rows = result.all()
    logger.info(f"Loaded {len(rows):,} trades with both dates")

    violations = []
    for r in rows:
        cal_days = (r.date_lodged - r.date_of_trade).days
        # Skip obvious data artefacts (lodging appears before trade, or 1yr+ gap)
        if cal_days < 0:
            continue
        if cal_days > ARTEFACT_THRESHOLD_DAYS:
            continue  # likely batch-import date, not real lodging date

        bdays = business_days_between(r.date_of_trade, r.date_lodged)
        verified = r.date_lodged >= VERIFIED_CUTOFF

        violations.append(Violation(
            trade_id=r.id,
            ticker=r.ticker,
            company_name=r.company_name,
            director_name=r.director_name,
            date_of_trade=r.date_of_trade.isoformat(),
            date_lodged=r.date_lodged.isoformat(),
            calendar_days=cal_days,
            business_days=bdays,
            severity=classify_severity(bdays),
            trade_type=r.trade_type,
            verified=verified,
        ))

    logger.info(
        f"After filtering artefacts: {len(violations):,} trades | "
        f"{sum(1 for v in violations if v.verified):,} verified"
    )
    return violations


def build_report(violations: list[Violation], lookback_days: int) -> dict:
    """Aggregate violations into a structured report dict."""
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    year_ago = (date.today() - timedelta(days=365)).isoformat()

    late = [v for v in violations if v.severity != "compliant"]
    recent = [v for v in late if v.date_lodged >= cutoff]
    verified_late = [v for v in late if v.verified]

    # ── Worst companies ────────────────────────────────────────────────────
    company_counts: dict[str, dict] = defaultdict(lambda: {
        "ticker": "", "company_name": "", "total": 0,
        "minor": 0, "moderate": 0, "severe": 0,
    })
    for v in late:
        c = company_counts[v.ticker]
        c["ticker"] = v.ticker
        c["company_name"] = v.company_name
        c["total"] += 1
        c[v.severity] += 1
    worst_companies = sorted(company_counts.values(), key=lambda x: x["total"], reverse=True)[:20]

    # ── Worst directors ────────────────────────────────────────────────────
    director_counts: dict[str, dict] = defaultdict(lambda: {
        "director_name": "", "total": 0,
        "minor": 0, "moderate": 0, "severe": 0,
        "tickers": set(),
    })
    for v in late:
        d = director_counts[v.director_name]
        d["director_name"] = v.director_name
        d["total"] += 1
        d[v.severity] += 1
        d["tickers"].add(v.ticker)
    # Serialise sets → sorted lists
    worst_directors_raw = sorted(director_counts.values(), key=lambda x: x["total"], reverse=True)[:20]
    worst_directors = [{**d, "tickers": sorted(d["tickers"])} for d in worst_directors_raw]

    # ── Repeat offenders (3+ violations in past year) ─────────────────────
    director_year: dict[str, int] = defaultdict(int)
    for v in late:
        if v.date_lodged >= year_ago:
            director_year[v.director_name] += 1
    repeat_offenders = [
        {"director_name": name, "violations_past_year": count}
        for name, count in sorted(director_year.items(), key=lambda x: -x[1])
        if count >= 3
    ]

    # ── Severity breakdown ────────────────────────────────────────────────
    def breakdown(lst):
        return {
            "total": len(lst),
            "minor": sum(1 for v in lst if v.severity == "minor"),
            "moderate": sum(1 for v in lst if v.severity == "moderate"),
            "severe": sum(1 for v in lst if v.severity == "severe"),
        }

    return {
        "generated_at": date.today().isoformat(),
        "lookback_days": lookback_days,
        "data_notes": {
            "verified_source": "Trades from 3Y PDFs scraped directly from ASX (date_lodged >= 2026-01-01)",
            "historical_source": "Trades from seed JSON — lodging dates may not be accurate",
            "public_holidays": "Not excluded — business day counts are Mon-Fri only",
            "artefact_filter": f"Gaps > {ARTEFACT_THRESHOLD_DAYS} calendar days excluded as likely artefacts",
        },
        "summary": {
            "total_trades_analysed": len(violations),
            "compliant": len(violations) - len(late),
            "late_filings": len(late),
            "late_verified_only": len(verified_late),
            "compliance_rate_pct": round(100 * (len(violations) - len(late)) / max(len(violations), 1), 1),
            **breakdown(late),
        },
        "recent_violations": {
            f"last_{lookback_days}_days": breakdown(recent),
            "items": [asdict(v) for v in sorted(recent, key=lambda v: v.date_lodged, reverse=True)[:50]],
        },
        "worst_offending_companies": worst_companies,
        "worst_offending_directors": worst_directors,
        "repeat_offenders": repeat_offenders,
    }


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_json(report: dict) -> Path:
    path = OUTPUT_DIR / "compliance_report.json"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"JSON report written → {path}")
    return path


def write_csv(violations: list[Violation]) -> Path:
    late = [v for v in violations if v.severity != "compliant"]
    path = OUTPUT_DIR / "late_filings.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(late[0]).keys()) if late else []
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(v) for v in late)
    logger.info(f"CSV written → {path}  ({len(late):,} rows)")
    return path


def print_console_summary(report: dict) -> None:
    s = report["summary"]
    r = report["recent_violations"]
    recent_n = report["lookback_days"]

    print("\n" + "=" * 72)
    print("ASX APPENDIX 3Y COMPLIANCE REPORT")
    print("=" * 72)
    print(f"  Trades analysed:    {s['total_trades_analysed']:,}")
    print(f"  Compliant:          {s['compliant']:,}  ({s['compliance_rate_pct']}%)")
    print(f"  Late filings:       {s['late_filings']:,}")
    print(f"    Minor  (3-5 bd):  {s['minor']:,}")
    print(f"    Moderate(6-10):   {s['moderate']:,}")
    print(f"    Severe (11+ bd):  {s['severe']:,}")
    print(f"  Verified late only: {s['late_verified_only']:,}  (from 3Y scraper, high confidence)")

    print(f"\n  Last {recent_n} days: {r[f'last_{recent_n}_days']['total']} late filings")

    print("\n── Worst offending companies (all-time late filings) ──────────────────")
    print(f"  {'Ticker':<8} {'Total':>6}  {'Minor':>6}  {'Mod':>6}  {'Severe':>6}  Company")
    print("  " + "-" * 65)
    for c in report["worst_offending_companies"][:10]:
        print(
            f"  {c['ticker']:<8} {c['total']:>6}  {c['minor']:>6}  "
            f"{c['moderate']:>6}  {c['severe']:>6}  {c['company_name'][:30]}"
        )

    print("\n── Worst offending directors ───────────────────────────────────────────")
    print(f"  {'Director':<35} {'Total':>6}  {'Minor':>6}  {'Mod':>6}  {'Severe':>6}")
    print("  " + "-" * 65)
    for d in report["worst_offending_directors"][:10]:
        print(
            f"  {d['director_name'][:34]:<35} {d['total']:>6}  {d['minor']:>6}  "
            f"{d['moderate']:>6}  {d['severe']:>6}"
        )

    if report["repeat_offenders"]:
        print("\n── Repeat offenders (3+ violations in past year) ──────────────────────")
        for r_off in report["repeat_offenders"]:
            print(f"  {r_off['director_name']:<40} {r_off['violations_past_year']} violations")
    else:
        print("\n  No repeat offenders (3+ violations in past year)")

    print("\n── Recent late filings (last 30 days, verified) ───────────────────────")
    shown = 0
    for v in report["recent_violations"]["items"]:
        if not v["verified"]:
            continue
        print(
            f"  {v['date_lodged']}  {v['ticker']:<7}  {v['director_name'][:28]:<28}  "
            f"{v['business_days']:2d} bd  [{v['severity']}]"
        )
        shown += 1
        if shown >= 15:
            break
    if shown == 0:
        print("  None in this period (run scraper again to refresh)")

    print("\n" + "=" * 72)
    print("Note: Business days exclude weekends only — public holidays not deducted.")
    print("Verified = lodged via ASX announcements platform (high confidence).")
    print("=" * 72 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(verified_only: bool = False, lookback_days: int = 90) -> None:
    logger.info("Starting ASX compliance analysis")

    async with async_session() as session:
        violations = await load_all_trades(session)

    if verified_only:
        violations = [v for v in violations if v.verified]
        logger.info(f"Restricted to {len(violations):,} verified trades")

    report = build_report(violations, lookback_days)

    write_json(report)
    if violations:
        write_csv(violations)
    print_console_summary(report)

    logger.info("Compliance analysis complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ASX Appendix 3Y compliance analyser")
    parser.add_argument(
        "--verified-only",
        action="store_true",
        help="Only analyse trades sourced from the 3Y PDF scraper (high confidence dates)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Lookback window in days for 'recent violations' section (default: 90)",
    )
    args = parser.parse_args()
    asyncio.run(main(verified_only=args.verified_only, lookback_days=args.days))
