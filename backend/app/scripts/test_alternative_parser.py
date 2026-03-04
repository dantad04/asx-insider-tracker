"""
Compare our regex-based PDF parser against a GPT-based alternative parser.

Runs both parsers on the same 105 PDFs and produces a side-by-side report.

Usage:
    docker-compose exec backend python -m app.scripts.test_alternative_parser

Requirements for the GPT parser:
    - OPENAI_API_KEY environment variable must be set
    - pip install openai PyMuPDF   (inside the container)

Notes:
    - Does NOT modify parse_3y_pdfs.py or the database
    - GPT parser uses gpt-4o-mini (~$0.05-0.15 for all 105 PDFs)
    - The model "gpt-5-mini" in the reference code does not exist;
      this script uses "gpt-4o-mini" instead
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── Attempt to import optional dependencies ──────────────────────────────────

try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except ImportError:
    FITZ_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# ── Import existing parser functions (read-only, no DB calls) ─────────────────

from app.scripts.parse_3y_pdfs import (
    extract_text_from_pdf as old_extract_text,
    parse_date_of_change,
    parse_director_name,
    parse_number_acquired,
    parse_number_disposed,
    parse_price_per_share,
    parse_total_consideration,
    parse_nature_of_change,
)

# ── Constants ─────────────────────────────────────────────────────────────────

PDF_DIR = Path("/app/data/pdfs/raw")

# GPT prompt from the reference implementation
DEVELOPER_MESSAGE = """
You are a financial document extraction engine.
You will be given the text of ASX Appendix 3Y notices (Change of Director's Interest).
Your task is to extract structured information and return it in strict JSON format.

The JSON output must always be an array of objects.
Each object represents one transaction and must follow this schema:

[
  {
    "issuer_name": "string | null",
    "director_name": "string | null",
    "date_of_change": "YYYY-MM-DD | null",
    "class": "string | null",
    "transaction_type": "Purchase | Sale | Exercise | DividendReinvestment | Placement | Conversion | Other",
    "quantity": "number | null",
    "per_share": "number | null",
    "total_value": "number | null",
    "holdings_before": "number | null",
    "holdings_after": "number | null",
    "nature_of_change": "string | null"
  }
]

Rules:
- Extract only from Appendix 3Y documents.
- Always return an array, even if there is only one transaction.
- If multiple transactions are reported in one notice, output multiple objects.
- date_of_change: If multiple dates are present, return the earliest one.
- class: Must include both instrument and class name.
- Only include classes that actually changed.
- quantity: positive for acquisitions, negative for disposals.
- per_share: Extract the per share value as a number if explicitly stated. If not mentioned, set to null.
- total_value: Extract the total value as a number if explicitly stated. If not mentioned, set to null.
- DO NOT calculate missing values. Only extract what is explicitly stated.
- Extract only the numeric value, removing currency symbols, commas (e.g. "$2.50" becomes 2.50).
- If a value is in cents (e.g. "21.0c"), convert to dollars (0.21).
- holdings_before and holdings_after represent the class listed in "class" only.
- nature_of_change: copy exactly the description provided.
- All numbers as plain integers or floats (e.g. 10000, not "10,000 shares").
- Dates must be ISO format YYYY-MM-DD where possible.
- If a field is missing, return null.
- Do not infer or guess missing data.

Output rules:
- Output must be a single valid JSON array.
- Do not write any explanations, summaries, or conversational text.
- Do not wrap the output in markdown code fences.
- Return only raw JSON.
"""


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class OldParserResult:
    pdf_name: str
    success: bool
    director: str | None = None
    date_of_change: Any = None
    quantity: int | None = None
    price_per_share: float | None = None
    total_consideration: float | None = None
    nature_of_change: str | None = None
    error: str | None = None


@dataclass
class NewParserResult:
    pdf_name: str
    success: bool
    transactions: list[dict] = field(default_factory=list)
    raw_response: str | None = None
    error: str | None = None


# ── Old parser runner ─────────────────────────────────────────────────────────

def run_old_parser(pdf_path: Path) -> OldParserResult:
    """Run the existing regex parser on a single PDF."""
    result = OldParserResult(pdf_name=pdf_path.name, success=False)
    try:
        text = old_extract_text(str(pdf_path))
        if not text.strip():
            result.error = "Empty text extracted"
            return result

        result.director = parse_director_name(text)
        result.date_of_change = parse_date_of_change(text)
        result.nature_of_change = parse_nature_of_change(text)

        acquired = parse_number_acquired(text)
        disposed = parse_number_disposed(text)
        if acquired is not None:
            result.quantity = acquired
        elif disposed is not None:
            result.quantity = disposed

        price = parse_price_per_share(text)
        consideration = parse_total_consideration(text)
        result.price_per_share = float(price) if price else None
        result.total_consideration = float(consideration) if consideration else None

        # Success = at minimum got a date and quantity
        if result.date_of_change and result.quantity is not None:
            result.success = True
        else:
            missing = []
            if not result.date_of_change:
                missing.append("date_of_change")
            if result.quantity is None:
                missing.append("quantity")
            result.error = f"Missing: {', '.join(missing)}"

    except Exception as e:
        result.error = str(e)

    return result


# ── New (GPT) parser runner ───────────────────────────────────────────────────

def extract_text_pymupdf(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF (fitz)."""
    doc = fitz.open(str(pdf_path))
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text.strip()


def run_new_parser(pdf_path: Path, client: "OpenAI") -> NewParserResult:
    """Run the GPT-based parser on a single PDF."""
    result = NewParserResult(pdf_name=pdf_path.name, success=False)
    try:
        text = extract_text_pymupdf(pdf_path)
        if not text.strip():
            result.error = "Empty text extracted"
            return result

        # Extract ticker from filename for context
        ticker = pdf_path.name.split("_")[0]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": DEVELOPER_MESSAGE},
                {"role": "user", "content": f"Ticker: {ticker}\n\n{text}"}
            ],
            temperature=0,
        )

        raw = response.choices[0].message.content.strip()
        result.raw_response = raw

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            parsed = [parsed]

        result.transactions = parsed

        # Success = at least one transaction with date and quantity
        for t in parsed:
            if t.get("date_of_change") and t.get("quantity") is not None:
                result.success = True
                break

        if not result.success:
            result.error = "No transaction with both date and quantity"

    except json.JSONDecodeError as e:
        result.error = f"JSON parse error: {e}"
    except Exception as e:
        result.error = str(e)

    return result


# ── Comparison helpers ────────────────────────────────────────────────────────

def values_match(old: OldParserResult, new: NewParserResult) -> tuple[bool, list[str]]:
    """
    Compare extracted values between old and new parser.
    Returns (all_match, list_of_differences).
    """
    diffs = []

    # Use the first transaction from the new parser for comparison
    if not new.transactions:
        return False, ["new parser has no transactions"]

    new_t = new.transactions[0]

    # Date
    old_date = str(old.date_of_change) if old.date_of_change else None
    new_date = new_t.get("date_of_change")
    if old_date != new_date:
        diffs.append(f"date_of_change: old={old_date!r}  new={new_date!r}")

    # Quantity (compare absolute values — old parser may store signed)
    old_qty = abs(old.quantity) if old.quantity is not None else None
    new_qty = abs(new_t.get("quantity") or 0) if new_t.get("quantity") is not None else None
    if old_qty != new_qty:
        diffs.append(f"quantity: old={old_qty}  new={new_qty}")

    # Price — only flag if both have a value and they differ
    old_price = old.price_per_share
    new_price = new_t.get("per_share")
    if old_price is not None and new_price is not None:
        if abs(old_price - new_price) > 0.0001:
            diffs.append(f"per_share: old={old_price}  new={new_price}")

    return len(diffs) == 0, diffs


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 70)
    print("  PDF PARSER COMPARISON: Regex (old) vs GPT (new)")
    print("=" * 70)

    # Check dependencies
    if not FITZ_AVAILABLE:
        print("\n❌ PyMuPDF not installed. Run inside container:")
        print("   pip install PyMuPDF")
        print("   Then re-run this script.")
        sys.exit(1)

    if not OPENAI_AVAILABLE:
        print("\n❌ openai package not installed. Run inside container:")
        print("   pip install openai")
        print("   Then re-run this script.")
        sys.exit(1)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("\n❌ OPENAI_API_KEY environment variable not set.")
        print("   Set it in Railway Variables or locally:")
        print("   export OPENAI_API_KEY=sk-...")
        sys.exit(1)

    # Collect PDFs
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"\n❌ No PDFs found in {PDF_DIR}")
        sys.exit(1)

    print(f"\n📁 Found {len(pdfs)} PDFs in {PDF_DIR}")
    print(f"🔑 OpenAI API key: {'set ✓' if api_key else 'NOT SET ✗'}")
    print(f"\nRunning both parsers... (GPT calls may take ~1-2 min for all PDFs)\n")

    client = OpenAI(api_key=api_key)

    old_results: dict[str, OldParserResult] = {}
    new_results: dict[str, NewParserResult] = {}

    for i, pdf_path in enumerate(pdfs, 1):
        name = pdf_path.name
        print(f"  [{i:3d}/{len(pdfs)}] {name[:50]:<50}", end=" ", flush=True)

        old_r = run_old_parser(pdf_path)
        old_results[name] = old_r

        # Small delay between GPT calls to avoid rate limits
        new_r = run_new_parser(pdf_path, client)
        new_results[name] = new_r
        time.sleep(0.3)

        old_icon = "✓" if old_r.success else "✗"
        new_icon = "✓" if new_r.success else "✗"
        print(f"old={old_icon}  new={new_icon}")

    # ── Build comparison buckets ──────────────────────────────────────────────

    both_success     = []
    only_old_success = []
    only_new_success = []
    both_failed      = []

    for name in sorted(old_results):
        old_r = old_results[name]
        new_r = new_results[name]

        if old_r.success and new_r.success:
            both_success.append(name)
        elif old_r.success and not new_r.success:
            only_old_success.append(name)
        elif not old_r.success and new_r.success:
            only_new_success.append(name)
        else:
            both_failed.append(name)

    old_total = sum(1 for r in old_results.values() if r.success)
    new_total = sum(1 for r in new_results.values() if r.success)
    total = len(pdfs)

    # ── Print report ──────────────────────────────────────────────────────────

    print("\n\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    print(f"\n  OLD parser (regex):  {old_total}/{total}  ({100*old_total/total:.1f}%)")
    print(f"  NEW parser (GPT):    {new_total}/{total}  ({100*new_total/total:.1f}%)")
    print()

    winner = "OLD" if old_total > new_total else ("NEW" if new_total > old_total else "TIE")
    if winner == "TIE":
        print("  Overall: TIE — both parsers succeed on the same number of PDFs")
    else:
        diff = abs(old_total - new_total)
        print(f"  Overall winner: {winner} parser (+{diff} PDFs)")

    # ── PDFs only old got right ───────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  PDFs only OLD parser got right ({len(only_old_success)})")
    print(f"{'─'*70}")
    if only_old_success:
        for name in only_old_success:
            new_r = new_results[name]
            print(f"  ✗ {name}")
            print(f"      GPT error: {new_r.error or '(no error message)'}")
    else:
        print("  (none)")

    # ── PDFs only new got right ───────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  PDFs only NEW parser got right ({len(only_new_success)})")
    print(f"{'─'*70}")
    if only_new_success:
        for name in only_new_success:
            old_r = old_results[name]
            new_r = new_results[name]
            t = new_r.transactions[0] if new_r.transactions else {}
            print(f"  ✓ {name}")
            print(f"      Regex error:  {old_r.error}")
            print(f"      GPT extracted: date={t.get('date_of_change')}  "
                  f"qty={t.get('quantity')}  director={t.get('director_name')!r}")
    else:
        print("  (none)")

    # ── Both succeeded — value comparison ────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  PDFs both parsers got right ({len(both_success)}) — value comparison")
    print(f"{'─'*70}")
    matches = 0
    mismatches = []
    for name in both_success:
        old_r = old_results[name]
        new_r = new_results[name]
        match, diffs = values_match(old_r, new_r)
        if match:
            matches += 1
        else:
            mismatches.append((name, diffs))

    print(f"\n  Identical values:  {matches}/{len(both_success)}")
    print(f"  Value mismatches:  {len(mismatches)}/{len(both_success)}")

    if mismatches:
        print(f"\n  Mismatch details:")
        for name, diffs in mismatches:
            print(f"\n  ⚠  {name}")
            for d in diffs:
                print(f"       {d}")

    # ── Both failed ───────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  PDFs both parsers failed on ({len(both_failed)})")
    print(f"{'─'*70}")
    if both_failed:
        for name in both_failed:
            old_r = old_results[name]
            new_r = new_results[name]
            print(f"  ✗ {name}")
            print(f"      Regex: {old_r.error}")
            print(f"      GPT:   {new_r.error}")
    else:
        print("  (none — impressive!)")

    # ── Final recommendation ──────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("  RECOMMENDATION")
    print(f"{'='*70}")
    if old_total >= new_total and len(only_new_success) == 0:
        print("\n  ✅ OLD parser is at least as good — no reason to switch.")
    elif new_total > old_total:
        print(f"\n  ⚡ NEW parser succeeds on {new_total - old_total} extra PDFs.")
        print(f"     Consider migrating IF cost/latency of GPT calls is acceptable.")
        if mismatches:
            print(f"     ⚠  Review {len(mismatches)} value mismatch(es) before switching.")
    else:
        print(f"\n  ⚡ NEW parser succeeds on {len(only_new_success)} PDFs the old misses.")
        print(f"     OLD parser succeeds on {len(only_old_success)} PDFs the new misses.")
        print(f"     Consider a hybrid: use GPT as fallback for regex failures only.")

    print()


if __name__ == "__main__":
    main()
