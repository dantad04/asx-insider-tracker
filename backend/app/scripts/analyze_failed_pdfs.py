"""
Diagnostic script to analyze failed PDF parsing attempts.

Examines failed Appendix 3Y PDFs to determine if they're truly unparseable
or if better parsing logic is needed.

Usage:
    docker-compose exec backend python -m app.scripts.analyze_failed_pdfs
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

import pdfplumber
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.pending_3y_parse import Pending3YParse, ParseStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text from PDF."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
            return text
    except Exception as e:
        logger.error(f"Error reading PDF {pdf_path}: {e}")
        return ""


def find_date_patterns(text: str) -> list[str]:
    """Find potential date patterns in text."""
    patterns = [
        r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b",  # DD/MM/YYYY or DD-MM-YYYY
        r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})\b",  # DD Month YYYY
        r"\b(\d{4}-\d{1,2}-\d{1,2})\b",  # YYYY-MM-DD
    ]

    found_dates = []
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        found_dates.extend(matches)

    return list(set(found_dates))  # Remove duplicates


def find_quantity_patterns(text: str) -> list[str]:
    """Find potential quantity patterns in text."""
    # Look for numbers that could be quantities
    # Pattern: numbers ranging from a few hundred to millions
    pattern = r"\b(?:Number\s+(?:acquired|disposed)[\s\S]*?)(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\b"

    matches = re.findall(pattern, text, re.IGNORECASE)

    # Also look for standalone large numbers (potential quantities)
    # Heuristic: numbers between 100 and 1 million
    large_numbers = re.findall(r"\b(\d{3,7})\b", text)
    potential_quantities = [n for n in large_numbers if 100 <= int(n) <= 9999999]

    return matches + potential_quantities[:10]  # Limit to top 10


def analyze_pdf(record: Pending3YParse) -> dict:
    """Analyze a single failed PDF."""
    logger.info(f"Analyzing {record.ticker}: {Path(record.pdf_path).name}")

    if not Path(record.pdf_path).exists():
        logger.warning(f"PDF file not found: {record.pdf_path}")
        return {
            "ticker": record.ticker,
            "filename": Path(record.pdf_path).name,
            "error_message": record.error_message,
            "file_exists": False,
            "text_length": 0,
            "dates_found": [],
            "quantities_found": [],
            "text_sample": "FILE NOT FOUND",
        }

    text = extract_text_from_pdf(record.pdf_path)

    if not text:
        logger.warning(f"Could not extract text from {record.pdf_path}")
        return {
            "ticker": record.ticker,
            "filename": Path(record.pdf_path).name,
            "error_message": record.error_message,
            "file_exists": True,
            "text_length": 0,
            "dates_found": [],
            "quantities_found": [],
            "text_sample": "NO TEXT EXTRACTED",
        }

    dates = find_date_patterns(text)
    quantities = find_quantity_patterns(text)

    # Get first 500 chars of text
    text_sample = text[:500].replace("\n", " ")

    return {
        "ticker": record.ticker,
        "filename": Path(record.pdf_path).name,
        "error_message": record.error_message,
        "file_exists": True,
        "text_length": len(text),
        "dates_found": dates,
        "quantities_found": quantities,
        "text_sample": text_sample,
        "full_text": text,  # Keep for later inspection
    }


async def main() -> None:
    """Main entry point."""
    logger.info("Starting analysis of failed PDFs")

    output_file = Path("/app/data/failed_pdfs_analysis.txt")
    output_file.parent.mkdir(exist_ok=True, parents=True)

    async with async_session() as session:
        # Query failed records
        result = await session.execute(
            select(Pending3YParse)
            .where(Pending3YParse.status == ParseStatus.FAILED)
            .order_by(Pending3YParse.ticker)
        )
        records = result.scalars().all()

        logger.info(f"Found {len(records)} failed PDFs to analyze")

        if not records:
            logger.info("No failed PDFs to analyze")
            return

        # Analyze all PDFs
        analyses = []
        for record in records:
            try:
                analysis = analyze_pdf(record)
                analyses.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing {record.ticker}: {e}")
                analyses.append({
                    "ticker": record.ticker,
                    "filename": Path(record.pdf_path).name,
                    "error_message": f"Analysis error: {str(e)}",
                    "file_exists": False,
                    "text_length": 0,
                    "dates_found": [],
                    "quantities_found": [],
                    "text_sample": f"ERROR: {str(e)}",
                })

        # Write summary to file
        with open(output_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write("FAILED PDF ANALYSIS REPORT\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Total Failed PDFs: {len(analyses)}\n")
            f.write("=" * 80 + "\n\n")

            # Categorize by failure type
            missing_date = []
            missing_quantity = []
            other_failures = []

            for analysis in analyses:
                error_msg = analysis.get("error_message", "").lower()
                if "date" in error_msg:
                    missing_date.append(analysis)
                elif "quantity" in error_msg or "acquired or disposed" in error_msg:
                    missing_quantity.append(analysis)
                else:
                    other_failures.append(analysis)

            f.write(f"FAILURE BREAKDOWN:\n")
            f.write(f"  Missing date_of_change: {len(missing_date)}\n")
            f.write(f"  Missing quantity: {len(missing_quantity)}\n")
            f.write(f"  Other failures: {len(other_failures)}\n\n")

            # Detailed analysis for each category
            if missing_date:
                f.write("=" * 80 + "\n")
                f.write("MISSING DATE_OF_CHANGE\n")
                f.write("=" * 80 + "\n\n")
                for analysis in missing_date:
                    f.write(f"Ticker: {analysis['ticker']}\n")
                    f.write(f"Filename: {analysis['filename']}\n")
                    f.write(f"Error: {analysis['error_message']}\n")
                    f.write(f"Text Length: {analysis['text_length']} chars\n")
                    f.write(f"Dates Found: {analysis['dates_found']}\n")
                    f.write(f"Text Sample: {analysis['text_sample']}\n")
                    f.write("-" * 80 + "\n\n")

            if missing_quantity:
                f.write("=" * 80 + "\n")
                f.write("MISSING QUANTITY\n")
                f.write("=" * 80 + "\n\n")
                for analysis in missing_quantity:
                    f.write(f"Ticker: {analysis['ticker']}\n")
                    f.write(f"Filename: {analysis['filename']}\n")
                    f.write(f"Error: {analysis['error_message']}\n")
                    f.write(f"Text Length: {analysis['text_length']} chars\n")
                    f.write(f"Quantities Found: {analysis['quantities_found']}\n")
                    f.write(f"Text Sample: {analysis['text_sample']}\n")
                    f.write("-" * 80 + "\n\n")

            if other_failures:
                f.write("=" * 80 + "\n")
                f.write("OTHER FAILURES\n")
                f.write("=" * 80 + "\n\n")
                for analysis in other_failures:
                    f.write(f"Ticker: {analysis['ticker']}\n")
                    f.write(f"Filename: {analysis['filename']}\n")
                    f.write(f"Error: {analysis['error_message']}\n")
                    f.write(f"Text Length: {analysis['text_length']} chars\n")
                    f.write(f"Text Sample: {analysis['text_sample']}\n")
                    f.write("-" * 80 + "\n\n")

    logger.info(f"Analysis complete. Results written to {output_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("FAILED PDF ANALYSIS SUMMARY")
    print("=" * 80)
    print(f"Total failed PDFs analyzed: {len(analyses)}")
    print(f"Missing date_of_change: {len(missing_date)}")
    print(f"Missing quantity: {len(missing_quantity)}")
    print(f"Other failures: {len(other_failures)}")
    print(f"\nDetailed report: {output_file}")
    print("=" * 80 + "\n")

    # Show samples of 3-5 failed PDFs
    sample_size = min(5, len(analyses))
    print(f"SAMPLE OF {sample_size} FAILED PDFs:\n")

    for i, analysis in enumerate(analyses[:sample_size], 1):
        print(f"\n[Sample {i}] {analysis['ticker']} - {analysis['filename']}")
        print(f"Error: {analysis['error_message']}")
        print(f"Text extracted: {analysis['text_length']} chars")
        print(f"Dates found: {analysis['dates_found']}")
        print(f"Quantities found: {analysis['quantities_found']}")
        print(f"Text preview: {analysis['text_sample'][:200]}...")
        print("-" * 80)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
