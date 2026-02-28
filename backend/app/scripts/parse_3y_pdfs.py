"""
Parse Appendix 3Y PDFs and extract director trade data.

Usage:
    docker-compose exec backend python -m app.scripts.parse_3y_pdfs
    docker-compose exec backend python -m app.scripts.parse_3y_pdfs --limit 10
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pdfplumber
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.director import Director
from app.models.director_company import DirectorCompany
from app.models.pending_3y_parse import Pending3YParse, ParseStatus
from app.models.trade import Trade, TradeType

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


def parse_director_name(text: str) -> str | None:
    """Extract director name from PDF text."""
    # Pattern: "Name of Director <name>"
    pattern = r"Name of Director\s+([A-Za-z\s\.\-\']+?)(?:\n|Date of)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        # Clean up any extra whitespace
        name = re.sub(r'\s+', ' ', name)
        return name
    return None


def try_parse_date(date_str: str) -> datetime.date | None:
    """Try to parse a date string with multiple formats."""
    if not date_str:
        return None

    # Clean up corrupted OCR text (e.g., "2In5d Fierebcrutary" -> "25 February")
    # Common OCR corruptions:
    # - Numbers get letters: "2In5d" -> "25", "2026" -> "2026"
    # - Month names get mangled: "Fierebcrutary" -> "February"
    # Try to extract just digits and letters, then parse

    date_formats = [
        "%d %B %Y",      # 17 February 2026
        "%d %b %Y",      # 17 Feb 2026
        "%d/%m/%Y",      # 17/02/2026
        "%Y-%m-%d",      # 2026-02-17
        "%d %m %Y",      # 17 02 2026
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue

    return None


def extract_dates_from_text(text: str) -> list[datetime.date]:
    """Extract all dates found in text, handling OCR corruption."""
    dates = []

    # Pattern: DD Month YYYY or DD/MM/YYYY or YYYY-MM-DD
    # More lenient to handle OCR corruption
    patterns = [
        r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
        r"(\d{4}-\d{1,2}-\d{1,2})",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            date_str = match.group(1)
            parsed_date = try_parse_date(date_str)
            if parsed_date:
                dates.append(parsed_date)

    return list(set(dates))  # Remove duplicates


def parse_date_of_change(text: str) -> datetime.date | None:
    """Extract date of change from PDF text with OCR error tolerance."""
    # First try: strict pattern with "Date of change" label
    pattern = r"Date of change\s+(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        date_str = match.group(1).strip()
        parsed = try_parse_date(date_str)
        if parsed:
            return parsed

    # Second try: "Date of change" followed by more flexible text
    # Capture text up to next field or newline, handle corrupted values
    pattern = r"Date of change\s+([\w\s/\-\.]+?)(?:\n|No\.|Part\s+[0-9]|$)"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        date_text = match.group(1).strip()
        # Try to parse even if corrupted
        # Extract just the numbers and month names
        if len(date_text) < 50:  # Reasonable date field length
            parsed = try_parse_date(date_text)
            if parsed:
                return parsed

    return None


def parse_date_of_last_notice(text: str) -> datetime.date | None:
    """Extract date of last notice as fallback for date of change."""
    pattern = r"Date of last notice\s+(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        date_str = match.group(1).strip()
        parsed = try_parse_date(date_str)
        if parsed:
            return parsed

    return None


def extract_quantity(text: str, field_type: str) -> int | None:
    """
    Extract quantity with flexible regex to handle various formatting.

    Handles formats like:
    - "Number acquired 123,456"
    - "Number acquired: 123,456"
    - "Number acquired Indirect – 123,456"
    - "Number acquired\nIndirect: 123,456"
    - "Number acquired 123,456* Ordinary Shares"
    """
    # First: Check for "Not applicable" or "N/A"
    na_pattern = rf"Number {field_type}\s*:?\s*(?:Indirect|Direct)?\s*(?:–|-|—)?\s*(Not applicable|N/?A)"
    if re.search(na_pattern, text, re.IGNORECASE):
        return 0

    # Main pattern: Match "Number [field_type]" followed by optional text, then capture digits
    # Handles: descriptors (Indirect/Direct), dashes, colons, asterisks, multiline
    pattern = rf"Number {field_type}\s*:?\s*(?:Indirect|Direct)?\s*(?:–|-|—)?\s*[\w\s]*?(\d{{1,3}}(?:,\d{{3}})*)\s*(?:\*)?(?:\s+\w+)?"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        try:
            num_str = match.group(1).replace(",", "").strip()
            if num_str and num_str.isdigit():
                return int(num_str)
        except ValueError:
            pass

    # Fallback: Try more lenient multiline pattern
    # Capture anything that looks like a number after the label
    pattern = rf"Number {field_type}[\s\S]{{0,50}}?(\d{{1,3}}(?:,\d{{3}})*)"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        try:
            num_str = match.group(1).replace(",", "").strip()
            if num_str and num_str.isdigit():
                return int(num_str)
        except ValueError:
            pass

    return None


def parse_number_acquired(text: str) -> int | None:
    """Extract number of securities acquired."""
    return extract_quantity(text, "acquired")


def parse_number_disposed(text: str) -> int | None:
    """Extract number of securities disposed."""
    return extract_quantity(text, "disposed")


def parse_price_per_share(text: str) -> Decimal | None:
    """Extract price per share from consideration."""
    # Pattern: "Value/Consideration $X.XX per share"
    pattern = r"Value/Consideration\s+\$?([\d,]+\.?\d*)\s*per share"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        try:
            price_str = match.group(1).replace(",", "")
            return Decimal(price_str)
        except (InvalidOperation, ValueError):
            pass

    # Try without "per share"
    pattern = r"Value/Consideration\s+\$?([\d,]+\.?\d*)"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        try:
            price_str = match.group(1).replace(",", "")
            return Decimal(price_str)
        except (InvalidOperation, ValueError):
            pass

    return None


def parse_nature_of_change(text: str) -> str | None:
    """Extract nature of change (transaction type)."""
    # Pattern: "Nature of change <description>"
    pattern = r"Nature of change\s+([A-Za-z\s\-,]+?)(?:\n|Example:)"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        nature = match.group(1).strip()
        # Clean up extra whitespace
        nature = re.sub(r'\s+', ' ', nature)
        return nature

    return None


def map_trade_type(nature_of_change: str) -> TradeType:
    """Map nature of change to TradeType enum."""
    if not nature_of_change:
        return TradeType.OTHER

    nature_lower = nature_of_change.lower()

    # Map common patterns
    if "on-market" in nature_lower and "buy" in nature_lower:
        return TradeType.ON_MARKET_BUY
    elif "on-market" in nature_lower and ("sell" in nature_lower or "sale" in nature_lower):
        return TradeType.ON_MARKET_SELL
    elif "on-market" in nature_lower:
        # Default on-market to buy (most common in 3Y forms)
        return TradeType.ON_MARKET_BUY
    elif "off-market" in nature_lower:
        return TradeType.OFF_MARKET
    elif "exercise" in nature_lower and "option" in nature_lower:
        return TradeType.EXERCISE_OPTIONS
    else:
        return TradeType.OTHER


def calculate_parse_confidence(
    director_name: str | None,
    date_of_change: datetime.date | None,
    quantity: int | None,
    price: Decimal | None,
    nature: str | None
) -> float:
    """Calculate confidence score for parsed data (0.0 to 1.0)."""
    score = 0.0

    # Required fields (60% total)
    if director_name:
        score += 0.20
    if date_of_change:
        score += 0.20
    if quantity is not None and quantity > 0:
        score += 0.20

    # Optional but important fields (40% total)
    if price and price > 0:
        score += 0.20
    if nature:
        score += 0.20

    return score


async def get_or_create_director(
    session: AsyncSession,
    director_name: str
) -> Director:
    """Get existing director or create new one."""
    # Check if director exists
    result = await session.execute(
        select(Director).where(Director.full_name == director_name)
    )
    director = result.scalar_one_or_none()

    if not director:
        # Create new director
        director = Director(full_name=director_name)
        session.add(director)
        await session.flush()  # Get the ID
        logger.info(f"Created new director: {director_name}")

    return director


async def get_or_create_director_company(
    session: AsyncSession,
    director_id: str,
    company_id: str
) -> DirectorCompany:
    """Get existing director-company relationship or create new one."""
    result = await session.execute(
        select(DirectorCompany).where(
            DirectorCompany.director_id == director_id,
            DirectorCompany.company_id == company_id
        )
    )
    dc = result.scalar_one_or_none()

    if not dc:
        # Create new relationship
        dc = DirectorCompany(
            director_id=director_id,
            company_id=company_id,
            trade_count=0
        )
        session.add(dc)
        await session.flush()
        logger.debug(f"Created director-company relationship: D{director_id}-C{company_id}")

    return dc


async def parse_pdf(
    record: Pending3YParse,
    session: AsyncSession,
    stats: dict[str, int]
) -> None:
    """Parse a single PDF and create trade record."""
    logger.info(f"Parsing {record.ticker}: {Path(record.pdf_path).name}")

    # Extract text from PDF
    text = extract_text_from_pdf(record.pdf_path)

    if not text:
        logger.error(f"Could not extract text from {record.pdf_path}")
        record.status = ParseStatus.FAILED
        record.parse_attempts += 1
        record.error_message = "Failed to extract text from PDF"
        stats["failed"] += 1
        return

    # Extract fields
    director_name = parse_director_name(text)
    date_of_change = parse_date_of_change(text)
    num_acquired = parse_number_acquired(text)
    num_disposed = parse_number_disposed(text)
    price_per_share = parse_price_per_share(text)
    nature_of_change = parse_nature_of_change(text)

    # Try fallback dates if main date extraction failed
    if not date_of_change:
        logger.debug(f"Date of change not found, trying fallbacks...")

        # Fallback 1: Try "Date of last notice"
        date_of_change = parse_date_of_last_notice(text)
        if date_of_change:
            logger.debug(f"  ✓ Using 'Date of last notice' as fallback: {date_of_change}")

        # Fallback 2: Try to extract any dates from the document
        if not date_of_change:
            all_dates = extract_dates_from_text(text)
            if all_dates:
                # Use the most recent date (most likely to be the trade date)
                date_of_change = max(all_dates)
                logger.debug(f"  ✓ Using most recent extracted date as fallback: {date_of_change}")

        # Fallback 3: Use document submission date
        if not date_of_change:
            logger.debug(f"  ✓ Using document submission date as final fallback: {record.document_date}")
            date_of_change = record.document_date

    # Check CRITICAL fields - these are required
    # Missing any of these = reject the record
    if not date_of_change:
        logger.error(f"No date found for {record.ticker} (no fallback available)")
        record.status = ParseStatus.FAILED
        record.parse_attempts += 1
        record.error_message = "No date of change found (all fallbacks exhausted)"
        stats["failed"] += 1
        return

    # Determine transaction type and quantity
    if num_acquired and num_acquired > 0:
        trade_type = map_trade_type(nature_of_change) if nature_of_change else TradeType.ON_MARKET_BUY
        quantity = num_acquired
    elif num_disposed and num_disposed > 0:
        trade_type = TradeType.ON_MARKET_SELL
        quantity = -num_disposed  # Negative for sales
    else:
        logger.warning(f"No quantity found for {record.ticker}")
        record.status = ParseStatus.FAILED
        record.parse_attempts += 1
        record.error_message = "No quantity (acquired or disposed) found"
        stats["failed"] += 1
        return

    # Handle missing director name - use generic placeholder
    # Better to capture partial data than reject the record
    if not director_name or director_name.strip() == "":
        director_name = "Unknown Director"
        logger.warning(f"No director name found for {record.ticker}, using '{director_name}'")

    # Calculate confidence
    # Lower confidence if using generic director name
    confidence = calculate_parse_confidence(
        director_name if director_name != "Unknown Director" else None,
        date_of_change,
        abs(quantity),
        price_per_share,
        nature_of_change
    )

    # Accept records with lower confidence if we have the critical fields
    # Even partial data is valuable
    if confidence < 0.2:
        logger.warning(f"Very low confidence ({confidence:.2f}) for {record.ticker}")
        record.status = ParseStatus.FAILED
        record.parse_attempts += 1
        record.error_message = f"Parse confidence too low: {confidence:.2f}"
        stats["failed"] += 1
        return

    # Log extracted data
    director_label = f"{director_name} {'[GENERIC]' if director_name == 'Unknown Director' else ''}"
    logger.debug(f"  Director: {director_label}")
    logger.debug(f"  Date: {date_of_change}")
    logger.debug(f"  Quantity: {quantity}")
    logger.debug(f"  Price: ${price_per_share if price_per_share else 'N/A'}")
    logger.debug(f"  Nature: {nature_of_change if nature_of_change else 'N/A'}")
    logger.debug(f"  Confidence: {confidence:.2f}")

    # Get company
    result = await session.execute(
        select(Company).where(Company.ticker == record.ticker)
    )
    company = result.scalar_one_or_none()

    if not company:
        logger.error(f"Company not found for ticker {record.ticker}")
        record.status = ParseStatus.FAILED
        record.parse_attempts += 1
        record.error_message = f"Company not found: {record.ticker}"
        stats["failed"] += 1
        return

    # Get or create director (including "Unknown Director" placeholder)
    director = await get_or_create_director(session, director_name)

    # Get or create director-company relationship
    dc = await get_or_create_director_company(session, director.id, company.id)

    # Create trade record
    # Note: date_lodged is the date the form was lodged with ASX (document_date)
    trade = Trade(
        director_id=director.id,
        company_id=company.id,
        date_of_trade=date_of_change or record.document_date,  # Fallback to document date
        date_lodged=record.document_date,  # Date the 3Y form was lodged
        quantity=quantity,
        price_per_share=float(price_per_share) if price_per_share else None,
        trade_type=trade_type
    )

    session.add(trade)

    # Update director-company trade count
    dc.trade_count += 1

    # Update parse record
    record.status = ParseStatus.PARSED
    record.parse_attempts += 1

    stats["parsed"] += 1
    logger.info(f"✓ Parsed successfully (confidence: {confidence:.2f})")


async def main(limit: int | None = None) -> None:
    """Main entry point."""
    logger.info("Starting PDF parser for Appendix 3Y forms")

    # Initialize stats
    stats = {
        "parsed": 0,
        "failed": 0,
        "skipped": 0,
    }

    async with async_session() as session:
        # Query pending records
        query = select(Pending3YParse).where(
            Pending3YParse.status == ParseStatus.PENDING
        ).order_by(Pending3YParse.document_date.desc())

        if limit:
            query = query.limit(limit)

        result = await session.execute(query)
        records = result.scalars().all()

        logger.info(f"Found {len(records)} pending PDFs to parse")

        if not records:
            logger.info("No pending PDFs to parse")
            return

        # Parse each PDF
        for i, record in enumerate(records, 1):
            logger.info(f"\n[{i}/{len(records)}] Processing record {i}")

            try:
                await parse_pdf(record, session, stats)

                # Commit every 10 records
                if i % 10 == 0:
                    await session.commit()
                    logger.info(f"Committed batch at {i} records")

            except Exception as e:
                logger.error(f"Error parsing {ticker}: {e}")
                record.status = ParseStatus.FAILED
                record.parse_attempts += 1
                record.error_message = str(e)[:500]
                stats["failed"] += 1
                await session.rollback()

        # Final commit
        await session.commit()

    # Print summary
    print("\n" + "=" * 60)
    print("PARSER SUMMARY")
    print("=" * 60)
    print(f"Successfully parsed: {stats['parsed']}")
    print(f"Failed: {stats['failed']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"Total processed: {stats['parsed'] + stats['failed']}")
    print("=" * 60)

    logger.info("PDF parsing completed")


if __name__ == "__main__":
    import asyncio

    parser = argparse.ArgumentParser(
        description="Parse Appendix 3Y PDFs and extract trade data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of PDFs to parse (for testing)"
    )

    args = parser.parse_args()

    asyncio.run(main(limit=args.limit))
