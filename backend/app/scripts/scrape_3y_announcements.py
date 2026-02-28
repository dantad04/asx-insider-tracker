"""
Scrape Appendix 3Y announcements from ASX website using BeautifulSoup and download PDFs.

Usage:
    # Scrape today's announcements (recommended - fastest, no JavaScript)
    python -m app.scripts.scrape_3y_announcements --today

    # Scrape specific tickers (for backlog/historical)
    python -m app.scripts.scrape_3y_announcements --tickers BHP,CBA,WES

    # Scrape all companies in database
    python -m app.scripts.scrape_3y_announcements --all

    # Retry failed tickers
    python -m app.scripts.scrape_3y_announcements --retry-from /app/data/failed_tickers_20240223_140000.json
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.company import Company
from app.models.pending_3y_parse import Pending3YParse, ParseStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
ASX_ANNOUNCEMENTS_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"
ASX_TODAY_ANNS_URL = "https://www.asx.com.au/asx/v2/statistics/todayAnns.do"
ASX_BASE_URL = "https://www.asx.com.au"
PDF_DOWNLOAD_DIR = Path("/app/data/pdfs/raw")
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 60  # seconds
# How many years back to search for 3Y announcements
YEARS_TO_SEARCH = 3

# User agent to avoid bot detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class RateLimiter:
    """Rate limiter with jitter to prevent API throttling."""

    def __init__(self, requests_per_second: float = 1.0, jitter_ms: int = 500):
        self.min_interval = 1.0 / requests_per_second
        self.jitter_ms = jitter_ms
        self._lock = asyncio.Lock()
        self._last_request = 0.0

    async def acquire(self):
        """Wait until next request is allowed."""
        async with self._lock:
            now = time.time()
            time_since_last = now - self._last_request
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                jitter = random.uniform(0, self.jitter_ms / 1000)
                await asyncio.sleep(sleep_time + jitter)
            self._last_request = time.time()


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    rate_limiter: RateLimiter,
    max_retries: int = MAX_RETRIES
) -> httpx.Response | None:
    """
    Fetch URL with exponential backoff retry on 403/429.

    Returns None if all retries fail.
    """
    for attempt in range(max_retries):
        await rate_limiter.acquire()

        try:
            response = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)

            if response.status_code == 200:
                return response

            if response.status_code in (403, 429):
                backoff_time = RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    f"Rate limited ({response.status_code}) on {url}. "
                    f"Waiting {backoff_time}s before retry {attempt + 1}/{max_retries}"
                )
                await asyncio.sleep(backoff_time)
                continue

            logger.error(f"HTTP {response.status_code} for {url}")
            return None

        except httpx.TimeoutException:
            logger.error(f"Timeout fetching {url}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
                continue
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    return None


async def is_duplicate(
    session: AsyncSession,
    pdf_url: str
) -> bool:
    """Check if PDF URL already exists in database."""
    result = await session.execute(
        select(Pending3YParse).where(Pending3YParse.pdf_url == pdf_url)
    )
    return result.scalar_one_or_none() is not None


def is_3y_announcement(title: str) -> bool:
    """Check if announcement title contains '3Y' or 'Appendix 3Y'."""
    title_lower = title.lower()
    return "3y" in title_lower or "appendix 3y" in title_lower


async def download_pdf(
    client: httpx.AsyncClient,
    pdf_url: str,
    rate_limiter: RateLimiter
) -> bytes | None:
    """Download PDF content from ASX."""
    response = await fetch_with_retry(client, pdf_url, rate_limiter)
    if response and response.headers.get("content-type", "").startswith("application/pdf"):
        return response.content
    return None


def compute_pdf_hash(content: bytes) -> str:
    """Compute SHA256 hash of PDF content (first 12 chars)."""
    return hashlib.sha256(content).hexdigest()[:12]


async def save_pdf(
    ticker: str,
    document_date: str,
    pdf_content: bytes
) -> str:
    """
    Save PDF to disk with unique filename.
    Returns absolute path.
    """
    pdf_hash = compute_pdf_hash(pdf_content)
    filename = f"{ticker}_{document_date}_{pdf_hash}.pdf"
    pdf_path = PDF_DOWNLOAD_DIR / filename

    # Check if file already exists
    if pdf_path.exists():
        logger.info(f"PDF already exists: {filename}")
        return str(pdf_path)

    # Write PDF
    pdf_path.write_bytes(pdf_content)
    logger.info(f"Saved PDF: {filename}")
    return str(pdf_path)


def parse_asx_date(date_str: str) -> datetime.date | None:
    """
    Parse ASX date string to datetime.date.

    ASX uses formats like: "23/02/2026", "2026-02-23", "23 Feb 2026"
    """
    date_formats = [
        "%d/%m/%Y",      # 23/02/2026
        "%Y-%m-%d",      # 2026-02-23
        "%d %b %Y",      # 23 Feb 2026
        "%d %B %Y",      # 23 February 2026
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def extract_direct_pdf_url(link_element) -> str | None:
    """
    Extract direct PDF URL from announcement link.

    Direct URLs have format: https://announcements.asx.com.au/asxpdf/YYYYMMDD/pdf/[HASH].pdf
    Old displayAnnouncement.do URLs are NOT used.
    """
    if not link_element:
        return None

    href = link_element.get("href", "")

    # If it's already a direct PDF URL, return it
    if "announcements.asx.com.au/asxpdf/" in href or "/asxpdf/" in href:
        # Make it absolute if needed
        if href.startswith("http"):
            return href
        elif href.startswith("/"):
            return f"https://announcements.asx.com.au{href}"
        else:
            return f"https://announcements.asx.com.au/{href}"

    # If it's a displayAnnouncement.do URL, we can't use it (needs JavaScript)
    # Return None and log a warning
    if "displayAnnouncement" in href:
        logger.debug(f"Skipping displayAnnouncement.do URL (needs JavaScript): {href}")
        return None

    return None


async def scrape_today_announcements(
    session: AsyncSession,
    client: httpx.AsyncClient,
    rate_limiter: RateLimiter,
    stats: dict[str, int]
) -> None:
    """
    Scrape today's announcements from ASX todayAnns.do endpoint.

    This is the RECOMMENDED approach - it's faster and doesn't require JavaScript.
    Gets all 3Y announcements for ALL tickers from today in one request.
    """
    logger.info("Fetching today's announcements from ASX")

    # Fetch today's announcements page
    response = await fetch_with_retry(client, ASX_TODAY_ANNS_URL, rate_limiter)
    if not response:
        logger.error("Failed to fetch today's announcements")
        return

    # Parse HTML with BeautifulSoup
    try:
        soup = BeautifulSoup(response.text, "lxml")
    except Exception as e:
        logger.error(f"Failed to parse HTML: {e}")
        return

    # Find the announcements table
    # The todayAnns.do page has a table with columns: Code, Time, Headline
    table = soup.find("table")
    if not table:
        logger.warning("No announcements table found on todayAnns.do page")
        return

    # Get all table rows (skip header row)
    rows = table.find_all("tr")
    if len(rows) <= 1:
        logger.info("No announcements found for today")
        return

    # Skip the header row
    data_rows = rows[1:]
    logger.info(f"Found {len(data_rows)} announcements for today")

    # Track tickers processed
    tickers_found = set()

    # Parse each announcement row
    for row in data_rows:
        try:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Expected structure: [Code, Time, Headline (with PDF link)]
            ticker_cell = cells[0]
            time_cell = cells[1]
            headline_cell = cells[2]

            # Extract ticker
            ticker = ticker_cell.get_text(strip=True).upper()
            if not ticker:
                continue

            # Extract title from headline cell
            title = headline_cell.get_text(strip=True)

            # Remove PDF size info from title (e.g., "2 pages 119.9 KB")
            title = re.sub(r'\d+\s+pages?\s+\d+(\.\d+)?\s*[KM]B', '', title, flags=re.I).strip()

            # Check if it's a 3Y announcement
            if not is_3y_announcement(title):
                continue

            stats["3y_announcements_found"] += 1
            tickers_found.add(ticker)
            logger.info(f"Found 3Y announcement for {ticker}: {title}")

            # Extract PDF link from headline cell
            # Look for direct PDF URLs (announcements.asx.com.au/asxpdf/...)
            pdf_link = headline_cell.find("a", href=True)
            pdf_url = extract_direct_pdf_url(pdf_link)

            if not pdf_url:
                logger.warning(f"No direct PDF URL found for {ticker}: {title}")
                # Try to find any link with .pdf in it
                all_links = headline_cell.find_all("a", href=True)
                for link in all_links:
                    href = link.get("href", "")
                    if ".pdf" in href.lower():
                        pdf_url = extract_direct_pdf_url(link)
                        if pdf_url:
                            logger.info(f"Found PDF URL on retry: {pdf_url}")
                            break

                if not pdf_url:
                    logger.error(f"Could not find direct PDF URL for {ticker}: {title}")
                    stats["pdf_download_failed"] += 1
                    continue

            # Check if already processed
            if await is_duplicate(session, pdf_url):
                logger.debug(f"Duplicate PDF URL: {pdf_url}")
                stats["duplicates_skipped"] += 1
                continue

            # Extract time and construct today's date
            # Time format is like: "09:15 AM"
            time_str = time_cell.get_text(strip=True)
            document_date = datetime.now().date()

            # Download PDF
            pdf_content = await download_pdf(client, pdf_url, rate_limiter)
            if not pdf_content:
                logger.error(f"Failed to download PDF: {pdf_url}")
                stats["pdf_download_failed"] += 1
                continue

            # Save PDF
            pdf_path = await save_pdf(ticker, document_date.strftime("%Y-%m-%d"), pdf_content)

            # Create database record
            try:
                parse_record = Pending3YParse(
                    ticker=ticker,
                    pdf_path=pdf_path,
                    pdf_url=pdf_url,
                    document_date=document_date,
                    announcement_header=title[:500],  # Truncate if needed
                    status=ParseStatus.PENDING,
                    parse_attempts=0,
                )
                session.add(parse_record)
                stats["pdfs_downloaded"] += 1

                # Commit every 20 records
                if stats["pdfs_downloaded"] % 20 == 0:
                    await session.commit()
                    logger.info(f"Committed {stats['pdfs_downloaded']} PDFs to database")
            except Exception as e:
                logger.error(f"Error creating database record for {ticker}: {e}")
                await session.rollback()

        except Exception as e:
            logger.error(f"Error parsing announcement row: {e}")
            continue

    # Final commit
    await session.commit()
    logger.info(f"Completed today's announcements: found 3Y announcements for {len(tickers_found)} tickers")


async def scrape_ticker_announcements_api(
    ticker: str,
    session: AsyncSession,
    client: httpx.AsyncClient,
    rate_limiter: RateLimiter,
    stats: dict[str, int]
) -> None:
    """
    Scrape announcements for a single ticker using ASX API.

    Uses the ASX Market Data API to fetch announcements and filter for 3Y forms.
    Much faster than Playwright approach!
    """
    logger.info(f"Fetching announcements for {ticker} via API")

    # Apply rate limiting
    await rate_limiter.acquire()

    # Fetch announcements from API
    api_url = f"https://asx.api.markitdigital.com/asx-research/1.0/companies/{ticker}/announcements?count=200"

    try:
        response = await fetch_with_retry(client, api_url, rate_limiter)
        if not response:
            logger.error(f"Failed to fetch announcements for {ticker}")
            return

        data = response.json()
        items = data.get("data", {}).get("items", [])
        logger.debug(f"Found {len(items)} total announcements for {ticker}")

        # Filter for 3Y announcements
        for item in items:
            headline = item.get("headline", "")

            # Check if it's a 3Y announcement
            if not is_3y_announcement(headline):
                continue

            stats["3y_announcements_found"] += 1
            logger.info(f"Found 3Y announcement for {ticker}: {headline}")

            # Extract document details
            document_key = item.get("documentKey", "")
            date_str = item.get("date", "")

            # Parse date (format: "2026-02-16T21:39:43.000Z")
            document_date = None
            if date_str:
                try:
                    document_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                except ValueError:
                    logger.warning(f"Could not parse date: {date_str}")
                    document_date = datetime.now().date()
            else:
                document_date = datetime.now().date()

            # Construct PDF URL from documentKey
            # documentKey format: "2924-03057190-3A687227"
            # PDF URL format: https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/{documentKey}&v=undefined
            if document_key:
                pdf_url = f"https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/{document_key}&v=undefined"
            else:
                logger.warning(f"No documentKey for {ticker}: {headline}")
                stats["pdf_download_failed"] += 1
                continue

            # Check if already processed
            if await is_duplicate(session, pdf_url):
                logger.debug(f"Duplicate PDF URL: {pdf_url}")
                stats["duplicates_skipped"] += 1
                continue

            # Download PDF
            pdf_content = await download_pdf(client, pdf_url, rate_limiter)
            if not pdf_content:
                logger.error(f"Failed to download PDF: {pdf_url}")
                stats["pdf_download_failed"] += 1
                continue

            # Save PDF
            pdf_path = await save_pdf(ticker, document_date.strftime("%Y-%m-%d"), pdf_content)

            # Create database record
            try:
                parse_record = Pending3YParse(
                    ticker=ticker,
                    pdf_path=pdf_path,
                    pdf_url=pdf_url,
                    document_date=document_date,
                    announcement_header=headline[:500],  # Truncate if needed
                    status=ParseStatus.PENDING,
                    parse_attempts=0,
                )
                session.add(parse_record)
                stats["pdfs_downloaded"] += 1

                # Commit every 20 records
                if stats["pdfs_downloaded"] % 20 == 0:
                    await session.commit()
                    logger.info(f"Committed {stats['pdfs_downloaded']} PDFs to database")
            except Exception as e:
                logger.error(f"Error creating database record for {ticker}: {e}")
                await session.rollback()

    except Exception as e:
        logger.error(f"Error scraping {ticker} via API: {e}")
        return

    # Log completion
    if stats.get("3y_announcements_found", 0) == 0:
        logger.debug(f"No 3Y announcements found for {ticker}")


async def scrape_tickers(
    tickers: list[str],
    max_concurrent: int = 5  # Higher concurrency for API (no browser overhead)
) -> dict[str, int]:
    """
    Scrape announcements for multiple tickers using ASX API.

    Returns statistics dictionary.
    """
    # Ensure PDF directory exists
    PDF_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize stats
    stats = {
        "tickers_processed": 0,
        "failed_tickers": 0,
        "3y_announcements_found": 0,
        "pdfs_downloaded": 0,
        "duplicates_skipped": 0,
        "pdf_download_failed": 0,
    }

    failed_tickers = []

    # Create HTTP client and rate limiter
    async with httpx.AsyncClient() as client:
        rate_limiter = RateLimiter(requests_per_second=2.0, jitter_ms=500)  # Faster for API
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_with_limit(ticker: str):
            async with semaphore:
                async with async_session() as session:
                    try:
                        await scrape_ticker_announcements_api(
                            ticker, session, client, rate_limiter, stats
                        )
                        await session.commit()
                        stats["tickers_processed"] += 1
                        logger.info(f"Completed {ticker}: {stats['tickers_processed']}/{len(tickers)} tickers")
                    except Exception as e:
                        logger.error(f"Error processing {ticker}: {e}")
                        failed_tickers.append({"ticker": ticker, "error": str(e)})
                        stats["failed_tickers"] += 1
                        await session.rollback()

        # Process all tickers
        tasks = [process_with_limit(ticker) for ticker in tickers]
        await asyncio.gather(*tasks, return_exceptions=True)

    # Save failed tickers if any
    if failed_tickers:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failure_file = PDF_DOWNLOAD_DIR.parent / f"failed_tickers_{timestamp}.json"
        with open(failure_file, "w") as f:
            json.dump(failed_tickers, f, indent=2)
        logger.warning(f"Failed tickers saved to {failure_file}")

    return stats


async def get_all_tickers() -> list[str]:
    """Get all company tickers from database."""
    async with async_session() as session:
        result = await session.execute(select(Company.ticker))
        return [row[0] for row in result.all()]


async def main(args: argparse.Namespace) -> None:
    """Main entry point."""
    logger.info("Starting ASX Appendix 3Y scraper")

    # Ensure PDF directory exists
    PDF_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    start_time = time.time()

    # Check if using today's announcements approach (recommended)
    if args.today:
        logger.info("Using TODAY'S ANNOUNCEMENTS mode (recommended)")

        # Initialize stats
        stats = {
            "3y_announcements_found": 0,
            "pdfs_downloaded": 0,
            "duplicates_skipped": 0,
            "pdf_download_failed": 0,
        }

        # Run today's announcements scraper
        async with httpx.AsyncClient() as client:
            rate_limiter = RateLimiter(requests_per_second=1.0, jitter_ms=500)
            async with async_session() as session:
                try:
                    await scrape_today_announcements(session, client, rate_limiter, stats)
                    await session.commit()
                except Exception as e:
                    logger.error(f"Error scraping today's announcements: {e}")
                    await session.rollback()

        elapsed_time = time.time() - start_time

        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPER SUMMARY (TODAY'S ANNOUNCEMENTS)")
        print("=" * 60)
        print(f"Appendix 3Y announcements found: {stats['3y_announcements_found']}")
        print(f"PDFs downloaded: {stats['pdfs_downloaded']}")
        print(f"Duplicates skipped: {stats['duplicates_skipped']}")
        print(f"PDF download failures: {stats['pdf_download_failed']}")
        print(f"Elapsed time: {elapsed_time:.2f}s")
        print("=" * 60)
        logger.info("Scraper completed successfully")
        return

    # Otherwise, use ticker-based approach (for backlog/historical)
    logger.info("Using TICKER-BASED mode (for backlog/historical)")

    # Determine tickers to process
    if args.retry_from:
        with open(args.retry_from, "r") as f:
            failed_data = json.load(f)
            tickers = [item["ticker"] for item in failed_data]
        logger.info(f"Retrying {len(tickers)} failed tickers from {args.retry_from}")
    elif args.all:
        tickers = await get_all_tickers()
        logger.info(f"Processing all {len(tickers)} tickers from database")
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
        logger.info(f"Processing {len(tickers)} specified tickers")
    else:
        logger.error("Must specify --today, --tickers, --all, or --retry-from")
        sys.exit(1)

    if not tickers:
        logger.error("No tickers to process")
        sys.exit(1)

    # Run scraper
    stats = await scrape_tickers(tickers, max_concurrent=5)
    elapsed_time = time.time() - start_time

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPER SUMMARY (TICKER-BASED)")
    print("=" * 60)
    print(f"Tickers processed: {stats['tickers_processed']}/{len(tickers)}")
    print(f"Failed tickers: {stats['failed_tickers']}")
    print(f"Appendix 3Y announcements found: {stats['3y_announcements_found']}")
    print(f"PDFs downloaded: {stats['pdfs_downloaded']}")
    print(f"Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"PDF download failures: {stats['pdf_download_failed']}")
    print(f"Elapsed time: {elapsed_time:.2f}s")
    print("=" * 60)
    logger.info("Scraper completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape ASX Appendix 3Y announcements and download PDFs"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--today",
        action="store_true",
        help="Scrape today's announcements (recommended - fastest, no JavaScript)"
    )
    group.add_argument(
        "--tickers",
        help="Comma-separated list of tickers for backlog/historical (e.g., BHP,CBA,WES)"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Process all tickers from companies table (for backlog/historical)"
    )
    group.add_argument(
        "--retry-from",
        help="Retry failed tickers from JSON file"
    )

    args = parser.parse_args()
    asyncio.run(main(args))
