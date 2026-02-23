"""
Scrape Appendix 3Y announcements from ASX API and download PDFs.

Usage:
    # Scrape specific tickers
    python -m app.scripts.scrape_3y_announcements --tickers BHP,CBA,WES

    # Scrape all companies in database
    python -m app.scripts.scrape_3y_announcements --all

    # Retry failed tickers
    python -m app.scripts.scrape_3y_announcements --retry-from /app/data/failed_tickers_20240223_140000.json
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
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
ASX_ANNOUNCEMENTS_URL = "https://asx.api.markitdigital.com/asx-research/1.0/companies/{ticker}/announcements"
ASX_ACCESS_TOKEN = "83ff96335c2d45a094df02a206a39ff4"
PDF_BASE_URL = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file"
PDF_DOWNLOAD_DIR = Path("/app/data/pdfs/raw")
MAX_ANNOUNCEMENTS_PER_TICKER = 20
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 60  # seconds


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
            response = await client.get(url, timeout=REQUEST_TIMEOUT)

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


def is_3y_announcement(header: str) -> bool:
    """Check if announcement header contains '3Y' or 'Appendix 3Y'."""
    header_lower = header.lower()
    return "3y" in header_lower or "appendix 3y" in header_lower


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


async def scrape_ticker_announcements(
    ticker: str,
    session: AsyncSession,
    client: httpx.AsyncClient,
    rate_limiter: RateLimiter,
    stats: dict[str, int]
) -> None:
    """
    Scrape announcements for a single ticker.
    Filters for Appendix 3Y and downloads new PDFs.
    """
    url = ASX_ANNOUNCEMENTS_URL.format(ticker=ticker)
    url += f"?access_token={ASX_ACCESS_TOKEN}"

    logger.info(f"Fetching announcements for {ticker}")

    response = await fetch_with_retry(client, url, rate_limiter)
    if not response:
        stats["failed_tickers"] += 1
        return

    try:
        data = response.json()
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response for {ticker}")
        stats["failed_tickers"] += 1
        return

    # New API structure: data.items
    response_data = data.get("data", {})
    announcements = response_data.get("items", [])
    logger.debug(f"Found {len(announcements)} announcements for {ticker}")

    for announcement in announcements:
        headline = announcement.get("headline", "")
        if not is_3y_announcement(headline):
            continue

        stats["3y_announcements_found"] += 1

        # Extract document key to build PDF URL
        document_key = announcement.get("documentKey")
        if not document_key:
            logger.warning(f"No document key for {ticker}: {headline}")
            continue

        # Build full PDF URL from document key
        pdf_url = f"{PDF_BASE_URL}/{document_key}"

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

        # Parse date from ISO datetime string (e.g., "2026-02-16T21:39:43.000Z")
        date_str = announcement.get("date", "")
        document_date = date_str[:10]  # Extract YYYY-MM-DD
        if not document_date:
            logger.warning(f"No date for {ticker}: {headline}")
            continue

        pdf_path = await save_pdf(ticker, document_date, pdf_content)

        # Create database record
        try:
            parse_record = Pending3YParse(
                ticker=ticker,
                pdf_path=pdf_path,
                pdf_url=pdf_url,
                document_date=datetime.strptime(document_date, "%Y-%m-%d").date(),
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


async def scrape_tickers(
    tickers: list[str],
    max_concurrent: int = 5
) -> dict[str, int]:
    """
    Scrape announcements for multiple tickers with concurrency control.

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
        rate_limiter = RateLimiter(requests_per_second=1.0, jitter_ms=500)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_with_limit(ticker: str):
            async with semaphore:
                async with async_session() as session:
                    try:
                        await scrape_ticker_announcements(
                            ticker, session, client, rate_limiter, stats
                        )
                        await session.commit()
                        stats["tickers_processed"] += 1
                        logger.info(f"Completed {ticker}: {stats['tickers_processed']}/{len(tickers)} tickers")
                    except Exception as e:
                        logger.error(f"Error processing {ticker}: {e}")
                        failed_tickers.append({"ticker": ticker, "error": str(e)})
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
        logger.error("Must specify --tickers, --all, or --retry-from")
        sys.exit(1)

    if not tickers:
        logger.error("No tickers to process")
        sys.exit(1)

    # Run scraper
    start_time = time.time()
    stats = await scrape_tickers(tickers, max_concurrent=5)
    elapsed_time = time.time() - start_time

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPER SUMMARY")
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
        "--tickers",
        help="Comma-separated list of tickers (e.g., BHP,CBA,WES)"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Process all tickers from companies table"
    )
    group.add_argument(
        "--retry-from",
        help="Retry failed tickers from JSON file"
    )

    args = parser.parse_args()
    asyncio.run(main(args))
