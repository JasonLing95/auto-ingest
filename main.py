from edgar import set_identity, Company, get_filings, httpclient
import httpx
import random
import datetime as dt
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from collections import deque
import os
import argparse
import time
import requests
from threading import Thread
from types import SimpleNamespace
from httpx import HTTPStatusError
from lxml import etree  # type: ignore
import psycopg2
import sys
from typing import Optional

from utils import (
    connect_db,
    get_latest_filing_using_cik_number,
    insert_filing,
    insert_holdings_batch,
    gather_holdings_using_lxml,
    filing_exists,
    gather_data_with_bs4,
    update_company_aum_post_ingest,
    prioritize_ciks,
)
from get_logger import get_logger

# Configure logging
logger = get_logger(__name__)

# User Agent
set_identity(os.getenv("SEC_IDENTITY", "example121.company@access.com"))


def configure_edgar_http_timeout(
    read_timeout: float = 30.0, connect_timeout: float = 10.0
) -> httpx.Timeout:
    """Configure explicit HTTP timeouts on edgartools client manager."""
    timeout = httpx.Timeout(read_timeout, connect=connect_timeout)
    if not hasattr(httpclient, "HTTP_MGR"):
        params_dict = (
            httpclient.DEFAULT_PARAMS
            if hasattr(httpclient, "DEFAULT_PARAMS")
            and isinstance(httpclient.DEFAULT_PARAMS, dict)
            else {}
        )
        httpclient.HTTP_MGR = SimpleNamespace(httpx_params=params_dict)
    if hasattr(httpclient.HTTP_MGR, "httpx_params"):
        httpclient.HTTP_MGR.httpx_params["timeout"] = timeout
    if hasattr(httpclient, "DEFAULT_PARAMS") and isinstance(
        httpclient.DEFAULT_PARAMS, dict
    ):
        httpclient.DEFAULT_PARAMS["timeout"] = timeout
    if hasattr(httpclient, "close_clients"):
        httpclient.close_clients()
    return timeout


configure_edgar_http_timeout()

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "https://example.com/healthcheck")
MAX_RETRIES = 3
VALID_FORMS = ["13F-HR", "13F-HR/A", "13F-HR/A/A"]  # Valid forms to fetch filings for
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "edgar_db.db")

LAST_MAIN_LOOP_TIMESTAMP = time.time()
HEARTBEAT_INTERVAL_SECONDS = 30
HEARTBEAT_MAX_INACTIVE_SECONDS = 180
INDEX_SWEEP_INTERVAL_SECONDS = 600
_HEARTBEAT_STARTED = False


def update_heartbeat_timestamp() -> None:
    """Update the global main-loop activity timestamp for the heartbeat watchdog."""
    global LAST_MAIN_LOOP_TIMESTAMP
    LAST_MAIN_LOOP_TIMESTAMP = time.time()


def heartbeat() -> None:
    """Background daemon thread that pings HEALTHCHECK_URL every 30s if the main loop is active."""
    while True:
        inactive_seconds = time.time() - LAST_MAIN_LOOP_TIMESTAMP
        if inactive_seconds <= HEARTBEAT_MAX_INACTIVE_SECONDS:
            try:
                requests.get(HEALTHCHECK_URL, timeout=10)
            except Exception as e:
                logger.error(f"Healthcheck ping failed: {str(e)}")
        else:
            logger.warning(
                f"Main loop inactive for {int(inactive_seconds)}s (> {HEARTBEAT_MAX_INACTIVE_SECONDS}s). Suppressing heartbeat ping."
            )
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def start_heartbeat_thread() -> Optional[Thread]:
    """Start the background daemon heartbeat thread once."""
    global _HEARTBEAT_STARTED
    if _HEARTBEAT_STARTED:
        return None
    _HEARTBEAT_STARTED = True
    t = Thread(target=heartbeat, daemon=True, name="healthcheck-heartbeat")
    t.start()
    return t


def normalize_cik(cik) -> str:
    """Normalize a CIK string/int by stripping whitespace and leading zeros."""
    s = str(cik).strip()
    if not s:
        return ""
    return str(int(s)) if s.isdigit() else (s.lstrip("0") or s)


def compute_query_start_date(
    latest_filing_date: Optional[dt.date],
    today: Optional[dt.date] = None,
) -> dt.date:
    """Return the EDGAR query start date for a CIK.

    Advances past ``latest_filing_date`` by 1 day when strictly older than
    yesterday so EDGAR's inclusive date filter does not re-fetch already-ingested
    filings on every cycle, while keeping yesterday/today inclusive so same-day
    amendments or multi-filing batches are not missed.
    """
    current_date = today or dt.date.today()
    if not latest_filing_date:
        return current_date - dt.timedelta(days=730)
    if isinstance(latest_filing_date, dt.datetime):
        latest_filing_date = latest_filing_date.date()
    yesterday = current_date - dt.timedelta(days=1)
    if latest_filing_date < yesterday:
        return latest_filing_date + dt.timedelta(days=1)
    return min(latest_filing_date, current_date)


def find_namespaces(tree):
    """Find all unique namespaces in the XML tree."""
    all_elements = tree.xpath("//*")
    unique_prefixes = set()
    for elem in all_elements:
        if elem.prefix:
            unique_prefixes.add(elem.prefix)

    return list(unique_prefixes)


def ingest_single_filing(conn, cik_number: str, filing) -> bool:
    """Ingest a single 13F filing, its XML holdings, and update companies.aum post-ingest."""
    accession_number = filing.accession_number

    if filing_exists(conn, accession_number):
        logger.info(
            f"Filing {accession_number} already exists in database, skipping."
        )
        return False

    new_filing_id = insert_filing(conn, cik_number, filing)
    if not new_filing_id:
        logger.info(
            f"Filing {accession_number} already handled concurrently; skipping holdings."
        )
        return False

    for attachment in list(filing.attachments):
        if (
            attachment.document.endswith(".xml")
            and "primary_doc" not in attachment.document
        ):
            logger.info(f"Processing XML attachment: {attachment.url}")
            assert attachment.content

            if isinstance(attachment.content, str):
                xml_content = attachment.content.encode("utf-8")
            else:
                xml_content = attachment.content

            try:
                tree = etree.fromstring(xml_content)
                nsmap = tree.nsmap
                ns = {
                    "ns": (
                        nsmap[None]
                        if None in nsmap
                        else (
                            nsmap["ns1"]
                            if "ns1" in nsmap
                            else nsmap[find_namespaces(tree)[0]]
                        )
                    )
                }
                info_tables = tree.xpath("//ns:infoTable", namespaces=ns)
                raw_holdings = gather_holdings_using_lxml(
                    info_tables,
                    ns,
                    cik_number,
                    accession_number,
                )
            except Exception:
                raw_holdings = gather_data_with_bs4(
                    attachment.content,
                    cik_number,
                    accession_number,
                )

            if raw_holdings:
                insert_holdings_batch(conn, raw_holdings)
            update_heartbeat_timestamp()

    # Event-driven: update company's latest AUM immediately post-ingest
    try:
        update_company_aum_post_ingest(conn, company_id=None, filing_id=new_filing_id)
    except Exception as aum_err:
        logger.error(
            f"Error updating company AUM post-ingest for filing {new_filing_id}: {aum_err}"
        )

    return True


@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=10, period=1)
def run_daily_index_sweep(
    conn,
    relevant_ciks: set[str],
    filing_date_str: Optional[str] = None,
) -> int:
    """Bulk daily index sweep across all relevant CIKs using a single SEC index lookup."""
    if not relevant_ciks:
        return 0
    if not filing_date_str:
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        filing_date_str = f"{yesterday.strftime('%Y-%m-%d')}:{today.strftime('%Y-%m-%d')}"

    logger.info(f"Running bulk daily index sweep for filing_date={filing_date_str}...")
    filings = get_filings(form=VALID_FORMS, filing_date=filing_date_str)
    if not filings:
        logger.info(f"Bulk daily index sweep found 0 filings for {filing_date_str}.")
        return 0

    ingested = 0
    for filing in filings:
        update_heartbeat_timestamp()
        cik_str = normalize_cik(getattr(filing, "cik", ""))
        if cik_str not in relevant_ciks:
            continue
        try:
            if ingest_single_filing(conn, cik_str, filing):
                ingested += 1
        except Exception as e:
            logger.error(
                f"Failed ingesting filing during index sweep for CIK {cik_str}: {e}"
            )

    logger.info(f"Bulk daily index sweep complete: ingested {ingested} new filings.")
    return ingested


@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=10, period=1)  # Limit to 10 calls per second
def fetch_new_filing(
    conn,
    cik_number,
    most_recent_date: dt.date,
):
    """
    Fetch new filings for a given CIK number since the most recent date in the database.
    1. Fetch filings from SEC Edgar using edgar package.
    2. For each filing, check if it exists in the database. If not, insert it.
    3. For each filing, parse XML attachments to extract holdings and insert them into the database.
    4. Handle HTTP errors with retries.
    """
    c = Company(cik_number)
    today_date = dt.date.today().strftime("%Y-%m-%d")

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(
                f"filing_date: {most_recent_date.strftime('%Y-%m-%d')}:{today_date}"
            )
            # Fetch filings from (equal or greater than) latest filing date to today
            filings = c.get_filings(
                sort_by="filing_date",
                form=VALID_FORMS,
                filing_date=(
                    f"{most_recent_date.strftime('%Y-%m-%d')}:{today_date}"
                ),  # this is fetching from most recent date to today's date
            )

            logger.info(f"Fetched {len(filings)} filings for CIK {cik_number}")

            if not filings:
                logger.info(
                    f"No new valid filings found for CIK {cik_number} since {most_recent_date}"
                )
                return

            ingested_count = 0
            for filing in filings:
                if ingest_single_filing(conn, cik_number, filing):
                    ingested_count += 1

            if ingested_count == 0:
                logger.info(
                    f"No new filings ingested for CIK {cik_number} since {most_recent_date} (checked {len(filings)} filings)."
                )

            return
        except HTTPStatusError as e:
            # Handle HTTP errors, retry on server errors
            if (
                e.response.status_code in [500, 503, 504]
                and (attempt + 1) < MAX_RETRIES
            ):
                logger.info(
                    f"HTTP error {e.response.status_code} for CIK {cik_number}, retrying..."
                )
                time.sleep(2 * (attempt + 1))
                continue
            logger.error(e)
        except Exception as e:
            # should raise any other error outside of HTTP errors
            raise Exception(f"Error fetching filings for CIK {cik_number}: {str(e)}")


def main(conn, cik_queue: deque):
    """Main processing loop for a list of CIKs."""
    start_heartbeat_thread()

    base_ciks = [str(c).strip() for c in cik_queue if str(c).strip()]
    relevant_ciks = {normalize_cik(c) for c in base_ciks}
    cik_queue = deque(prioritize_ciks(conn, base_ciks))
    cycle_remaining = len(cik_queue)
    last_index_sweep_time = 0.0

    logger.info(f"Processing {len(cik_queue)} CIKs")

    while True:
        update_heartbeat_timestamp()

        if not cik_queue:
            logger.info("CIK queue is empty. Sleeping for 3 seconds...")
            time.sleep(3)
            continue

        if conn.closed != 0:
            logger.warning("Database connection lost. Reconnecting...")
            try:
                conn = connect_db()
            except Exception as e:
                logger.error(
                    f"Failed to reconnect to database: {e}. Retrying in 10s..."
                )
                time.sleep(10)
                continue

        if time.time() - last_index_sweep_time >= INDEX_SWEEP_INTERVAL_SECONDS:
            try:
                run_daily_index_sweep(conn, relevant_ciks)
            except Exception as sweep_err:
                logger.error(f"Periodic daily index sweep failed: {sweep_err}")
            last_index_sweep_time = time.time()

        cik = cik_queue.popleft()
        logger.info(f"Processing CIK: {cik}")

        try:
            latest_filing_date_in_db: dt.date = get_latest_filing_using_cik_number(
                conn, cik
            )

            if latest_filing_date_in_db:
                logger.info(
                    f"Most recent filing date for {cik}: {latest_filing_date_in_db}"
                )
            else:
                logger.info(f"No filings found for {cik}. Fetching all available.")

            start_date = compute_query_start_date(latest_filing_date_in_db)
            fetch_new_filing(conn, cik, start_date)

        except Exception as e:
            # If a specific CIK fails, log it, but KEEP RUNNING!
            logger.error(f"Failed processing CIK {cik}: {e}")

        cik_queue.append(cik)
        cycle_remaining -= 1
        if cycle_remaining <= 0 and base_ciks:
            cik_queue = deque(prioritize_ciks(conn, base_ciks))
            cycle_remaining = len(cik_queue)

        sleep_time = 2.0 + random.uniform(0.5, 1.5)
        logger.info(f"Sleeping for {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    # connect to database
    conn = connect_db()

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cik", default=os.getenv("CIK_FILE", "ciks.txt"), help="Path to CIK file"
    )
    args = parser.parse_args()

    logger.info(f"Reading: {args.cik}")

    # Read CIKs from file
    try:
        with open(args.cik, "r") as f:
            ciks = [line.strip() for line in f.read().splitlines() if line.strip()]
        logger.info(f"Loaded {len(ciks)} CIKs")
    except FileNotFoundError:
        logger.critical(f"CIK file not found at {args.cik}. Exiting.")
        exit(1)

    # Loop through CIKs and process filings
    cik_queue = deque(ciks)
    main(conn, cik_queue)
