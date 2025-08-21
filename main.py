from edgar import set_identity, Company
import datetime as dt
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from bs4 import BeautifulSoup
import os
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
from pathlib import Path
import time
import requests
from threading import Thread
from httpx import HTTPStatusError

from utils import (
    get_latest_filing_using_cik_number,
    insert_filing,
    get_manager_by_cik,
    get_or_create_issuer,
    insert_holdings_batch,
    gather_holdings_from_soup,
    connect_db,
    filing_exists,
)
from get_logger import get_logger

# Configure logging
logger = get_logger(__name__)

# User Agent
set_identity(os.getenv("SEC_IDENTITY", "example2.company@access.com"))

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "https://example.com/healthcheck")
MAX_RETRIES = 3
VALID_FORMS = ["13F-HR", "13F-HR/A"]  # Valid forms to fetch filings for


class CikFileHandler(FileSystemEventHandler):
    """Handler to watch for changes in the CIK file."""

    def __init__(self, callback, cik_file_path):
        self.callback = callback
        self.cik_file_path = cik_file_path
        self.last_hash = self.get_file_hash()
        self.last_mtime = 0

    def check_for_changes(self):
        current_mtime = os.path.getmtime(self.cik_file_path)
        if current_mtime > self.last_mtime:
            self.last_mtime = current_mtime
            current_hash = self.get_file_hash()
            if current_hash != self.last_hash:
                self.last_hash = current_hash
                self.callback()
                logger.info("CIK file modified - reloading CIKs")

    def get_file_hash(self):
        with open(self.cik_file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()


def ping_healthchecks():
    """Ping the healthcheck URL every minute."""
    while True:
        try:
            requests.get(HEALTHCHECK_URL, timeout=10)
        except Exception as e:
            logger.error(f"Healthcheck ping failed: {str(e)}")
        time.sleep(60)  # Ping every minute


@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=10, period=1)  # Limit to 10 calls per second
def fetch_new_filing(conn, cik_number, most_recent_date: dt.date):
    c = Company(cik_number)
    today_date = dt.date.today().strftime("%Y-%m-%d")

    for attempt in range(MAX_RETRIES):
        try:
            # Fetch filings from lastest filing date to today
            filings = c.get_filings(
                sort_by='filing_date',
                form=VALID_FORMS,
                filing_date=(f"{most_recent_date.strftime('%Y-%m-%d')}:{today_date}"),
            )

            if not filings:
                logger.info(
                    f"No new valid filings found for CIK {cik_number} since {most_recent_date}"
                )
                return

            for filing in filings:
                # technically there is no need for this check because
                # we are fetching only valid forms
                if filing.form in VALID_FORMS:
                    filing_date = filing.filing_date

                    assert isinstance(filing_date, dt.date)

                    if filing_date > most_recent_date:
                        accession_number = filing.accession_number

                        # Check if the filing already exists in the database
                        if filing_exists(conn, accession_number):
                            continue

                        # this should exist in database because
                        # we are fetching CIKs that we know
                        manager = get_manager_by_cik(conn, cik_number)
                        if not manager:
                            logger.error("Manager not found for CIK: %s", cik_number)
                            continue

                        manager_id = manager.manager_id
                        new_filing_id = insert_filing(conn, manager_id, filing)
                        holding_inserted_count = 0

                        # insert holdings
                        # Looping through attachments to find XML files
                        for attachment in filing.attachments:
                            if (
                                attachment.document.endswith('.xml')
                                and 'primary_doc' not in attachment.document
                            ):
                                assert attachment.content

                                soup = BeautifulSoup(attachment.content, 'xml')
                                holdings = []

                                for holding in gather_holdings_from_soup(conn, soup):

                                    assert (
                                        holding._issuer_cusip
                                    ), "CUSIP is required for each holding"
                                    assert (
                                        holding._issuer_name
                                    ), "Issuer name is required for each holding"

                                    issuer = get_or_create_issuer(
                                        conn,
                                        holding._issuer_cusip,
                                        holding._issuer_name,
                                    )

                                    holding.issuer_id = issuer.issuer_id
                                    holding.filing_id = new_filing_id
                                    holdings.append(holding)

                                    # insert_holding(conn, holding)
                                    holding_inserted_count += 1

                                if holdings:
                                    insert_holdings_batch(conn, holdings)

                        logger.info(
                            f"Inserted filling_id {new_filing_id} for {cik_number}. "
                            f"Total holdings inserted: {holding_inserted_count}"
                        )
                    else:
                        logger.info(
                            f"Filing date {filing_date} is not newer than most recent date {most_recent_date} for CIK {cik_number}"
                        )
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


def process_all_ciks(conn, ciks):
    """Process all CIKs in the list"""
    processed_count = 0
    for cik in ciks:
        try:
            logger.info(f"Processing CIK: {cik}")
            latest_filing = get_latest_filing_using_cik_number(conn, cik)

            if latest_filing:
                logger.info(
                    f"Most recent filing date for {cik}: {latest_filing.filing_date}"
                )
                fetch_new_filing(conn, cik, latest_filing.filing_date)
            else:
                logger.info(f"No filings found for {cik}. Fetching all available.")
                fetch_new_filing(conn, cik, dt.date(1970, 1, 1))

            processed_count += 1

        except Exception as e:
            logger.error(f"Failed to process CIK {cik}: {str(e)}")
            continue

    return processed_count


if __name__ == "__main__":
    # Start healthcheck thread
    health_thread = Thread(target=ping_healthchecks, daemon=True)
    health_thread.start()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cik', default=os.getenv('CIK_FILE', "ciks.txt"), help="Path to CIK file"
    )
    args = parser.parse_args()

    logger.info(f"Reading: {args.cik}")
    with open(args.cik, "r") as f:
        ciks = f.read().splitlines()

    # connect to the database
    conn = connect_db(
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=os.getenv("DB_PORT", "5432"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_name=os.getenv("DB_NAME", "sec"),
    )

    state = {'current_ciks': [], 'conn': conn}

    # Function to reload CIKs from the file
    def reload_ciks():
        with open(args.cik, "r") as f:
            state['current_ciks'] = [line.strip() for line in f if line.strip()]
        logger.info(f"Reloaded {len(state['current_ciks'])} CIKs")

    # Initial load
    reload_ciks()

    # Set up file watcher
    event_handler = CikFileHandler(reload_ciks, args.cik)
    observer = Observer()
    watch_path = str(Path(args.cik).parent)
    logger.info(f"Watching for changes in {watch_path}")
    observer.schedule(event_handler, path=watch_path)
    observer.start()

    try:
        while True:
            # Check for changes in the CIK file
            event_handler.check_for_changes()

            if state['current_ciks']:
                logger.info(f"Processing {len(state['current_ciks'])} CIKs")
                process_all_ciks(conn, state['current_ciks'])
            else:
                logger.debug("No CIKs to process, waiting for changes...")

            logger.info("Loop completed, sleeping for 3 seconds...")
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received, stopping observer...")
    finally:
        observer.stop()
        observer.join()

        state['conn'].close()
        logger.info("Database connection closed")
