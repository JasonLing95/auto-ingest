from edgar import set_identity, Company
import datetime as dt
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from collections import deque
import os
import argparse
import time
import requests
from threading import Thread
from httpx import HTTPStatusError
from lxml import etree  # type: ignore
import psycopg2
import sys

from utils import (
    get_latest_filing_using_cik_number,
    insert_filing,
    insert_holdings_batch,
    gather_holdings_using_lxml,
    filing_exists,
    gather_data_with_bs4,
)
from get_logger import get_logger

# Configure logging
logger = get_logger(__name__)

# User Agent
set_identity(os.getenv("SEC_IDENTITY", "example121.company@access.com"))

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "https://example.com/healthcheck")
MAX_RETRIES = 3
VALID_FORMS = ["13F-HR", "13F-HR/A", "13F-HR/A/A"]  # Valid forms to fetch filings for
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "edgar_db.db")


def connect_db() -> psycopg2.extensions.connection:
    """Connect to PostgreSQL database using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "sec"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", 5432),
        )
        print("Connected to PostgreSQL database successfully.")
        return conn
    # Catch the critical database connection error
    except psycopg2.OperationalError as e:
        logger.critical(f"Database connection error: {e}. Shutting down.")
        sys.exit(1)  # Exit with an error code

    # Catch any other unexpected, fatal errors
    except Exception as e:
        logger.critical(f"An unrecoverable error occurred: {e}. Shutting down.")
        sys.exit(1)  # Exit with an error code


def find_namespaces(tree):
    """Find all unique namespaces in the XML tree."""
    all_elements = tree.xpath("//*")
    unique_prefixes = set()
    for elem in all_elements:
        if elem.prefix:
            unique_prefixes.add(elem.prefix)

    return list(unique_prefixes)


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
            # Fetch filings from (equal or greater than) lastest filing date to today
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

            for filing in filings:
                filing_date = filing.filing_date
                accession_number = filing.accession_number

                # check if filing in database
                if filing_exists(conn, accession_number):
                    logger.info(
                        f"Filing {accession_number} already exists in database, skipping."
                    )
                    continue

                new_filing_id = insert_filing(conn, cik_number, filing)

                # insert holdings
                # Looping through attachments to find XML files
                for attachment in filing.attachments:
                    if (
                        attachment.document.endswith(".xml")
                        and "primary_doc" not in attachment.document
                    ):
                        logger.info(f"Processing XML attachment: {attachment.url}")
                        assert attachment.content

                        if isinstance(attachment.content, str):
                            # Convert string to bytes
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
                        except:
                            # try parsing with BeautifulSoup
                            raw_holdings = gather_data_with_bs4(
                                attachment.content,
                                cik_number,
                                accession_number,
                            )

                        if raw_holdings:
                            insert_holdings_batch(conn, raw_holdings)
            else:
                logger.info(
                    f"Filing date {filing_date} is not newer than most recent date {most_recent_date} for CIK {cik_number}"
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
    logger.info(f"Processing {len(cik_queue)} CIKs")

    while True:
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
                fetch_new_filing(conn, cik, latest_filing_date_in_db)
            else:
                logger.info(f"No filings found for {cik}. Fetching all available.")
                start_date = dt.date.today() - dt.timedelta(days=730)
                fetch_new_filing(conn, cik, start_date)

            try:
                requests.get(HEALTHCHECK_URL, timeout=10)
            except Exception as e:
                logger.error(f"Healthcheck ping failed: {str(e)}")

        except Exception as e:
            # If a specific CIK fails, log it, but KEEP RUNNING!
            logger.error(f"Failed processing CIK {cik}: {e}")

        cik_queue.append(cik)
        time.sleep(1)


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
            ciks = f.read().splitlines()
        logger.info(f"Loaded {len(ciks)} CIKs")
    except FileNotFoundError:
        logger.critical(f"CIK file not found at {args.cik}. Exiting.")
        exit(1)

    # Loop through CIKs and process filings
    cik_queue = deque(ciks)
    main(conn, cik_queue)
