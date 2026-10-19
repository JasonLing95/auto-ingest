import os
import sys
import psycopg2
from typing import List, Optional, Tuple
from contextlib import nullcontext
from edgar import Filing as EdgarFiling
import datetime as dt
from bs4 import BeautifulSoup
from psycopg2 import extras
import pandas as pd
from io import StringIO

from get_logger import get_logger

logger = get_logger(__name__)


def connect_db() -> psycopg2.extensions.connection:
    """Connect to PostgreSQL database using environment variables."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "sec"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
        )
        # CRITICAL: Prevents 'idle in transaction' leaks on read queries
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '10s';")
            cur.execute("SET statement_timeout = '120s';")
            try:
                cur.execute(
                    """
                    ALTER TABLE filings 
                    ADD COLUMN IF NOT EXISTS reported_value_total BIGINT,
                    ADD COLUMN IF NOT EXISTS reported_entry_total INT;
                    ALTER TABLE companies
                    ADD COLUMN IF NOT EXISTS aum BIGINT;
                    """
                )
            except Exception as schema_err:
                logger.warning(f"Could not verify/add schema enhancement columns: {schema_err}")
        logger.info("Connected to PostgreSQL database successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logger.critical(f"Database connection error: {e}. Shutting down.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unrecoverable error occurred: {e}. Shutting down.")
        sys.exit(1)


HOLDINGS_COLUMNS = [
    "cik",
    "accession_number",
    "shares_amount",
    "value",
    "title_of_class",
    "share_type",
    "investment_discretion",
    "put_call",
    "sole",
    "shared",
    "none",
    "name_of_issuer",
    "cusip",
]


def normalize_cusip(cusip: Optional[str]) -> str:
    """Normalize CUSIP to uppercase and stripped of whitespace."""
    if not cusip:
        return ""
    return str(cusip).strip().upper()


def normalize_form_type(raw_form: Optional[str]) -> str:
    """Normalize SEC form type to fit VARCHAR(10) and match canonical forms ('13F-HR', '13F-HR/A')."""
    if not raw_form:
        return "13F-HR"
    raw_form_upper = str(raw_form).strip().upper()
    if "13F-HR/A" in raw_form_upper:
        return "13F-HR/A"
    elif "13F-HR" in raw_form_upper:
        return "13F-HR"
    else:
        return raw_form_upper.split(" - ")[0].split()[0][:10].strip()


def get_latest_filing_using_cik_number(conn, cik: str) -> dt.date:
    """Fetch the most recent filing for a given CIK"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(t1.filing_date)
            FROM filings AS t1
            JOIN companies AS t2
            ON t1.company_id = t2.company_id
            WHERE t2.cik_number = %s
            """,
            (cik,),
        )
        latest_date = cur.fetchone()[0]

    return latest_date


def gather_data_with_bs4(data, cik, accession_number) -> list[list]:
    soup = BeautifulSoup(data, "xml")

    result = []
    for row in soup.find_all("infoTable"):
        name_of_issuer = row.find("nameOfIssuer").text.strip() if row.find("nameOfIssuer") else ""
        raw_cusip = row.find("cusip")
        issuer_cusip = normalize_cusip(raw_cusip.text if raw_cusip else None)

        share_amount = int(float(row.find("sshPrnamt").text.strip()))  # type: ignore

        share_type = row.find("sshPrnamtType").text.strip().upper()  # type: ignore
        if share_type not in ["SH", "PRN"]:
            raise ValueError(f"Invalid share type: {share_type}")  # type: ignore

        value = int(float(row.find("value").text.strip()))  # type: ignore

        investment_discretion = row.find("investmentDiscretion").text.strip().upper()  # type: ignore
        if investment_discretion not in ["SOLE", "DFND", "OTR"]:
            raise ValueError(f"Invalid investment discretion: {investment_discretion}")

        put_call: str = row.find("putCall").text.strip() if row.find("putCall") else "NONE"  # type: ignore
        if put_call.strip().upper() not in ["NONE", "PUT", "CALL"]:
            raise ValueError(f"Invalid put/call type: {put_call}")

        title_of_class = row.find("titleOfClass").text.strip()  # type: ignore

        voting_authority = row.find("votingAuthority")  # type: ignore
        voting_authority_sole = int(float(voting_authority.find("Sole").text.strip()))  # type: ignore
        voting_authority_shared = int(
            float(voting_authority.find("Shared").text.strip())  # type: ignore
        )
        voting_authority_none = int(float(voting_authority.find("None").text.strip()))  # type: ignore

        result.append(
            [
                cik,
                accession_number,  # 1
                share_amount,  # share amount
                value,  # share value 3
                title_of_class,  # com etc.
                share_type,  # share type 5
                investment_discretion,  # investment discretion
                put_call,  # mostly none 7
                voting_authority_sole,  # voting authority (sole)
                voting_authority_shared,  # voting authority (shared) 9
                voting_authority_none,  # voting authority (none)
                name_of_issuer,  # issuer name 11
                issuer_cusip,  # cusip
            ]
        )

    return result


def gather_holdings_using_lxml(tables, ns, cik, accession_number) -> List[list]:
    """Parse XML soup and return list of Holding objects"""
    holdings = []

    for table in tables:
        # name of issuer
        name_of_issuer = table.xpath("string(ns:nameOfIssuer)", namespaces=ns).strip()
        # CUSIP (normalized to uppercase)
        cusip = normalize_cusip(table.xpath("string(ns:cusip)", namespaces=ns))
        # common stock (COM) or etc.
        title_of_class = table.xpath("string(ns:titleOfClass)", namespaces=ns).strip()
        # stock value
        value = int(float(table.xpath("string(ns:value)", namespaces=ns).strip()))
        # stock amount
        shares_amount = int(
            float(
                table.xpath(
                    "string(ns:shrsOrPrnAmt/ns:sshPrnamt)", namespaces=ns
                ).strip()
                or 0
            )
        )
        # share type SH / PRN
        share_type = (
            table.xpath("string(ns:shrsOrPrnAmt/ns:sshPrnamtType)", namespaces=ns)
            .strip()
            .upper()
        )
        if share_type not in ["SH", "PRN"]:
            raise ValueError(f"Invalid share type: {share_type}")

        # DFND etc.
        investment_discretion = (
            table.xpath("string(ns:investmentDiscretion)", namespaces=ns)
            .strip()
            .upper()
        )
        if investment_discretion not in ["SOLE", "DFND", "OTR"]:
            raise ValueError(f"Invalid investment discretion: {investment_discretion}")

        # often blank
        put_call = (
            table.xpath("string(ns:putCall)", namespaces=ns).strip().upper() or "NONE"
        )
        if put_call not in ["NONE", "PUT", "CALL"]:
            raise ValueError(f"Invalid put/call type: {put_call}")

        # voting authority - sole
        sole = (
            int(
                float(
                    table.xpath(
                        "string(ns:votingAuthority/ns:Sole)", namespaces=ns
                    ).strip()
                )
            )
            or 0
        )
        # voting authority - shared
        shared = (
            int(
                float(
                    table.xpath(
                        "string(ns:votingAuthority/ns:Shared)", namespaces=ns
                    ).strip()
                )
            )
            or 0
        )
        # voting authority - none
        none = (
            int(
                float(
                    table.xpath(
                        "string(ns:votingAuthority/ns:None)", namespaces=ns
                    ).strip()
                )
            )
            or 0
        )

        holdings.append(
            [
                cik,
                accession_number,
                shares_amount,
                value,
                title_of_class,
                share_type,
                investment_discretion,
                put_call,
                sole,
                shared,
                none,
                name_of_issuer,
                cusip,
            ]
        )

    return holdings


def extract_reported_totals(filing: EdgarFiling) -> Tuple[Optional[int], Optional[int]]:
    """Extract official tableValueTotal and tableEntryTotal from Form 13F primary document."""
    reported_value_total = None
    reported_entry_total = None

    # Method 1: Try edgartools ThirteenF parser
    try:
        from edgar.thirteenf import ThirteenF

        thirteen_f = ThirteenF(filing)
        if thirteen_f.total_value is not None:
            reported_value_total = int(float(thirteen_f.total_value))
        if thirteen_f.total_holdings is not None:
            reported_entry_total = int(thirteen_f.total_holdings)
        if reported_value_total is not None and reported_entry_total is not None:
            return reported_value_total, reported_entry_total
    except Exception:
        pass

    # Method 2: Scan attachments for primary_doc.xml
    try:
        for attachment in list(getattr(filing, "attachments", [])):
            doc_name = getattr(attachment, "document", "").lower()
            if "primary_doc" in doc_name and doc_name.endswith(".xml"):
                content = attachment.content
                if content:
                    soup = BeautifulSoup(content, "xml")
                    val_el = soup.find("tableValueTotal")
                    if val_el and val_el.text and val_el.text.strip():
                        reported_value_total = int(float(val_el.text.strip()))
                    entry_el = soup.find("tableEntryTotal")
                    if entry_el and entry_el.text and entry_el.text.strip():
                        reported_entry_total = int(float(entry_el.text.strip()))
                    break
    except Exception:
        pass

    return reported_value_total, reported_entry_total


def extract_period_of_report(filing: Optional[EdgarFiling]) -> Optional[str]:
    """Extracts period_of_report with fallback to primary_doc.xml if SGML header is missing it."""
    if not filing:
        return None

    import re

    def _normalize_date_str(val: str) -> Optional[str]:
        val = val.strip()
        if not val:
            return None
        for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d"):
            try:
                return dt.datetime.strptime(val, fmt).date().isoformat()
            except ValueError:
                continue
        return val

    try:
        por = getattr(filing, "period_of_report", None)
        if por:
            if hasattr(por, "isoformat"):
                return por.isoformat()
            normalized = _normalize_date_str(str(por))
            if normalized:
                return normalized
    except Exception:
        pass

    if hasattr(filing, "attachments"):
        try:
            for attachment in list(getattr(filing, "attachments", [])):
                doc_name = getattr(attachment, "document", "").lower()
                if "primary" in doc_name or doc_name.endswith(".xml"):
                    content = attachment.content
                    if content:
                        if isinstance(content, bytes):
                            content = content.decode("utf-8", errors="ignore")
                        for tag in ("periodOfReport", "reportCalendarOrQuarter"):
                            match = re.search(
                                rf"<{tag}>(.*?)</{tag}>", content, re.IGNORECASE
                            )
                            if match and match.group(1).strip():
                                normalized = _normalize_date_str(match.group(1))
                                if normalized:
                                    return normalized
        except Exception as e:
            logger.warning(f"Could not extract fallback period_of_report from XML: {e}")

    return None


def insert_filing(
    conn: psycopg2.extensions.connection, cik: str, filing: EdgarFiling
) -> Optional[int]:
    accession_number = filing.accession_number

    try:
        filing_date = filing.filing_date.isoformat()  # type: ignore
    except ArithmeticError:
        filing_date = None

    period_of_report = extract_period_of_report(filing)

    filing_form_type = normalize_form_type(getattr(filing, "form", None))

    try:
        file_number = filing.file_number  # type: ignore
    except AttributeError:
        file_number = None

    try:
        file_dir = filing.filing_directory.name
    except ArithmeticError:
        file_dir = None

    reported_value_total, reported_entry_total = extract_reported_totals(filing)

    try:
        # Get company_id from database
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_id FROM companies WHERE cik_number = %s", (cik,)
            )
            company_id_result = cur.fetchone()

            if not company_id_result:
                logger.info(f"Error: Company with CIK {cik} not found.")
                # this should raise an error because technically the CIK should exist

                raise ValueError(f"Company with CIK {cik} not found.")

            company_id = company_id_result[0]

            # Step 2: Prepare the INSERT query with ON CONFLICT DO NOTHING
            query = """
                INSERT INTO filings (
                    company_id, form_type, accession_number,
                    filing_date, file_number, filing_directory,
                    period_of_report, reported_value_total, reported_entry_total,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (accession_number) DO NOTHING
                RETURNING filing_id;
            """

            # Step 3: Prepare the parameters as a tuple.
            params = (
                company_id,
                filing_form_type,
                accession_number,
                filing_date,
                file_number,
                file_dir,
                period_of_report,
                reported_value_total,
                reported_entry_total,
                dt.datetime.now(),
                dt.datetime.now(),
            )

            # Step 4: Execute query with fallback if columns are not present
            try:
                cur.execute(query, params)
            except psycopg2.Error as col_err:
                if "reported_value_total" in str(col_err) or "reported_entry_total" in str(col_err):
                    fallback_query = """
                        INSERT INTO filings (
                            company_id, form_type, accession_number,
                            filing_date, file_number, filing_directory,
                            period_of_report, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (accession_number) DO NOTHING
                        RETURNING filing_id;
                    """
                    fallback_params = (
                        company_id,
                        filing_form_type,
                        accession_number,
                        filing_date,
                        file_number,
                        file_dir,
                        period_of_report,
                        dt.datetime.now(),
                        dt.datetime.now(),
                    )
                    cur.execute(fallback_query, fallback_params)
                else:
                    raise col_err

            res = cur.fetchone()
            new_filing_id = res[0] if res else None

            # Step 5: Commit immediately to release all locks
            conn.commit()

            if new_filing_id:
                logger.info(
                    f"✅ Successfully inserted filing {accession_number} with ID {new_filing_id}"
                )
            else:
                logger.info(
                    f"⚠️ Filing {accession_number} already exists in database (conflict avoided). Skipping insertion."
                )

            return new_filing_id

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL Error: {e}")
        conn.rollback()

        raise Exception(f"Error inserting filing {accession_number}: {str(e)}")


def update_company_aum_post_ingest(
    conn_or_cursor,
    company_id: Optional[int] = None,
    filing_id: Optional[int] = None,
) -> bool:
    """
    Updates the filer's latest AUM immediately after a new filing lands.
    Execution time: ~5ms (targets only the newly ingested filing).

    Guardrails:
    1. Backfill protection: Only updates if the filing is the latest for the company.
    2. Amendment protection (13F-HR/A): Skips partial amendments (<50% of original holdings).
    3. Normalization: Uses holdings_normalised to adjust pre-2023 values to whole dollars.

    Returns:
        bool: True if companies.aum was updated, False if skipped or failed.
    """
    if not filing_id:
        return False

    # Support either a psycopg2 connection or cursor
    if hasattr(conn_or_cursor, "cursor"):
        cursor_context = conn_or_cursor.cursor()
    else:
        cursor_context = nullcontext(conn_or_cursor)

    with cursor_context as cur:
        # Step 1: Resolve filing metadata
        cur.execute(
            """
            SELECT company_id, period_of_report, filing_date, form_type
            FROM filings
            WHERE filing_id = %(filing_id)s;
            """,
            {"filing_id": filing_id},
        )
        filing_meta = cur.fetchone()
        if not filing_meta:
            logger.warning(f"Filing {filing_id} not found when updating AUM.")
            return False

        meta_company_id, period_of_report, filing_date, form_type = filing_meta
        target_company_id = company_id or meta_company_id

        # Step 2: Backfill Protection Guardrail
        # Check if there is any newer filing for this company
        cur.execute(
            """
            SELECT 1 FROM filings
            WHERE company_id = %(company_id)s
              AND filing_id != %(filing_id)s
              AND (
                  period_of_report > %(period_of_report)s
                  OR (period_of_report = %(period_of_report)s AND filing_date > %(filing_date)s)
              )
            LIMIT 1;
            """,
            {
                "company_id": target_company_id,
                "filing_id": filing_id,
                "period_of_report": period_of_report,
                "filing_date": filing_date,
            },
        )
        if cur.fetchone():
            logger.info(
                f"Filing {filing_id} (period: {period_of_report}, date: {filing_date}) "
                f"is not the latest filing for company {target_company_id}. "
                f"Skipping companies.aum update (backfill protection)."
            )
            return False

        # Step 3: Amendment Handling Guardrail (13F-HR/A)
        norm_form = normalize_form_type(form_type)
        if "13F-HR/A" in norm_form:
            # Find original/previous filing for the same period
            cur.execute(
                """
                SELECT f.filing_id,
                       (SELECT COUNT(*) FROM holdings WHERE filing_id = f.filing_id) AS prev_count
                FROM filings f
                WHERE f.company_id = %(company_id)s
                  AND f.period_of_report = %(period_of_report)s
                  AND f.filing_id != %(filing_id)s
                ORDER BY f.filing_date DESC, f.filing_id DESC
                LIMIT 1;
                """,
                {
                    "company_id": target_company_id,
                    "period_of_report": period_of_report,
                    "filing_id": filing_id,
                },
            )
            prev_filing = cur.fetchone()
            if prev_filing and prev_filing[1] is not None:
                prev_count = int(prev_filing[1])
                # Count current holdings
                cur.execute(
                    "SELECT COUNT(*) FROM holdings WHERE filing_id = %(filing_id)s;",
                    {"filing_id": filing_id},
                )
                curr_row = cur.fetchone()
                curr_count = int(curr_row[0]) if curr_row and curr_row[0] is not None else 0

                if prev_count > 0 and curr_count < 0.5 * prev_count:
                    logger.warning(
                        f"Filing {filing_id} is a partial amendment ({curr_count} holdings vs original {prev_count} holdings). "
                        f"Skipping companies.aum overwrite to protect total portfolio value."
                    )
                    return False

        # Step 4: Targeted AUM update using holdings_normalised
        update_query = """
            UPDATE companies
            SET aum = (
                SELECT COALESCE(SUM(value), 0)
                FROM holdings_normalised
                WHERE filing_id = %(filing_id)s
            ),
            updated_at = NOW()
            WHERE company_id = %(company_id)s;
        """
        try:
            cur.execute(update_query, {"filing_id": filing_id, "company_id": target_company_id})
        except psycopg2.Error as e:
            # Fallback if holdings_normalised view is not present
            logger.warning(f"Failed to query holdings_normalised view: {e}. Falling back to manual calculation.")
            fallback_aum_query = """
                UPDATE companies
                SET aum = (
                    SELECT COALESCE(SUM(
                        CASE 
                            WHEN f.period_of_report < '2023-01-01' THEN h.value * 1000
                            ELSE h.value
                        END
                    ), 0)
                    FROM holdings h
                    JOIN filings f ON h.filing_id = f.filing_id
                    WHERE h.filing_id = %(filing_id)s
                ),
                updated_at = NOW()
                WHERE company_id = %(company_id)s;
            """
            cur.execute(fallback_aum_query, {"filing_id": filing_id, "company_id": target_company_id})

        logger.info(f"Successfully updated companies.aum for company_id={target_company_id} from filing_id={filing_id}")
        return True


def insert_lookups(conn, df):
    """
    Inserts unique values from holdings data into lookup tables.
    Returns a dictionary of mappings for each lookup table.
    """
    lookup_tables = {
        "title_of_class_table": "title_of_class",
        "share_type_table": "share_type",
        "put_or_call_table": "put_call",
        "investment_discretion_table": "investment_discretion",
    }

    mappings = {}
    with conn.cursor() as cur:
        for table, col in lookup_tables.items():
            logger.info(f"Processing unique values for {table} from column '{col}'...")

            # Get unique values from the DataFrame column, drop NaNs, and prepare for insertion
            unique_values = df[col].dropna().unique().tolist()
            data_to_insert = [(val,) for val in unique_values]

            insert_query = f"INSERT INTO {table} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;"

            # Execute batch insert in autocommit mode without transaction leaks
            extras.execute_batch(cur, insert_query, data_to_insert)

            # Get the mapping from the database
            cur.execute(f"SELECT name, id FROM {table}")
            mappings[col] = {row[0]: row[1] for row in cur.fetchall()}

    return mappings


def insert_issuers(conn, df):
    """
    Inserts unique issuers (name_of_issuer and cusip) into the issuers table.
    Returns a dictionary mapping normalized uppercase cusip to issuer_id.
    Uses a lock-free check-before-insert pattern with ON CONFLICT DO NOTHING.
    """
    logger.info("Processing unique issuers...")
    unique_issuers_df = df[["name_of_issuer", "cusip"]].dropna().copy()
    unique_issuers_df["cusip"] = unique_issuers_df["cusip"].apply(normalize_cusip)
    unique_issuers_df["name_of_issuer"] = (
        unique_issuers_df["name_of_issuer"].astype(str).str.strip()
    )
    unique_issuers_df = unique_issuers_df[
        unique_issuers_df["cusip"] != ""
    ].drop_duplicates(subset=["cusip"])

    if unique_issuers_df.empty:
        return {}

    cusip_list = unique_issuers_df["cusip"].tolist()

    with conn.cursor() as cur:
        # Step 1: Lock-free read to find which CUSIPs already exist
        cur.execute(
            """
            SELECT cusip, issuer_id
            FROM issuers
            WHERE cusip = ANY(%s)
            ORDER BY (cusip = UPPER(TRIM(cusip))) ASC, issuer_id DESC;
            """,
            (cusip_list,),
        )
        mapping = {}
        for cusip_val, issuer_id in cur.fetchall():
            if cusip_val:
                mapping[normalize_cusip(cusip_val)] = issuer_id

        # Step 2: Filter to only brand-new CUSIPs that do not exist yet
        new_issuers_df = unique_issuers_df[~unique_issuers_df["cusip"].isin(mapping)]

        # Step 3: Insert only brand-new CUSIPs with ON CONFLICT DO NOTHING (lock-free for existing rows)
        if not new_issuers_df.empty:
            data_to_insert = new_issuers_df[["cusip", "name_of_issuer"]].values.tolist()
            insert_query = """
                INSERT INTO issuers (cusip, issuer_name)
                VALUES (%s, %s)
                ON CONFLICT (cusip) DO NOTHING;
            """
            extras.execute_batch(cur, insert_query, data_to_insert)

            # Re-fetch mapping for newly inserted CUSIPs
            missing_cusips = [c for c in cusip_list if c not in mapping]
            if missing_cusips:
                cur.execute(
                    """
                    SELECT cusip, issuer_id
                    FROM issuers
                    WHERE cusip = ANY(%s)
                    ORDER BY (cusip = UPPER(TRIM(cusip))) ASC, issuer_id DESC;
                    """,
                    (missing_cusips,),
                )
                for cusip_val, issuer_id in cur.fetchall():
                    if cusip_val:
                        mapping[normalize_cusip(cusip_val)] = issuer_id

        return mapping


def insert_holdings_batch(
    conn: psycopg2.extensions.connection,
    holdings: list[list],
) -> None:
    logger.info(f"Preparing to insert {len(holdings)} new holding records...")

    holdings = pd.DataFrame(holdings, columns=HOLDINGS_COLUMNS)
    holdings["cusip"] = holdings["cusip"].apply(normalize_cusip)

    with conn.cursor() as cur:
        cur.execute("SELECT accession_number, filing_id FROM filings")
        filing_map = {row[0]: row[1] for row in cur.fetchall()}

    lookup_mappings = insert_lookups(conn, holdings)
    issuer_mapping = insert_issuers(conn, holdings)

    # Step 3: Map the string columns to their corresponding integer IDs
    logger.info("Mapping holdings data to IDs...")
    holdings["filing_id"] = holdings["accession_number"].map(filing_map)
    holdings["issuer_id"] = holdings["cusip"].map(issuer_mapping)
    holdings["title_of_class_id"] = holdings["title_of_class"].map(
        lookup_mappings["title_of_class"]
    )
    holdings["share_type_id"] = holdings["share_type"].map(
        lookup_mappings["share_type"]
    )
    holdings["put_call_id"] = holdings["put_call"].map(lookup_mappings["put_call"])
    holdings["investment_discretion_id"] = holdings["investment_discretion"].map(
        lookup_mappings["investment_discretion"]
    )

    # # Step 4: Drop any rows that failed to map
    holdings.dropna(
        subset=[
            "filing_id",
            "issuer_id",
            "title_of_class_id",
            "share_type_id",
            "put_call_id",
            "investment_discretion_id",
        ],
        inplace=True,
    )

    int64_columns = [
        "filing_id",
        "issuer_id",
        "title_of_class_id",
        "share_type_id",
        "put_call_id",
        "investment_discretion_id",
        "shares_amount",
        "value",
        "sole",
        "shared",
        "none",
    ]

    holdings[int64_columns] = holdings[int64_columns].astype("Int64")

    final_df = holdings[
        [
            "filing_id",
            "issuer_id",
            "title_of_class_id",
            "shares_amount",
            "share_type_id",
            "value",
            "put_call_id",
            "investment_discretion_id",
            "sole",
            "shared",
            "none",
        ]
    ].copy()

    final_df.rename(
        columns={
            "title_of_class_id": "title_of_class",
            "shares_amount": "shares_or_principal_amount",
            "share_type_id": "shares_or_principal_type",
            "put_call_id": "put_or_call",
            "investment_discretion_id": "investment_discretion",
            "sole": "voting_authority_sole",
            "shared": "voting_authority_shared",
            "none": "voting_authority_none",
        },
        inplace=True,
    )

    logger.info("Reading holdings data into memory buffer for COPY command...")
    csv_buffer = StringIO()
    final_df.to_csv(csv_buffer, index=False, header=False, sep="\t")
    csv_buffer.seek(0)

    logger.info("Starting bulk insert of holdings data using COPY command...")
    with conn.cursor() as cur:
        logger.info(
            f"Starting bulk insert of {len(final_df)} holdings records using COPY command..."
        )
        cur.copy_from(
            csv_buffer,
            "holdings",
            sep="\t",
            columns=(
                "filing_id",
                "issuer_id",
                "title_of_class",
                "shares_or_principal_amount",
                "shares_or_principal_type",
                "value",
                "put_or_call",
                "investment_discretion",
                "voting_authority_sole",
                "voting_authority_shared",
                "voting_authority_none",
            ),
        )
        conn.commit()
    logger.info("Holdings data inserted successfully")


def filing_exists(conn, accession_number) -> bool:
    """
    Check if a filing with the given SEC accession number already exists in the database
    """
    with conn.cursor() as cur:
        # The query uses %s as a placeholder for the accession number
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM filings WHERE accession_number = %s)",
            (accession_number,),
        )
        # Fetch the result, which will be a single row with one column (True or False)
        result = cur.fetchone()

    # Return the boolean value from the query result.
    return result[0] if result else False
