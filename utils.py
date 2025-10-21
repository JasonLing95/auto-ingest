import psycopg2
from typing import List
from edgar import Filing as EdgarFiling
import datetime as dt
from bs4 import BeautifulSoup
from psycopg2 import extras
import pandas as pd
from io import StringIO

from get_logger import get_logger

logger = get_logger(__name__)

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
        name_of_issuer = row.find("nameOfIssuer").text.strip()  # type: ignore
        issuer_cusip = row.find("cusip").text.strip()  # type: ignore

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
        # CUSIP
        cusip = table.xpath("string(ns:cusip)", namespaces=ns).strip()
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


def insert_filing(
    conn: psycopg2.extensions.connection, cik: str, filing: EdgarFiling
) -> int:
    accession_number = filing.accession_number

    try:
        filing_date = filing.filing_date.isoformat()  # type: ignore
    except ArithmeticError:
        filing_date = None

    try:
        period_of_report = filing.period_of_report  # in str format
    except ArithmeticError:
        period_of_report = None

    filing_form_type = filing.form

    try:
        file_number = filing.file_number  # type: ignore
    except AttributeError:
        file_number = None

    try:
        file_dir = filing.filing_directory.name
    except ArithmeticError:
        file_dir = None

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

            # Step 2: Prepare the INSERT query with psycopg2-style placeholders.
            query = """
                INSERT INTO filings (
                    company_id, form_type, accession_number,
                    filing_date, file_number, filing_directory,
                    period_of_report, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING filing_id
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
                dt.datetime.now(),
                dt.datetime.now(),
            )

            # Step 4: Execute the query within a cursor block.
            cur.execute(query, params)
            new_filing_id = cur.fetchone()[0]  # type: ignore

            # Step 5: Commit the transaction.
            conn.commit()

            logger.info(
                f"✅ Successfully inserted filing {accession_number} with ID {new_filing_id}"
            )

            return new_filing_id

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL Error: {e}")
        conn.rollback()

        raise Exception(f"Error inserting filing {accession_number}: {str(e)}")


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

            # Execute batch insert for each lookup table
            cur.execute("BEGIN;")
            extras.execute_batch(cur, insert_query, data_to_insert)
            conn.commit()

            # Get the mapping from the database
            cur.execute(f"SELECT name, id FROM {table}")
            mappings[col] = {row[0]: row[1] for row in cur.fetchall()}

    return mappings


def insert_issuers(conn, df):
    """
    Inserts unique issuers (name_of_issuer and cusip) into the issuers table.
    Returns a dictionary mapping cusip to issuer_id.
    """
    logger.info("Processing unique issuers...")
    unique_issuers_df = df[["name_of_issuer", "cusip"]].dropna().drop_duplicates()

    data_to_insert = unique_issuers_df[["cusip", "name_of_issuer"]].values.tolist()

    insert_query = "INSERT INTO issuers (cusip, issuer_name) VALUES (%s, %s) ON CONFLICT (cusip) DO UPDATE SET issuer_name = EXCLUDED.issuer_name;"

    with conn.cursor() as cur:
        cur.execute("BEGIN;")
        extras.execute_batch(cur, insert_query, data_to_insert)
        conn.commit()

        # Get the mapping from the database
        cur.execute("SELECT cusip, issuer_id FROM issuers")
        return {row[0]: row[1] for row in cur.fetchall()}


def insert_holdings_batch(
    conn: psycopg2.extensions.connection,
    holdings: list[list],
) -> None:
    logger.info(f"Preparing to insert {len(holdings)} new holding records...")

    holdings = pd.DataFrame(holdings, columns=HOLDINGS_COLUMNS)

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
