import psycopg2
from typing import List
from edgar import Filing as EdgarFiling
import datetime as dt

from sec_models import (
    Filing,
    Holding,
    Manager,
    Issuer,
    SecurityClass,
    HoldingType,
    OptionType,
    DiscretionType,
)
from get_logger import get_logger

logger = get_logger(__name__)


def get_latest_filing_using_cik_number(conn, cik: str):
    """Fetch the most recent filing for a given CIK"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 
                f.filing_id,
                f.manager_id,
                f.form_type,
                f.sec_accession_number,
                f.filing_date,
                f.file_number,
                f.filing_directory,
                f.reporting_period,
                f.created_at,
                f.updated_at
            FROM filings f
            JOIN managers m ON f.manager_id = m.manager_id
            WHERE m.cik_number = %s
            ORDER BY f.filing_date DESC
            LIMIT 1
            """,
            (cik.zfill(10),),
        )
        result = cur.fetchone()

        return Filing(*result) if result else None


def gather_holdings_from_soup(conn, soup) -> List[Holding]:
    """Parse XML soup and return list of Holding objects"""
    holdings = []

    for row in soup.find_all('infoTable'):
        # Extract basic data
        name_of_issuer = row.find('nameOfIssuer').text.strip()
        cusip = row.find('cusip').text.strip()
        title_of_class = row.find('titleOfClass').text.strip()

        # Convert numerical values
        try:
            value = int(float(row.find('value').text.strip()))
            shares_amount = int(float(row.find('sshPrnamt').text.strip()))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid numerical value in holding data: {e}")

        # Extract enums with validation
        share_type = row.find('sshPrnamtType').text.strip().upper()
        if share_type not in ('SH', 'PRN'):
            raise ValueError(f"Invalid share type: {share_type}")

        investment_discretion = row.find('investmentDiscretion').text.strip().upper()
        if investment_discretion not in ('SOLE', 'DFND', 'OTR'):
            raise ValueError(f"Invalid investment discretion: {investment_discretion}")

        put_call = row.find('putCall')
        put_call = put_call.text.strip().upper() if put_call else "NONE"

        # Extract voting authority
        voting = row.find('votingAuthority')
        try:
            sole = int(voting.find('Sole').text.strip()) if voting else 0
            shared = int(voting.find('Shared').text.strip()) if voting else 0
            none = int(voting.find('None').text.strip()) if voting else 0
        except (AttributeError, ValueError):
            sole = shared = none = 0

        security = get_or_create_security_class(conn, title_of_class)
        holding_type = get_or_create_holding_type(conn, share_type)
        option = get_or_create_option_type(conn, put_call)
        discretion = get_or_create_discretion_type(conn, investment_discretion)

        # Create Holding instance
        holding = Holding(
            shares_or_principal_amount=shares_amount,
            value=value,
            title_of_class_id=security.id,
            shares_or_principal_type_id=holding_type.id,
            put_or_call_id=option.id,
            investment_discretion_id=discretion.id,
            voting_authority_sole=sole,
            voting_authority_shared=shared,
            voting_authority_none=none,
            # Additional metadata
            _issuer_name=name_of_issuer,
            _issuer_cusip=cusip,
        )

        holdings.append(holding)

    return holdings


def insert_filing(conn, manager_id: int, filing: EdgarFiling) -> int:
    try:
        with conn.cursor() as cur:
            if not isinstance(filing.filing_date, dt.date):
                raise ValueError("filing_date must be a datetime object")

            # Insert the filing
            cur.execute(
                """
                INSERT INTO filings (
                    manager_id,
                    form_type,
                    sec_accession_number,
                    filing_date,
                    file_number,
                    filing_directory,
                    reporting_period
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING filing_id
                """,
                (
                    manager_id,
                    filing.form,
                    filing.accession_number,
                    filing.filing_date.isoformat(),
                    filing.file_number if hasattr(filing, 'file_number') else None,  # type: ignore
                    filing.filing_directory.name,
                    filing.period_of_report,
                ),
            )

            filing_id = cur.fetchone()[0]
            conn.commit()
            logger.info(
                f"Inserted filing with ID: {filing_id} for manager ID: {manager_id}"
            )
            return filing_id

    except psycopg2.Error as e:
        conn.rollback()
        raise psycopg2.Error(f"Failed to insert filing: {e}")


def get_manager_by_cik(conn, cik_number):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    manager_id,
                    cik_number,
                    manager_name,
                    manager_business_address,
                    manager_mailing_address,
                    manager_phone,
                    created_at,
                    updated_at
                FROM managers
                WHERE cik_number = %s
                """,
                (cik_number.zfill(10),),
            )

            result = cur.fetchone()
            if not result:
                return None

            return Manager(*result)

    except psycopg2.Error as e:
        raise psycopg2.Error(f"Database error fetching manager: {e}")


def get_or_create_issuer(conn, cusip: str, issuer_name: str) -> Issuer:
    """Get issuer by CUSIP if exists"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO issuers (cusip, issuer_name)
            VALUES (%s, %s)
            ON CONFLICT (cusip) DO UPDATE
            SET issuer_name = EXCLUDED.issuer_name,
                updated_at = CURRENT_TIMESTAMP
            RETURNING issuer_id, cusip, issuer_name, created_at, updated_at
            """,
            (cusip, issuer_name),
        )
        result = cur.fetchone()
        return Issuer(*result)


def get_or_create_security_class(conn, class_name: str) -> SecurityClass:
    """Get or create a security class (e.g., 'COM', 'PUT')"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO security_classes (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE
            SET name = EXCLUDED.name
            RETURNING id, name
            """,
            (class_name,),
        )
        result = cur.fetchone()

        return SecurityClass(*result)


def get_or_create_holding_type(conn, type_code: str) -> HoldingType:
    """Get or create a holding type ('SH' or 'PRN')"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holding_types (code)
            VALUES (%s)
            ON CONFLICT (code) DO UPDATE
            SET code = EXCLUDED.code
            RETURNING id, code
            """,
            (type_code,),
        )
        result = cur.fetchone()
        return HoldingType(*result)


def get_or_create_option_type(conn, option_name: str) -> OptionType:
    """Get or create an option type ('PUT', 'CALL', 'NONE')"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO option_types (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE
            SET name = EXCLUDED.name
            RETURNING id, name
            """,
            (option_name,),
        )
        result = cur.fetchone()
        return OptionType(*result)


def get_or_create_discretion_type(conn, discretion_code: str) -> DiscretionType:
    """Get or create investment discretion type ('SOLE', 'DFND', 'OTR')"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO discretion_types (code)
            VALUES (%s)
            ON CONFLICT (code) DO UPDATE
            SET code = EXCLUDED.code
            RETURNING id, code
            """,
            (discretion_code,),
        )
        result = cur.fetchone()
        return DiscretionType(*result)


def insert_holdings_batch(conn, holdings: list[Holding]) -> None:
    """
    Insert multiple holdings in a single batch operation.
    """
    try:
        with conn.cursor() as cur:
            # Create a list of tuples with all values
            values = [
                (
                    h.filing_id,
                    h.issuer_id,
                    h.title_of_class_id,
                    h.shares_or_principal_amount,
                    h.shares_or_principal_type_id,
                    h.value,
                    h.put_or_call_id,
                    h.investment_discretion_id,
                    h.voting_authority_sole or 0,
                    h.voting_authority_shared or 0,
                    h.voting_authority_none or 0,
                )
                for h in holdings
            ]

            # Use execute_values for batch insert
            from psycopg2.extras import execute_values

            execute_values(
                cur,
                """
                INSERT INTO holdings (
                    filing_id, issuer_id, title_of_class, 
                    shares_or_principal_amount, shares_or_principal_type,
                    value, put_or_call, investment_discretion,
                    voting_authority_sole, voting_authority_shared, voting_authority_none
                ) VALUES %s
                RETURNING holding_id
                """,
                values,
                page_size=5000,  # Adjust based on your needs
            )
            conn.commit()

    except psycopg2.Error as e:
        conn.rollback()
        raise psycopg2.Error(f"Failed to insert holdings: {e}")


def insert_holding(conn, holding: Holding) -> int:
    """
    Insert a single holding into the database. This function is too slow!
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO holdings (
                    filing_id,
                    issuer_id,
                    title_of_class,
                    shares_or_principal_amount,
                    shares_or_principal_type,
                    value,
                    put_or_call,
                    investment_discretion,
                    voting_authority_sole,
                    voting_authority_shared,
                    voting_authority_none
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING holding_id
                """,
                (
                    holding.filing_id,
                    holding.issuer_id,
                    holding.title_of_class_id,
                    holding.shares_or_principal_amount,
                    holding.shares_or_principal_type_id,
                    holding.value,
                    holding.put_or_call_id,
                    holding.investment_discretion_id,
                    holding.voting_authority_sole or 0,
                    holding.voting_authority_shared or 0,
                    holding.voting_authority_none or 0,
                ),
            )
            holding_id = cur.fetchone()[0]
            conn.commit()
            return holding_id

    except psycopg2.Error as e:
        conn.rollback()
        raise psycopg2.Error(f"Failed to insert holding: {e}")


def connect_db(
    db_host="localhost",
    db_port="5432",
    db_user="postgres",
    db_password="",
    db_name="sec",
) -> psycopg2.extensions.connection:

    try:
        logger.info(
            f"Database parameters: Host {db_host}, Port {db_port}, User {db_user}, Database {db_name}"
        )
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
        )
        logger.info("Database connection successful")  # Temporary debug

        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")


def filing_exists(conn, accession_number) -> bool:
    """
    Check if a filing with the given SEC accession number already exists in the database
    """
    try:
        with conn.cursor() as cur:
            # Check if filing exists and return its ID
            cur.execute(
                """
                SELECT filing_id FROM filings 
                WHERE sec_accession_number = %s
                """,
                (accession_number,),
            )
            result = cur.fetchone()

            if result:
                return True  # Filing exists, return filing_id
            return False

    except Exception as e:
        logger.error(f"Error checking if filing exists: {e}")
        return False
