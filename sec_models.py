# All database models
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any


@dataclass
class Filing:
    """Represents an SEC filing with all relevant metadata."""

    filing_id: int
    manager_id: int
    form_type: str
    sec_accession_number: str
    filing_date: date
    file_number: Optional[str] = None
    filing_directory: Optional[str] = None
    reporting_period: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def accession_number_short(self) -> str:
        """Returns the accession number without dashes (e.g., '0000320193-22-000108' -> '000032019322000108')"""
        return self.sec_accession_number.replace("-", "")

    def get_edgar_url(self) -> str:
        """Returns the full EDGAR URL for this filing"""
        return f"https://www.sec.gov/{self.filing_directory}"


@dataclass
class Manager:
    """Represents a filing manager (investment firm or individual)."""

    manager_id: int
    cik_number: str
    manager_name: str
    business_address: Optional[Dict[str, Any]] = None
    mailing_address: Optional[Dict[str, Any]] = None
    phone: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def cik(self) -> str:
        """Returns the CIK in standard 10-digit format."""
        return self.cik_number.zfill(10)

    def get_business_address_str(self) -> str:
        """Formats the business address as a multi-line string."""
        if not self.business_address:
            return "No business address on file"
        return "\n".join(f"{k}: {v}" for k, v in self.business_address.items())


@dataclass
class Holding:
    """
    Represents a single security holding from a 13F filing.
    """

    shares_or_principal_amount: int
    value: int

    # Database fields
    filing_id: Optional[int] = None  # can be temporarily None if not yet inserted
    issuer_id: Optional[int] = None  # can be temporarily None if not yet inserted
    title_of_class_id: Optional[int] = None
    shares_or_principal_type_id: Optional[int] = None
    put_or_call_id: Optional[int] = None
    investment_discretion_id: Optional[int] = None

    voting_authority_sole: Optional[int] = None
    voting_authority_shared: Optional[int] = None
    voting_authority_none: Optional[int] = None

    # Database fields
    holding_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Temporary fields for issuer metadata
    _issuer_name: Optional[str] = None
    _issuer_cusip: Optional[str] = None


@dataclass
class Issuer:
    """Represents a security issuer (company)"""

    issuer_id: int
    cusip: str
    issuer_name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def validate(self):
        """Validate issuer data before insertion"""
        if not self.cusip or len(self.cusip) > 9:
            raise ValueError("CUSIP must be 1-9 characters")
        if not self.issuer_name or len(self.issuer_name) > 255:
            raise ValueError("Issuer name must be 1-255 characters")


@dataclass
class SecurityClass:
    # title_of_class
    id: int
    name: str


@dataclass
class HoldingType:
    # shares_or_principal_type
    id: int
    code: str


@dataclass
class OptionType:
    # put_or_call
    id: int
    name: str


@dataclass
class DiscretionType:
    # investment_discretion
    id: int
    code: str
