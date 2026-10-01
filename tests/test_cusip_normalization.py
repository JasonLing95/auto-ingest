import unittest
from unittest.mock import MagicMock
import pandas as pd
from lxml import etree

from utils import (
    normalize_cusip,
    gather_data_with_bs4,
    gather_holdings_using_lxml,
    insert_issuers,
    HOLDINGS_COLUMNS,
)
from sec_models import Issuer, Holding


class TestCusipNormalization(unittest.TestCase):
    def test_normalize_cusip_basic(self):
        """Test lowercase, mixed-case, and whitespace handling in normalize_cusip."""
        self.assertEqual(normalize_cusip("17275r102"), "17275R102")
        self.assertEqual(normalize_cusip("02079k305"), "02079K305")
        self.assertEqual(normalize_cusip("46434g103"), "46434G103")
        self.assertEqual(normalize_cusip("  17275R102  \t\n"), "17275R102")
        self.assertEqual(normalize_cusip("  17275r102  "), "17275R102")

    def test_normalize_cusip_edge_cases(self):
        """Test None, empty strings, and non-string inputs."""
        self.assertEqual(normalize_cusip(None), "")
        self.assertEqual(normalize_cusip(""), "")
        self.assertEqual(normalize_cusip("   "), "")

    def test_bs4_parsing_normalizes_cusip(self):
        """Test gather_data_with_bs4 normalizes lowercase CUSIPs in XML."""
        xml_data = """<?xml version="1.0" encoding="UTF-8"?>
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>CISCO SYS INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip> 17275r102 </cusip>
                <value>1000</value>
                <sshPrnamt>100</sshPrnamt>
                <sshPrnamtType>SH</sshPrnamtType>
                <investmentDiscretion>SOLE</investmentDiscretion>
                <putCall>NONE</putCall>
                <votingAuthority>
                    <Sole>100</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
            <infoTable>
                <nameOfIssuer>ALPHABET INC</nameOfIssuer>
                <titleOfClass>CAP STK CL A</titleOfClass>
                <cusip>02079k305</cusip>
                <value>5000</value>
                <sshPrnamt>50</sshPrnamt>
                <sshPrnamtType>SH</sshPrnamtType>
                <investmentDiscretion>SOLE</investmentDiscretion>
                <putCall>NONE</putCall>
                <votingAuthority>
                    <Sole>50</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """
        records = gather_data_with_bs4(xml_data, "0000000001", "0000000001-26-000001")
        self.assertEqual(len(records), 2)
        cusip_idx = HOLDINGS_COLUMNS.index("cusip")
        self.assertEqual(records[0][cusip_idx], "17275R102")
        self.assertEqual(records[1][cusip_idx], "02079K305")

    def test_lxml_parsing_normalizes_cusip(self):
        """Test gather_holdings_using_lxml normalizes lowercase CUSIPs."""
        xml_data = """<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ISHARES CORE MSCI EM</nameOfIssuer>
                <titleOfClass>ETF</titleOfClass>
                <cusip>  46434g103  </cusip>
                <value>2500</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>500</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <investmentDiscretion>SOLE</investmentDiscretion>
                <putCall>NONE</putCall>
                <votingAuthority>
                    <Sole>500</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>"""
        tree = etree.fromstring(xml_data.encode("utf-8"))
        ns = {"ns": "http://www.sec.gov/edgar/document/thirteenf/informationtable"}
        tables = tree.xpath("//ns:infoTable", namespaces=ns)

        records = gather_holdings_using_lxml(tables, ns, "0000000001", "0000000001-26-000001")
        self.assertEqual(len(records), 1)
        cusip_idx = HOLDINGS_COLUMNS.index("cusip")
        self.assertEqual(records[0][cusip_idx], "46434G103")

    def test_insert_issuers_deduplicates_and_maps_canonical_issuer_id(self):
        """Test that insert_issuers resolves lowercase and uppercase CUSIPs to canonical issuer_id."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.mogrify.side_effect = lambda sql, params: b"MOCKED SQL"
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate database having both a legacy lowercase duplicate and the canonical uppercase issuer:
        # In SQL query: ORDER BY (cusip = UPPER(TRIM(cusip))) ASC, issuer_id DESC
        # The lowercase row (214086) comes first, followed by canonical row 556
        mock_cursor.fetchall.return_value = [
            ("17275r102", 214086),
            ("17275R102", 556),
            ("02079K305", 43),
        ]

        df = pd.DataFrame([
            ["0000000001", "acc1", 100, 1000, "COM", "SH", "SOLE", "NONE", 100, 0, 0, "CISCO SYS INC", "17275r102"],
            ["0000000001", "acc1", 200, 2000, "COM", "SH", "SOLE", "NONE", 200, 0, 0, "CISCO SYS INC", "17275R102"],
            ["0000000001", "acc1", 50, 5000, "COM", "SH", "SOLE", "NONE", 50, 0, 0, "ALPHABET INC", " 02079k305 "],
        ], columns=HOLDINGS_COLUMNS)

        mapping = insert_issuers(mock_conn, df)

        # Mapping keys must be uppercase
        self.assertIn("17275R102", mapping)
        self.assertIn("02079K305", mapping)

        # Canonical uppercase row (issuer_id = 556) should override duplicate lowercase row (214086)
        self.assertEqual(mapping["17275R102"], 556)
        self.assertEqual(mapping["02079K305"], 43)

    def test_sec_models_cusip_normalization(self):
        """Test that dataclass models normalize CUSIP on initialization."""
        issuer = Issuer(
            issuer_id=1,
            cusip=" 17275r102 ",
            issuer_name="CISCO SYS INC",
        )
        self.assertEqual(issuer.cusip, "17275R102")
        issuer.validate()
        self.assertEqual(issuer.cusip, "17275R102")

        holding = Holding(
            shares_or_principal_amount=100,
            value=1000,
            _issuer_cusip=" 02079k305 ",
            _issuer_name="ALPHABET INC",
        )
        self.assertEqual(holding._issuer_cusip, "02079K305")

    def test_insert_issuers_lock_free_skips_existing_rows(self):
        """Test that insert_issuers executes 0 insert operations when all CUSIPs already exist."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # All CUSIPs in the dataframe already exist in the database
        mock_cursor.fetchall.return_value = [
            ("AAPL12345", 101),
            ("MSFT67890", 102),
        ]

        df = pd.DataFrame([
            ["0000000001", "acc1", 100, 1000, "COM", "SH", "SOLE", "NONE", 100, 0, 0, "APPLE INC", "AAPL12345"],
            ["0000000001", "acc1", 200, 2000, "COM", "SH", "SOLE", "NONE", 200, 0, 0, "MICROSOFT CORP", "MSFT67890"],
        ], columns=HOLDINGS_COLUMNS)

        mapping = insert_issuers(mock_conn, df)

        self.assertEqual(mapping["AAPL12345"], 101)
        self.assertEqual(mapping["MSFT67890"], 102)

        # Confirm cur.execute was only called for SELECT queries, never INSERT or BEGIN
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        for sql in executed_sqls:
            self.assertIn("SELECT", sql)
            self.assertNotIn("INSERT", sql)
            self.assertNotIn("BEGIN", sql)


if __name__ == "__main__":
    unittest.main()
