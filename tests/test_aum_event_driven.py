import unittest
from unittest.mock import MagicMock
from types import SimpleNamespace

from utils import (
    extract_reported_totals,
    update_company_aum_post_ingest,
)


class TestAumEventDriven(unittest.TestCase):
    def test_extract_reported_totals_from_xml(self):
        """Test extracting tableValueTotal and tableEntryTotal from primary_doc.xml."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
        <edgarSubmission>
            <formData>
                <summaryPage>
                    <tableEntryTotal>550</tableEntryTotal>
                    <tableValueTotal>123456789</tableValueTotal>
                </summaryPage>
            </formData>
        </edgarSubmission>
        """
        mock_attachment = SimpleNamespace(
            document="primary_doc.xml",
            content=xml_content,
        )
        mock_filing = SimpleNamespace(
            attachments=[mock_attachment],
            form="13F-HR",
        )

        val_total, entry_total = extract_reported_totals(mock_filing)
        self.assertEqual(val_total, 123456789)
        self.assertEqual(entry_total, 550)

    def test_extract_reported_totals_missing(self):
        """Test extract_reported_totals when no summary totals exist."""
        mock_filing = SimpleNamespace(
            attachments=[],
            form="13F-HR",
        )
        val_total, entry_total = extract_reported_totals(mock_filing)
        self.assertIsNone(val_total)
        self.assertIsNone(entry_total)

    def test_update_company_aum_standard(self):
        """Test standard AUM update when filing is the latest."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock query returns:
        # 1. Filing metadata: (company_id=1, period='2026-03-31', date='2026-05-15', form='13F-HR')
        # 2. Check for newer filings: None (this is the latest filing)
        mock_cursor.fetchone.side_effect = [
            (1, "2026-03-31", "2026-05-15", "13F-HR"),
            None,
        ]

        result = update_company_aum_post_ingest(mock_conn, company_id=1, filing_id=100)
        self.assertTrue(result)

        # Verify UPDATE companies query was executed
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        self.assertTrue(any("UPDATE companies" in sql for sql in executed_sqls))
        self.assertTrue(any("holdings_normalised" in sql for sql in executed_sqls))

    def test_update_company_aum_backfill_protection(self):
        """Test that AUM update is skipped if a newer filing already exists."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock query returns:
        # 1. Filing metadata: (company_id=1, period='2025-12-31', date='2026-02-15', form='13F-HR')
        # 2. Check for newer filings: (1,) -> A newer filing exists!
        mock_cursor.fetchone.side_effect = [
            (1, "2025-12-31", "2026-02-15", "13F-HR"),
            (1,),
        ]

        result = update_company_aum_post_ingest(mock_conn, company_id=1, filing_id=50)
        self.assertFalse(result)

        # Verify UPDATE companies query was NOT executed
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        self.assertFalse(any("UPDATE companies" in sql for sql in executed_sqls))

    def test_update_company_aum_partial_amendment_skipped(self):
        """Test that partial amendment with < 50% holdings does not overwrite companies.aum."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock query returns:
        # 1. Filing metadata: (company_id=1, period='2026-03-31', date='2026-06-01', form='13F-HR/A')
        # 2. Check for newer filings: None (same or latest period)
        # 3. Previous filing for same period: (orig_filing_id=90, prev_count=10000)
        # 4. Current filing holdings count: (100,) -> 100 < 50% of 10000!
        mock_cursor.fetchone.side_effect = [
            (1, "2026-03-31", "2026-06-01", "13F-HR/A"),
            None,
            (90, 10000),
            (100,),
        ]

        result = update_company_aum_post_ingest(mock_conn, company_id=1, filing_id=101)
        self.assertFalse(result)

        # Verify UPDATE companies query was NOT executed
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        self.assertFalse(any("UPDATE companies" in sql for sql in executed_sqls))

    def test_update_company_aum_restatement_amendment_applied(self):
        """Test that full restatement amendment (>= 50% holdings) updates companies.aum."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock query returns:
        # 1. Filing metadata: (company_id=1, period='2026-03-31', date='2026-06-01', form='13F-HR/A')
        # 2. Check for newer filings: None
        # 3. Previous filing: (orig_filing_id=90, prev_count=1000)
        # 4. Current filing holdings count: (980,) -> 980 >= 50% of 1000
        mock_cursor.fetchone.side_effect = [
            (1, "2026-03-31", "2026-06-01", "13F-HR/A"),
            None,
            (90, 1000),
            (980,),
        ]

        result = update_company_aum_post_ingest(mock_conn, company_id=1, filing_id=102)
        self.assertTrue(result)

        # Verify UPDATE companies query was executed
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        self.assertTrue(any("UPDATE companies" in sql for sql in executed_sqls))


if __name__ == "__main__":
    unittest.main()
