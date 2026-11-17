import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from main import compute_query_start_date, fetch_new_filing
from utils import insert_holdings_batch


class TestQueryStartDate(unittest.TestCase):
    def test_none_defaults_to_730_days_ago(self):
        today = dt.date(2026, 11, 17)
        self.assertEqual(
            compute_query_start_date(None, today=today),
            dt.date(2024, 11, 17),
        )

    def test_older_than_yesterday_advances_one_day(self):
        today = dt.date(2026, 11, 17)
        self.assertEqual(
            compute_query_start_date(dt.date(2026, 8, 14), today=today),
            dt.date(2026, 8, 15),
        )
        self.assertEqual(
            compute_query_start_date(dt.date(2026, 11, 15), today=today),
            dt.date(2026, 11, 16),
        )

    def test_yesterday_and_today_stay_inclusive(self):
        today = dt.date(2026, 11, 17)
        self.assertEqual(
            compute_query_start_date(dt.date(2026, 11, 16), today=today),
            dt.date(2026, 11, 16),
        )
        self.assertEqual(
            compute_query_start_date(dt.date(2026, 11, 17), today=today),
            dt.date(2026, 11, 17),
        )

    def test_future_date_clamped_to_today(self):
        today = dt.date(2026, 11, 17)
        self.assertEqual(
            compute_query_start_date(dt.date(2026, 11, 18), today=today),
            today,
        )

    def test_datetime_input_converted_to_date(self):
        today = dt.date(2026, 11, 17)
        self.assertEqual(
            compute_query_start_date(dt.datetime(2026, 8, 14, 15, 30), today=today),
            dt.date(2026, 8, 15),
        )


class TestInsertHoldingsBatchScopedFilingQuery(unittest.TestCase):
    @patch("utils.insert_issuers")
    @patch("utils.insert_lookups")
    def test_queries_filings_filtered_by_accession_number(
        self, mock_insert_lookups, mock_insert_issuers
    ):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        acc = "0001172661-26-005424"
        mock_cursor.fetchall.return_value = [(acc, 346987)]
        mock_insert_lookups.return_value = {
            "title_of_class": {"COM": 1},
            "share_type": {"SH": 1},
            "put_call": {"NONE": 1},
            "investment_discretion": {"SOLE": 1},
        }
        mock_insert_issuers.return_value = {"17275R102": 556}

        holdings = [
            [
                "0002007591",
                acc,
                100,
                5000,
                "COM",
                "SH",
                "SOLE",
                "NONE",
                100,
                0,
                0,
                "CISCO SYS INC",
                "17275r102",
            ]
        ]

        insert_holdings_batch(mock_conn, holdings)

        filings_select_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "FROM filings" in call[0][0]
        ]
        self.assertEqual(len(filings_select_calls), 1)
        sql, params = filings_select_calls[0][0]
        self.assertIn("WHERE accession_number = ANY(%s)", sql)
        self.assertEqual(params, ([acc],))
        mock_cursor.copy_from.assert_called_once()


class TestFetchNewFilingLogging(unittest.TestCase):
    @patch("main.update_company_aum_post_ingest")
    @patch("main.insert_holdings_batch")
    @patch("main.insert_filing", return_value=346987)
    @patch("main.filing_exists", return_value=False)
    @patch("main.Company")
    def test_no_misleading_not_newer_log_after_ingest(
        self,
        mock_company_cls,
        _mock_exists,
        _mock_insert_filing,
        _mock_insert_holdings,
        _mock_update_aum,
    ):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>CISCO SYS INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>17275R102</cusip>
                <value>1000</value>
                <shrsOrPrnAmt><sshPrnamt>100</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
                <investmentDiscretion>SOLE</investmentDiscretion>
                <votingAuthority><Sole>100</Sole><Shared>0</Shared><None>0</None></votingAuthority>
            </infoTable>
        </informationTable>"""
        attachment = SimpleNamespace(
            document="infotable.xml",
            url="https://example.com/infotable.xml",
            content=xml,
        )
        filing = SimpleNamespace(
            filing_date=dt.date(2026, 11, 16),
            accession_number="0001172661-26-005424",
            attachments=[attachment],
        )
        mock_company_cls.return_value.get_filings.return_value = [filing]

        mock_conn = MagicMock()
        with self.assertLogs("main", level="INFO") as cm:
            fetch_new_filing(mock_conn, "2007591", dt.date(2026, 8, 15))

        joined = "\n".join(cm.output)
        self.assertNotIn("is not newer than most recent date", joined)
        self.assertNotIn("No new filings ingested", joined)

    @patch("main.filing_exists", return_value=True)
    @patch("main.Company")
    def test_logs_when_zero_new_filings_ingested(self, mock_company_cls, _mock_exists):
        filing = SimpleNamespace(
            filing_date=dt.date(2026, 11, 16),
            accession_number="0001172661-26-005424",
            attachments=[],
        )
        mock_company_cls.return_value.get_filings.return_value = [filing]

        mock_conn = MagicMock()
        with self.assertLogs("main", level="INFO") as cm:
            fetch_new_filing(mock_conn, "2007591", dt.date(2026, 11, 16))

        joined = "\n".join(cm.output)
        self.assertIn("No new filings ingested for CIK 2007591", joined)


if __name__ == "__main__":
    unittest.main()
