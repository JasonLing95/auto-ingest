import datetime as dt
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx

import main as main_module
from main import (
    compute_query_start_date,
    configure_edgar_http_timeout,
    fetch_new_filing,
    heartbeat,
    run_daily_index_sweep,
)
from utils import get_active_quarter_end, insert_holdings_batch, prioritize_ciks


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


class TestEdgarHttpTimeout(unittest.TestCase):
    @patch("main.httpclient.close_clients")
    def test_configure_edgar_http_timeout_sets_params_and_closes_clients(
        self, mock_close_clients
    ):
        timeout = configure_edgar_http_timeout(read_timeout=30.0, connect_timeout=10.0)
        self.assertIsInstance(timeout, httpx.Timeout)
        self.assertEqual(timeout.read, 30.0)
        self.assertEqual(timeout.connect, 10.0)
        self.assertEqual(main_module.httpclient.HTTP_MGR.httpx_params["timeout"], timeout)
        if hasattr(main_module.httpclient, "DEFAULT_PARAMS"):
            self.assertEqual(main_module.httpclient.DEFAULT_PARAMS["timeout"], timeout)
        mock_close_clients.assert_called_once()


class TestPrioritizeCiks(unittest.TestCase):
    def test_get_active_quarter_end(self):
        self.assertEqual(get_active_quarter_end(dt.date(2026, 2, 15)), dt.date(2025, 12, 31))
        self.assertEqual(get_active_quarter_end(dt.date(2026, 5, 15)), dt.date(2026, 3, 31))
        self.assertEqual(get_active_quarter_end(dt.date(2026, 8, 15)), dt.date(2026, 6, 30))
        self.assertEqual(get_active_quarter_end(dt.date(2026, 11, 16)), dt.date(2026, 9, 30))

    def test_prioritize_ciks_unfiled_before_filed_and_aum_desc(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate DB rows: (cik_number, aum, has_filed_active_quarter)
        mock_cursor.fetchall.return_value = [
            ("111", 5_000_000_000, True),     # filed, high AUM -> Priority 2
            ("222", 100_000_000, False),      # unfiled, lower AUM -> Priority 1 (#2)
            ("333", 900_000_000_000, False),  # unfiled, highest AUM -> Priority 1 (#1)
            ("444", None, False),             # unfiled, NULL AUM -> Priority 1 (after non-null AUMs)
            ("555", 10_000_000_000, True),    # filed, higher AUM -> Priority 2 (#1 in P2)
        ]

        input_ciks = ["111", "222", "333", "666", "444", "555"]
        result = prioritize_ciks(mock_conn, input_ciks, today=dt.date(2026, 11, 16))

        # 666 has no DB row -> treated as (aum=None, has_filed=False), preserves input order vs 444
        self.assertEqual(result, ["333", "222", "666", "444", "555", "111"])

        executed_sql, params = mock_cursor.execute.call_args[0]
        self.assertIn("period_of_report >= %s", executed_sql)
        self.assertEqual(params[0], dt.date(2026, 9, 30))

    def test_prioritize_ciks_db_error_falls_back_to_original_order(self):
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = RuntimeError("DB unavailable")
        input_ciks = ["1067983", "735286", "93751"]
        self.assertEqual(prioritize_ciks(mock_conn, input_ciks), input_ciks)


class _StopLoop(Exception):
    pass


class TestHeartbeat(unittest.TestCase):
    @patch("main.requests.get")
    @patch("main.time.sleep", side_effect=_StopLoop)
    def test_heartbeat_pings_when_main_loop_active(self, _mock_sleep, mock_get):
        main_module.LAST_MAIN_LOOP_TIMESTAMP = time.time()
        with self.assertRaises(_StopLoop):
            heartbeat()
        mock_get.assert_called_once_with(main_module.HEALTHCHECK_URL, timeout=10)

    @patch("main.requests.get")
    @patch("main.time.sleep", side_effect=_StopLoop)
    def test_heartbeat_suppresses_ping_when_inactive(self, _mock_sleep, mock_get):
        main_module.LAST_MAIN_LOOP_TIMESTAMP = time.time() - 300
        with self.assertLogs("main", level="WARNING") as cm:
            with self.assertRaises(_StopLoop):
                heartbeat()
        mock_get.assert_not_called()
        self.assertIn("Suppressing heartbeat ping", "\n".join(cm.output))


class TestDailyIndexSweep(unittest.TestCase):
    @patch("main.ingest_single_filing", return_value=True)
    @patch("main.get_filings")
    def test_index_sweep_filters_to_relevant_ciks(self, mock_get_filings, mock_ingest):
        matching_filing = SimpleNamespace(
            cik=1067983,
            accession_number="0001193125-26-463399",
            filing_date="2026-11-16",
        )
        non_matching_filing = SimpleNamespace(
            cik=9999999,
            accession_number="0009999999-26-000001",
            filing_date="2026-11-16",
        )
        mock_get_filings.return_value = [matching_filing, non_matching_filing]

        mock_conn = MagicMock()
        ingested = run_daily_index_sweep(
            mock_conn,
            relevant_ciks={"1067983"},
            filing_date_str="2026-11-16",
        )

        self.assertEqual(ingested, 1)
        mock_get_filings.assert_called_once_with(
            form=main_module.VALID_FORMS, filing_date="2026-11-16"
        )
        mock_ingest.assert_called_once_with(mock_conn, "1067983", matching_filing)


if __name__ == "__main__":
    unittest.main()
