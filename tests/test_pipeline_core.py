from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import MOEX_API

from repositories.details_repository import DetailsRepository


class PipelineCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        db_path = Path(self.tmp.name) / "test.sqlite3"
        MOEX_API.CACHE_DB_PATH = db_path
        MOEX_API.DB_DIR = Path(self.tmp.name)
        MOEX_API.init_db()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_diff_returns_incremental_secids(self) -> None:
        prev_csv = "SECID,ISIN,SHORTNAME\nAAA,ISIN1,Old AAA\n"
        with sqlite3.connect(MOEX_API.CACHE_DB_PATH) as connection:
            connection.execute(
                "INSERT INTO bonds_cache(fetch_date, csv_data, created_at) VALUES (?, ?, datetime('now'))",
                ("2026-01-01", prev_csv),
            )
            connection.commit()

        current = pd.DataFrame(
            [
                {"SECID": "AAA", "ISIN": "ISIN1", "SHORTNAME": "Old AAA"},
                {"SECID": "BBB", "ISIN": "ISIN2", "SHORTNAME": "New BBB"},
            ]
        )
        added_secids, removed_keys, _, _ = MOEX_API._diff_bonds_against_previous_day(current, "2026-01-02")

        self.assertEqual(added_secids, ["BBB"])
        self.assertEqual(removed_keys, [])

    def test_refresh_bonds_read_model_uses_latest_snapshot(self) -> None:
        with sqlite3.connect(MOEX_API.CACHE_DB_PATH) as connection:
            pd.DataFrame([{"SECID": "AAA", "ISIN": "ISIN1"}]).to_sql("bonds_enriched", connection, if_exists="replace", index=False)
            connection.execute(
                "INSERT INTO intraday_quotes_snapshot(snapshot_at, secid, boardid, tradingstatus, open, close, lclose, last, numtrades, volvalue, updatetime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("2026-01-02T10:00:00", "AAA", "TQOB", "T", 100.0, 101.0, 99.0, 101.0, 1.0, 1000.0, "10:00:00"),
            )
            connection.execute(
                "INSERT INTO intraday_quotes_snapshot(snapshot_at, secid, boardid, tradingstatus, open, close, lclose, last, numtrades, volvalue, updatetime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("2026-01-02T10:05:00", "AAA", "TQOB", "T", 100.0, 102.0, 99.0, 102.0, 2.0, 2000.0, "10:05:00"),
            )
            connection.commit()

        MOEX_API._refresh_bonds_read_model()

        with sqlite3.connect(MOEX_API.CACHE_DB_PATH) as connection:
            row = connection.execute("SELECT quote_last, quote_snapshot_at FROM bonds_read_model WHERE SECID='AAA'").fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], 102.0)
        self.assertEqual(row[1], "2026-01-02T10:05:00")

    def test_details_repository_bulk_load(self) -> None:
        with sqlite3.connect(MOEX_API.CACHE_DB_PATH) as connection:
            connection.execute(
                "INSERT INTO details_cache(endpoint, secid, response_json, fetched_at) VALUES (?, ?, ?, ?)",
                ("security_overview", "AAA", '{"x":{"columns":[],"data":[]}}', "2099-01-01T00:00:00"),
            )
            connection.commit()

        repo = DetailsRepository(MOEX_API.CACHE_DB_PATH)
        fresh, latest = repo.load_cached_records_bulk(["AAA"], ["security_overview"], details_ttl_hours=24)
        self.assertIn(("security_overview", "AAA"), fresh)
        self.assertIn(("security_overview", "AAA"), latest)


if __name__ == "__main__":
    unittest.main()
