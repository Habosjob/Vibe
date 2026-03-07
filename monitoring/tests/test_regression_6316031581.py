from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta

from monitoring.main import EmitentRow, SCHEMA_SQL, parse_reports_page, stage_reports_prepare


class TestEDisclosureRegression6316031581(unittest.TestCase):
    def test_parse_type4_row_extracts_fileload_and_date(self) -> None:
        html = """
        <html><body>
        <table class='zebra'>
          <tr>
            <td>Годовая консолидированная финансовая отчетность</td>
            <td>2025 год</td>
            <td>10.02.2026</td>
            <td>10.02.2026</td>
            <td><span>Документ</span> <a href="/portal/FileLoad.ashx?Fileid=123456">скачать</a></td>
          </tr>
        </table>
        </body></html>
        """
        rows, top_hash = parse_reports_page(
            html=html,
            company_id="225",
            type_id=4,
            type_name="Консолидированная",
            known_state=None,
        )
        self.assertTrue(top_hash)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["placement_date"], "2026-02-10")
        self.assertIn("FileLoad.ashx", rows[0]["file_url"])
        self.assertTrue(rows[0]["file_url"].startswith("https://www.e-disclosure.ru/portal/FileLoad.ashx"))

    def test_missing_history_forces_recheck_even_if_schedule_not_due(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA_SQL)

        inn = "6316031581"
        future_check = (datetime.now() + timedelta(days=10)).isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT INTO company_map (inn, company_id, company_name, company_url, verified_inn, validation_status, last_success_at, full_scan_at, fast_scan_at, last_checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (inn, "225", "TEST", "", inn, "verified", "", "", "", datetime.now().isoformat(timespec="seconds")),
        )
        conn.execute(
            """
            INSERT INTO emitent_schedule (inn, company_id, last_checked_at, next_check_at, last_change_at, stable_run_count, last_mode, last_event_gate_at, last_files_scan_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (inn, "225", datetime.now().isoformat(timespec="seconds"), future_check, "", 5, "event_gate_only", "", ""),
        )
        conn.commit()

        emitents = [
            EmitentRow(
                inn=inn,
                company_name="TEST",
                scoring="",
                scoring_date="",
                nra_rate="",
                acra_rate="",
                nkr_rate="",
                raex_rate="",
            )
        ]

        tasks, skipped, _, _, _, prep = stage_reports_prepare(conn, emitents)
        self.assertEqual(prep["processed_emitents"], 1)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(len(skipped), 0)
        self.assertTrue(tasks[0]["force_missing_recheck"])


if __name__ == "__main__":
    unittest.main()
