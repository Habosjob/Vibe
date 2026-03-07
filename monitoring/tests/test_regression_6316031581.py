from __future__ import annotations

import unittest

from monitoring.main import parse_reports_page


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


if __name__ == "__main__":
    unittest.main()
