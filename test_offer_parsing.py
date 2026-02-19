import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import moex_bonds_to_excel as script
from moex_bonds_to_excel import (
    merge_offer_metrics,
    normalize_coupon_formula_source,
    normalize_offer_type,
    parse_corpbonds_offer_metrics,
    parse_dohod_offer_metrics,
    parse_offer_metrics,
    select_offer_jobs_for_refresh,
    validate_rows,
)


class OfferParsingTests(unittest.TestCase):
    """Проверка парсинга оферт MOEX без сетевых запросов."""

    def test_normalize_offer_type(self) -> None:
        self.assertEqual(normalize_offer_type("PUT оферта"), "PUT")
        self.assertEqual(normalize_offer_type("Call-опцион"), "Call")
        self.assertEqual(normalize_offer_type("Оферта"), "PUT")
        self.assertEqual(normalize_offer_type("Погашение"), "✖")

    def test_parse_offer_metrics_ignores_redemption_rows(self) -> None:
        columns = ["offertype", "offerdate", "offerdatestart", "offerdateend"]
        offers_data = [
            ["Погашение", "2099-12-31", None, None],
            ["Оферта", "2099-05-01", None, None],
        ]

        offer_type, offer_date = parse_offer_metrics(offers_data, columns)

        self.assertEqual(offer_type, "PUT")
        self.assertEqual(offer_date, "2099-05-01")


    def test_normalize_coupon_formula_source(self) -> None:
        self.assertEqual(normalize_coupon_formula_source("dohod"), "DOHOD")
        self.assertEqual(normalize_coupon_formula_source("corpbonds"), "CORPBONDS")
        self.assertEqual(normalize_coupon_formula_source(""), "")

    def test_parse_offer_metrics_returns_call_type(self) -> None:
        columns = ["offertype", "offerdate", "offerdatestart", "offerdateend"]
        offers_data = [["Call-оферта", "2099-07-15", None, None]]

        offer_type, offer_date = parse_offer_metrics(offers_data, columns)

        self.assertEqual(offer_type, "Call")
        self.assertEqual(offer_date, "2099-07-15")

    def test_validate_rows_logs_offer_quality_counters(self) -> None:
        rows = [
            {"ISIN": "A", "SECID": "A", "HAS_PUT_CALL_OFFER": "✔", "PUT_CALL_OFFER_DATE": "", "MATDATE": "2099-01-01", "COUPONPERCENT": 10},
            {"ISIN": "B", "SECID": "B", "HAS_PUT_CALL_OFFER": "✔", "PUT_CALL_OFFER_DATE": "2099-02-01", "MATDATE": "2099-02-01", "COUPONPERCENT": 9},
            {"ISIN": "C", "SECID": "C", "HAS_PUT_CALL_OFFER": "✖", "PUT_CALL_OFFER_DATE": "", "MATDATE": "2099-03-01", "COUPONPERCENT": 8},
        ]

        with self.assertLogs(level="INFO") as logs:
            validate_rows(rows)

        summary = "\n".join(logs.output)
        self.assertIn("бумаг с офертами=2", summary)
        self.assertIn("с пустой датой оферты=1", summary)
        self.assertIn("оферта совпадает с датой погашения=1", summary)


    def test_merge_offer_metrics_does_not_convert_date_without_type_to_put(self) -> None:
        offer_type, offer_date = merge_offer_metrics("✖", "2031-02-24", "✖", None)
        self.assertEqual(offer_type, "✖")
        self.assertIsNone(offer_date)

    def test_parse_dohod_offer_metrics_ignores_ytm_date_without_offer_label(self) -> None:
        html = """
        <html><body>
        <div>Событие в ближ. дату: Погашение</div>
        <div>Дата, к которой рассчитана YTM: 24.02.2031</div>
        </body></html>
        """
        with patch.object(script, "fetch_dohod_offer_page_html", return_value=html):
            offer_type, offer_date = parse_dohod_offer_metrics(session=None, isin="RU000TEST")

        self.assertEqual(offer_type, "✖")
        self.assertIsNone(offer_date)


    def test_merge_offer_metrics_requires_date_for_offer_type(self) -> None:
        offer_type, offer_date = merge_offer_metrics("PUT", None, "✖", None)
        self.assertEqual(offer_type, "✖")
        self.assertIsNone(offer_date)

    def test_parse_corpbonds_offer_metrics_detects_call(self) -> None:
        html = """
        <html><body>
        <div>Наличие call-опциона: Да</div>
        <div>Дата ближайшей оферты: 15.09.2029</div>
        </body></html>
        """
        with patch.object(script, "fetch_corpbonds_offer_page_html", return_value=html):
            offer_type, offer_date, lookup = parse_corpbonds_offer_metrics(session=None, isin="RU000TEST", secid=None)

        self.assertEqual(offer_type, "Call")
        self.assertEqual(offer_date, "2029-09-15")
        self.assertEqual(lookup, "isin")

    def test_load_offer_verification_cache_clears_old_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "offer_verification_cache.json"
            cache_path.write_text(
                '{"schema_version": 5, "rows": {"RU000A": {"HAS_PUT_CALL_OFFER": "PUT"}}}',
                encoding="utf-8",
            )

            with patch.object(script, "OFFER_VERIFICATION_CACHE_FILE", cache_path):
                rows = script.load_offer_verification_cache()

            self.assertEqual(rows, {})
            self.assertFalse(cache_path.exists())

    def test_select_offer_jobs_for_refresh_checks_only_moex_without_offer(self) -> None:
        rows = [
            {"ISIN": "I1", "SECID": "S1"},
            {"ISIN": "I2", "SECID": "S2"},
        ]
        daily_cache = {
            "S1": {"MOEX_HAS_PUT_CALL_OFFER": "PUT", "MOEX_PUT_CALL_OFFER_DATE": "2030-01-01"},
            "S2": {"MOEX_HAS_PUT_CALL_OFFER": "✖", "MOEX_PUT_CALL_OFFER_DATE": ""},
        }
        jobs = select_offer_jobs_for_refresh(rows, offer_cache={}, daily_cache=daily_cache, now=script.datetime.now())

        self.assertEqual(jobs, [("I2", "S2")])



if __name__ == "__main__":
    unittest.main()
