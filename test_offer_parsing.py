import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import moex_bonds_to_excel as script
from moex_bonds_to_excel import (
    calculate_coupon_value,
    calculate_total_price,
    enrich_total_price,
    has_non_zero_value,
    enrich_coupon_value_from_percent,
    calculate_cashflow_yield,
    enrich_calculated_yield,
    enrich_with_daily_metrics,
    fetch_emitter_info_for_security,
    format_issuer_rating_from_cci_rows,
    merge_offer_metrics,
    normalize_issuer_rating,
    normalize_coupon_formula_source,
    normalize_offer_type,
    parse_corpbonds_offer_metrics,
    parse_dohod_offer_metrics,
    extract_issuer_rating_from_description,
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

    def test_calculate_total_price(self) -> None:
        result = calculate_total_price(face_value=1000, prev_price=97.5, accrued_int=12.34)
        self.assertAlmostEqual(result, 987.83367, places=6)

    def test_has_non_zero_value(self) -> None:
        self.assertTrue(has_non_zero_value("101.25"))
        self.assertTrue(has_non_zero_value(" 0,10 "))
        self.assertFalse(has_non_zero_value(""))
        self.assertFalse(has_non_zero_value("0"))

    def test_enrich_total_price_adds_column_only_when_conditions_met(self) -> None:
        rows = [
            {
                "ISIN": "RU000A",
                "SECID": "S1",
                "FACEVALUE": "1000",
                "PREVPRICE": "95.4",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "SUR",
                "VOLTODAY": "250",
            },
            {
                "ISIN": "RU000B",
                "SECID": "S2",
                "FACEVALUE": "1000",
                "PREVPRICE": "95.4",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "SUR",
                "VOLTODAY": "0",
            },
            {
                "ISIN": "RU000C",
                "SECID": "S3",
                "FACEVALUE": "1000",
                "PREVPRICE": "0",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "SUR",
                "VOLTODAY": "250",
            },
            {
                "ISIN": "RU000D",
                "SECID": "S4",
                "FACEVALUE": "1000",
                "PREVPRICE": "95.4",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "USD",
                "VOLTODAY": "250",
            },
        ]
        with patch.object(script, "load_total_price_cache", return_value={}),              patch.object(script, "save_total_price_cache") as save_cache_mock:
            result = enrich_total_price(rows)

        self.assertAlmostEqual(result[0]["TOTAL_PRICE"], 965.732625, places=6)
        self.assertEqual(result[1]["TOTAL_PRICE"], "")
        self.assertEqual(result[2]["TOTAL_PRICE"], "")
        self.assertEqual(result[3]["TOTAL_PRICE"], "")
        saved_payload = save_cache_mock.call_args.args[0]
        self.assertIn("RU000A", saved_payload)
        self.assertNotIn("RU000B", saved_payload)
        self.assertNotIn("RU000D", saved_payload)

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

    def test_extract_issuer_rating_from_description_by_title(self) -> None:
        description_rows = [
            ["SECID", "Код бумаги", "RU000A"],
            ["ISSUER_RATING", "Кредитный рейтинг эмитента", "AA(RU)"],
        ]

        rating = extract_issuer_rating_from_description(description_rows)

        self.assertEqual(rating, "AA(RU)")

    def test_extract_issuer_rating_from_description_returns_empty_when_no_rating(self) -> None:
        description_rows = [
            ["SECID", "Код бумаги", "RU000A"],
            ["SHORTNAME", "Краткое название", "Тест БО"],
        ]

        rating = extract_issuer_rating_from_description(description_rows)

        self.assertEqual(rating, "")

    def test_normalize_issuer_rating_sets_fallback_when_empty(self) -> None:
        self.assertEqual(normalize_issuer_rating(""), "Нет данных на MOEX")
        self.assertEqual(normalize_issuer_rating("  A+(RU)  "), "A+(RU)")

    def test_format_issuer_rating_from_cci_rows_collects_latest_per_agency(self) -> None:
        rows = [
            {"agency_name_short_ru": "АКРА", "rating_level_name_short_ru": "AA(RU)", "rating_date": "2024-01-01 00:00:00"},
            {"agency_name_short_ru": "АКРА", "rating_level_name_short_ru": "AAA(RU)", "rating_date": "2025-01-01 00:00:00"},
            {"agency_name_short_ru": "Эксперт РА", "rating_level_name_short_ru": "ruAAA", "rating_date": "2025-02-01 00:00:00"},
        ]

        rating = format_issuer_rating_from_cci_rows(rows)

        self.assertEqual(rating, "АКРА: AAA(RU); Эксперт РА: ruAAA")

    def test_fetch_emitter_info_for_security_uses_cci_fallback(self) -> None:
        description_payload = {
            "description": {
                "data": [
                    ["EMITTER_ID", "ID эмитента", 712],
                    ["ISQUALIFIEDINVESTORS", "Только для квалов", 0],
                    ["BOND_TYPE", "Тип облигации", "Биржевая"],
                    ["COUPONFREQUENCY", "Частота купона", 4],
                ]
            }
        }

        class DummyResponse:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        class DummySession:
            def get(self, *args, **kwargs):
                return DummyResponse(description_payload)

        with patch.object(script, "fetch_issuer_rating_from_moex_cci", return_value="АКРА: AAA(RU)"):
            emitter_id, qualified, bond_type, coupon_period, is_structural, rating = fetch_emitter_info_for_security(
                DummySession(),
                "RU000A0JTU85",
                isin="RU000A0JTU85",
            )

        self.assertEqual(emitter_id, 712)
        self.assertEqual(qualified, "✖")
        self.assertEqual(coupon_period, 91)
        self.assertFalse(is_structural)
        self.assertEqual(rating, "АКРА: AAA(RU)")

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



    def test_parse_corpbonds_offer_metrics_prefers_nearest_date_label(self) -> None:
        html = """
        <html><body>
        <div>Ближайшая дата</div>
        <div>28.09.2027</div>
        <div>Дата ближайшей оферты</div>
        <div>24.09.2027</div>
        <div>put-оферта</div>
        </body></html>
        """
        with patch.object(script, "fetch_corpbonds_offer_page_html", return_value=html):
            offer_type, offer_date, lookup = parse_corpbonds_offer_metrics(session=None, isin="RU000TEST", secid=None)

        self.assertEqual(offer_type, "PUT")
        self.assertEqual(offer_date, "2027-09-28")
        self.assertEqual(lookup, "isin")

    def test_parse_corpbonds_offer_metrics_finds_date_in_nearby_lines(self) -> None:
        html = """
        <html><body>
        <div>Дата ближайшей оферты</div>
        <div>служебная строка</div>
        <div>ещё строка</div>
        <div>28.09.2027</div>
        <div>put-оферта</div>
        </body></html>
        """
        with patch.object(script, "fetch_corpbonds_offer_page_html", return_value=html):
            offer_type, offer_date, lookup = parse_corpbonds_offer_metrics(session=None, isin="RU000TEST", secid=None)

        self.assertEqual(offer_type, "PUT")
        self.assertEqual(offer_date, "2027-09-28")
        self.assertEqual(lookup, "isin")

    def test_load_offer_verification_cache_clears_old_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "offer_verification_cache.json"
            cache_path.write_text(
                '{"schema_version": 7, "rows": {"RU000A": {"HAS_PUT_CALL_OFFER": "PUT"}}}',
                encoding="utf-8",
            )

            with patch.object(script, "OFFER_VERIFICATION_CACHE_FILE", cache_path):
                rows = script.load_offer_verification_cache()

            self.assertEqual(rows, {})
            self.assertFalse(cache_path.exists())


    def test_enrich_with_daily_metrics_no_external_offer_does_not_fail(self) -> None:
        rows = [{"SECID": "S1", "ISIN": "I1", "MATDATE": "2032-01-01"}]
        daily_cache = {
            "S1": {
                "MOEX_HAS_PUT_CALL_OFFER": "PUT",
                "MOEX_PUT_CALL_OFFER_DATE": "2031-06-01",
                "HAS_AMORTIZATION": "✖",
                "AMORTIZATION_START_DATE": "",
            }
        }

        with patch.object(script, "load_daily_security_cache", return_value=(script.DAILY_METRICS_CACHE_SCHEMA_VERSION, daily_cache)), \
             patch.object(script, "load_offer_verification_cache", return_value={}), \
             patch.object(script, "save_daily_security_cache"):
            result = enrich_with_daily_metrics(rows, include_daily_metrics=False, include_external_offers=False)

        self.assertEqual(result[0]["HAS_PUT_CALL_OFFER"], "✔")
        self.assertEqual(result[0]["PUT_CALL_OFFER_DATE"], "2031-06-01")

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

    def test_calculate_coupon_value_formula(self) -> None:
        result = calculate_coupon_value(face_value=1000, coupon_percent=12.5, coupon_period=91)
        self.assertAlmostEqual(result, (1000 * 12.5 / 100 / 365) * 91, places=10)


    def test_enrich_total_price_uses_cache_for_same_inputs(self) -> None:
        rows = [
            {
                "ISIN": "RU000A",
                "SECID": "S1",
                "FACEVALUE": "1000",
                "PREVPRICE": "95.4",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "SUR",
                "VOLTODAY": "250",
            }
        ]
        cached_rows = {
            "RU000A": {
                "secid": "S1",
                "faceunit": "SUR",
                "face_value": 1000.0,
                "prev_price": 95.4,
                "accrued_int": 11.25,
                "prev_legal_close_price": 95.5,
                "volume": 250.0,
                "total_price": 777.123456,
                "updated_at": "2026-01-01T00:00:00",
            }
        }

        with patch.object(script, "load_total_price_cache", return_value=cached_rows),              patch.object(script, "save_total_price_cache"):
            result = enrich_total_price(rows)

        self.assertAlmostEqual(result[0]["TOTAL_PRICE"], 777.123456, places=6)


    def test_enrich_total_price_drops_cache_when_faceunit_not_sur(self) -> None:
        rows = [
            {
                "ISIN": "RU000A",
                "SECID": "S1",
                "FACEVALUE": "1000",
                "PREVPRICE": "95.4",
                "ACCRUEDINT": "11.25",
                "PREVLEGALCLOSEPRICE": "95.5",
                "FACEUNIT": "USD",
                "VOLTODAY": "250",
            }
        ]
        cached_rows = {
            "RU000A": {
                "secid": "S1",
                "faceunit": "SUR",
                "face_value": 1000.0,
                "prev_price": 95.4,
                "accrued_int": 11.25,
                "prev_legal_close_price": 95.5,
                "volume": 250.0,
                "total_price": 777.123456,
                "updated_at": "2026-01-01T00:00:00",
            }
        }

        with patch.object(script, "load_total_price_cache", return_value=cached_rows),              patch.object(script, "save_total_price_cache") as save_cache_mock:
            result = enrich_total_price(rows)

        self.assertEqual(result[0]["TOTAL_PRICE"], "")
        saved_payload = save_cache_mock.call_args.args[0]
        self.assertNotIn("RU000A", saved_payload)

    def test_load_total_price_cache_clears_old_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "total_price_cache.json"
            cache_path.write_text(
                '{"schema_version": 1, "rows": {"RU000A": {"total_price": 1}}}',
                encoding="utf-8",
            )

            with patch.object(script, "TOTAL_PRICE_CACHE_FILE", cache_path):
                rows = script.load_total_price_cache()

            self.assertEqual(rows, {})
            self.assertFalse(cache_path.exists())

    def test_enrich_coupon_value_from_percent_uses_cache_when_inputs_same(self) -> None:
        rows = [
            {
                "ISIN": "RU000A",
                "SECID": "S1",
                "COUPONVALUE": 0,
                "COUPONPERCENT": 10,
                "FACEVALUE": 1000,
                "COUPONPERIOD": 91,
            }
        ]
        cached_rows = {
            "RU000A": {
                "secid": "S1",
                "coupon_value": 24.931507,
                "coupon_percent": 10.0,
                "coupon_period": 91,
                "face_value": 1000.0,
                "updated_at": "2026-01-01T00:00:00",
            }
        }

        with patch.object(script, "load_coupon_value_cache", return_value=cached_rows), \
             patch.object(script, "save_coupon_value_cache") as save_cache_mock:
            result_rows, calculated_cells = enrich_coupon_value_from_percent(rows)

        self.assertAlmostEqual(result_rows[0]["COUPONVALUE"], 24.931507, places=6)
        self.assertEqual(calculated_cells, {("RU000A", "S1")})
        save_cache_mock.assert_called_once()

    def test_enrich_coupon_value_from_percent_recalculates_when_coupon_percent_changed(self) -> None:
        rows = [
            {
                "ISIN": "RU000A",
                "SECID": "S1",
                "COUPONVALUE": 0,
                "COUPONPERCENT": 12,
                "FACEVALUE": 1000,
                "COUPONPERIOD": 91,
            }
        ]
        cached_rows = {
            "RU000A": {
                "secid": "S1",
                "coupon_value": 24.931507,
                "coupon_percent": 10.0,
                "coupon_period": 91,
                "face_value": 1000.0,
                "updated_at": "2026-01-01T00:00:00",
            }
        }

        with patch.object(script, "load_coupon_value_cache", return_value=cached_rows), \
             patch.object(script, "save_coupon_value_cache") as save_cache_mock:
            result_rows, _ = enrich_coupon_value_from_percent(rows)

        self.assertAlmostEqual(result_rows[0]["COUPONVALUE"], round((1000 * 12 / 100 / 365) * 91, 6), places=6)
        saved_payload = save_cache_mock.call_args.args[0]
        self.assertEqual(saved_payload["RU000A"]["coupon_percent"], 12.0)



    def test_calculate_cashflow_yield_uses_cashflows_without_reinvestment(self) -> None:
        today = script.datetime.now()
        cashflows = [
            (today + script.timedelta(days=365), 60.0),
            (today + script.timedelta(days=730), 1060.0),
        ]

        ytm = calculate_cashflow_yield(total_price=950.0, cashflows=cashflows)

        self.assertIsNotNone(ytm)
        self.assertGreater(ytm, 8.0)
        self.assertLess(ytm, 9.0)

    def test_enrich_calculated_yield_uses_offer_date_and_total_price(self) -> None:
        rows = [
            {
                "SECID": "S1",
                "ISIN": "RU000A",
                "TOTAL_PRICE": 950.0,
                "FACEVALUE": 1000.0,
                "MATDATE": "2030-01-01",
                "PUT_CALL_OFFER_DATE": "2027-01-01",
            }
        ]

        offer_date = script.parse_date_safe("2027-01-01")
        assert offer_date is not None

        with patch.object(script, "fetch_bond_cashflows_until_date", return_value=([(offer_date, 80.0)], [])), \
             patch.object(script, "build_session") as build_session_mock:
            build_session_mock.return_value.close = lambda: None
            result = enrich_calculated_yield(rows)

        self.assertIn("YIELD", result[0])
        self.assertGreater(result[0]["YIELD"], 0)


if __name__ == "__main__":
    unittest.main()
