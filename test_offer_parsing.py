import unittest

from moex_bonds_to_excel import normalize_coupon_formula_source, normalize_offer_type, parse_offer_metrics, validate_rows


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
            {"ISIN": "A", "SECID": "A", "HAS_PUT_CALL_OFFER": "PUT", "PUT_CALL_OFFER_DATE": "", "MATDATE": "2099-01-01", "COUPONPERCENT": 10},
            {"ISIN": "B", "SECID": "B", "HAS_PUT_CALL_OFFER": "Call", "PUT_CALL_OFFER_DATE": "2099-02-01", "MATDATE": "2099-02-01", "COUPONPERCENT": 9},
            {"ISIN": "C", "SECID": "C", "HAS_PUT_CALL_OFFER": "✖", "PUT_CALL_OFFER_DATE": "", "MATDATE": "2099-03-01", "COUPONPERCENT": 8},
        ]

        with self.assertLogs(level="INFO") as logs:
            validate_rows(rows)

        summary = "\n".join(logs.output)
        self.assertIn("бумаг с офертами=2", summary)
        self.assertIn("с пустой датой оферты=1", summary)
        self.assertIn("оферта совпадает с датой погашения=1", summary)


if __name__ == "__main__":
    unittest.main()
