import unittest

from moex_bonds_to_excel import normalize_offer_type, parse_offer_metrics


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

    def test_parse_offer_metrics_returns_call_type(self) -> None:
        columns = ["offertype", "offerdate", "offerdatestart", "offerdateend"]
        offers_data = [["Call-оферта", "2099-07-15", None, None]]

        offer_type, offer_date = parse_offer_metrics(offers_data, columns)

        self.assertEqual(offer_type, "Call")
        self.assertEqual(offer_date, "2099-07-15")


if __name__ == "__main__":
    unittest.main()
