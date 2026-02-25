from __future__ import annotations

import logging
from datetime import datetime, timezone

from moex_bond_screener.config import AppConfig
from moex_bond_screener.dohod_enrichment import DOHOD_CHECKPOINT_VERSION, DohodBondPayload, DohodEnricher


class _DummyResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_parse_bond_payload_parses_prices_index_and_event() -> None:
    html = """
    <tr><td>Цена (last/bid/ask)</td><td>92.99 / 92.78 / 92.99</td></tr>
    <tr><td>Привязка к индексу</td><td>Z_CURVE_RUS + 0.7</td></tr>
    <tr><td>Описание формулы изменяемого купона/номинала</td><td>Купон определяется как значение кривой бескупонной доходности ОФЗ сроком погашения 7 лет + 0.70%.</td></tr>
    <tr><td>Событие в ближ. дату</td><td>право продать (put)</td></tr>
    <tr><td>Дата, к которой рассчит. YTM</td><td>26.08.2027</td></tr>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price == 92.99
    assert payload.index_name == "Z_CURVE_RUS"
    assert payload.index_spread == 0.7
    assert payload.index_tenor_years == 7
    assert payload.event_name == "право продать (put)"
    assert payload.ytm_date == "2027-08-26"


def test_enrich_bonds_fills_realprice_coupon_and_offerdate() -> None:
    config = AppConfig(retries=1, dohod_index_values={"RUONIA": 13.5, "CBR_RATE": 16.0, "Z_CURVE_RUS": 11.0, "Z_CURVE_RUS_7Y": 12.3})
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(92.99, "Z_CURVE_RUS", 0.7, 7, "право продать (put)", "2027-08-26"), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [{"SECID": "SU26228RMFS5", "ISIN": "RU000A0ZZTL5", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert bonds[0]["RealPrice"] == 92.99
    assert bonds[0]["COUPONPERCENT"] == 13.0
    assert bonds[0]["_COUPONPERCENT_APPROX"] is True
    assert bonds[0]["OFFERDATE"] == "2027-08-26"


def test_enrich_bonds_uses_fresh_checkpoint_without_requests() -> None:
    config = AppConfig(retries=1, dohod_index_values={"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0})
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    called = {"count": 0}

    def fail_fetch(_: str):
        called["count"] += 1
        return DohodBondPayload(None, "", 0.0, None, "", ""), 1

    enricher._fetch_and_parse = fail_fetch  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "ask_price": 100.5,
                "index_name": "",
                "index_spread": 0.0,
                "index_tenor_years": None,
                "event_name": "погашение",
                "ytm_date": "2030-01-01",
            }
        },
    }
    bonds = [{"SECID": "RU1", "COUPONPERCENT": "5.0", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert called["count"] == 0
    assert bonds[0]["RealPrice"] == 100.5


def test_enrich_bonds_falls_back_to_secid_when_isin_missing() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    called: list[str] = []

    def fake_fetch(identifier: str):
        called.append(identifier)
        return DohodBondPayload(101.0, "", 0.0, None, "", ""), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [{"SECID": "SU26228RMFS5", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert called == ["SU26228RMFS5"]
    assert bonds[0]["RealPrice"] == 101.0
