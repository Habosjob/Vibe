from __future__ import annotations

import logging
from datetime import datetime, timezone

from moex_bond_screener.config import AppConfig
from moex_bond_screener.dohod_enrichment import (
    DOHOD_CHECKPOINT_VERSION,
    DohodBondPayload,
    DohodEnricher,
    _should_enrich_coupon,
    _should_enrich_offer,
)


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
    assert enricher.last_stats.coupon_added == 1
    assert enricher.last_stats.offer_added == 1
    assert enricher.last_stats.realprice_added == 1


def test_enrich_bonds_overrides_zero_coupon_and_updates_offer_without_event() -> None:
    config = AppConfig(retries=1, dohod_index_values={"RUONIA": 15.2, "CBR_RATE": 16.0, "Z_CURVE_RUS": 11.0})
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(101.1, "RUONIA", 0.4, None, "", "2027-08-26"), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [
        {
            "SECID": "SU26228RMFS5",
            "ISIN": "RU000A0ZZTL5",
            "COUPONPERCENT": "0",
            "OFFERDATE": "",
            "MATDATE": "2030-01-01",
            "RealPrice": 100.0,
        }
    ]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == 15.6
    assert bonds[0]["OFFERDATE"] == "2027-08-26"
    assert bonds[0]["RealPrice"] == 101.1
    assert enricher.last_stats.coupon_updated == 1
    assert enricher.last_stats.offer_added == 1
    assert enricher.last_stats.realprice_updated == 1


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


def test_enrich_bonds_refetches_when_cached_payload_is_empty() -> None:
    config = AppConfig(retries=1, dohod_index_values={"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0})
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    called = {"count": 0}

    def fake_fetch(identifier: str):
        called["count"] += 1
        assert identifier == "RU1"
        return DohodBondPayload(101.2, "RUONIA", 0.3, None, "", "2028-05-01"), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "ask_price": None,
                "index_name": "",
                "index_spread": 0.0,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            }
        },
    }
    bonds = [{"SECID": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert called["count"] == 1
    assert enricher.last_stats.cache_hits == 0
    assert enricher.last_stats.requested == 1
    assert bonds[0]["COUPONPERCENT"] == 15.3
    assert bonds[0]["OFFERDATE"] == "2028-05-01"


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


def test_should_enrich_coupon_for_zero_and_empty_markers() -> None:
    assert _should_enrich_coupon("", "RUONIA") is True
    assert _should_enrich_coupon("—", "RUONIA") is True
    assert _should_enrich_coupon("0", "RUONIA") is True
    assert _should_enrich_coupon("2.5", "RUONIA") is False
    assert _should_enrich_coupon("", "") is False


def test_should_enrich_offer_without_event_name_if_not_maturity() -> None:
    assert _should_enrich_offer("", "2028-01-01", "2030-01-01", "") is True
    assert _should_enrich_offer("", "2030-01-01", "2030-01-01", "") is False
    assert _should_enrich_offer("", "2028-01-01", "2030-01-01", "погашение") is False
