from __future__ import annotations

import logging
from datetime import datetime, timezone

from moex_bond_screener.config import AppConfig
from moex_bond_screener.dohod_enrichment import (
    DOHOD_CHECKPOINT_VERSION,
    DohodBondPayload,
    DohodEnricher,
    _is_payload_empty,
    _should_enrich_coupon,
    _should_enrich_offer,
    _to_iso_date,
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


def test_parse_bond_payload_parses_nested_table_markup() -> None:
    html = """
    <table>
      <tr><th><span>Цена</span> (last/bid/ask)</th><td><div>95.10 / 94.90 / 95.25</div></td></tr>
      <tr><th><div>Привязка к индексу</div></th><td><span>RUONIA + 1,25</span></td></tr>
      <tr><th>Описание формулы изменяемого купона/номинала</th><td><p>Купон = RUONIA + 1,25%</p></td></tr>
      <tr><th><span>Событие в ближ. дату</span></th><td><div>Оферта</div></td></tr>
      <tr><th>Дата, к которой рассчит. YTM</th><td><span>15.11.2028</span></td></tr>
    </table>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price == 95.25
    assert payload.index_name == "RUONIA"
    assert payload.index_spread == 1.25
    assert payload.event_name == "оферта"
    assert payload.ytm_date == "2028-11-15"






def test_parse_bond_payload_parses_definition_list_markup() -> None:
    html = """
    <dl class="bond-props">
      <dt>Цена (last/bid/ask)</dt><dd>100.10 / 100.00 / 100.35</dd>
      <dt>Привязка к индексу</dt><dd>RUONIA + 0,45</dd>
      <dt>Описание формулы изменяемого купона/номинала</dt><dd>Купон = RUONIA + 0,45%</dd>
      <dt>Событие в ближ. дату</dt><dd>Оферта</dd>
      <dt>Дата, к которой рассчит. YTM</dt><dd>11.03.2030</dd>
    </dl>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price == 100.35
    assert payload.index_name == "RUONIA"
    assert payload.index_spread == 0.45
    assert payload.event_name == "оферта"
    assert payload.ytm_date == "2030-03-11"

def test_parse_bond_payload_parses_fuzzy_labels() -> None:
    html = """
    <table>
      <tr><th>Цена (last / bid / ask):</th><td>101,10 / 100,90</td></tr>
      <tr><th>Привязка к индексу:</th><td>CBR_RATE + 0,75</td></tr>
      <tr><th>Описание формулы изменяемого купона/номинала:</th><td>Купон = CBR_RATE + 0,75%</td></tr>
      <tr><th>Событие в ближ дату:</th><td>Оферта</td></tr>
      <tr><th>Дата к которой рассчит YTM:</th><td>05.09.2029</td></tr>
    </table>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price == 100.9
    assert payload.index_name == "CBR_RATE"
    assert payload.index_spread == 0.75
    assert payload.event_name == "оферта"
    assert payload.ytm_date == "2029-09-05"



def test_parse_bond_payload_parses_script_values_when_labels_absent() -> None:
    html = """
    <script>
      window.__BOND__ = {"ask":"101.77","ytmDate":"2031-12-01","event":"Оферта"};
    </script>
    <div>Формула купона: RUONIA + 0,55</div>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price == 101.77
    assert payload.index_name == "RUONIA"
    assert payload.index_spread == 0.55
    assert payload.event_name == "оферта"
    assert payload.ytm_date == "2031-12-01"


def test_fetch_and_parse_saves_empty_payload_html_even_when_raw_dump_disabled(tmp_path) -> None:
    config = AppConfig(retries=1, raw_dump_enabled=False)
    from moex_bond_screener.raw_store import RawStore

    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"), raw_store=RawStore(str(tmp_path)))

    html = "<html><body><h1>stub</h1></body></html>"

    def fake_get_with_rate_limit(url: str, timeout: int, delay_seconds: float):
        _ = (url, timeout, delay_seconds)
        return _DummyResponse(html)

    enricher._get_with_rate_limit = fake_get_with_rate_limit  # type: ignore[method-assign]

    payload, errors = enricher._fetch_and_parse("RU000A10A7D2")

    assert errors == 0
    assert payload.ask_price is None
    saved = tmp_path / "dohod_empty_RU000A10A7D2.html"
    assert saved.exists()
    assert saved.read_text(encoding="utf-8") == html


def test_to_iso_date_accepts_ddmmyyyy_and_iso() -> None:
    assert _to_iso_date("11.03.2030") == "2030-03-11"
    assert _to_iso_date("2030-03-11") == "2030-03-11"

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
    bonds = [{"SECID": "RU1_SEC", "ISIN": "RU1", "COUPONPERCENT": "5.0", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

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
    bonds = [{"SECID": "RU1_SEC", "ISIN": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert called["count"] == 1
    assert enricher.last_stats.cache_hits == 0
    assert enricher.last_stats.requested == 1
    assert bonds[0]["COUPONPERCENT"] == 15.3
    assert bonds[0]["OFFERDATE"] == "2028-05-01"


def test_enrich_bonds_skips_record_without_isin() -> None:
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
    assert called == []
    assert enricher.last_stats.bonds_total == 0


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



def test_is_payload_empty_helper_for_backward_compatibility() -> None:
    assert _is_payload_empty(DohodBondPayload(None, "", 0.0, None, "", "")) is True
    assert _is_payload_empty(DohodBondPayload(99.9, "", 0.0, None, "", "")) is False



def test_compat_methods_for_mixed_versions_do_not_fail() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(100.0, "", 0.0, None, "", ""), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    payload, errors = enricher._fetch_with_fallback("RU000A0ZZTL5", "SU26228RMFS5")

    assert errors == 0
    assert payload.ask_price == 100.0
    assert enricher._resolve_secondary_identifier({"ISIN": "RU1", "SECID": "SU1"}, "RU1") == "SU1"


def test_fetch_with_fallback_uses_secid_when_isin_payload_empty() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    calls: list[str] = []

    def fake_fetch(identifier: str):
        calls.append(identifier)
        if identifier == "RU000A0JTYK4":
            return DohodBondPayload(None, "", 0.0, None, "", ""), 0
        assert identifier == "BOND500"
        return DohodBondPayload(99.8, "", 0.0, None, "", ""), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    payload, errors = enricher._fetch_with_fallback("RU000A0JTYK4", "BOND500")

    assert errors == 0
    assert payload.ask_price == 99.8
    assert calls == ["RU000A0JTYK4", "BOND500"]






def test_enrich_bonds_counts_empty_parsed_payload_as_error() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(None, "", 0.0, None, "", ""), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [{"ISIN": "RU000A0ZZTL5", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 1
    assert enricher.last_stats.parse_empty_payloads == 1
    assert "RealPrice" not in bonds[0]
