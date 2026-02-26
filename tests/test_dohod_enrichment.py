from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from moex_bond_screener.config import AppConfig
from moex_bond_screener.dohod_enrichment import (
    DOHOD_CHECKPOINT_VERSION,
    DohodBondPayload,
    DohodEnricher,
    _extract_ytm_date_from_html,
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


class _DummyJsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.text = json.dumps(payload)

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


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


def test_parse_bond_payload_parses_russian_index_aliases() -> None:
    html = """
    <table>
      <tr><th>Цена (last / bid / ask):</th><td>101,10 / 100,90</td></tr>
      <tr><th>Привязка к индексу:</th><td>КЛЮЧЕВАЯ СТАВКА ЦБ + 1,50</td></tr>
      <tr><th>Описание формулы изменяемого купона/номинала:</th><td>Купон = КБД ОФЗ + 0,75%</td></tr>
      <tr><th>Событие в ближ дату:</th><td>Оферта</td></tr>
      <tr><th>Дата к которой рассчит YTM:</th><td>05.09.2029</td></tr>
    </table>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.index_name == "CBR_RATE"
    assert payload.index_spread == 1.5







def test_parse_bond_payload_drops_zero_realprice_artifacts() -> None:
    html = """
    <tr><td>Цена (last/bid/ask)</td><td>0,00 / 0,00 / 0,00</td></tr>
    <tr><td>Дата, к которой рассчит. YTM</td><td>26.08.2027</td></tr>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.ask_price is None


def test_cached_payload_with_zero_ask_price_is_refetched() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    calls = {"count": 0}

    def fake_fetch(identifier: str):
        calls["count"] += 1
        assert identifier == "RU1"
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=101.2), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "real_price": 0.0,
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
    assert calls["count"] == 1
    assert bonds[0]["RealPrice"] == 101.2

def test_parse_bond_payload_parses_hyphenated_and_russian_alias_indexes() -> None:
    html = """
    <table>
      <tr><th>Привязка к индексу</th><td>Z-CURVE-RUS + 1,10</td></tr>
      <tr><th>Описание формулы изменяемого купона/номинала</th><td>Купон = Z-CURVE-RUS + 1,10%</td></tr>
    </table>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.index_name == "Z_CURVE_RUS"
    assert payload.index_spread == 1.1


def test_resolve_index_values_normalizes_hyphenated_keys() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {}  # type: ignore[method-assign]

    checkpoint = {"index_values": {"z-curve-rus": "14,20", "key-rate": "16,00", "r-uonia": "15,40"}}
    index_values = enricher._resolve_index_values(checkpoint)

    assert index_values["Z_CURVE_RUS"] == 14.2
    assert index_values["CBR_RATE"] == 16.0
    assert index_values["RUONIA"] == 15.4


def test_parse_bond_payload_extracts_index_base_rate_from_text() -> None:
    html = """
    <table>
      <tr><th>Привязка к индексу</th><td>RUONIA + 1,25</td></tr>
      <tr><th>Описание формулы изменяемого купона/номинала</th><td>Купон = RUONIA + 1,25%</td></tr>
    </table>
    <div>Значение RUONIA на дату расчёта составляет 15,40%</div>
    """

    payload = DohodEnricher.parse_bond_payload(html)

    assert payload.index_name == "RUONIA"
    assert payload.index_spread == 1.25


def test_enrich_bonds_uses_live_base_rate_when_formula_present() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.4, "CBR_RATE": 16.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.4, "CBR_RATE": 16.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU_BASE": {
                "real_price": 100.0,
                "index_name": "RUONIA",
                "index_spread": 1.25,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            }
        },
    }
    bonds = [{"ISIN": "RU_BASE", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2033-10-12"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == 16.65
    assert enricher.last_stats.coupon_skipped_no_base == 0

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
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 13.5, "CBR_RATE": 16.0, "Z_CURVE_RUS": 11.0, "Z_CURVE_RUS_7Y": 12.3}  # type: ignore[method-assign]

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(index_name="Z_CURVE_RUS", index_spread=0.7, index_tenor_years=7, event_name="право продать (put)", ytm_date="2027-08-26", real_price=92.99), 0

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
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.2, "CBR_RATE": 16.0, "Z_CURVE_RUS": 11.0}  # type: ignore[method-assign]

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(index_name="RUONIA", index_spread=0.4, index_tenor_years=None, event_name="", ytm_date="2027-08-26", real_price=101.1), 0

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
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    called = {"count": 0}

    def fail_fetch(_: str):
        called["count"] += 1
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=None), 1

    enricher._fetch_and_parse = fail_fetch  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "real_price": 100.5,
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
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    called = {"count": 0}

    def fake_fetch(identifier: str):
        called["count"] += 1
        assert identifier == "RU1"
        return DohodBondPayload(index_name="RUONIA", index_spread=0.3, index_tenor_years=None, event_name="", ytm_date="2028-05-01", real_price=101.2), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "real_price": None,
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


def test_enrich_bonds_recalculates_legacy_spread_only_coupon_from_cache() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 16.0, "CBR_RATE": 15.5, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 16.0, "CBR_RATE": 15.5, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU000A105KW6": {
                "real_price": 101.0,
                "index_name": "CBR_RATE",
                "index_spread": 1.5,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            }
        },
    }
    bonds = [
        {
            "ISIN": "RU000A105KW6",
            "COUPONPERCENT": "1.5",  # legacy: только спред без базовой ставки
            "OFFERDATE": "",
            "MATDATE": "2033-10-12",
        }
    ]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == 17.0
    assert bonds[0]["_COUPONPERCENT_APPROX"] is True


def test_enrich_bonds_skips_coupon_enrichment_when_index_base_missing() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 16.0, "CBR_RATE": 0.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 16.0, "CBR_RATE": 0.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU000A105KW6": {
                "real_price": 101.0,
                "index_name": "CBR_RATE",
                "index_spread": 1.5,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            }
        },
    }
    bonds = [{"ISIN": "RU000A105KW6", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2033-10-12"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == ""
    assert "_COUPONPERCENT_APPROX" not in bonds[0]
    assert enricher.last_stats.coupon_skipped_no_base == 1


def test_enrich_bonds_aggregates_repeated_missing_base_warnings(caplog) -> None:
    caplog.set_level(logging.WARNING)
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 16.0, "CBR_RATE": 0.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 16.0, "CBR_RATE": 0.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "real_price": 101.0,
                "index_name": "CBR_RATE",
                "index_spread": 1.5,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            },
            "RU2": {
                "real_price": 102.0,
                "index_name": "CBR_RATE",
                "index_spread": 1.5,
                "index_tenor_years": None,
                "event_name": "",
                "ytm_date": "",
            },
        },
    }
    bonds = [
        {"ISIN": "RU1", "SECID": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2033-10-12"},
        {"ISIN": "RU2", "SECID": "RU2", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2033-10-12"},
    ]

    errors = enricher.enrich_bonds(bonds, checkpoint_data=checkpoint)

    assert errors == 0
    assert enricher.last_stats.coupon_skipped_no_base == 2
    missing_base_logs = [r for r in caplog.records if "Пропуск расчета COUPONPERCENT" in r.message]
    assert len(missing_base_logs) == 1
    summary_logs = [r for r in caplog.records if "COUPONPERCENT пропущен из-за отсутствия базовой ставки" in r.message]
    assert len(summary_logs) == 1
    assert "всего=2" in summary_logs[0].message
    assert "CBR_RATE@1.5000: 2" in summary_logs[0].message


def test_enrich_bonds_skips_record_without_isin() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    called: list[str] = []

    def fake_fetch(identifier: str):
        called.append(identifier)
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=101.0), 0

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


def test_should_enrich_coupon_when_legacy_value_equals_spread_only() -> None:
    assert _should_enrich_coupon("1.5", "CBR_RATE", base_rate=15.5, index_spread=1.5) is True
    assert _should_enrich_coupon("17.0", "CBR_RATE", base_rate=15.5, index_spread=1.5) is False


def test_should_enrich_offer_without_event_name_if_not_maturity() -> None:
    assert _should_enrich_offer("", "2028-01-01", "2030-01-01", "") is True
    assert _should_enrich_offer("", "2030-01-01", "2030-01-01", "") is False
    assert _should_enrich_offer("", "2028-01-01", "2030-01-01", "погашение") is False
    assert _should_enrich_offer("", "2020-01-01", "2030-01-01", "") is False


def test_extract_ytm_date_from_html_ignores_unrelated_dates() -> None:
    html = """
    <div>Дата размещения: 09.06.2014</div>
    <div>Дюрация: 2.3</div>
    """

    assert _extract_ytm_date_from_html(html) == ""


def test_fetch_cbr_metric_parses_json_value(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    monkeypatch.setattr(
        enricher.session,
        "get",
        lambda *args, **kwargs: _DummyJsonResponse({"date": "2026-02-25", "value": 15.5}),
    )

    value = enricher._fetch_cbr_metric("https://cbr.example/key-rate", metric_name="CBR_RATE")

    assert value == 15.5


def test_fetch_cbr_metric_parses_keyrate_html_table(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    html = """
    <table class="data">
      <tr><th>Дата</th><th>Ставка</th></tr>
      <tr><td>26.02.2026</td><td>15,50</td></tr>
      <tr><td>25.02.2026</td><td>15,25</td></tr>
    </table>
    """

    monkeypatch.setattr(enricher.session, "get", lambda *args, **kwargs: _DummyResponse(html))

    value = enricher._fetch_cbr_metric("https://cbr.example/KeyRate", metric_name="CBR_RATE")

    assert value == 15.5


def test_fetch_cbr_metric_parses_ruonia_latest_column(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    html = """
    <table class="data without_header">
      <tr><td>Дата ставки</td><td>20.02.2026</td><td>24.02.2026</td></tr>
      <tr><td>Ставка RUONIA, % годовых</td><td>15,30</td><td>15,31</td></tr>
    </table>
    """
    monkeypatch.setattr(enricher.session, "get", lambda *args, **kwargs: _DummyResponse(html))

    value = enricher._fetch_cbr_metric("https://cbr.example/ruonia", metric_name="RUONIA")

    assert value == 15.31


def test_resolve_index_values_accepts_comma_and_russian_aliases() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {}  # type: ignore[method-assign]

    index_values = enricher._resolve_index_values(
        {"index_values": {"RUONIA": "16,15", "Ключевая ставка ЦБ": "15,00", "КБД ОФЗ": "14,35", "Z_CURVE_RUS_7Y": "13,25"}}
    )

    assert index_values["RUONIA"] == 16.15
    assert index_values["CBR_RATE"] == 15.0
    assert index_values["Z_CURVE_RUS"] == 14.35
    assert index_values["Z_CURVE_RUS_7Y"] == 13.25


def test_fetch_moex_z_curve_values_builds_tenor_map(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    payload = {
        "securities": {
            "columns": ["term", "value"],
            "data": [[1, 13.1], [7, 12.4]],
        }
    }
    monkeypatch.setattr(enricher.session, "get", lambda *args, **kwargs: _DummyJsonResponse(payload))

    values = enricher._fetch_moex_z_curve_values()

    assert values["Z_CURVE_RUS_1Y"] == 13.1
    assert values["Z_CURVE_RUS_7Y"] == 12.4
    assert values["Z_CURVE_RUS"] == 13.1


def test_fetch_cbr_z_curve_values_parses_latest_row(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    html = """
    <table class="data spaced">
      <tr><th rowspan="2">Дата</th><th colspan="12">Срок до погашения, лет</th></tr>
      <tr><th>0,25</th><th>0,5</th><th>0,75</th><th>1</th><th>2</th><th>3</th><th>5</th><th>7</th><th>10</th><th>15</th><th>20</th><th>30</th></tr>
      <tr><td>25.02.2026</td><td>13,77</td><td>13,95</td><td>14,08</td><td>14,18</td><td>14,44</td><td>14,62</td><td>14,70</td><td>14,60</td><td>14,34</td><td>13,99</td><td>13,79</td><td>13,62</td></tr>
    </table>
    """
    monkeypatch.setattr(enricher.session, "get", lambda *args, **kwargs: _DummyResponse(html))

    values = enricher._fetch_cbr_z_curve_values()

    assert values["Z_CURVE_RUS_1Y"] == 14.18
    assert values["Z_CURVE_RUS_7Y"] == 14.6
    assert values["Z_CURVE_RUS_30Y"] == 13.62
    assert values["Z_CURVE_RUS"] == 14.18


def test_fetch_live_index_values_falls_back_to_moex_zcurve(monkeypatch) -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_cbr_metric(url: str, metric_name: str):
        if metric_name == "CBR_RATE":
            return 15.5
        if metric_name == "RUONIA":
            return 15.31
        return None

    enricher._fetch_cbr_metric = fake_cbr_metric  # type: ignore[method-assign]
    enricher._fetch_cbr_z_curve_values = lambda: {}  # type: ignore[method-assign]
    enricher._fetch_moex_z_curve_values = lambda: {"Z_CURVE_RUS": 14.2, "Z_CURVE_RUS_1Y": 14.2}  # type: ignore[method-assign]

    values = enricher._fetch_live_index_values()

    assert values["CBR_RATE"] == 15.5
    assert values["RUONIA"] == 15.31
    assert values["Z_CURVE_RUS"] == 14.2


def test_enrich_bonds_uses_live_ruonia_base_for_coupon() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 16.2, "CBR_RATE": 16.2, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    def fake_fetch(identifier: str):
        assert identifier == "RU1"
        return DohodBondPayload(index_name="RUONIA", index_spread=0.8, index_tenor_years=None, event_name="", ytm_date="", real_price=100.0), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [{"ISIN": "RU1", "COUPONPERCENT": "", "MATDATE": "2030-01-01", "OFFERDATE": ""}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == 17.0



def test_is_payload_empty_helper_for_backward_compatibility() -> None:
    assert _is_payload_empty(DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=None)) is True
    assert _is_payload_empty(DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=99.9)) is False



def test_compat_methods_for_mixed_versions_do_not_fail() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=100.0), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    payload, errors = enricher._fetch_with_fallback("RU000A0ZZTL5", "SU26228RMFS5")

    assert errors == 0
    assert payload.real_price == 100.0
    assert enricher._resolve_secondary_identifier({"ISIN": "RU1", "SECID": "SU1"}, "RU1") == ""


def test_fetch_with_fallback_uses_secid_when_isin_payload_empty() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    calls: list[str] = []

    def fake_fetch(identifier: str):
        calls.append(identifier)
        if identifier == "RU000A0JTYK4":
            return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=None), 0
        assert identifier == "BOND500"
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=99.8), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    payload, errors = enricher._fetch_with_fallback("RU000A0JTYK4", "BOND500")

    assert errors == 0
    assert payload.real_price == 99.8
    assert calls == ["RU000A0JTYK4", "BOND500"]






def test_enrich_bonds_counts_empty_parsed_payload_as_error() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))

    def fake_fetch(identifier: str):
        assert identifier == "RU000A0ZZTL5"
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=None), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [{"ISIN": "RU000A0ZZTL5", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 1
    assert enricher.last_stats.parse_empty_payloads == 1
    assert "RealPrice" not in bonds[0]



def test_enrich_bonds_batches_checkpoint_saves_for_initial_run() -> None:
    config = AppConfig(retries=1, dohod_checkpoint_save_every=2)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 14.0, "CBR_RATE": 16.0, "Z_CURVE_RUS": 12.0}  # type: ignore[method-assign]

    def fake_fetch(identifier: str):
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=100.0), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    bonds = [
        {"ISIN": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"},
        {"ISIN": "RU2", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"},
        {"ISIN": "RU3", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"},
    ]
    checkpoints: list[dict[str, object]] = []

    errors = enricher.enrich_bonds(bonds, checkpoint_saver=lambda payload: checkpoints.append(payload))

    assert errors == 0
    assert len(checkpoints) == 2
    assert checkpoints[0]["completed"] is False
    assert checkpoints[-1]["completed"] is True


def test_enrich_bonds_saves_final_checkpoint_even_with_large_interval() -> None:
    config = AppConfig(retries=1, dohod_checkpoint_save_every=50)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 14.0, "CBR_RATE": 16.0, "Z_CURVE_RUS": 12.0}  # type: ignore[method-assign]

    def fake_fetch(identifier: str):
        assert identifier == "RU1"
        return DohodBondPayload(index_name="", index_spread=0.0, index_tenor_years=None, event_name="", ytm_date="", real_price=100.0), 0

    enricher._fetch_and_parse = fake_fetch  # type: ignore[method-assign]

    checkpoints: list[dict[str, object]] = []
    bonds = [{"ISIN": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

    errors = enricher.enrich_bonds(bonds, checkpoint_saver=lambda payload: checkpoints.append(payload))

    assert errors == 0
    assert len(checkpoints) == 1
    assert checkpoints[0]["completed"] is True



def test_parse_corpbonds_payload_handles_tooltip_labels() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Тип купона <span class="tooltip">?</span><span class="tooltip-content">Подсказка</span></p></td><td><p class="val">Фикс</p></td></tr>
      <tr><td><p>Купон лесенкой <span class="tooltip">?</span><span class="tooltip-content">Подсказка</span></p></td><td><p class="val">Нет</p></td></tr>
      <tr><td><p>Дата ближайшей оферты <span class="tooltip">?</span><span class="tooltip-content">Подсказка</span></p></td><td><p class="val">Нет</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.coupon_type == "Фикс"
    assert payload.lesenka == "Нет"

def test_parse_corpbonds_payload_handles_tooltip_labels_for_price_and_offerdate() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Цена последняя <span class="tooltip">?</span><span class="tooltip-content">Подсказка</span></p></td><td><p class="val">102,75</p></td></tr>
      <tr><td><p>Дата ближайшей оферты <span class="tooltip">?</span><span class="tooltip-content">Подсказка</span></p></td><td><p class="val">24.10.2039</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.real_price == 102.75
    assert payload.ytm_date == "2039-10-24"


def test_parse_corpbonds_payload_extracts_price_coupon_and_offer() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Цена последняя</p></td><td><p class=\"val\">101,25</p></td></tr>
      <tr><td><p>Тип купона</p></td><td><p class=\"val\">Флоатер</p></td></tr>
      <tr><td><p>Формула купона</p></td><td><p class=\"val\">RUONIA + 2,10%</p></td></tr>
      <tr><td><p>Дата ближайшей оферты</p></td><td><p class=\"val\">24.10.2039</p></td></tr>
      <tr><td><p>Купон лесенкой</p></td><td><p class=\"val\">Да</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.real_price == 101.25
    assert payload.coupon_type == "Флоатер"
    assert payload.index_name == "RUONIA"
    assert payload.index_spread == 2.1
    assert payload.ytm_date == "2039-10-24"
    assert payload.lesenka == "Да"


def test_fetch_and_parse_does_not_fill_realprice_from_dohod_ask_when_corpbonds_price_missing() -> None:
    class _Response:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._corpbonds_secid_by_isin = {"RU1": "RU1_SEC"}
    enricher._get_with_rate_limit = lambda *_args, **_kwargs: _Response('<script>{"ask":"97.4"}</script>')  # type: ignore[method-assign]
    enricher._fetch_and_parse_corpbonds = lambda _secid: DohodBondPayload(real_price=None)  # type: ignore[method-assign]

    payload, errors = enricher._fetch_and_parse("RU1")

    assert errors == 0
    assert payload.ask_price == 97.4
    assert payload.real_price is None


def test_enrich_uses_corpbonds_realprice_instead_of_dohod_ask() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 14.5, "CBR_RATE": 14.0, "Z_CURVE_RUS": 13.0}  # type: ignore[method-assign]

    def fake_fetch(_primary: str, _secondary: str | None):
        return DohodBondPayload(ask_price=88.0, real_price=99.5), 0

    enricher._fetch_with_fallback = fake_fetch  # type: ignore[method-assign]

    bonds = [{"SECID": "RU1_SEC", "ISIN": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2030-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert bonds[0]["RealPrice"] == 99.5

def test_parse_corpbonds_payload_handles_offerdate_alias_label() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Дата оферты</p></td><td><p class=\"val\">15.07.2031</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.ytm_date == "2031-07-15"
    assert payload.offer_source == "corpbonds"


def test_enrich_bonds_counts_corpbonds_offerdate_and_formula_stats() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 14.5, "CBR_RATE": 14.0, "Z_CURVE_RUS": 13.0}  # type: ignore[method-assign]

    def fake_fetch(_primary: str, _secondary: str | None):
        return DohodBondPayload(index_name="RUONIA", index_spread=1.1, ytm_date="2030-01-01", event_name="оферта", formula_source="corpbonds", offer_source="corpbonds"), 0

    enricher._fetch_with_fallback = fake_fetch  # type: ignore[method-assign]

    bonds = [{"SECID": "RU1_SEC", "ISIN": "RU1", "COUPONPERCENT": "", "OFFERDATE": "", "MATDATE": "2035-01-01"}]
    errors = enricher.enrich_bonds(bonds)

    assert errors == 0
    assert bonds[0]["COUPONPERCENT"] == 15.6
    assert bonds[0]["OFFERDATE"] == "2030-01-01"
    assert enricher.last_stats.corpbonds_coupon_formula_applied == 1
    assert enricher.last_stats.corpbonds_offerdate_added == 1

def test_is_payload_empty_returns_false_when_only_corpbonds_fields_present() -> None:
    payload = DohodBondPayload(coupon_type="Фикс", lesenka="Нет")
    assert _is_payload_empty(payload) is False


def test_cached_payload_usable_when_only_corpbonds_fields_present() -> None:
    payload = {
        "real_price": None,
        "index_name": "",
        "ytm_date": "",
        "event_name": "",
        "coupon_type": "Фикс",
        "lesenka": "Нет",
    }
    assert DohodEnricher._is_cached_payload_usable(payload) is True

def test_parse_corpbonds_payload_parses_percent_realprice_and_sigma_ks_formula() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Цена последняя</p></td><td><p class=\"val\">100,39 %</p></td></tr>
      <tr><td><p>Формула купона</p></td><td><p class=\"val\">∑КС + 2%</p></td></tr>
      <tr><td><p>Ближайшая дата</p></td><td><p class=\"val\">15.07.2031</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.real_price == 100.39
    assert payload.index_name == "CBR_RATE"
    assert payload.index_spread == 2.0
    assert payload.ytm_date == "2031-07-15"


def test_parse_corpbonds_payload_ignores_non_last_price_rows() -> None:
    html = """
    <table><tbody>
      <tr><td><p>Цена размещения</p></td><td><p class=\"val\">0,21</p></td></tr>
      <tr><td><p>Цена предыдущего закрытия</p></td><td><p class=\"val\">0,00</p></td></tr>
    </tbody></table>
    """

    payload = DohodEnricher.parse_corpbonds_payload(html)

    assert payload.real_price is None


def test_enrich_bonds_uses_monthly_cache_for_redlist_when_checkpoint_not_fresh() -> None:
    config = AppConfig(retries=1)
    enricher = DohodEnricher(config=config, logger=logging.getLogger("test"))
    enricher._fetch_live_index_values = lambda: {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0}  # type: ignore[method-assign]

    called = {"count": 0}

    def fail_fetch(_: str):
        called["count"] += 1
        return DohodBondPayload(), 1

    enricher._fetch_and_parse = fail_fetch  # type: ignore[method-assign]
    fetched_at = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    checkpoint = {
        "version": DOHOD_CHECKPOINT_VERSION,
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "completed": True,
        "index_values": {"RUONIA": 15.0, "CBR_RATE": 15.0, "Z_CURVE_RUS": 14.0},
        "bonds": {
            "RU1": {
                "real_price": 100.5,
                "index_name": "",
                "index_spread": 0.0,
                "index_tenor_years": None,
                "event_name": "погашение",
                "ytm_date": "2030-01-01",
                "fetched_at": fetched_at,
            }
        },
    }
    bonds = [{"SECID": "RU1_SEC", "ISIN": "RU1", "COUPONPERCENT": "5.0", "OFFERDATE": "", "MATDATE": "2030-01-01"}]

    errors = enricher.enrich_bonds(
        bonds,
        checkpoint_data=checkpoint,
        monthly_cached_identifiers={"RU1"},
        monthly_cache_ttl_days=30,
    )

    assert errors == 0
    assert called["count"] == 0
    assert bonds[0]["RealPrice"] == 100.5
