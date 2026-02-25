from __future__ import annotations

import logging

from moex_bond_screener.config import AppConfig
from moex_bond_screener.emitents import build_emitents_reference
from moex_bond_screener.moex_client import MoexClient
from moex_bond_screener.state_store import ScreenerStateStore


class DummyResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.text = "{}"

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_build_emitents_reference_reuses_cached_static_fields(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Старый Эмитент", "inn": "7701000000"}})

    eligible_bonds = [
        {"SECID": "BOND1", "EMITTER_ID": "111"},
        {"SECID": "BOND2", "EMITTER_ID": "222"},
    ]

    def fake_description(secid: str):
        if secid == "BOND2":
            return {"EMITTER_ID": "222", "EMITTER_FULL_NAME": "Новый Эмитент", "EMITTER_INN": "7702000000"}, 0
        return {"EMITTER_ID": "111", "EMITTER_FULL_NAME": "Не должен перезаписаться", "EMITTER_INN": "999"}, 0

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(
        client,
        "fetch_market_securities",
        lambda market: (
            [
                {"EMITTER_ID": "111", "ISIN": "RU000000001", "SECID": "S111"},
                {"EMITTER_ID": "222", "ISIN": "RU000000002", "SECID": "S222"},
            ],
            0,
        ),
    )

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.new_emitters == 1
    assert result.processed_emitters == 2
    assert len(result.rows) == 2

    by_inn = {row["ИНН"]: row for row in result.rows}
    assert by_inn["7701000000"]["Полное наименование"] == "Старый Эмитент"
    assert by_inn["7702000000"]["Полное наименование"] == "Новый Эмитент"
    assert by_inn["7702000000"]["Тикеры акций"] == "S222"
    assert by_inn["7702000000"]["ISIN облигаций"] == "RU000000002"


def test_fetch_security_description_parses_name_value(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: DummyResponse(
            {
                "description": {
                    "columns": ["name", "title", "value"],
                    "data": [["EMITTER_ID", "", 123], ["EMITTER_INN", "", "7703000000"]],
                }
            }
        ),
    )

    payload, errors = client.fetch_security_description("SEC")

    assert errors == 0
    assert payload == {"EMITTER_ID": "123", "EMITTER_INN": "7703000000"}


def test_fetch_security_description_parses_uppercase_columns(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: DummyResponse(
            {
                "description": {
                    "columns": ["NAME", "TITLE", "VALUE"],
                    "data": [["EMITTER_FULL_NAME", "", "Тестовый эмитент"], ["EMITTER_INN", "", "7703555555"]],
                }
            }
        ),
    )

    payload, errors = client.fetch_security_description("SEC")

    assert errors == 0
    assert payload == {"EMITTER_FULL_NAME": "Тестовый эмитент", "EMITTER_INN": "7703555555"}


def test_build_emitents_reference_recovers_when_eligible_missing_emitter_id(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [
        {"SECID": "BOND1"},
        {"SECID": "BOND2"},
    ]

    def fake_description(secid: str):
        if secid == "BOND1":
            return {"EMITTER_ID": "111", "EMITTER_FULL_NAME": "Первый", "EMITTER_INN": "7701111111"}, 0
        return {"EMITTER_ID": "222", "EMITTER_FULL_NAME": "Второй", "EMITTER_INN": "7702222222"}, 0

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(
        client,
        "fetch_market_securities",
        lambda market: (
            [
                {"EMITTER_ID": "111", "ISIN": "RU000000011", "SECID": "S111"},
                {"EMITTER_ID": "222", "ISIN": "RU000000022", "SECID": "S222"},
            ],
            0,
        ),
    )

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.processed_emitters == 2
    assert result.new_emitters == 2
    assert sorted(row["ИНН"] for row in result.rows) == ["7701111111", "7702222222"]


def test_build_emitents_reference_requests_one_card_per_known_emitter(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [
        {"SECID": "BOND1", "EMITTER_ID": "111"},
        {"SECID": "BOND2", "EMITTER_ID": "111"},
        {"SECID": "BOND3", "EMITTER_ID": "111"},
        {"SECID": "BOND4", "EMITTER_ID": "222"},
        {"SECID": "BOND5", "EMITTER_ID": "222"},
    ]

    calls: list[str] = []

    def fake_description(secid: str):
        calls.append(secid)
        if secid == "BOND1":
            return {"EMITTER_ID": "111", "EMITTER_FULL_NAME": "Первый", "EMITTER_INN": "7701111111"}, 0
        return {"EMITTER_ID": "222", "EMITTER_FULL_NAME": "Второй", "EMITTER_INN": "7702222222"}, 0

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.processed_emitters == 2
    assert set(calls) == {"BOND1", "BOND4"}
    assert len(calls) == 2


def test_build_emitents_reference_reuses_secid_to_emitter_cache(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Кэш Эмитент", "inn": "7701999999"}})
    store.save_secid_to_emitter_map({"BOND1": "111"})

    eligible_bonds = [{"SECID": "BOND1"}]

    calls: list[str] = []

    def fake_description(secid: str):
        calls.append(secid)
        return {}, 1

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.processed_emitters == 1
    assert result.new_emitters == 0
    assert len(result.rows) == 1
    assert result.rows[0]["ИНН"] == "7701999999"
    assert calls == []


def test_build_emitents_reference_fills_stage_timers(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BOND1", "EMITTER_ID": "111"}]

    monkeypatch.setattr(
        client,
        "fetch_security_description",
        lambda secid: ({"EMITTER_ID": "111", "EMITTER_FULL_NAME": "Первый", "EMITTER_INN": "7701111111"}, 0),
    )
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert set(result.stage_durations.keys()) == {
        "emitents_cards_seconds",
        "emitents_market_bonds_seconds",
        "emitents_market_shares_seconds",
    }
    assert all(value >= 0 for value in result.stage_durations.values())


def test_build_emitents_reference_reuses_market_cache(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BOND1", "EMITTER_ID": "111"}]
    monkeypatch.setattr(
        client,
        "fetch_security_description",
        lambda secid: ({"EMITTER_ID": "111", "EMITTER_FULL_NAME": "Первый", "EMITTER_INN": "7701111111"}, 0),
    )

    market_calls: list[str] = []

    def fake_market(market: str):
        market_calls.append(market)
        return ([{"EMITTER_ID": "111", "ISIN": "RU000000011", "SECID": "S111"}], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    first = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)
    second = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert first.processed_emitters == 1
    assert second.processed_emitters == 1
    assert market_calls == ["bonds", "shares"]


def test_build_emitents_reference_marks_quality_and_infers_emitters_from_market(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BONDX", "ISIN": "RU0000000XX"}]

    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 1))

    def fake_market(market: str):
        if market == "bonds":
            return ([{"EMITTER_ID": "991", "ISIN": "RU0000000XX", "SECID": "BONDX"}], 0)
        return ([{"EMITTER_ID": "991", "SECID": "S991"}], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.processed_emitters == 1
    assert len(result.rows) == 1
    row = result.rows[0]
    assert row["missing_full_name"] == "1"
    assert row["missing_inn"] == "1"
    assert row["Флаг качества"] == "warning"


def test_build_emitents_reference_keeps_old_emitters_when_no_new_bonds(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Старый Эмитент", "inn": "7701000000"}})
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=[], client=client, state_store=store)

    assert result.processed_emitters == 1
    assert result.rows == [
        {
            "Полное наименование": "Старый Эмитент",
            "ИНН": "7701000000",
            "Тикеры акций": "",
            "ISIN облигаций": "",
            "missing_full_name": "0",
            "missing_inn": "0",
            "Флаг качества": "ok",
        }
    ]
