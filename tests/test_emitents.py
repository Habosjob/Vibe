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
