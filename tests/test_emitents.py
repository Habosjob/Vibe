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

    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(
        client,
        "fetch_emitter_details",
        lambda emitter_id: (
            {"EMITTER_ID": emitter_id, "TITLE": "Новый Эмитент", "INN": "7702000000"},
            0,
        )
        if emitter_id == "222"
        else ({"EMITTER_ID": emitter_id, "TITLE": "Не должен перезаписаться", "INN": "999"}, 0),
    )
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
            return {"EMITTER_ID": "111"}, 0
        return {"EMITTER_ID": "222"}, 0

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(
        client,
        "fetch_emitter_details",
        lambda emitter_id: (
            {"EMITTER_ID": emitter_id, "TITLE": "Первый", "INN": "7701111111"},
            0,
        )
        if emitter_id == "111"
        else ({"EMITTER_ID": emitter_id, "TITLE": "Второй", "INN": "7702222222"}, 0),
    )
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


def test_build_emitents_reference_requests_one_emitter_card_per_known_emitter(monkeypatch, tmp_path):
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

    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(
        client,
        "fetch_emitter_details",
        lambda emitter_id: (
            calls.append(emitter_id) or {"EMITTER_ID": emitter_id, "TITLE": f"Эмитент {emitter_id}", "INN": f"770{emitter_id}"},
            0,
        ),
    )
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.processed_emitters == 2
    assert set(calls) == {"111", "222"}
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
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
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
        lambda secid: ({"EMITTER_ID": "111"}, 0),
    )
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({"TITLE": "Первый", "INN": "7701111111"}, 0))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert set(result.stage_durations.keys()) == {
        "emitents_cards_seconds",
        "emitents_market_descriptions_seconds",
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
        lambda secid: ({"EMITTER_ID": "111"}, 0),
    )
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({"TITLE": "Первый", "INN": "7701111111"}, 0))

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
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))

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
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(eligible_bonds=[], client=client, state_store=store)

    assert result.processed_emitters == 1
    assert result.rows == [
        {
            "Полное наименование": "Старый Эмитент",
            "ИНН": "7701000000",
            "Scorerate": "",
            "DateScore": "",
            "Тикеры акций": "",
            "ISIN облигаций": "",
            "EMITTER_ID": "111",
            "missing_full_name": "0",
            "missing_inn": "0",
            "Флаг качества": "ok",
        }
    ]


def test_build_emitents_reference_uses_issuer_id_for_market_maps(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BOND1", "ISSUER_ID": "111", "ISIN": "RU000A"}]

    monkeypatch.setattr(
        client,
        "fetch_security_description",
        lambda secid: ({"EMITTER_ID": "111"}, 0) if secid == "S111" else ({}, 0),
    )
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({"TITLE": "Эмитент", "INN": "7701111111"}, 0))

    def fake_market(market: str):
        if market == "shares":
            return ([{"ISSUER_ID": "111", "SECID": "S111"}], 0)
        return ([], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert len(result.rows) == 1
    assert result.rows[0]["Тикеры акций"] == "S111"
    assert result.rows[0]["ISIN облигаций"] == "RU000A"


def test_build_emitents_reference_resolves_market_secids_via_description(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BOND1", "EMITTER_ID": "111", "ISIN": "RU000A"}]

    description_calls: list[str] = []

    def fake_description(secid: str):
        description_calls.append(secid)
        if secid == "S111":
            return {"EMITTER_ID": "111"}, 0
        return {}, 0

    monkeypatch.setattr(client, "fetch_security_description", fake_description)
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({"TITLE": "Эмитент", "INN": "7701111111"}, 0))

    def fake_market(market: str):
        if market == "shares":
            return ([{"SECID": "S111"}], 0)
        return ([], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.rows[0]["Тикеры акций"] == "S111"
    assert "S111" in description_calls


def test_build_emitents_reference_includes_emitters_from_moex_markets(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = []

    monkeypatch.setattr(
        client,
        "fetch_security_description",
        lambda secid: ({"EMITTER_ID": "500"}, 0) if secid == "B500" else ({"EMITTER_ID": "600"}, 0),
    )

    def fake_emitter_details(emitter_id: str):
        if emitter_id == "500":
            return {"TITLE": "РЖД", "INN": "7708503727"}, 0
        return {"TITLE": "РСХБ", "INN": "7725114488"}, 0

    monkeypatch.setattr(client, "fetch_emitter_details", fake_emitter_details)

    def fake_market(market: str):
        if market == "bonds":
            return ([{"SECID": "B500", "ISIN": "RU000A0JTU85"}], 0)
        return ([{"SECID": "S600"}], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert result.processed_emitters == 2
    names = {row["Полное наименование"] for row in result.rows}
    assert names == {"РЖД", "РСХБ"}


def test_build_emitents_reference_skips_bonds_market_when_emitters_known(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    eligible_bonds = [{"SECID": "BOND1", "EMITTER_ID": "111", "ISIN": "RU000B"}]

    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({"TITLE": "Эмитент", "INN": "7701111111"}, 0))

    market_calls: list[str] = []

    def fake_market(market: str):
        market_calls.append(market)
        return ([{"EMITTER_ID": "111", "SECID": "S111", "ISIN": "RU000B"}], 0)

    monkeypatch.setattr(client, "fetch_market_securities", fake_market)

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=store)

    assert result.errors == 0
    assert market_calls == ["bonds", "shares"]
    assert result.rows[0]["ISIN облигаций"] == "RU000B"

def test_build_emitents_reference_marks_forced_blacklist(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Эмитент", "inn": "7701000000", "scorerate": "", "datescore": ""}})
    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(
        eligible_bonds=[{"SECID": "B1", "EMITTER_ID": "111"}],
        client=client,
        state_store=store,
        forced_blacklist_emitters={"111"},
    )

    assert result.scorerate_by_emitter["111"] == "Blacklist"
    assert result.rows[0]["Scorerate"] == "Blacklist"
    assert result.rows[0]["DateScore"]

def test_build_emitents_reference_marks_forced_blacklist_even_without_eligible_bond_entry(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(
        eligible_bonds=[],
        client=client,
        state_store=store,
        forced_blacklist_emitters={"999"},
    )

    assert result.scorerate_by_emitter["999"] == "Blacklist"
    assert any(row["EMITTER_ID"] == "999" and row["Scorerate"] == "Blacklist" for row in result.rows)

def test_build_emitents_reference_applies_manual_overrides(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Эмитент", "inn": "7701000000", "scorerate": "", "datescore": ""}})
    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(
        eligible_bonds=[{"SECID": "B1", "EMITTER_ID": "111"}],
        client=client,
        state_store=store,
        manual_overrides={"111": {"scorerate": "Yellowlist", "datescore": "2026-02-26"}},
    )

    assert result.scorerate_by_emitter["111"] == "Yellowlist"
    assert result.rows[0]["Scorerate"] == "Yellowlist"
    assert result.rows[0]["DateScore"] == "2026-02-26"


def test_build_emitents_reference_sets_datescore_for_manual_scorerate_change(monkeypatch, tmp_path):
    config = AppConfig(retries=1, page_size=50, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_emitents_registry({"111": {"full_name": "Эмитент", "inn": "7701000000", "scorerate": "", "datescore": ""}})
    monkeypatch.setattr(client, "fetch_security_description", lambda secid: ({}, 0))
    monkeypatch.setattr(client, "fetch_emitter_details", lambda emitter_id: ({}, 1))
    monkeypatch.setattr(client, "fetch_market_securities", lambda market: ([], 0))

    result = build_emitents_reference(
        eligible_bonds=[{"SECID": "B1", "EMITTER_ID": "111"}],
        client=client,
        state_store=store,
        manual_overrides={"111": {"scorerate": "Redlist", "datescore": ""}},
    )

    assert result.rows[0]["Scorerate"] == "Redlist"
    assert result.rows[0]["DateScore"]
