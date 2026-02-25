from __future__ import annotations

from moex_bond_screener.state_store import ScreenerStateStore


def test_state_store_load_eligible_bonds_json_backend(tmp_path):
    store = ScreenerStateStore(str(tmp_path / "state"), storage_backend="json")
    store.update_eligible_bonds([{"SECID": "A"}, {"SECID": "B", "EMITTER_ID": "2"}])

    rows = sorted(store.load_eligible_bonds(), key=lambda row: row["SECID"])

    assert rows == [{"SECID": "A"}, {"SECID": "B", "EMITTER_ID": "2"}]


def test_state_store_load_eligible_bonds_sqlite_backend(tmp_path):
    store = ScreenerStateStore(str(tmp_path / "state"), storage_backend="sqlite")
    store.update_eligible_bonds([{"SECID": "A"}, {"SECID": "B", "EMITTER_ID": "2"}])

    rows = sorted(store.load_eligible_bonds(), key=lambda row: row["SECID"])

    assert rows == [{"SECID": "A"}, {"SECID": "B", "EMITTER_ID": "2"}]
