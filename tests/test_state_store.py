from __future__ import annotations

import json

from moex_bond_screener.state_store import ScreenerStateStore


def test_state_store_save_and_load_exclusions(tmp_path) -> None:
    store = ScreenerStateStore(str(tmp_path / "state"))

    store.save_exclusions({"SU1": {"rule": "mat_lt_1y", "exclude_until": "2026-10-10"}})

    loaded = store.load_exclusions()
    assert loaded == {"SU1": {"rule": "mat_lt_1y", "exclude_until": "2026-10-10"}}


def test_state_store_incremental_updates(tmp_path) -> None:
    store = ScreenerStateStore(str(tmp_path / "state"))

    first = store.update_eligible_bonds(
        [{"SECID": "A", "MATDATE": "2030-01-01"}, {"SECID": "B", "MATDATE": "2031-01-01"}]
    )
    assert (first.inserted, first.updated, first.unchanged, first.removed) == (2, 0, 0, 0)

    second = store.update_eligible_bonds(
        [{"SECID": "A", "MATDATE": "2030-01-01"}, {"SECID": "B", "MATDATE": "2032-01-01"}]
    )
    assert (second.inserted, second.updated, second.unchanged, second.removed) == (0, 1, 1, 0)

    third = store.update_eligible_bonds([{"SECID": "B", "MATDATE": "2032-01-01"}])
    assert (third.inserted, third.updated, third.unchanged, third.removed) == (0, 0, 1, 1)

    payload = json.loads((tmp_path / "state" / "eligible_bonds.json").read_text(encoding="utf-8"))
    assert payload["bonds"].keys() == {"B"}
