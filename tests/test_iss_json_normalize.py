from __future__ import annotations

from vibe.data_sources.moex_bonds_endpoints import iss_json_to_single_frame


def test_iss_json_to_single_frame_adds_table_marker() -> None:
    payload = {
        "history": {
            "columns": ["TRADEDATE", "CLOSE"],
            "data": [["2024-10-01", 100.1], ["2024-10-02", 100.5]],
        },
        "marketdata": {
            "columns": ["SECID", "WAPRICE"],
            "data": [["RU000A", 99.9]],
        },
        "ignored": {"foo": "bar"},
    }

    frame = iss_json_to_single_frame(payload)

    assert "__table" in frame.columns
    assert len(frame) == 3
    assert set(frame["__table"]) == {"history", "marketdata"}
    assert {"TRADEDATE", "CLOSE", "SECID", "WAPRICE"}.issubset(set(frame.columns))
