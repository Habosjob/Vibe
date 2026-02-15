from __future__ import annotations

from vibe.data_sources.moex_bonds_endpoints import build_probe_cache_key


def test_cache_key_is_stable_for_same_payload() -> None:
    params = {"from": "2024-01-01", "till": "2024-01-10", "interval": 24}
    key1 = build_probe_cache_key(
        isin="RU000A",
        endpoint_name="history",
        url="https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/TQCB/securities/RU000A.json",
        params=params,
    )
    key2 = build_probe_cache_key(
        isin="RU000A",
        endpoint_name="history",
        url="https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/TQCB/securities/RU000A.json",
        params=dict(params),
    )

    assert key1 == key2


def test_cache_key_changes_when_params_change() -> None:
    key1 = build_probe_cache_key(
        isin="RU000A",
        endpoint_name="history",
        url="https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/TQCB/securities/RU000A.json",
        params={"from": "2024-01-01", "till": "2024-01-10"},
    )
    key2 = build_probe_cache_key(
        isin="RU000A",
        endpoint_name="history",
        url="https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/TQCB/securities/RU000A.json",
        params={"from": "2024-01-01", "till": "2024-01-11"},
    )

    assert key1 != key2
