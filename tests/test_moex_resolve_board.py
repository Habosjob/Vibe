from __future__ import annotations

from vibe.data_sources.moex_bonds_endpoints import MoexBondEndpointsClient


class _StubClient(MoexBondEndpointsClient):
    def __init__(self, payload: dict):
        super().__init__()
        self._payload = payload

    def fetch_json(self, path: str, params=None):  # type: ignore[override]
        return self._payload


def _boards_payload(rows: list[list[object]]) -> dict:
    return {
        "boards": {
            "columns": ["boardid", "engine", "market", "is_traded", "is_primary"],
            "data": rows,
        }
    }


def test_resolve_board_prefers_bonds_market_and_primary() -> None:
    payload = _boards_payload(
        [
            ["EQRP", "stock", "repo", 1, 1],
            ["TQCB", "stock", "bonds", 1, 1],
            ["TQOB", "stock", "bonds", 1, 0],
        ]
    )

    assert _StubClient(payload).resolve_board("RU000A") == "TQCB"


def test_resolve_board_uses_priority_when_no_primary() -> None:
    payload = _boards_payload(
        [
            ["TQIR", "stock", "bonds", 1, 0],
            ["TQOB", "stock", "bonds", 1, 0],
        ]
    )

    assert _StubClient(payload).resolve_board("RU000A") == "TQOB"
