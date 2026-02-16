from __future__ import annotations

from pathlib import Path

from vibe.data_sources.moex_bonds_endpoints import BondEndpointSpec, MoexBondEndpointsClient


class _Resp:
    status_code = 200
    headers = {"Content-Type": "application/json"}
    content = b'{"marketdata": {"columns": ["BID"], "data": [[100.0]]}}'
    url = "https://iss.moex.com/final"


def test_client_captures_response_when_cache_reads_disabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("vibe.data_sources.moex_bonds_endpoints.get_with_retries", lambda *a, **k: _Resp())

    client = MoexBondEndpointsClient(cache_dir=tmp_path, use_cache=False, capture=True)
    spec = BondEndpointSpec("marketdata", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}.json")
    payload, _meta = client.fetch_endpoint(isin="RU000A", board="TQCB", spec=spec)

    assert payload is not None
    cache_files = list(tmp_path.glob("*.json"))
    assert len(cache_files) == 1
