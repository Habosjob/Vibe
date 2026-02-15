from __future__ import annotations

from datetime import date
from pathlib import Path

from vibe.data_sources.moex_bonds_endpoints import BondEndpointSpec, FetchMeta, MoexBondEndpointsClient
from vibe.ingest.moex_bonds_endpoints_probe import build_probe_summary_df


class _Resp:
    status_code = 200
    headers = {"Content-Type": "text/html"}
    content = b"<html>gateway</html>"


def test_fetch_endpoint_non_json_returns_error_meta(monkeypatch, tmp_path: Path, caplog) -> None:
    monkeypatch.setattr("vibe.data_sources.moex_bonds_endpoints.get_with_retries", lambda *a, **k: _Resp())

    client = MoexBondEndpointsClient(cache_dir=tmp_path, use_cache=True)
    spec = BondEndpointSpec("orderbook", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}/orderbook.json")
    payload, meta = client.fetch_endpoint(isin="RU000A", board="TQCB", spec=spec)

    assert payload is None
    assert meta.error is not None and "invalid_json" in meta.error
    assert meta.status_code == 200
    assert meta.content_type == "text/html"
    assert "<html>gateway</html>" in (meta.response_head or "")
    assert "url=https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities/RU000A/orderbook.json" in caplog.text
    assert "response_head='<html>gateway</html>'" in caplog.text


def test_build_probe_summary_captures_response_diagnostics() -> None:
    meta = FetchMeta(
        status_code=500,
        from_cache=False,
        elapsed_ms=15,
        url="https://iss.moex.com/test",
        params={},
        content_type="text/plain",
        response_head="upstream error",
        error="invalid_json",
    )

    summary_df = build_probe_summary_df(
        meta=meta,
        payload=None,
        board="TQCB",
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 2),
        interval=24,
    )

    row = summary_df.iloc[0]
    assert row["__status"] == "ERROR"
    assert row["content_type"] == "text/plain"
    assert row["response_head"] == "upstream error"
