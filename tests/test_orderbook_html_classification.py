from __future__ import annotations

from datetime import date
from pathlib import Path

from vibe.data_sources.moex_bonds_endpoints import BondEndpointSpec, MoexBondEndpointsClient
from vibe.ingest.moex_bonds_endpoints_probe import build_probe_summary_df


class _Resp:
    status_code = 200
    headers = {"Content-Type": "text/html", "Server": "cloudflare", "CF-RAY": "ray-id"}
    content = b"<html><body>blocked</body></html>"
    url = "https://iss.moex.com/cdn-cgi/challenge"


def test_orderbook_html_classification_and_meta(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("vibe.data_sources.moex_bonds_endpoints.get_with_retries", lambda *a, **k: _Resp())

    client = MoexBondEndpointsClient(cache_dir=tmp_path, use_cache=True)
    spec = BondEndpointSpec("orderbook", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}/orderbook.json")

    payload, meta = client.fetch_endpoint(isin="RU000A", board="TQCB", spec=spec)

    assert payload is None
    assert meta.error == "HTML_INSTEAD_OF_JSON"
    assert meta.headers_subset is not None
    assert meta.headers_subset["Content-Type"] == "text/html"
    assert meta.final_url == "https://iss.moex.com/cdn-cgi/challenge"

    # Probe summary should still be buildable (pipeline does not crash on blocked orderbook).
    summary = build_probe_summary_df(
        meta=meta,
        payload=payload,
        board="TQCB",
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 2),
        interval=24,
    )
    assert summary.iloc[0]["error"] == "HTML_INSTEAD_OF_JSON"
