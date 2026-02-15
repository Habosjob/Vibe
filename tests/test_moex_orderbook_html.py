from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from vibe.data_sources.moex_bonds_endpoints import BondEndpointSpec, FetchMeta
from vibe.ingest import moex_bonds_endpoints_probe as probe


class _DummyClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def resolve_board(self, isin: str) -> str:
        return "TQCB"

    def fetch_endpoint(self, isin: str, board: str, spec: BondEndpointSpec, params=None):
        if spec.name == "marketdata":
            payload = {
                "marketdata": {
                    "columns": ["BESTBID", "BESTOFFER", "NUMBIDS", "NUMOFFERS"],
                    "data": [[101.2, 101.5, 3, 2]],
                }
            }
            return payload, FetchMeta(
                status_code=200,
                from_cache=False,
                elapsed_ms=5,
                url="https://iss.moex.com/marketdata",
                params=params or {},
                content_type="application/json",
            )

        if spec.name == "orderbook":
            return None, FetchMeta(
                status_code=200,
                from_cache=False,
                elapsed_ms=6,
                url="https://iss.moex.com/orderbook",
                params=params or {},
                content_type="text/html",
                response_head="<html>blocked</html>",
                error="HTML_INSTEAD_OF_JSON",
                final_url="https://iss.moex.com/orderbook?guard=1",
                headers_subset={"Content-Type": "text/html", "Server": "cloudflare"},
            )

        payload = {"dummy": {"columns": ["X"], "data": [[1]]}}
        return payload, FetchMeta(
            status_code=200,
            from_cache=False,
            elapsed_ms=4,
            url=f"https://iss.moex.com/{spec.name}",
            params=params or {},
            content_type="application/json",
        )


def test_orderbook_html_blocked_uses_marketdata_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(probe, "MoexBondEndpointsClient", _DummyClient)
    monkeypatch.setattr(probe, "cleanup_old_dirs", lambda *a, **k: None)
    monkeypatch.setattr(probe, "_load_latest_bond_rates_snapshot", lambda: (pd.DataFrame(), Path("snapshot.xlsx")))

    captured: dict[str, object] = {}

    def _capture_write(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(probe, "write_isin_workbook", _capture_write)

    result = probe.run_probe(
        isins=["RU000A"],
        out_dir=tmp_path,
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 2),
        interval=24,
        use_cache=False,
        max_workers=1,
    )

    assert result.files_written == 1
    assert result.orderbook_blocked_html == 1

    summaries = captured["endpoint_summaries_map"]
    orderbook_summary = summaries["orderbook"].iloc[0]
    assert orderbook_summary["__status"] == "BLOCKED_HTML"
    assert orderbook_summary["error"] == "HTML_INSTEAD_OF_JSON"

    sheets = captured["endpoint_frames_map"]
    orderbook_frame = sheets["orderbook"]
    assert float(orderbook_frame.iloc[0]["bestbid"]) == 101.2
    assert float(orderbook_frame.iloc[0]["bestoffer"]) == 101.5
    assert orderbook_frame.iloc[0]["source"] == "marketdata_fallback"
