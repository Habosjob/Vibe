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
        if spec.name == "orderbook":
            return None, FetchMeta(
                status_code=200,
                from_cache=False,
                elapsed_ms=5,
                url="https://iss.moex.com/orderbook",
                params=params or {},
                content_type="text/html",
                response_head="<html>blocked</html>",
                error="HTML_INSTEAD_OF_JSON",
            )

        if spec.name == "marketdata":
            payload = {
                "marketdata": {
                    "columns": ["BESTBID", "BESTOFFER"],
                    "data": [[99.95, 100.05]],
                }
            }
            return payload, FetchMeta(
                status_code=200,
                from_cache=False,
                elapsed_ms=4,
                url="https://iss.moex.com/marketdata",
                params=params or {},
                content_type="application/json",
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


def test_orderbook_html_uses_marketdata_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(probe, "MoexBondEndpointsClient", _DummyClient)
    monkeypatch.setattr(probe, "cleanup_old_dirs", lambda *a, **k: None)
    monkeypatch.setattr(probe, "_load_latest_bond_rates_snapshot", lambda: (pd.DataFrame(), Path("snapshot.xlsx")))

    original_specs = probe.default_endpoint_specs

    def _orderbook_before_marketdata() -> list[BondEndpointSpec]:
        specs = original_specs()
        by_name = {spec.name: spec for spec in specs}
        return [by_name["orderbook"], by_name["marketdata"], *[s for s in specs if s.name not in {"orderbook", "marketdata"}]]

    monkeypatch.setattr(probe, "default_endpoint_specs", _orderbook_before_marketdata)

    captured: dict[str, object] = {}
    monkeypatch.setattr(probe, "write_isin_workbook", lambda **kwargs: captured.update(kwargs))

    result = probe.run_probe(
        isins=["RU000A"],
        out_dir=tmp_path,
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 2),
        interval=24,
        use_cache=False,
        max_workers=1,
    )

    assert result.orderbook_blocked_html == 1

    orderbook_summary = captured["endpoint_summaries_map"]["orderbook"].iloc[0]
    assert orderbook_summary["error"] == "HTML_INSTEAD_OF_JSON"

    orderbook_frame = captured["endpoint_frames_map"]["orderbook"]
    assert float(orderbook_frame.iloc[0]["bestbid"]) == 99.95
    assert float(orderbook_frame.iloc[0]["bestoffer"]) == 100.05
    assert orderbook_frame.iloc[0]["top_of_book_source"] == "marketdata_fallback"
