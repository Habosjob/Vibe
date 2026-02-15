from __future__ import annotations

from datetime import date

from vibe.data_sources.moex_bonds_endpoints import FetchMeta
from vibe.ingest.moex_bonds_endpoints_probe import build_probe_summary_df


def test_build_probe_summary_marks_no_data_for_empty_payload() -> None:
    meta = FetchMeta(
        status_code=200,
        from_cache=False,
        elapsed_ms=12,
        url="https://iss.moex.com/test.json",
        params={"from": "2024-01-01", "till": "2024-01-10"},
        error=None,
    )

    summary_df = build_probe_summary_df(
        meta=meta,
        payload={},
        board="TQCB",
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 10),
        interval=24,
    )

    assert summary_df.iloc[0]["__status"] == "NO_DATA"
    assert summary_df.iloc[0]["reason"] == "no_tables_in_payload"
