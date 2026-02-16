from __future__ import annotations

from datetime import date

from vibe.data_sources.moex_bonds_endpoints import FetchMeta
from vibe.ingest.moex_bonds_endpoints_probe import build_probe_summary_df


def test_build_probe_summary_marks_only_dataversion_as_no_data() -> None:
    meta = FetchMeta(
        status_code=200,
        from_cache=False,
        elapsed_ms=10,
        url="https://iss.moex.com/test.json",
        params={},
        error=None,
    )

    payload = {
        "dataversion": {
            "columns": ["VER"],
            "data": [["1.0"]],
        }
    }

    summary_df = build_probe_summary_df(
        meta=meta,
        payload=payload,
        board="TQCB",
        from_date=date(2024, 1, 1),
        till_date=date(2024, 1, 2),
        interval=24,
    )

    row = summary_df.iloc[0]
    assert row["__status"] == "NO_DATA"
    assert row["reason"] == "only_meta_tables"
    assert row["rows_total"] == 1
    assert row["rows_meta"] == 1
    assert row["rows_effective"] == 0
