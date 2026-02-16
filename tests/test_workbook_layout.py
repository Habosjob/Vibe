from __future__ import annotations

from pathlib import Path

import pandas as pd

from vibe.ingest.moex_bonds_endpoints_probe import write_isin_workbook


def test_write_isin_workbook_creates_data_summary_and_aggregate_sheets(tmp_path: Path) -> None:
    out_path = tmp_path / "RU000TEST.xlsx"
    endpoint_frames = {
        "marketdata": pd.DataFrame(),
    }
    endpoint_summaries = {
        "marketdata": pd.DataFrame(
            {
                "__status": ["NO_DATA"],
                "http_status": [200],
                "reason": ["empty_table"],
                "error": [""],
                "content_type": ["application/json"],
                "from_cache": [False],
                "elapsed_ms": [15],
            }
        ),
    }

    write_isin_workbook(
        isin="RU000TEST",
        endpoint_frames_map=endpoint_frames,
        endpoint_summaries_map=endpoint_summaries,
        meta={"isin": "RU000TEST"},
        out_path=out_path,
    )

    workbook = pd.ExcelFile(out_path)
    assert "marketdata" in workbook.sheet_names
    assert "marketdata_summary" in workbook.sheet_names
    assert "summary_all" in workbook.sheet_names
    assert "meta" in workbook.sheet_names

    data_df = pd.read_excel(out_path, sheet_name="marketdata")
    summary_df = pd.read_excel(out_path, sheet_name="marketdata_summary")
    summary_all_df = pd.read_excel(out_path, sheet_name="summary_all")

    assert data_df.empty
    assert len(summary_df) == 1
    assert summary_df.loc[0, "__status"] == "NO_DATA"

    assert len(summary_all_df) == 1
    assert summary_all_df.loc[0, "endpoint"] == "marketdata"
    assert summary_all_df.loc[0, "__status"] == "NO_DATA"
    assert summary_all_df.loc[0, "rows"] == 0
