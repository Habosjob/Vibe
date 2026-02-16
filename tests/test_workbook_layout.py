from __future__ import annotations

from pathlib import Path

import pandas as pd

from vibe.ingest.moex_bonds_endpoints_probe import write_isin_workbook


def test_write_isin_workbook_creates_data_and_summary_sheets(tmp_path: Path) -> None:
    out_path = tmp_path / "RU000TEST.xlsx"
    endpoint_frames = {
        "marketdata": pd.DataFrame({"BESTBID": [99.9], "BESTOFFER": [100.1]}),
    }
    endpoint_summaries = {
        "marketdata": pd.DataFrame({"status": ["OK"], "rows": [1]}),
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

    data_df = pd.read_excel(out_path, sheet_name="marketdata")
    summary_df = pd.read_excel(out_path, sheet_name="marketdata_summary")

    assert len(data_df) == 1
    assert data_df.loc[0, "BESTBID"] == 99.9
    assert len(summary_df) == 1
    assert summary_df.loc[0, "status"] == "OK"
