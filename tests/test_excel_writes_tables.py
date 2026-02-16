from __future__ import annotations

from pathlib import Path

import pandas as pd

from vibe.ingest.moex_bonds_endpoints_probe import write_isin_workbook


def test_write_isin_workbook_creates_expected_table_sheets(tmp_path: Path) -> None:
    out_path = tmp_path / "RU000TEST.xlsx"
    endpoint_frames = {
        "marketdata": pd.DataFrame(
            [
                {"__table": "dataversion", "VER": "1.0"},
                {"__table": "marketdata", "BID": 99.5, "OFFER": 100.1},
            ]
        )
    }
    endpoint_summaries = {
        "marketdata": pd.DataFrame(
            {
                "__status": ["OK"],
                "http_status": [200],
                "reason": [""],
                "rows_total": [2],
                "rows_meta": [1],
                "rows_effective": [1],
            }
        )
    }

    write_isin_workbook(
        isin="RU000TEST",
        endpoint_frames_map=endpoint_frames,
        endpoint_summaries_map=endpoint_summaries,
        meta={"isin": "RU000TEST"},
        out_path=out_path,
    )

    workbook = pd.ExcelFile(out_path)
    assert "marketdata_summary" in workbook.sheet_names
    assert "marketdata__dataversion" in workbook.sheet_names
    assert "marketdata__marketdata" in workbook.sheet_names

    marketdata_df = pd.read_excel(out_path, sheet_name="marketdata__marketdata")
    assert len(marketdata_df) == 1
    assert marketdata_df.loc[0, "BID"] == 99.5
