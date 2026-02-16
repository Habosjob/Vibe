from __future__ import annotations

from pathlib import Path

import pandas as pd

from vibe.ingest.moex_bonds_endpoints_probe import export_probe_run_artifacts


def test_export_probe_run_artifacts_writes_xlsx_with_data(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    records = [
        {
            "isin": "RU000A",
            "board": "TQCB",
            "endpoint": "marketdata",
            "status": "OK",
            "reason": "",
            "http_status": 200,
            "rows_total": 3,
            "rows_effective": 3,
            "error": "",
        },
        {
            "isin": "RU000B",
            "board": "TQCB",
            "endpoint": "securities",
            "status": "OK",
            "reason": "",
            "http_status": 200,
            "rows_total": 1,
            "rows_effective": 1,
            "error": "",
        },
    ]

    results_xlsx, results_json, results_csv = export_probe_run_artifacts(
        records=records,
        run_dir=run_dir,
        endpoints_checked=2,
        successful_endpoints=2,
        http_errors=0,
        parse_errors=0,
        filtered_out=0,
        rows_effective_total=4,
    )

    assert results_xlsx.exists()
    assert results_json.exists()
    assert results_csv.exists()

    endpoints_df = pd.read_excel(results_xlsx, sheet_name="endpoints")
    assert len(endpoints_df.index) >= 2
