import time
from pathlib import Path

import pandas as pd

from vibe.storage.excel import write_dataframe_to_excel_atomic


def _meta_to_dict(meta_df: pd.DataFrame) -> dict[str, str]:
    return dict(zip(meta_df["key"], meta_df["value"]))


def test_excel_atomic_write_overwrite(tmp_path: Path) -> None:
    out_path = tmp_path / "bond_rates.xlsx"
    df = pd.DataFrame({"SECID": ["SU26212RMFS9"], "LAST": [95.31]})

    write_dataframe_to_excel_atomic(
        df,
        out_path,
        meta={
            "downloaded_at_utc": "2025-01-01T00:00:00+00:00",
            "source_url": "https://example.com",
            "rows": 1,
            "cols": 2,
            "sha256_raw_csv": "aaa",
        },
    )

    assert out_path.exists()
    xls = pd.ExcelFile(out_path)
    assert {"rates", "meta"}.issubset(set(xls.sheet_names))
    meta_first = _meta_to_dict(pd.read_excel(out_path, sheet_name="meta"))

    time.sleep(0.01)
    write_dataframe_to_excel_atomic(
        df,
        out_path,
        meta={
            "downloaded_at_utc": "2025-01-01T00:00:01+00:00",
            "source_url": "https://example.com",
            "rows": 1,
            "cols": 2,
            "sha256_raw_csv": "bbb",
        },
    )

    meta_second = _meta_to_dict(pd.read_excel(out_path, sheet_name="meta"))
    assert meta_first["downloaded_at_utc"] != meta_second["downloaded_at_utc"]
