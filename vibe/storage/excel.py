from __future__ import annotations

import tempfile
from itertools import islice
from pathlib import Path

import pandas as pd

from vibe.utils.fs import atomic_replace_with_retry, ensure_parent_dir


def _sort_rates_columns(df: pd.DataFrame) -> pd.DataFrame:
    priority = [col for col in ["ISIN", "SHORTNAME"] if col in df.columns]
    rest = sorted([col for col in df.columns if col not in priority])
    return df[priority + rest]


def _format_rates_sheet(ws, df: pd.DataFrame) -> None:
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    rows_preview = list(islice(df.itertuples(index=False, name=None), 200))
    for idx, column in enumerate(df.columns, start=1):
        max_len = len(str(column))
        for row in rows_preview:
            value = row[idx - 1]
            if pd.isna(value):
                continue
            max_len = max(max_len, len(str(value)))
        ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max(max_len + 2, 8), 50)


def write_dataframe_to_excel_atomic(
    df: pd.DataFrame,
    out_path: Path,
    sheet_name: str = "rates",
    meta: dict | None = None,
) -> None:
    ensure_parent_dir(out_path)
    meta = meta or {}

    with tempfile.NamedTemporaryFile(
        dir=out_path.parent, suffix=".xlsx", delete=False
    ) as tmp:
        temp_path = Path(tmp.name)

    try:
        rates_df = _sort_rates_columns(df)
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            rates_df.to_excel(writer, sheet_name=sheet_name, index=False)
            meta_df = pd.DataFrame(list(meta.items()), columns=["key", "value"])
            meta_df.to_excel(writer, sheet_name="meta", index=False)
            _format_rates_sheet(writer.sheets[sheet_name], rates_df)
        atomic_replace_with_retry(temp_path, out_path)
    finally:
        if temp_path.exists() and temp_path != out_path:
            temp_path.unlink(missing_ok=True)
