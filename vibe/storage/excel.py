from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from vibe.utils.fs import atomic_replace_with_retry, ensure_parent_dir


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
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            meta_df = pd.DataFrame(list(meta.items()), columns=["key", "value"])
            meta_df.to_excel(writer, sheet_name="meta", index=False)
        atomic_replace_with_retry(temp_path, out_path)
    finally:
        if temp_path.exists() and temp_path != out_path:
            temp_path.unlink(missing_ok=True)
