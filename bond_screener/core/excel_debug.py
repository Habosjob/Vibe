from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.settings import AppSettings


def should_export(settings: AppSettings, export_name: str) -> bool:
    if not settings.excel_debug:
        return False
    allowed = {item.lower() for item in settings.excel_debug_exports}
    return export_name.lower() in allowed


def export_dataframe(settings: AppSettings, filename: str, df: pd.DataFrame, export_name: str = "stage0") -> Path | None:
    if not should_export(settings, export_name):
        return None

    out_path = settings.paths.source_xlsx_dir / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
    return out_path
