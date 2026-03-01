from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter


def export_screener(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Screener")
        ws = writer.book["Screener"]

        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")

        ws.auto_filter.ref = ws.dimensions
        ws.freeze_panes = "A2"

        for idx, column_cells in enumerate(ws.columns, start=1):
            max_len = 0
            for cell in column_cells:
                val = cell.value
                if isinstance(val, (datetime, date)):
                    cell.number_format = "DD.MM.YYYY"
                    text = val.strftime("%d.%m.%Y")
                else:
                    text = "" if val is None else str(val)
                max_len = max(max_len, len(text))
            ws.column_dimensions[get_column_letter(idx)].width = min(60, max(10, max_len + 2))
