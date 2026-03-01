from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill


def export_screener(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.sort_values(["ytm_calc", "days_to_amort"], ascending=[False, False], na_position="last")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Screener", index=False)
        ws = writer.book["Screener"]

        header_fill = PatternFill("solid", fgColor="D9D9D9")
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        ytm_col = None
        method_col = None
        filter_col = None
        for i, c in enumerate(out.columns, start=1):
            max_len = max(len(str(c)), *(len(str(v)) for v in out[c].head(5000).fillna("")))
            ws.column_dimensions[ws.cell(1, i).column_letter].width = min(60, max(10, max_len + 2))
            if "date" in c.lower():
                for r in range(2, ws.max_row + 1):
                    ws.cell(r, i).number_format = "DD.MM.YYYY"
            if c in {"ytm_calc", "ytm_zero_coupon", "yield_perpetual_compounded"}:
                ytm_col = i
                for r in range(2, ws.max_row + 1):
                    ws.cell(r, i).number_format = "0.00%"
            if c == "days_to_amort":
                for r in range(2, ws.max_row + 1):
                    ws.cell(r, i).number_format = "0"
            if c == "ytm_method":
                method_col = i
            if c == "filter_amort_ok":
                filter_col = i

        method_colors = {
            "floater_scenario": "DDEBF7",
            "zero_coupon": "E4DFEC",
            "perpetual_compounded": "FCE4D6",
            "linker_scenario": "E2F0D9",
        }

        for r in range(2, ws.max_row + 1):
            if filter_col and ws.cell(r, filter_col).value is False:
                for c in range(1, ws.max_column + 1):
                    ws.cell(r, c).fill = PatternFill("solid", fgColor="F8CBAD")
            if method_col:
                method = ws.cell(r, method_col).value
                if method in method_colors:
                    fill = PatternFill("solid", fgColor=method_colors[method])
                    ws.cell(r, method_col).fill = fill
                    if ytm_col:
                        ws.cell(r, ytm_col).fill = fill
