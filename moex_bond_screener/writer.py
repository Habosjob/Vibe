"""Сохранение результатов в Excel/CSV."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import csv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


DEFAULT_FIELDS = ["SECID", "SHORTNAME", "ISIN", "FACEUNIT", "LISTLEVEL", "PREVLEGALCLOSEPRICE"]
HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ROW_FILL = PatternFill(fill_type="solid", fgColor="F2F7FF")


def _resolve_fields(bonds: list[dict[str, Any]]) -> list[str]:
    if not bonds:
        return DEFAULT_FIELDS.copy()

    fields = list(bonds[0].keys())
    for bond in bonds[1:]:
        for key in bond.keys():
            if key not in fields:
                fields.append(key)

    return fields


def save_bonds_excel(path: str, bonds: list[dict[str, Any]]) -> None:
    """Сохраняет список облигаций в Excel (.xlsx) без проблем с кодировкой."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = _resolve_fields(bonds)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "MOEX_BONDS"

    sheet.append(fields)
    for bond in bonds:
        sheet.append([bond.get(field, "") for field in fields])

    _apply_excel_formatting(sheet)

    workbook.save(target)


def _apply_excel_formatting(sheet: Any) -> None:
    header_row = 1
    max_col = sheet.max_column
    max_row = sheet.max_row

    for cell in sheet[header_row]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for row_idx in range(2, max_row + 1):
        if row_idx % 2 == 0:
            for col_idx in range(1, max_col + 1):
                sheet.cell(row=row_idx, column=col_idx).fill = ROW_FILL

    for col_idx in range(1, max_col + 1):
        column_letter = get_column_letter(col_idx)
        values = [sheet.cell(row=row_idx, column=col_idx).value for row_idx in range(1, max_row + 1)]
        max_len = max((len(str(value)) for value in values if value is not None), default=0)
        sheet.column_dimensions[column_letter].width = min(max(max_len + 2, 10), 50)

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"


def save_bonds_csv(path: str, bonds: list[dict[str, Any]]) -> None:
    """Сохраняет список облигаций в CSV (UTF-8 BOM для корректного открытия в Excel)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = _resolve_fields(bonds)

    with target.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(bonds)


def save_bonds_file(path: str, bonds: list[dict[str, Any]]) -> None:
    """Сохраняет результат в формате по расширению файла.

    По умолчанию поддерживаются `.xlsx` и `.csv`.
    """
    extension = Path(path).suffix.lower()
    if extension == ".csv":
        save_bonds_csv(path, bonds)
        return

    if extension == ".xlsx":
        save_bonds_excel(path, bonds)
        return

    raise ValueError("Поддерживаются только форматы .xlsx и .csv")
