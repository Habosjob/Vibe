"""Сохранение результатов в Excel/CSV."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import csv
from openpyxl import Workbook


FIELDS = ["SECID", "SHORTNAME", "ISIN", "FACEUNIT", "LISTLEVEL", "PREVLEGALCLOSEPRICE"]


def save_bonds_excel(path: str, bonds: list[dict[str, Any]]) -> None:
    """Сохраняет список облигаций в Excel (.xlsx) без проблем с кодировкой."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "MOEX_BONDS"

    sheet.append(FIELDS)
    for bond in bonds:
        sheet.append([bond.get(field, "") for field in FIELDS])

    workbook.save(target)


def save_bonds_csv(path: str, bonds: list[dict[str, Any]]) -> None:
    """Сохраняет список облигаций в CSV (UTF-8 BOM для корректного открытия в Excel)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
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
