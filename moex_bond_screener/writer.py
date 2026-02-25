"""Сохранение результатов в CSV."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def save_bonds_csv(path: str, bonds: list[dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fields = ["SECID", "SHORTNAME", "ISIN", "FACEUNIT", "LISTLEVEL", "PREVLEGALCLOSEPRICE"]
    with target.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(bonds)
