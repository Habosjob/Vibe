from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import load_workbook

from moex_bond_screener.writer import save_bonds_file


@pytest.fixture
def bonds_sample() -> list[dict[str, object]]:
    return [
        {
            "SECID": "SU26218RMFS6",
            "SHORTNAME": "ОФЗ 26218",
            "ISIN": "RU000A0JVW48",
            "FACEUNIT": "SUR",
            "LISTLEVEL": 1,
            "PREVLEGALCLOSEPRICE": 99.13,
        }
    ]


def test_save_bonds_file_as_excel(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.xlsx"

    save_bonds_file(str(target), bonds_sample)

    assert target.exists()
    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]

    assert sheet["A1"].value == "SECID"
    assert sheet["B2"].value == "ОФЗ 26218"


def test_save_bonds_file_as_csv_with_bom(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.csv"

    save_bonds_file(str(target), bonds_sample)

    assert target.exists()
    content = target.read_text(encoding="utf-8-sig")
    assert "ОФЗ 26218" in content


def test_save_bonds_file_with_unsupported_extension(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.txt"

    with pytest.raises(ValueError, match=".xlsx и .csv"):
        save_bonds_file(str(target), bonds_sample)
