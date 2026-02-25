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
            "MATDATE": "2029-05-16",
        }
    ]


def test_save_bonds_file_as_excel(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.xlsx"

    save_bonds_file(str(target), bonds_sample)

    assert target.exists()
    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]

    assert sheet["A1"].value == "Служебная информация"
    headers = [sheet.cell(row=2, column=idx).value for idx in range(1, sheet.max_column + 1)]

    assert "SECID" in headers
    assert "SHORTNAME" in headers
    assert "CURRENCYID" in headers
    assert "PREVLEGALCLOSEPRICE" in headers
    assert "MATDATE" in headers
    assert "|" in headers

    shortname_column = headers.index("SHORTNAME") + 1
    matdate_column = headers.index("MATDATE") + 1
    assert sheet.cell(row=3, column=shortname_column).value == "ОФЗ 26218"
    assert sheet.cell(row=3, column=matdate_column).value == "16.05.2029"


def test_save_bonds_file_as_csv_with_bom(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.csv"

    save_bonds_file(str(target), bonds_sample)

    assert target.exists()
    content = target.read_text(encoding="utf-8-sig")
    assert "ОФЗ 26218" in content
    assert "MATDATE" in content
    assert "16.05.2029" in content


def test_save_bonds_excel_applies_readable_formatting(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.xlsx"

    save_bonds_file(str(target), bonds_sample)

    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]

    assert sheet.freeze_panes == "A3"
    assert sheet.auto_filter.ref == f"A2:{sheet.cell(row=2, column=sheet.max_column).column_letter}3"
    assert sheet["A1"].font.bold is True
    assert sheet.column_dimensions["B"].width >= 10
    assert sheet["A3"].fill.fgColor.rgb == "00F2F7FF"


def test_save_bonds_file_removes_unwanted_and_merged_columns(tmp_path: Path) -> None:
    target = tmp_path / "output" / "bonds.xlsx"
    bonds = [
        {
            "SECID": "SU26218RMFS6",
            "SHORTNAME": "ОФЗ 26218",
            "FACEUNIT": "SUR",
            "CURRENCYID": "SUR",
            "BOARDID": "TQOB",
            "ISIN": "RU000A0JVW48",
            "MATDATE": "2029-05-16",
        }
    ]

    save_bonds_file(str(target), bonds)

    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]
    headers = [sheet.cell(row=2, column=idx).value for idx in range(1, sheet.max_column + 1)]

    assert "BOARDID" not in headers
    assert "FACEUNIT" not in headers
    assert "CURRENCYID" in headers


def test_save_bonds_file_with_unsupported_extension(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.txt"

    with pytest.raises(ValueError, match=".xlsx и .csv"):
        save_bonds_file(str(target), bonds_sample)
