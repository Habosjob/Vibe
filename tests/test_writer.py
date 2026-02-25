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
    assert "|" not in headers

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
    assert sheet["A1"].alignment.wrap_text is True
    assert sheet.row_dimensions[1].height == 42


def test_save_bonds_file_removes_unwanted_and_merged_columns(tmp_path: Path) -> None:
    target = tmp_path / "output" / "bonds.xlsx"
    bonds = [
        {
            "SECID": "SU26218RMFS6",
            "SHORTNAME": "ОФЗ 26218",
            "FACEUNIT": "SUR",
            "CURRENCYID": "SUR",
            "BOARDID": "TQOB",
            "SECTORID": "GOV",
            "ISIN": "RU000A0JVW48",
            "MATDATE": "2029-05-16",
        }
    ]

    save_bonds_file(str(target), bonds)

    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]
    headers = [sheet.cell(row=2, column=idx).value for idx in range(1, sheet.max_column + 1)]

    assert "BOARDID" not in headers
    assert "SECTORID" not in headers
    assert "FACEUNIT" not in headers
    assert "CURRENCYID" in headers


def test_save_bonds_excel_keeps_headers_unmerged_and_reorders_groups(tmp_path: Path) -> None:
    target = tmp_path / "output" / "bonds.xlsx"
    bonds = [
        {
            "SECID": "SU26218RMFS6",
            "SHORTNAME": "ОФЗ 26218",
            "ISIN": "RU000A0JVW48",
            "CURRENCYID": "SUR",
            "COUPONVALUE": 62.33,
        }
    ]

    save_bonds_file(str(target), bonds)

    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]
    headers = [sheet.cell(row=2, column=idx).value for idx in range(1, sheet.max_column + 1)]

    assert not sheet.merged_cells.ranges

    secid_column = headers.index("SECID") + 1
    currency_column = headers.index("CURRENCYID") + 1
    assert sheet.cell(row=1, column=secid_column).value == "Прочее"
    assert sheet.cell(row=1, column=currency_column).value == "Купоны и номинал"

    grouped_ranges = [
        (dimension.min, dimension.max)
        for dimension in sheet.column_dimensions.values()
        if dimension.outlineLevel == 1
    ]
    assert len(grouped_ranges) >= 3


def test_save_bonds_excel_formats_only_issue_size_columns_and_empty_zero_dates(tmp_path: Path) -> None:
    target = tmp_path / "output" / "bonds.xlsx"
    bonds = [
        {
            "SHORTNAME": "ОФЗ 26218",
            "ISSUESIZE": 1000000000,
            "ISSUESIZEPLACED": 950000000,
            "COUPONVALUE": 62.33,
            "MATDATE": "0000-00-00",
        }
    ]

    save_bonds_file(str(target), bonds)

    workbook = load_workbook(target)
    sheet = workbook["MOEX_BONDS"]
    headers = [sheet.cell(row=2, column=idx).value for idx in range(1, sheet.max_column + 1)]

    issuesize_column = headers.index("ISSUESIZE") + 1
    issuesizeplaced_column = headers.index("ISSUESIZEPLACED") + 1
    coupon_column = headers.index("COUPONVALUE") + 1
    matdate_column = headers.index("MATDATE") + 1

    assert sheet.cell(row=3, column=matdate_column).value in ("", None)
    assert sheet.cell(row=3, column=issuesize_column).number_format == "# ##0"
    assert sheet.cell(row=3, column=issuesizeplaced_column).number_format == "# ##0"
    assert sheet.cell(row=3, column=coupon_column).number_format == "General"


def test_save_bonds_file_with_unsupported_extension(tmp_path: Path, bonds_sample: list[dict[str, object]]) -> None:
    target = tmp_path / "output" / "bonds.txt"

    with pytest.raises(ValueError, match=".xlsx и .csv"):
        save_bonds_file(str(target), bonds_sample)
