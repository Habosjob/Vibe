#!/usr/bin/env python3
"""Выгружает облигации Московской биржи (MOEX) в Excel-файл."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

MOEX_BONDS_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

NUMERIC_COLUMNS = {
    "FACEVALUE": "#,##0.00",
    "COUPONVALUE": "#,##0.00",
    "LAST": "#,##0.00",
    "WAPRICE": "#,##0.00",
    "YIELD": "0.00",
    "VALUE": "#,##0.00",
    "VOLRUR": "#,##0.00",
    "NUMTRADES": "#,##0",
    "COUPONPERIOD": "0",
}

COLUMN_WIDTHS = {
    "SECID": 14,
    "ISIN": 14,
    "SHORTNAME": 16,
    "SECNAME": 34,
    "BOARDID": 9,
    "REGNUMBER": 16,
    "LISTLEVEL": 10,
    "FACEVALUE": 14,
    "FACEUNIT": 10,
    "COUPONVALUE": 14,
    "COUPONPERIOD": 13,
    "MATDATE": 12,
    "STATUS": 10,
    "TRADINGSTATUS": 14,
    "LAST": 10,
    "WAPRICE": 10,
    "YIELD": 10,
    "VALUE": 14,
    "VOLRUR": 14,
    "NUMTRADES": 12,
}

CENTER_COLUMNS = {"BOARDID", "LISTLEVEL", "FACEUNIT", "STATUS", "TRADINGSTATUS", "MATDATE"}


def fetch_moex_bonds(session: requests.Session) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Получает данные по облигациям и рыночным котировкам из ISS MOEX."""
    params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata",
        "securities.columns": (
            "SECID,SHORTNAME,SECNAME,ISIN,REGNUMBER,BOARDID,FACEUNIT,"
            "FACEVALUE,COUPONVALUE,COUPONPERIOD,MATDATE,STATUS,LISTLEVEL"
        ),
        "marketdata.columns": "SECID,LAST,WAPRICE,YIELD,VALUE,VOLRUR,NUMTRADES,TRADINGSTATUS",
    }
    response = session.get(MOEX_BONDS_URL, params=params, timeout=30)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()

    securities = pd.DataFrame(
        payload["securities"]["data"],
        columns=payload["securities"]["columns"],
    )
    marketdata = pd.DataFrame(
        payload["marketdata"]["data"],
        columns=payload["marketdata"]["columns"],
    )
    return securities, marketdata


def build_report_dataframe(
    securities: pd.DataFrame,
    marketdata: pd.DataFrame,
    only_active: bool,
) -> pd.DataFrame:
    """Объединяет справочник бумаг и рыночные данные в единый датафрейм."""
    report = securities.merge(marketdata, on="SECID", how="left")

    if only_active and "STATUS" in report.columns:
        report = report[report["STATUS"] == "A"].copy()

    if "MATDATE" in report.columns:
        report["MATDATE"] = pd.to_datetime(report["MATDATE"], errors="coerce")

    ordered_columns = [
        "SECID",
        "ISIN",
        "SHORTNAME",
        "SECNAME",
        "BOARDID",
        "REGNUMBER",
        "LISTLEVEL",
        "FACEVALUE",
        "FACEUNIT",
        "COUPONVALUE",
        "COUPONPERIOD",
        "MATDATE",
        "STATUS",
        "TRADINGSTATUS",
        "LAST",
        "WAPRICE",
        "YIELD",
        "VALUE",
        "VOLRUR",
        "NUMTRADES",
    ]
    existing_columns = [col for col in ordered_columns if col in report.columns]
    report = report[existing_columns].sort_values(by=["MATDATE", "SECID"], na_position="last")
    return report


def save_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    """Сохраняет датафрейм в Excel."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl", datetime_format="yyyy-mm-dd") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        worksheet = writer.sheets["MOEX_BONDS"]

        worksheet.freeze_panes = "A2"
        worksheet.sheet_view.zoomScale = 110
        worksheet.row_dimensions[1].height = 22

        for idx, column_name in enumerate(df.columns, start=1):
            column_letter = get_column_letter(idx)
            header_cell = worksheet.cell(row=1, column=idx)
            header_cell.fill = HEADER_FILL
            header_cell.font = HEADER_FONT
            header_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=False)
            header_cell.border = BORDER

            worksheet.column_dimensions[column_letter].width = COLUMN_WIDTHS.get(column_name, 14)

        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for cell in row:
                column_name = df.columns[cell.column - 1]
                cell.border = BORDER

                if column_name in NUMERIC_COLUMNS and isinstance(cell.value, (int, float)):
                    cell.number_format = NUMERIC_COLUMNS[column_name]
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif column_name == "MATDATE":
                    if cell.value:
                        cell.number_format = "yyyy-mm-dd"
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                elif column_name in CENTER_COLUMNS:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center")

        if worksheet.max_row >= 2 and worksheet.max_column >= 1:
            table = Table(displayName="MOEX_BONDS_TABLE", ref=worksheet.dimensions)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False,
            )
            worksheet.add_table(table)


def log_step(message: str) -> None:
    """Печатает этап выполнения скрипта с текущим временем."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Собирает все облигации MOEX и сохраняет их в Excel-файл.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("moex_bonds.xlsx"),
        help="Путь до Excel-файла (по умолчанию: moex_bonds.xlsx)",
    )
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Добавить неактивные инструменты (по умолчанию выгружаются только STATUS=A)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_step("Запускаю выгрузку облигаций MOEX...")

    with requests.Session() as session:
        session.headers.update({"User-Agent": "moex-bonds-export-script/1.0"})
        log_step("Отправляю запрос к ISS MOEX...")
        securities, marketdata = fetch_moex_bonds(session)
        log_step(
            f"Данные получены: securities={len(securities)}, marketdata={len(marketdata)}."
        )

    log_step("Формирую итоговую таблицу...")
    report = build_report_dataframe(
        securities=securities,
        marketdata=marketdata,
        only_active=not args.include_inactive,
    )

    log_step(f"Сохраняю Excel-файл: {args.output}")
    save_to_excel(report, args.output)

    log_step(f"Готово. Сохранено строк: {len(report)}")
    log_step(f"Файл: {args.output.resolve()}")


if __name__ == "__main__":
    main()
