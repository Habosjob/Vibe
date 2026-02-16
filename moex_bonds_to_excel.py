#!/usr/bin/env python3
"""Выгружает облигации Московской биржи (MOEX) в Excel-файл."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

MOEX_BONDS_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)


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
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        worksheet = writer.sheets["MOEX_BONDS"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for idx, column_name in enumerate(df.columns, start=1):
            header_cell = worksheet.cell(row=1, column=idx)
            header_cell.fill = HEADER_FILL
            header_cell.font = HEADER_FONT
            header_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

            column_values = [column_name, *df[column_name].astype(str).tolist()]
            max_length = max((len(value) for value in column_values), default=10)
            worksheet.column_dimensions[get_column_letter(idx)].width = min(max(max_length + 2, 10), 40)

        number_formats = {
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
        date_columns = {"MATDATE"}

        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for cell in row:
                column_name = df.columns[cell.column - 1]
                if column_name in number_formats and isinstance(cell.value, (int, float)):
                    cell.number_format = number_formats[column_name]
                    cell.alignment = Alignment(horizontal="right")
                elif column_name in date_columns and cell.value:
                    cell.number_format = "yyyy-mm-dd"
                    cell.alignment = Alignment(horizontal="center")
                else:
                    cell.alignment = Alignment(horizontal="left")


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
