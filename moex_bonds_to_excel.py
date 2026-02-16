#!/usr/bin/env python3
"""Выгружает облигации Московской биржи (MOEX) в Excel-файл."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import requests

MOEX_BONDS_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"


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
    with requests.Session() as session:
        session.headers.update({"User-Agent": "moex-bonds-export-script/1.0"})
        securities, marketdata = fetch_moex_bonds(session)

    report = build_report_dataframe(
        securities=securities,
        marketdata=marketdata,
        only_active=not args.include_inactive,
    )
    save_to_excel(report, args.output)

    print(f"Готово. Сохранено строк: {len(report)}")
    print(f"Файл: {args.output.resolve()}")


if __name__ == "__main__":
    main()
