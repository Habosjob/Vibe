#!/usr/bin/env python3
"""Скрипт выгрузки облигаций MOEX в Excel с форматированием и логированием."""

from __future__ import annotations

import argparse
import io
import logging
import sys
from pathlib import Path

import pandas as pd
import requests
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

DEFAULT_URL = (
    "https://iss.moex.com/iss/apps/infogrid/emission/rates.csv?"
    "sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,"
    "stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&"
    "iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&"
    "iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
)

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ALT_ROW_FILL = PatternFill("solid", fgColor="F2F8FC")


def build_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("Moex_Bonds")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def print_progress(step: int, total: int, message: str) -> None:
    width = 30
    ratio = max(0.0, min(1.0, step / total))
    filled = int(width * ratio)
    bar = "█" * filled + "-" * (width - filled)
    print(f"\r[{bar}] {step:>2}/{total} | {message}", end="", flush=True)
    if step == total:
        print()


def _detect_delimiter(header_line: str) -> str:
    if "\t" in header_line:
        return "\t"
    if ";" in header_line:
        return ";"
    return ","


def download_rates(url: str, timeout: int, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Начинаю загрузку CSV: %s", url)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    text = response.text.replace("\r\n", "\n")
    lines = text.split("\n")
    header_idx = next((i for i, line in enumerate(lines) if line.startswith("SECID")), None)
    if header_idx is None:
        raise ValueError("Не найдена строка заголовков SECID.")

    relevant_lines = [ln for ln in lines[header_idx:] if ln.strip()]
    delimiter = _detect_delimiter(relevant_lines[0])

    df = pd.read_csv(io.StringIO("\n".join(relevant_lines)), sep=delimiter, decimal=",", dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    logger.info("CSV загружен. Размер: %s байт; строк: %s; столбцов: %s", len(response.content), len(df), len(df.columns))
    return df


def auto_convert_types(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    converted = df.copy()
    date_keys = ("DATE", "MATDATE", "ISSUEDATE", "OFFERDATE")

    for col in converted.columns:
        upper = col.upper()
        series = converted[col]

        if any(key in upper for key in date_keys):
            converted[col] = pd.to_datetime(series, format="%d.%m.%Y", errors="coerce")
            continue

        sanitized = series.astype(str).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
        if sanitized.str.fullmatch(r"-?\d+(\.\d+)?").mean() > 0.8:
            converted[col] = pd.to_numeric(sanitized, errors="coerce")

    logger.info("Автоконвертация типов завершена")
    return converted


def _set_column_widths(ws) -> None:
    for idx, column_cells in enumerate(ws.iter_cols(min_row=1, max_row=ws.max_row), start=1):
        max_len = 0
        for cell in column_cells:
            if cell.value is not None:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[get_column_letter(idx)].width = min(max(10, max_len + 2), 45)


def apply_styles(ws, df: pd.DataFrame, logger: logging.Logger) -> None:
    logger.info("Применяю стили к листу Excel")
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Подсветка чётных строк (минимально затратная для читаемости)
    for row_idx in range(2, ws.max_row + 1, 2):
        for cell in ws[row_idx]:
            cell.fill = ALT_ROW_FILL

    # Форматы только по типам колонок
    for col_idx, column_name in enumerate(df.columns, start=1):
        series = df[column_name]
        if pd.api.types.is_datetime64_any_dtype(series):
            for row_idx in range(2, ws.max_row + 1):
                ws.cell(row=row_idx, column=col_idx).number_format = "DD.MM.YYYY"
        elif pd.api.types.is_integer_dtype(series):
            for row_idx in range(2, ws.max_row + 1):
                ws.cell(row=row_idx, column=col_idx).number_format = "#,##0"
        elif pd.api.types.is_float_dtype(series):
            for row_idx in range(2, ws.max_row + 1):
                ws.cell(row=row_idx, column=col_idx).number_format = "#,##0.00"

    _set_column_widths(ws)


def save_to_excel(df: pd.DataFrame, output_path: Path, sheet_name: str, logger: logging.Logger) -> None:
    logger.info("Сохраняю Excel: %s", output_path)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.book[sheet_name]
        apply_styles(ws, df, logger)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Загрузить CSV MOEX и сохранить в Excel с форматированием")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL CSV")
    parser.add_argument("--output", default="Moex_Bonds.xlsx", help="Путь выходного XLSX")
    parser.add_argument("--sheet", default="MOEX_BONDS", help="Имя листа")
    parser.add_argument("--timeout", type=int, default=60, help="Таймаут HTTP (сек)")
    parser.add_argument("--log", default="Moex_Bonds.log", help="Путь лог-файла")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = build_logger(Path(args.log))
    total = 5

    try:
        print_progress(1, total, "Загрузка CSV из MOEX")
        raw_df = download_rates(args.url, args.timeout, logger)

        print_progress(2, total, "Очистка пустых колонок")
        raw_df = raw_df.dropna(axis=1, how="all")

        print_progress(3, total, "Определение форматов данных")
        final_df = auto_convert_types(raw_df, logger)

        print_progress(4, total, "Экспорт в Excel")
        save_to_excel(final_df, Path(args.output), args.sheet, logger)

        print_progress(5, total, f"Готово: {args.output}")
        logger.info("Скрипт завершён успешно")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка выполнения: %s", exc)
        print("\nОшибка. Подробности см. в лог-файле.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
