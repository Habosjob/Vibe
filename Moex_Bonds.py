#!/usr/bin/env python3
"""Скрипт выгрузки облигаций MOEX в Excel с быстрым форматированием и логированием."""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

DEFAULT_URL = (
    "https://iss.moex.com/iss/apps/infogrid/emission/rates.csv?"
    "sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,"
    "stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&"
    "iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&"
    "iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
)


class ConsoleProgress:
    """Интерактивный прогресс по шагам и подшагам без сторонних зависимостей."""

    def __init__(self, total_steps: int) -> None:
        self.total_steps = total_steps
        self.width = 32
        self.spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.spin_idx = 0

    def update(self, step: int, message: str) -> None:
        ratio = max(0.0, min(1.0, step / self.total_steps))
        filled = int(self.width * ratio)
        bar = "█" * filled + "-" * (self.width - filled)
        print(f"\r[{bar}] {step:>2}/{self.total_steps} | {message:60}", end="", flush=True)
        if step == self.total_steps:
            print()

    def pulse(self, message: str) -> None:
        spin = self.spinner[self.spin_idx % len(self.spinner)]
        self.spin_idx += 1
        print(f"\r{spin} {message:80}", end="", flush=True)

    @staticmethod
    def done_line() -> None:
        print()


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

        if series.dtype != "object":
            continue

        sanitized = series.str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
        numeric = pd.to_numeric(sanitized, errors="coerce")
        if numeric.notna().mean() > 0.8:
            converted[col] = numeric

    logger.info("Автоконвертация типов завершена")
    return converted


def _estimate_col_width(series: pd.Series, header_name: str) -> int:
    if pd.api.types.is_datetime64_any_dtype(series):
        max_len = 10
    else:
        sample = series.dropna().astype(str).head(2000)
        max_len = sample.str.len().max() if not sample.empty else 0
    return int(min(max(10, max(len(header_name), int(max_len)) + 2), 45))


def save_to_excel(df: pd.DataFrame, output_path: Path, sheet_name: str, logger: logging.Logger, progress: ConsoleProgress) -> None:
    logger.info("Сохраняю Excel: %s", output_path)
    start = time.perf_counter()

    with pd.ExcelWriter(
        output_path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"constant_memory": True}},
        datetime_format="dd.mm.yyyy",
    ) as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        row_count = len(df) + 1
        col_count = len(df.columns)

        header_fmt = workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#1F4E78",
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
            }
        )
        alt_row_fmt = workbook.add_format({"bg_color": "#F2F8FC"})
        date_fmt = workbook.add_format({"num_format": "dd.mm.yyyy"})
        int_fmt = workbook.add_format({"num_format": "#,##0"})
        float_fmt = workbook.add_format({"num_format": "#,##0.00"})

        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, row_count - 1, col_count - 1)
        worksheet.set_row(0, 28, header_fmt)

        progress.pulse("Шаг 4/5: Применение полосатой заливки")
        worksheet.conditional_format(
            1,
            0,
            row_count - 1,
            col_count - 1,
            {"type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": alt_row_fmt},
        )

        for col_idx, column_name in enumerate(df.columns):
            series = df[column_name]
            col_fmt = None
            if pd.api.types.is_datetime64_any_dtype(series):
                col_fmt = date_fmt
            elif pd.api.types.is_integer_dtype(series):
                col_fmt = int_fmt
            elif pd.api.types.is_float_dtype(series):
                col_fmt = float_fmt

            width = _estimate_col_width(series, column_name)
            worksheet.set_column(col_idx, col_idx, width, col_fmt)

            if (col_idx + 1) % 8 == 0 or (col_idx + 1) == col_count:
                progress.pulse(f"Шаг 4/5: Форматирование колонок {col_idx + 1}/{col_count}")

    elapsed = time.perf_counter() - start
    progress.done_line()
    logger.info("Excel сохранён за %.2f сек", elapsed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Загрузить CSV MOEX и сохранить в Excel с форматированием")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL CSV")
    parser.add_argument("--output", default="Moex_Bonds.xlsx", help="Путь выходного XLSX")
    parser.add_argument("--sheet", default="MOEX_BONDS", help="Имя листа")
    parser.add_argument("--timeout", type=int, default=60, help="Таймаут HTTP (сек)")
    parser.add_argument("--log", default=f"{Path(__file__).stem}.log", help="Путь лог-файла")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = build_logger(Path(args.log))
    progress = ConsoleProgress(total_steps=5)
    run_start = time.perf_counter()

    try:
        progress.update(1, "Загрузка CSV из MOEX")
        raw_df = download_rates(args.url, args.timeout, logger)

        progress.update(2, "Очистка пустых колонок")
        raw_df = raw_df.dropna(axis=1, how="all")

        progress.update(3, "Определение форматов данных")
        final_df = auto_convert_types(raw_df, logger)

        progress.update(4, "Экспорт в Excel (ускоренный движок xlsxwriter)")
        save_to_excel(final_df, Path(args.output), args.sheet, logger, progress)

        total_elapsed = time.perf_counter() - run_start
        progress.update(5, f"Готово: {args.output} | {total_elapsed:0.1f}с")
        logger.info("Скрипт завершён успешно за %.2f сек", total_elapsed)
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка выполнения: %s", exc)
        print("\nОшибка. Подробности см. в лог-файле.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
