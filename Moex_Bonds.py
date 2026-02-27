#!/usr/bin/env python3
"""Скрипт выгрузки облигаций MOEX в Excel с YAML-конфигом, логами и интерактивным прогрессом."""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml

DEFAULT_URL = (
    "https://iss.moex.com/iss/apps/infogrid/emission/rates.csv?"
    "sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,"
    "stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&"
    "iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&"
    "iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
)
DEFAULT_CONFIG_PATH = Path("config/moex_bonds.yaml")


class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"


@dataclass
class AppConfig:
    url: str
    output: Path
    sheet: str
    timeout_sec: int
    log_path: Path
    cache_path: Path
    cache_ttl_sec: int
    width_sample_rows: int


class ConsoleProgress:
    """Интерактивный прогресс-бар с цветами и спиннером."""

    def __init__(self, total_steps: int) -> None:
        self.total_steps = total_steps
        self.width = 28
        self.spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.spin_idx = 0

    def update(self, step: int, message: str) -> None:
        ratio = max(0.0, min(1.0, step / self.total_steps))
        filled = int(self.width * ratio)
        bar = f"{Ansi.GREEN}{'█' * filled}{Ansi.DIM}{'-' * (self.width - filled)}{Ansi.RESET}"
        pct = int(ratio * 100)
        print(
            f"\r{Ansi.BOLD}[{bar}{Ansi.BOLD}] {step:>2}/{self.total_steps} ({pct:>3}%) {Ansi.CYAN}{message:60}{Ansi.RESET}",
            end="",
            flush=True,
        )
        if step == self.total_steps:
            print()

    def pulse(self, message: str) -> None:
        spin = self.spinner[self.spin_idx % len(self.spinner)]
        self.spin_idx += 1
        print(f"\r{Ansi.BLUE}{spin}{Ansi.RESET} {message:90}", end="", flush=True)

    @staticmethod
    def done_line() -> None:
        print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Загрузить CSV MOEX и сохранить в Excel с форматированием")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к YAML-конфигу")
    return parser.parse_args()


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        raise FileNotFoundError(f"Не найден YAML-конфиг: {path}")

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError("YAML-конфиг должен быть словарём (mapping).")

    return AppConfig(
        url=str(_deep_get(loaded, "source", "url", default=DEFAULT_URL)),
        output=Path(str(_deep_get(loaded, "output", "excel_path", default="Moex_Bonds.xlsx"))),
        sheet=str(_deep_get(loaded, "output", "sheet_name", default="MOEX_BONDS")),
        timeout_sec=int(_deep_get(loaded, "network", "timeout_sec", default=60)),
        log_path=Path(str(_deep_get(loaded, "logging", "path", default="logs/Moex_Bonds.log"))),
        cache_path=Path(str(_deep_get(loaded, "cache", "csv_path", default="logs/cache/moex_rates.csv"))),
        cache_ttl_sec=int(_deep_get(loaded, "cache", "ttl_sec", default=3600)),
        width_sample_rows=int(_deep_get(loaded, "performance", "width_sample_rows", default=350)),
    )


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

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

    return logger


def _detect_delimiter(header_line: str) -> str:
    if "\t" in header_line:
        return "\t"
    if ";" in header_line:
        return ";"
    return ","


def _extract_table_text(csv_text: str) -> str:
    text = csv_text.replace("\r\n", "\n")
    lines = text.split("\n")
    header_idx = next((i for i, line in enumerate(lines) if line.startswith("SECID")), None)
    if header_idx is None:
        raise ValueError("Не найдена строка заголовков SECID.")
    relevant_lines = [ln for ln in lines[header_idx:] if ln.strip()]
    return "\n".join(relevant_lines)


def _read_csv_text_to_df(table_text: str) -> pd.DataFrame:
    delimiter = _detect_delimiter(table_text.split("\n", 1)[0])
    return pd.read_csv(io.StringIO(table_text), sep=delimiter, decimal=",", dtype=str)


def download_rates(config: AppConfig, logger: logging.Logger) -> pd.DataFrame:
    use_cache = config.cache_path.exists() and (time.time() - config.cache_path.stat().st_mtime) <= config.cache_ttl_sec

    if use_cache:
        logger.info("Использую кеш CSV: %s", config.cache_path)
        table_text = _extract_table_text(config.cache_path.read_text(encoding="utf-8"))
        df = _read_csv_text_to_df(table_text)
    else:
        logger.info("Начинаю загрузку CSV: %s", config.url)
        with requests.Session() as session:
            response = session.get(config.url, timeout=config.timeout_sec)
            response.raise_for_status()
            csv_text = response.text

        config.cache_path.parent.mkdir(parents=True, exist_ok=True)
        config.cache_path.write_text(csv_text, encoding="utf-8")

        table_text = _extract_table_text(csv_text)
        df = _read_csv_text_to_df(table_text)

    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    logger.info("CSV обработан. Строк: %s; столбцов: %s", len(df), len(df.columns))
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
        if numeric.notna().mean() > 0.85:
            converted[col] = numeric

    logger.info("Автоконвертация типов завершена")
    return converted


def _estimate_col_width(series: pd.Series, header_name: str, sample_rows: int) -> int:
    if pd.api.types.is_datetime64_any_dtype(series):
        max_len = 10
    else:
        sample = series.dropna().astype(str).head(sample_rows)
        max_len = sample.str.len().max() if not sample.empty else 0
    return int(min(max(10, max(len(header_name), int(max_len)) + 2), 45))


def save_to_excel(
    df: pd.DataFrame,
    output_path: Path,
    sheet_name: str,
    logger: logging.Logger,
    progress: ConsoleProgress,
    sample_rows: int,
) -> None:
    logger.info("Сохраняю Excel: %s", output_path)
    start = time.perf_counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(
        output_path,
        engine="xlsxwriter",
        datetime_format="dd.mm.yyyy",
        engine_kwargs={"options": {"strings_to_numbers": False}},
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

            width = _estimate_col_width(series, column_name, sample_rows)
            worksheet.set_column(col_idx, col_idx, width, col_fmt)

            if (col_idx + 1) % 10 == 0 or (col_idx + 1) == col_count:
                progress.pulse(f"Шаг 4/5: Форматирование колонок {col_idx + 1}/{col_count}")

    elapsed = time.perf_counter() - start
    progress.done_line()
    logger.info("Excel сохранён за %.2f сек", elapsed)


def main() -> int:
    args = parse_args()

    try:
        config = load_config(Path(args.config))
    except Exception as exc:  # noqa: BLE001
        print(f"{Ansi.RED}Ошибка конфигурации: {exc}{Ansi.RESET}")
        return 1

    logger = build_logger(config.log_path)
    progress = ConsoleProgress(total_steps=5)
    run_start = time.perf_counter()

    try:
        progress.update(1, "Загрузка CSV из MOEX/кеша")
        raw_df = download_rates(config, logger)

        progress.update(2, "Очистка пустых колонок")
        raw_df = raw_df.dropna(axis=1, how="all")

        progress.update(3, "Определение форматов данных")
        final_df = auto_convert_types(raw_df, logger)

        progress.update(4, "Экспорт в Excel (xlsxwriter)")
        save_to_excel(final_df, config.output, config.sheet, logger, progress, config.width_sample_rows)

        total_elapsed = time.perf_counter() - run_start
        progress.update(5, f"Готово: {config.output} | {total_elapsed:0.1f}с")
        logger.info("Скрипт завершён успешно за %.2f сек", total_elapsed)
        print(f"{Ansi.GREEN}Готово. Лог: {config.log_path}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка выполнения: %s", exc)
        print(f"\n{Ansi.RED}Ошибка. Подробности см. в лог-файле: {config.log_path}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
