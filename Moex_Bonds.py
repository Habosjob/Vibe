#!/usr/bin/env python3
"""Скрипт выгрузки облигаций MOEX в Excel с YAML-конфигом, логами и интерактивным прогрессом."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
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
DEFAULT_DROP_COLUMNS = [
    "TYPENAME",
    "REGNUMBER",
    "LISTLEVEL",
    "IS_COLLATERAL",
    "IS_EXTERNAL",
    "PRIMARY_BOARDID",
    "PRIMARY_BOARD_TITLE",
    "IS_RII",
    "INCLUDEDBYMOEX",
    "EVENINGSESSION",
    "MORNINGSESSION",
    "WEEKENDSESSION",
    "SUSPENSION_LISTING",
    "ZSPREAD",
    "COUPONDAYSPASSED",
    "COUPONDAYSREMAIN",
    "COUPONLENGTH",
    "INITIALFACEVALUE",
    "STARTDATEMOEX",
    "REPLBOND",
    "DAYSTOREDEMPTION",
    "LOTSIZE",
    "RTL1",
    "RTH1",
    "RTL2",
    "RTH2",
    "RTL3",
    "RTH3",
    "DISCOUNT1",
    "LIMIT1",
    "DISCOUNT2",
    "LIMIT2",
    "DISCOUNT3",
    "DISCOUNTL0",
    "DISCOUNTH0",
    "FULLCOVERED",
    "FULL_COVERED_LIMIT",
]


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
    state_path: Path
    cache_ttl_sec: int
    width_sample_rows: int
    skip_rebuild_if_unchanged: bool
    heatmap_columns: list[str]
    drop_columns: list[str]
    text_columns: list[str]
    move_to_end_columns: list[str]
    date_formats: list[str]


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


@dataclass
class DownloadResult:
    frame: pd.DataFrame
    source_hash: str
    from_cache: bool


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
        state_path=Path(str(_deep_get(loaded, "cache", "state_path", default="logs/cache/moex_rates_state.json"))),
        cache_ttl_sec=int(_deep_get(loaded, "cache", "ttl_sec", default=3600)),
        width_sample_rows=int(_deep_get(loaded, "performance", "width_sample_rows", default=350)),
        skip_rebuild_if_unchanged=bool(_deep_get(loaded, "performance", "skip_rebuild_if_unchanged", default=True)),
        heatmap_columns=list(_deep_get(loaded, "output", "heatmap_columns", default=["YIELD", "EFFECTIVEYIELD", "COUPON"])),
        drop_columns=list(_deep_get(loaded, "output", "drop_columns", default=DEFAULT_DROP_COLUMNS)),
        text_columns=list(_deep_get(loaded, "output", "text_columns", default=["INN"])),
        move_to_end_columns=list(_deep_get(loaded, "output", "move_to_end_columns", default=["WAPRICE"])),
        date_formats=list(
            _deep_get(
                loaded,
                "output",
                "date_formats",
                default=["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"],
            )
        ),
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


def _hash_text(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _save_state(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def download_rates(config: AppConfig, logger: logging.Logger) -> DownloadResult:
    use_cache = config.cache_path.exists() and (time.time() - config.cache_path.stat().st_mtime) <= config.cache_ttl_sec

    if use_cache:
        logger.info("Использую кеш CSV: %s", config.cache_path)
        csv_text = config.cache_path.read_text(encoding="utf-8")
        table_text = _extract_table_text(csv_text)
        df = _read_csv_text_to_df(table_text)
        source_hash = _hash_text(table_text)
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
        source_hash = _hash_text(table_text)

    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    logger.info("CSV обработан. Строк: %s; столбцов: %s", len(df), len(df.columns))
    return DownloadResult(frame=df, source_hash=source_hash, from_cache=use_cache)


def _parse_date_series(series: pd.Series, formats: list[str]) -> pd.Series:
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    cleaned = series.astype("string").str.strip()
    pending = cleaned.notna() & cleaned.ne("")

    for fmt in formats:
        if not pending.any():
            break
        candidate = pd.to_datetime(cleaned[pending], format=fmt, errors="coerce")
        success = candidate.notna()
        if success.any():
            parsed.loc[candidate.index[success]] = candidate.loc[success]
            pending.loc[candidate.index[success]] = False

    if pending.any():
        fallback = pd.to_datetime(cleaned[pending], errors="coerce", dayfirst=True)
        success = fallback.notna()
        if success.any():
            parsed.loc[fallback.index[success]] = fallback.loc[success]

    return parsed


def auto_convert_types(df: pd.DataFrame, logger: logging.Logger, text_columns: list[str], date_formats: list[str]) -> pd.DataFrame:
    converted = df.copy()
    date_keys = ("DATE", "MATDATE", "ISSUEDATE", "OFFERDATE")
    force_text = {name.upper() for name in text_columns}

    for col in converted.columns:
        upper = col.upper()
        series = converted[col]

        if upper in force_text:
            converted[col] = series.where(series.notna(), "")
            continue

        if any(key in upper for key in date_keys):
            parsed_dates = _parse_date_series(series, date_formats)
            converted[col] = parsed_dates
            non_empty = series.astype("string").str.strip().replace("", pd.NA).notna().sum()
            parsed_ok = parsed_dates.notna().sum()
            logger.info("Колонка %s: распознано дат %s из %s", col, parsed_ok, non_empty)
            continue

        if series.dtype != "object":
            continue

        sanitized = series.str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
        numeric = pd.to_numeric(sanitized, errors="coerce")
        if numeric.notna().mean() > 0.85:
            converted[col] = numeric

    logger.info("Автоконвертация типов завершена")
    return converted


def move_columns_to_end(df: pd.DataFrame, columns_to_move: list[str], logger: logging.Logger) -> pd.DataFrame:
    if not columns_to_move:
        return df

    existing = [column for column in columns_to_move if column in df.columns]
    if not existing:
        logger.info("Колонки для переноса в конец не найдены")
        return df

    ordered = [column for column in df.columns if column not in existing] + existing
    logger.info("Переношу в конец колонок: %s", ", ".join(existing))
    return df.loc[:, ordered]


def drop_unneeded_columns(df: pd.DataFrame, drop_columns: list[str], logger: logging.Logger) -> pd.DataFrame:
    if not drop_columns:
        return df

    existing = [column for column in drop_columns if column in df.columns]
    missing = [column for column in drop_columns if column not in df.columns]

    if existing:
        logger.info("Удаляю %s служебных столбцов: %s", len(existing), ", ".join(existing))
        df = df.drop(columns=existing)
    else:
        logger.info("Служебные столбцы для удаления не найдены в текущей выгрузке")

    if missing:
        logger.info("Отсутствовали в выгрузке (пропущены): %s", ", ".join(missing))

    return df


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
    heatmap_columns: list[str],
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

        worksheet.set_row(0, 28, header_fmt)
        worksheet.freeze_panes(1, 0)

        table_columns = [{"header": column_name} for column_name in df.columns]
        worksheet.add_table(
            0,
            0,
            row_count - 1,
            col_count - 1,
            {
                "style": "Table Style Medium 13",
                "columns": table_columns,
                "autofilter": True,
            },
        )

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

        heatmap_targets_upper = {column.upper() for column in heatmap_columns}
        heatmap_targets = [column for column in df.columns if column.upper() in heatmap_targets_upper]
        for col_name in heatmap_targets:
            col_idx = df.columns.get_loc(col_name)
            progress.pulse(f"Шаг 4/5: Цветовая шкала {col_name}")
            worksheet.conditional_format(
                1,
                col_idx,
                row_count - 1,
                col_idx,
                {
                    "type": "3_color_scale",
                    "min_color": "#F8696B",
                    "mid_color": "#FFEB84",
                    "max_color": "#63BE7B",
                },
            )

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
        rates = download_rates(config, logger)
        raw_df = rates.frame

        if config.skip_rebuild_if_unchanged and config.output.exists():
            state = _load_state(config.state_path)
            if state.get("source_hash") == rates.source_hash:
                total_elapsed = time.perf_counter() - run_start
                progress.update(5, f"Без изменений: {config.output} | {total_elapsed:0.1f}с")
                logger.info("Данные не изменились (hash=%s). Пересборка Excel пропущена.", rates.source_hash)
                print(f"{Ansi.YELLOW}Изменений в данных нет — пересборка Excel пропущена.{Ansi.RESET}")
                return 0

        progress.update(2, "Очистка мусорных и пустых колонок")
        raw_df = drop_unneeded_columns(raw_df, config.drop_columns, logger)
        raw_df = raw_df.dropna(axis=1, how="all")
        raw_df = move_columns_to_end(raw_df, config.move_to_end_columns, logger)

        progress.update(3, "Определение форматов данных")
        final_df = auto_convert_types(raw_df, logger, config.text_columns, config.date_formats)

        progress.update(4, "Экспорт в Excel (xlsxwriter)")
        save_to_excel(
            final_df,
            config.output,
            config.sheet,
            logger,
            progress,
            config.width_sample_rows,
            config.heatmap_columns,
        )

        _save_state(
            config.state_path,
            {
                "source_hash": rates.source_hash,
                "rows": len(final_df),
                "columns": len(final_df.columns),
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "from_cache": rates.from_cache,
            },
        )

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
