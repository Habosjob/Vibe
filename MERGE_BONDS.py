#!/usr/bin/env python3
"""Склейка Moex_Bonds и Dohod_Bonds по ISIN с сохранением в BondsFinal.xlsx."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from Moex_Bonds import save_to_excel

DEFAULT_CONFIG_PATH = Path("config/moex_bonds.yaml")
DEFAULT_DATE_FORMATS = ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]


class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    CYAN = "\033[36m"


class ConsoleProgress:
    """Интерактивный прогресс-бар с анимацией для долгих операций."""

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
            f"\r{Ansi.BOLD}[{bar}{Ansi.BOLD}] {step:>2}/{self.total_steps} ({pct:>3}%) {Ansi.CYAN}{message:66}{Ansi.RESET}",
            end="",
            flush=True,
        )
        if step == self.total_steps:
            print()

    def pulse(self, message: str) -> None:
        spin = self.spinner[self.spin_idx % len(self.spinner)]
        self.spin_idx += 1
        print(f"\r{Ansi.CYAN}{spin}{Ansi.RESET} {message:95}", end="", flush=True)

    @staticmethod
    def done_line() -> None:
        print()


@dataclass
class MergeConfig:
    moex_excel_path: Path
    moex_sheet_name: str
    dohod_excel_path: Path
    dohod_sheet_name: str | None
    output_excel_path: Path
    output_sheet_name: str
    log_path: Path
    width_sample_rows: int
    heatmap_columns: list[str]
    date_formats: list[str]
    dohod_columns: dict[str, list[str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Склеить Moex_Bonds и Dohod_Bonds по ISIN")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к YAML-конфигу")
    return parser.parse_args()


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def load_config(path: Path) -> MergeConfig:
    if not path.exists():
        raise FileNotFoundError(f"Не найден YAML-конфиг: {path}")

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError("YAML-конфиг должен быть словарём (mapping).")

    merge_raw = _deep_get(loaded, "merge_bonds", default={}) or {}

    default_columns = {
        "Дата": ["Ближайшая дата погашения/оферты (Дата)", "Дата погашения/оферты", "Дата"],
        "Текущий номинал": ["Текущий номинал"],
        "Тип купона": ["Тип купона"],
        "Субординированная (да/нет)": ["Субординированная (да/нет)", "Субординированная"],
        "Базовый индекс (для FRN)": ["Базовый индекс (для FRN)", "Базовый индекс"],
        "Премия/Дисконт к базовому индексу (для FRN)": [
            "Премия/Дисконт к базовому индексу (для FRN)",
            "Премия/Дисконт к базовому индексу",
        ],
    }

    configured_columns = _deep_get(merge_raw, "dohod_columns", default=default_columns)
    if not isinstance(configured_columns, dict):
        raise ValueError("merge_bonds.dohod_columns должен быть словарём target_column -> [aliases]")

    mapped: dict[str, list[str]] = {}
    for target_name, aliases in configured_columns.items():
        if isinstance(aliases, str):
            mapped[str(target_name)] = [aliases]
        elif isinstance(aliases, list):
            mapped[str(target_name)] = [str(item) for item in aliases if str(item).strip()]

    return MergeConfig(
        moex_excel_path=Path(str(_deep_get(merge_raw, "input", "moex_excel_path", default="Moex_Bonds.xlsx"))),
        moex_sheet_name=str(_deep_get(merge_raw, "input", "moex_sheet_name", default="MOEX_BONDS")),
        dohod_excel_path=Path(str(_deep_get(merge_raw, "input", "dohod_excel_path", default="Dohod_Bonds.xlsx"))),
        dohod_sheet_name=_deep_get(merge_raw, "input", "dohod_sheet_name", default=None),
        output_excel_path=Path(str(_deep_get(merge_raw, "output", "excel_path", default="BondsFinal.xlsx"))),
        output_sheet_name=str(_deep_get(merge_raw, "output", "sheet_name", default="BONDS_FINAL")),
        log_path=Path(str(_deep_get(merge_raw, "logging", "path", default="logs/MERGE_BONDS.log"))),
        width_sample_rows=int(_deep_get(loaded, "performance", "width_sample_rows", default=250)),
        heatmap_columns=list(_deep_get(loaded, "output", "heatmap_columns", default=["YIELD", "EFFECTIVEYIELD", "COUPON", "SPREAD"])),
        date_formats=list(_deep_get(loaded, "output", "date_formats", default=DEFAULT_DATE_FORMATS)),
        dohod_columns=mapped or default_columns,
    )


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("MERGE_BONDS")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def _normalize_name(name: str) -> str:
    return " ".join(str(name).replace("\n", " ").split()).strip().lower()


def _resolve_columns(dohod_df: pd.DataFrame, target_to_aliases: dict[str, list[str]], logger: logging.Logger) -> dict[str, str]:
    normalized_actual = {_normalize_name(col): col for col in dohod_df.columns}
    resolved: dict[str, str] = {}

    for target, aliases in target_to_aliases.items():
        chosen: str | None = None
        for alias in aliases:
            actual = normalized_actual.get(_normalize_name(alias))
            if actual:
                chosen = actual
                break
        if chosen:
            resolved[target] = chosen
        else:
            logger.warning("Колонка для '%s' в Dohod_Bonds не найдена. Ищу по алиасам: %s", target, aliases)

    return resolved


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


def _first_non_empty(series: pd.Series) -> str | pd.NA:
    """Вернуть первое непустое значение по группе дубликатов ISIN."""

    normalized = series.astype("string").str.strip()
    non_empty = normalized[normalized.notna() & normalized.ne("")]
    if non_empty.empty:
        return pd.NA
    return non_empty.iloc[0]


def merge_data(config: MergeConfig, logger: logging.Logger, progress: ConsoleProgress) -> pd.DataFrame:
    if not config.moex_excel_path.exists():
        raise FileNotFoundError(f"Не найден Moex Excel: {config.moex_excel_path}")
    if not config.dohod_excel_path.exists():
        raise FileNotFoundError(f"Не найден Dohod Excel: {config.dohod_excel_path}")

    progress.update(2, "Читаю Moex_Bonds.xlsx")
    moex_df = pd.read_excel(config.moex_excel_path, sheet_name=config.moex_sheet_name)
    logger.info("Moex загружен: %s строк, %s колонок", len(moex_df), len(moex_df.columns))

    progress.update(3, "Читаю Dohod_Bonds.xlsx")
    dohod_sheet = config.dohod_sheet_name if config.dohod_sheet_name is not None else 0
    dohod_df = pd.read_excel(config.dohod_excel_path, sheet_name=dohod_sheet, dtype=str)
    logger.info("Dohod загружен: %s строк, %s колонок", len(dohod_df), len(dohod_df.columns))

    if "ISIN" not in moex_df.columns:
        raise ValueError("В Moex_Bonds отсутствует колонка ISIN")
    if "ISIN" not in dohod_df.columns:
        raise ValueError("В Dohod_Bonds отсутствует колонка ISIN")

    resolved = _resolve_columns(dohod_df, config.dohod_columns, logger)
    if not resolved:
        raise ValueError("Не удалось сопоставить ни одной целевой колонки из Dohod_Bonds")

    moex_work = moex_df.copy()
    dohod_work = dohod_df.copy()

    moex_work["ISIN_KEY"] = moex_work["ISIN"].astype("string").str.strip().str.upper()
    dohod_work["ISIN_KEY"] = dohod_work["ISIN"].astype("string").str.strip().str.upper()
    dohod_work = dohod_work[dohod_work["ISIN_KEY"].notna() & dohod_work["ISIN_KEY"].ne("")].copy()

    selected_cols = ["ISIN_KEY", *resolved.values()]
    dohod_for_merge = dohod_work[selected_cols].copy()
    duplicated_isin_count = dohod_for_merge["ISIN_KEY"].duplicated(keep=False).sum()
    if duplicated_isin_count:
        logger.info(
            "В Dohod найдены дубликаты ISIN: %s строк будут схлопнуты с выбором первого непустого значения по каждому полю",
            duplicated_isin_count,
        )

    agg_map = {col: _first_non_empty for col in resolved.values()}
    dohod_selected = dohod_for_merge.groupby("ISIN_KEY", as_index=False).agg(agg_map)
    dohod_selected = dohod_selected.rename(columns={src: target for target, src in resolved.items()})

    progress.update(4, "Склеиваю данные по ISIN")
    final_df = moex_work.merge(dohod_selected, on="ISIN_KEY", how="left")
    final_df = final_df.drop(columns=["ISIN_KEY"])

    if "Дата" in final_df.columns:
        source_dates = final_df["Дата"].copy()
        parsed = _parse_date_series(source_dates, config.date_formats)
        final_df["Дата"] = parsed
        non_empty = source_dates.astype("string").str.strip().replace("", pd.NA).notna().sum()
        logger.info("Колонка 'Дата': успешно распознано %s из %s непустых значений", parsed.notna().sum(), non_empty)

    matched_count = final_df[next(iter(resolved.keys()))].notna().sum()
    logger.info("Сопоставлено строк по ISIN: %s из %s", matched_count, len(final_df))
    if matched_count < len(final_df):
        unmatched = final_df[final_df[next(iter(resolved.keys()))].isna()]["ISIN"].astype("string").head(20).tolist()
        logger.info("Примеры ISIN без матчей в Dohod (до 20): %s", unmatched)

    missing_targets = [target for target in config.dohod_columns if target not in final_df.columns]
    if missing_targets:
        for target in missing_targets:
            final_df[target] = pd.NA
        logger.info("Недостающие целевые колонки добавлены пустыми: %s", ", ".join(missing_targets))

    return final_df


def main() -> int:
    args = parse_args()
    run_started = time.perf_counter()

    try:
        config = load_config(Path(args.config))
    except Exception as exc:  # noqa: BLE001
        print(f"{Ansi.RED}Ошибка конфигурации: {exc}{Ansi.RESET}")
        return 1

    logger = build_logger(config.log_path)
    progress = ConsoleProgress(total_steps=6)

    try:
        progress.update(1, "Проверяю конфиг и входные файлы")
        merged = merge_data(config, logger, progress)

        progress.update(5, "Сохраняю BondsFinal.xlsx c форматированием Moex")
        save_to_excel(
            merged,
            config.output_excel_path,
            config.output_sheet_name,
            logger,
            progress,
            config.width_sample_rows,
            config.heatmap_columns,
        )

        elapsed = time.perf_counter() - run_started
        progress.update(6, f"Готово: {config.output_excel_path} | {elapsed:0.1f}с")
        logger.info("MERGE_BONDS завершен успешно за %.2f сек", elapsed)
        print(f"{Ansi.GREEN}Готово. Лог: {config.log_path}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка MERGE_BONDS: %s", exc)
        print(f"\n{Ansi.RED}Ошибка MERGE_BONDS. См. лог: {config.log_path}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
