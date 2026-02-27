#!/usr/bin/env python3
"""Сортировка выгрузки облигаций с применением фильтров из YAML."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

DEFAULT_CONFIG_PATH = Path("config/moex_bonds.yaml")


class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    CYAN = "\033[36m"


class ConsoleProgress:
    """Интерактивный прогресс-бар с базовой анимацией."""

    def __init__(self, total_steps: int) -> None:
        self.total_steps = total_steps
        self.width = 28

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


@dataclass
class FilterRule:
    name: str
    enabled: bool
    column: str
    equals: str
    reason: str


@dataclass
class SorterConfig:
    input_excel_path: Path
    input_sheet_name: str
    filtered_excel_path: Path
    filtered_sheet_name: str
    dropped_path: Path
    dropped_encoding: str
    log_path: Path
    filters: list[FilterRule]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Отфильтровать облигации и сохранить исключения")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к YAML-конфигу")
    return parser.parse_args()


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def load_config(path: Path) -> SorterConfig:
    if not path.exists():
        raise FileNotFoundError(f"Не найден YAML-конфиг: {path}")

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError("YAML-конфиг должен быть словарём (mapping).")

    filter_items = _deep_get(loaded, "sorter", "filters", default=[]) or []
    filters: list[FilterRule] = []
    for raw_item in filter_items:
        if not isinstance(raw_item, dict):
            continue
        filters.append(
            FilterRule(
                name=str(raw_item.get("name", "unnamed_filter")),
                enabled=bool(raw_item.get("enabled", True)),
                column=str(raw_item.get("column", "")),
                equals=str(raw_item.get("equals", "")),
                reason=str(raw_item.get("reason", "")) or str(raw_item.get("name", "Фильтр")),
            )
        )

    return SorterConfig(
        input_excel_path=Path(str(_deep_get(loaded, "sorter", "input", "excel_path", default="Moex_Bonds.xlsx"))),
        input_sheet_name=str(_deep_get(loaded, "sorter", "input", "sheet_name", default="MOEX_BONDS")),
        filtered_excel_path=Path(
            str(_deep_get(loaded, "sorter", "output", "filtered_excel_path", default="Moex_Bonds_Filtered.xlsx"))
        ),
        filtered_sheet_name=str(_deep_get(loaded, "sorter", "output", "sheet_name", default="MOEX_BONDS")),
        dropped_path=Path(str(_deep_get(loaded, "sorter", "output", "dropped_path", default="DropedBonds.csv"))),
        dropped_encoding=str(_deep_get(loaded, "sorter", "output", "dropped_encoding", default="utf-8-sig")),
        log_path=Path(str(_deep_get(loaded, "sorter", "logging", "path", default="logs/Python_Sorter.log"))),
        filters=filters,
    )


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("Python_Sorter")
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


def apply_filters(df: pd.DataFrame, rules: list[FilterRule], logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    excluded_parts: list[pd.DataFrame] = []
    working_df = df.copy()

    for rule in rules:
        if not rule.enabled:
            logger.info("Фильтр отключен: %s", rule.name)
            continue

        if rule.column not in working_df.columns:
            logger.warning("Фильтр '%s' пропущен: отсутствует колонка '%s'", rule.name, rule.column)
            continue

        column_values = working_df[rule.column].astype(str).str.strip()
        target_value = rule.equals.strip()
        mask = column_values == target_value

        matched = working_df[mask].copy()
        matched["Причина"] = rule.reason
        excluded_parts.append(matched)
        working_df = working_df.loc[~mask].copy()

        logger.info(
            "Фильтр '%s' применен. Исключено строк: %s. Осталось: %s",
            rule.name,
            len(matched),
            len(working_df),
        )

    excluded_df = pd.concat(excluded_parts, ignore_index=True) if excluded_parts else pd.DataFrame()
    return working_df, excluded_df


def save_outputs(kept_df: pd.DataFrame, excluded_df: pd.DataFrame, config: SorterConfig, logger: logging.Logger) -> None:
    config.filtered_excel_path.parent.mkdir(parents=True, exist_ok=True)
    config.dropped_path.parent.mkdir(parents=True, exist_ok=True)

    kept_df.to_excel(config.filtered_excel_path, index=False, sheet_name=config.filtered_sheet_name)
    logger.info("Сохранен отфильтрованный Excel: %s (строк: %s)", config.filtered_excel_path, len(kept_df))

    dropped_columns = ["ISIN", "SECID", "Причина"]
    prepared_dropped = excluded_df.copy()
    for required in dropped_columns:
        if required not in prepared_dropped.columns:
            prepared_dropped[required] = ""

    prepared_dropped.loc[:, dropped_columns].to_csv(
        config.dropped_path,
        index=False,
        sep=";",
        encoding=config.dropped_encoding,
    )
    logger.info("Сохранен файл исключений: %s (строк: %s)", config.dropped_path, len(prepared_dropped))


def main() -> int:
    args = parse_args()
    progress = ConsoleProgress(total_steps=4)
    run_start = time.perf_counter()

    try:
        config = load_config(Path(args.config))
    except Exception as exc:  # noqa: BLE001
        print(f"{Ansi.RED}Ошибка конфигурации Sorter: {exc}{Ansi.RESET}")
        return 1

    logger = build_logger(config.log_path)

    try:
        progress.update(1, "Чтение Moex_Bonds.xlsx")
        frame = pd.read_excel(config.input_excel_path, sheet_name=config.input_sheet_name, dtype=str)
        logger.info("Загружены входные данные: %s строк, %s колонок", len(frame), len(frame.columns))

        progress.update(2, "Применение фильтров")
        kept_df, excluded_df = apply_filters(frame, config.filters, logger)

        progress.update(3, "Сохранение результатов")
        save_outputs(kept_df, excluded_df, config, logger)

        elapsed = time.perf_counter() - run_start
        progress.update(4, f"Готово: kept={len(kept_df)}, dropped={len(excluded_df)} за {elapsed:0.1f}с")
        logger.info("Python_Sorter завершен успешно за %.2f сек", elapsed)
        print(f"{Ansi.GREEN}Sorter завершен. Лог: {config.log_path}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка выполнения Sorter: %s", exc)
        print(f"\n{Ansi.RED}Ошибка Sorter. Подробности см. в логе: {config.log_path}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

