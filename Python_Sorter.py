#!/usr/bin/env python3
"""Сортировка выгрузки облигаций с применением фильтров из YAML."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from Moex_Bonds import auto_convert_types, save_to_excel

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
        print(f"\r{Ansi.CYAN}{spin}{Ansi.RESET} {message:90}", end="", flush=True)

    @staticmethod
    def done_line() -> None:
        print()


@dataclass
class FilterRule:
    name: str
    enabled: bool
    column: str
    equals: str
    reason: str
    ttl_days: int | None
    exclude_until: date | None
    permanent: bool


@dataclass
class SorterConfig:
    input_excel_path: Path
    input_sheet_name: str
    output_excel_path: Path
    output_sheet_name: str
    dropped_path: Path
    dropped_encoding: str
    state_path: Path
    log_path: Path
    filters: list[FilterRule]
    width_sample_rows: int
    heatmap_columns: list[str]
    text_columns: list[str]
    skip_rebuild_if_unchanged: bool


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


def _parse_iso_date(value: Any, *, field_name: str, filter_name: str) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Неверный формат даты '{field_name}' для фильтра '{filter_name}': '{text}'. Ожидается YYYY-MM-DD"
        ) from exc


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
        filter_name = str(raw_item.get("name", "unnamed_filter"))
        ttl_raw = raw_item.get("ttl_days")
        ttl_days = int(ttl_raw) if ttl_raw not in (None, "") else None
        exclude_until = _parse_iso_date(
            raw_item.get("exclude_until"),
            field_name="exclude_until",
            filter_name=filter_name,
        )
        filters.append(
            FilterRule(
                name=filter_name,
                enabled=bool(raw_item.get("enabled", True)),
                column=str(raw_item.get("column", "")),
                equals=str(raw_item.get("equals", "")),
                reason=str(raw_item.get("reason", "")) or filter_name,
                ttl_days=ttl_days,
                exclude_until=exclude_until,
                permanent=bool(raw_item.get("permanent", False)),
            )
        )

    return SorterConfig(
        input_excel_path=Path(str(_deep_get(loaded, "sorter", "input", "excel_path", default="Moex_Bonds.xlsx"))),
        input_sheet_name=str(_deep_get(loaded, "sorter", "input", "sheet_name", default="MOEX_BONDS")),
        output_excel_path=Path(
            str(_deep_get(loaded, "sorter", "output", "excel_path", default="Moex_Bonds.xlsx"))
        ),
        output_sheet_name=str(_deep_get(loaded, "sorter", "output", "sheet_name", default="MOEX_BONDS")),
        dropped_path=Path(str(_deep_get(loaded, "sorter", "output", "dropped_path", default="DropedBonds.csv"))),
        dropped_encoding=str(_deep_get(loaded, "sorter", "output", "dropped_encoding", default="utf-8-sig")),
        state_path=Path(str(_deep_get(loaded, "sorter", "cache", "state_path", default="logs/cache/sorter_state.json"))),
        log_path=Path(str(_deep_get(loaded, "sorter", "logging", "path", default="logs/Python_Sorter.log"))),
        filters=filters,
        width_sample_rows=int(_deep_get(loaded, "performance", "width_sample_rows", default=350)),
        heatmap_columns=list(_deep_get(loaded, "output", "heatmap_columns", default=["YIELD", "EFFECTIVEYIELD", "COUPON"])),
        text_columns=list(_deep_get(loaded, "output", "text_columns", default=["INN"])),
        skip_rebuild_if_unchanged=bool(
            _deep_get(loaded, "sorter", "performance", "skip_rebuild_if_unchanged", default=True)
        ),
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


def _load_dropped_registry(config: SorterConfig, logger: logging.Logger) -> pd.DataFrame:
    if not config.dropped_path.exists():
        logger.info("Файл DropedBonds отсутствует: %s", config.dropped_path)
        return pd.DataFrame(columns=["ISIN", "SECID", "Причина", "Фильтр", "ИсключенДо"])

    dropped_df = pd.read_csv(config.dropped_path, sep=";", dtype=str, encoding=config.dropped_encoding).fillna("")
    for col in ["ISIN", "SECID", "Причина", "Фильтр", "ИсключенДо"]:
        if col not in dropped_df.columns:
            dropped_df[col] = ""
    logger.info("Загружен реестр DropedBonds: %s строк", len(dropped_df))
    return dropped_df[["ISIN", "SECID", "Причина", "Фильтр", "ИсключенДо"]]


def _is_exclusion_active(excluded_until: str, today: date) -> bool:
    value = str(excluded_until or "").strip()
    if not value or value.lower() == "бессрочно":
        return True
    try:
        until = datetime.strptime(value, "%Y-%m-%d").date()
        return today <= until
    except ValueError:
        return True


def _cleanup_expired_registry(dropped_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    today = date.today()
    mask_active = dropped_df["ИсключенДо"].apply(lambda x: _is_exclusion_active(str(x), today))
    active = dropped_df.loc[mask_active].copy()
    expired_count = len(dropped_df) - len(active)
    if expired_count:
        logger.info("Удалены истекшие исключения из DropedBonds: %s", expired_count)
    return active


def _resolve_excluded_until(rule: FilterRule, today: date) -> str:
    if rule.permanent:
        return "Бессрочно"
    if rule.exclude_until:
        return rule.exclude_until.isoformat()
    if rule.ttl_days is not None:
        return (today + timedelta(days=rule.ttl_days)).isoformat()
    return ""


def _build_bond_key(df: pd.DataFrame) -> pd.Series:
    secid_source = df["SECID"] if "SECID" in df.columns else pd.Series("", index=df.index)
    isin_source = df["ISIN"] if "ISIN" in df.columns else pd.Series("", index=df.index)
    secid = secid_source.fillna("").astype(str).str.strip()
    isin = isin_source.fillna("").astype(str).str.strip()
    return secid.where(secid != "", isin)


def _exclude_already_dropped(
    frame: pd.DataFrame,
    dropped_df: pd.DataFrame,
    rules: list[FilterRule],
    logger: logging.Logger,
) -> pd.DataFrame:
    active_reasons = {rule.reason.strip() for rule in rules if rule.enabled}
    active_filters = {rule.name.strip() for rule in rules if rule.enabled}

    if not active_reasons and not active_filters:
        logger.info("Нет активных фильтров: пропускаю исключение по DropedBonds")
        return frame

    registry = dropped_df.copy()
    reason_match = registry["Причина"].fillna("").str.strip().isin(active_reasons)
    filter_match = registry["Фильтр"].fillna("").str.strip().isin(active_filters)
    active_registry = registry.loc[reason_match | filter_match].copy()

    if active_registry.empty:
        logger.info("В DropedBonds нет строк с активными фильтрами")
        return frame

    dropped_keys = set(_build_bond_key(active_registry))
    if not dropped_keys:
        return frame

    frame_keys = _build_bond_key(frame)
    kept = frame.loc[~frame_keys.isin(dropped_keys)].copy()
    logger.info(
        "Исключены ранее отброшенные бумаги по активным фильтрам: %s. Осталось строк: %s",
        len(frame) - len(kept),
        len(kept),
    )
    return kept


def apply_filters(df: pd.DataFrame, rules: list[FilterRule], logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    excluded_parts: list[pd.DataFrame] = []
    working_df = df.copy()

    today = date.today()
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
        matched["Фильтр"] = rule.name
        matched["ИсключенДо"] = _resolve_excluded_until(rule, today)
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


def save_outputs(
    kept_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    config: SorterConfig,
    logger: logging.Logger,
    progress: ConsoleProgress,
) -> None:
    config.output_excel_path.parent.mkdir(parents=True, exist_ok=True)
    config.dropped_path.parent.mkdir(parents=True, exist_ok=True)

    progress.pulse("Подготовка типов данных для сохранения форматирования")
    prepared_kept = auto_convert_types(kept_df, logger, config.text_columns)

    progress.pulse("Сохранение Moex_Bonds.xlsx с форматированием")
    save_to_excel(
        prepared_kept,
        config.output_excel_path,
        config.output_sheet_name,
        logger,
        progress,
        config.width_sample_rows,
        config.heatmap_columns,
    )
    logger.info("Сохранен итоговый Excel с форматированием: %s (строк: %s)", config.output_excel_path, len(prepared_kept))

    dropped_columns = ["ISIN", "SECID", "Причина", "Фильтр", "ИсключенДо"]
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


def _hash_dataframe(df: pd.DataFrame) -> str:
    normalized = df.fillna("").astype(str)
    rows_payload = normalized.to_json(orient="records", force_ascii=False)
    return hashlib.sha256(rows_payload.encode("utf-8")).hexdigest()


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


def main() -> int:
    args = parse_args()
    progress = ConsoleProgress(total_steps=5)
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

        progress.update(2, "Обновление DropedBonds (очистка истекших)")
        dropped_registry = _load_dropped_registry(config, logger)
        dropped_registry = _cleanup_expired_registry(dropped_registry, logger)

        progress.update(3, "Исключение ранее отброшенных + фильтрация")
        frame = _exclude_already_dropped(frame, dropped_registry, config.filters, logger)
        kept_df, excluded_df = apply_filters(frame, config.filters, logger)

        merged_dropped = (
            pd.concat([dropped_registry, excluded_df[["ISIN", "SECID", "Причина", "Фильтр", "ИсключенДо"]]], ignore_index=True)
            if not excluded_df.empty
            else dropped_registry.copy()
        )
        if not merged_dropped.empty:
            merged_dropped["__key"] = _build_bond_key(merged_dropped)
            merged_dropped = (
                merged_dropped.sort_values(["__key", "Причина", "Фильтр"]).drop_duplicates("__key", keep="first")
            )
            merged_dropped = merged_dropped.drop(columns=["__key"])

        state_payload = {
            "input_hash": _hash_dataframe(frame),
            "kept_hash": _hash_dataframe(kept_df),
            "dropped_hash": _hash_dataframe(merged_dropped),
        }

        if config.skip_rebuild_if_unchanged and config.output_excel_path.exists():
            prev_state = _load_state(config.state_path)
            if all(prev_state.get(key) == value for key, value in state_payload.items()):
                elapsed = time.perf_counter() - run_start
                progress.update(5, f"Без изменений: пересборка Excel пропущена | {elapsed:0.1f}с")
                logger.info("Изменений в данных нет — пересборка Excel пропущена.")
                print(f"{Ansi.CYAN}Изменений в данных нет — пересборка Excel пропущена.{Ansi.RESET}")
                return 0

        progress.update(4, "Сохранение результатов")
        save_outputs(kept_df, merged_dropped, config, logger, progress)

        _save_state(
            config.state_path,
            {
                **state_payload,
                "rows_kept": len(kept_df),
                "rows_dropped": len(merged_dropped),
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        elapsed = time.perf_counter() - run_start
        progress.update(5, f"Готово: kept={len(kept_df)}, dropped={len(merged_dropped)} за {elapsed:0.1f}с")
        logger.info("Python_Sorter завершен успешно за %.2f сек", elapsed)
        print(f"{Ansi.GREEN}Sorter завершен. Лог: {config.log_path}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка выполнения Sorter: %s", exc)
        print(f"\n{Ansi.RED}Ошибка Sorter. Подробности см. в логе: {config.log_path}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
