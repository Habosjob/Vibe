from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable

import config


def ensure_directories() -> None:
    for folder in (
        config.DOCS_DIR,
        config.CACHE_DIR,
        config.DB_DIR,
        config.OUTPUT_DIR,
        config.RAW_DIR,
        config.LOGS_DIR,
    ):
        folder.mkdir(parents=True, exist_ok=True)


def setup_logging() -> Path:
    log_path = config.get_log_file_path()
    logger = logging.getLogger()
    logger.setLevel(config.LOG_LEVEL.upper())
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return log_path


def validate_config() -> list[str]:
    errors: list[str] = []
    positive_int_fields: Iterable[tuple[str, int]] = (
        ("LOG_MAX_BYTES", config.LOG_MAX_BYTES),
        ("LOG_BACKUP_COUNT", config.LOG_BACKUP_COUNT),
        ("REQUEST_CONNECT_TIMEOUT_SEC", config.REQUEST_CONNECT_TIMEOUT_SEC),
        ("REQUEST_READ_TIMEOUT_SEC", config.REQUEST_READ_TIMEOUT_SEC),
        ("REQUEST_RETRIES", config.REQUEST_RETRIES),
        ("MAX_CONCURRENT_TASKS", config.MAX_CONCURRENT_TASKS),
        ("CACHE_TTL_SEC", config.CACHE_TTL_SEC),
        ("BATCH_SIZE", config.BATCH_SIZE),
    )

    for name, value in positive_int_fields:
        if not isinstance(value, int) or value <= 0:
            errors.append(f"{name} должно быть целым числом > 0, сейчас: {value!r}")

    if not isinstance(config.REQUEST_BACKOFF_SEC, (int, float)) or config.REQUEST_BACKOFF_SEC <= 0:
        errors.append("REQUEST_BACKOFF_SEC должно быть числом > 0")

    if not isinstance(config.LOG_LEVEL, str) or not config.LOG_LEVEL.strip():
        errors.append("LOG_LEVEL должен быть непустой строкой")

    if not isinstance(config.EXPORT_MOEX_TO_EXCEL, bool):
        errors.append("EXPORT_MOEX_TO_EXCEL должен быть True/False")
    if not isinstance(config.EXPORT_CORPBONDS_TO_EXCEL, bool):
        errors.append("EXPORT_CORPBONDS_TO_EXCEL должен быть True/False")
    if not isinstance(config.SCORE_LIST_ALLOWED_VALUES, list) or not config.SCORE_LIST_ALLOWED_VALUES:
        errors.append("SCORE_LIST_ALLOWED_VALUES должен быть непустым списком")
    else:
        for item in config.SCORE_LIST_ALLOWED_VALUES:
            if not isinstance(item, str) or not item.strip():
                errors.append("SCORE_LIST_ALLOWED_VALUES должен содержать непустые строки")
                break

    for name, value in (
        ("SCREENER_INCLUDE_COLUMNS", config.SCREENER_INCLUDE_COLUMNS),
        ("SCREENER_EXCLUDE_COLUMNS", config.SCREENER_EXCLUDE_COLUMNS),
    ):
        if not isinstance(value, list):
            errors.append(f"{name} должен быть списком")
            continue
        for item in value:
            if not isinstance(item, str):
                errors.append(f"{name} должен содержать только строки")
                break

    if not isinstance(config.SCREENER_FILTERS, dict) or not config.SCREENER_FILTERS:
        errors.append("SCREENER_FILTERS должен быть непустым словарем")
    else:
        for filter_name, filter_data in config.SCREENER_FILTERS.items():
            if not isinstance(filter_data, dict):
                errors.append(f"SCREENER_FILTERS[{filter_name}] должен быть словарем")
                continue
            if not isinstance(filter_data.get("enabled"), bool):
                errors.append(f"SCREENER_FILTERS[{filter_name}]['enabled'] должен быть True/False")
            if "days" in filter_data:
                days = filter_data["days"]
                if not isinstance(days, int) or days < 0:
                    errors.append(f"SCREENER_FILTERS[{filter_name}]['days'] должен быть целым >= 0")

    for name, mapping in (
        ("YTM_KEY_RATE_FORECAST", config.YTM_KEY_RATE_FORECAST),
        ("YTM_INFLATION_FORECAST", config.YTM_INFLATION_FORECAST),
    ):
        if not isinstance(mapping, dict) or not mapping:
            errors.append(f"{name} должен быть непустым словарём")
            continue
        for key, value in mapping.items():
            if not isinstance(key, int) or key < 0:
                errors.append(f"{name}: ключи должны быть целыми >= 0")
                break
            if not isinstance(value, (int, float)):
                errors.append(f"{name}: значения должны быть числами")
                break

    path_fields = (
        ("DOCS_DIR", config.DOCS_DIR),
        ("CACHE_DIR", config.CACHE_DIR),
        ("DB_DIR", config.DB_DIR),
        ("OUTPUT_DIR", config.OUTPUT_DIR),
        ("RAW_DIR", config.RAW_DIR),
        ("LOGS_DIR", config.LOGS_DIR),
    )
    for name, path in path_fields:
        if not isinstance(path, Path):
            errors.append(f"{name} должен быть pathlib.Path")

    return errors


def load_state() -> dict:
    state_path = config.get_state_file_path()
    if not state_path.exists():
        return {"processed_ids": [], "last_stage": "init"}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {"processed_ids": [], "last_stage": "init"}


def save_state(state: dict) -> None:
    config.get_state_file_path().write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
