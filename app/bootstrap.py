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
