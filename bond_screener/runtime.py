from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_RAW_TTL_DAYS = 7


@dataclass(slots=True)
class RunSummary:
    processed: int = 0
    filtered: int = 0
    errors: int = 0
    removed_raw_files: int = 0


def default_configs() -> dict[str, dict[str, Any]]:
    return {
        "config.yml": {
            "app": {
                "name": "bond_screener",
                "timezone": "Europe/Moscow",
            },
            "logging": {
                "level": "INFO",
                "file": "logs/latest.log",
            },
            "raw": {
                "enabled": True,
                "ttl_days": DEFAULT_RAW_TTL_DAYS,
            },
            "output": {
                "excel_file": "out/bond_screener.xlsx",
                "screen_basic_excel": "out/screen_basic.xlsx",
            },
            "database": {
                "path": "data/bond_screener.sqlite",
            },
            "providers": {
                "moex_iss": {
                    "limit": 100,
                    "q": None,
                    "cache_ttl_seconds": 1800,
                    "cashflows_cache_ttl_seconds": 86400,
                    "cashflows_concurrency": 5,
                    "rate_limit_per_sec": 2.0,
                }
            },
        },
        "scenarios.yml": {
            "default": {
                "description": "Базовый сценарий отбора (заглушка)",
                "min_rating": "BB-",
                "max_duration_years": 5,
            }
        },
        "allowlist.yml": {
            "isins": [],
            "emitents": [],
        },
        "issuer_links.yml": {
            "issuers": [],
        },
        "portfolio.yml": {
            "positions": [],
        },
    }


def ensure_runtime_dirs(base_dir: Path) -> dict[str, Path]:
    dirs = {
        "config": base_dir / "config",
        "out": base_dir / "out",
        "logs": base_dir / "logs",
        "raw": base_dir / "raw",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def ensure_default_configs(config_dir: Path) -> list[Path]:
    created: list[Path] = []
    for filename, payload in default_configs().items():
        config_path = config_dir / filename
        if config_path.exists():
            continue
        with config_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(payload, f, allow_unicode=True, sort_keys=False)
        created.append(config_path)
    return created


def load_config(config_dir: Path) -> dict[str, Any]:
    config_path = config_dir / "config.yml"
    if not config_path.exists():
        return default_configs()["config.yml"]
    with config_path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    return loaded


def setup_logging(log_file: Path, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("bond_screener")
    logger.handlers.clear()
    logger.setLevel(level.upper())

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def cleanup_raw_by_ttl(raw_dir: Path, ttl_days: int, logger: logging.Logger) -> int:
    ttl_days = max(ttl_days, 0)
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    removed = 0

    for item in raw_dir.iterdir():
        mtime = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc)
        if mtime >= cutoff:
            continue
        if item.is_dir():
            shutil.rmtree(item)
            removed += 1
        else:
            item.unlink(missing_ok=True)
            removed += 1

    logger.info("Очистка raw завершена: удалено объектов=%s, ttl_days=%s", removed, ttl_days)
    return removed


def run(base_dir: Path) -> tuple[RunSummary, float]:
    started = time.perf_counter()
    dirs = ensure_runtime_dirs(base_dir)
    created_configs = ensure_default_configs(dirs["config"])

    config = load_config(dirs["config"])
    raw_ttl_days = int(config.get("raw", {}).get("ttl_days", DEFAULT_RAW_TTL_DAYS))
    log_file = base_dir / str(config.get("logging", {}).get("file", "logs/latest.log"))

    logger = setup_logging(log_file, config.get("logging", {}).get("level", "INFO"))
    logger.info("Запуск bond_screener")
    if created_configs:
        logger.info("Созданы дефолтные конфиги: %s", ", ".join(p.name for p in created_configs))
    else:
        logger.info("Дефолтные конфиги уже существуют")

    stages = [
        "1) Загрузка источников (заглушка)",
        "2) Очистка и нормализация данных (заглушка)",
        "3) Скоринг и фильтрация (заглушка)",
        "4) Экспорт в Excel (заглушка)",
    ]

    logger.info("Этапы выполнения:")
    for stage in stages:
        logger.info(stage)

    summary = RunSummary()
    summary.removed_raw_files = cleanup_raw_by_ttl(dirs["raw"], raw_ttl_days, logger)
    elapsed = time.perf_counter() - started
    return summary, elapsed
