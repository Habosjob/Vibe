"""Загрузка и валидация конфигурации скринера."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class AppConfig:
    """Настройки приложения с безопасными дефолтами."""

    base_url: str = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
    timeout_seconds: int = 20
    request_delay_seconds: float = 0.15
    retries: int = 3
    page_size: int = 100
    output_file: str = "output/moex_bonds.xlsx"
    raw_dump_enabled: bool = False
    raw_ttl_hours: int = 24
    raw_max_size_mb: int = 50
    exclusions_state_dir: str = "state"
    exclusion_window_days: int = 365
    amortization_workers: int = 8


DEFAULT_CONFIG_PATH = Path("config.yml")


def load_config(path: Path | None = None) -> AppConfig:
    config_path = path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return AppConfig()

    with config_path.open("r", encoding="utf-8") as file:
        data: dict[str, Any] = yaml.safe_load(file) or {}

    return AppConfig(**{**AppConfig().__dict__, **data})
