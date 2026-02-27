"""Загрузка и валидация конфигурации скринера."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class AppConfig:
    """Настройки приложения с безопасными дефолтами."""

    base_url: str = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
    timeout_seconds: int = 20
    request_delay_seconds: float = 0.15
    amortization_request_delay_seconds: float = 0.02
    retries: int = 3
    page_size: int = 100
    output_file: str = "output/moex_bonds.xlsx"
    emitents_output_file: str = "output/emitents.xlsx"
    raw_dump_enabled: bool = False
    raw_ttl_hours: int = 24
    raw_max_size_mb: int = 50
    exclusions_state_dir: str = "state"
    exclusion_window_days: int = 365
    qualified_investor_exclusion_days: int = 30
    amortization_workers: int = 8
    dohod_workers: int = 12
    dohod_request_delay_seconds: float = 0.03
    corpbonds_request_delay_seconds: float = 0.0
    dohod_checkpoint_save_every: int = 25
    cbr_key_rate_url: str = "https://www.cbr.ru/hd_base/KeyRate/"
    cbr_ruonia_url: str = "https://www.cbr.ru/hd_base/ruonia/"
    cbr_key_rate_timeout_seconds: int = 10
    z_curve_cbr_url: str = "https://www.cbr.ru/hd_base/zcyc_params/"
    z_curve_moex_url: str = "https://iss.moex.com/iss/engines/stock/zcyc/securities.json?iss.meta=off"
    storage_backend: str = "sqlite"
    sqlite_db_path: str = "screener_state.db"
    force_cache_refresh: bool = False
    floater_cb_rate_current_year: float = 14.0
    floater_cb_rate_next_year: float = 8.5
    floater_cb_rate_plus_one_year: float = 8.0
    linker_inflation_current_year: float = 5.0
    linker_inflation_next_year: float = 4.0
    linker_inflation_plus_one_year: float = 4.0
    floater_ruonia_spread_from_cb_rate: float = -0.5
    floater_z_curve_spread_from_cb_rate: float = -1.0
    floater_cbr_rate_spread_from_cb_rate: float = 0.0


DEFAULT_CONFIG_PATH = Path("config.yml")


def load_config(path: Path | None = None) -> AppConfig:
    config_path = path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return AppConfig()

    with config_path.open("r", encoding="utf-8") as file:
        data: dict[str, Any] = yaml.safe_load(file) or {}

    return AppConfig(**{**asdict(AppConfig()), **data})
