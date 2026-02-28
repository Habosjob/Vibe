from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from core.paths import ProjectPaths


@dataclass(frozen=True)
class TimeoutSettings:
    connect_s: float
    read_s: float
    write_s: float
    pool_s: float


@dataclass(frozen=True)
class RetrySettings:
    max_attempts: int
    backoff_initial_s: float
    backoff_max_s: float
    jitter_s: float


@dataclass(frozen=True)
class NetSettings:
    concurrency: int
    timeout: TimeoutSettings
    retry: RetrySettings
    cache_ttl_s_default: int


@dataclass(frozen=True)
class DbSettings:
    filename: str


@dataclass(frozen=True)
class Stage1Settings:
    ttl_hours: int


@dataclass(frozen=True)
class AppSettings:
    excel_debug: bool
    excel_debug_exports: list[str]
    net: NetSettings
    db: DbSettings
    stage1: Stage1Settings
    paths: ProjectPaths


@dataclass(frozen=True)
class ResetSettings:
    reset_mode: list[str]
    cache_clear_all: bool
    db_delete_db_file: bool
    checkpoints_clear_all: bool
    ttl_reset_fetched_at_tables: list[str]


def _read_yaml(file_path: Path) -> dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_settings(project_root: Path | None = None) -> AppSettings:
    root = (project_root or Path(__file__).resolve().parents[1]).resolve()
    config_file = root / "config" / "config.yaml"
    raw = _read_yaml(config_file)

    paths_cfg = raw.get("paths", {})
    db_filename = raw["db"]["filename"]
    paths = ProjectPaths(
        root=root,
        logs_dir=root / paths_cfg.get("logs_dir", "logs"),
        db_file=root / db_filename,
        cache_http_dir=root / paths_cfg.get("cache_http_dir", "cache/http"),
        cache_checkpoints_dir=root / paths_cfg.get("cache_checkpoints_dir", "cache/checkpoints"),
        source_xlsx_dir=root / paths_cfg.get("source_xlsx_dir", "source/xlsx"),
        source_parquet_dir=root / paths_cfg.get("source_parquet_dir", "source/parquet"),
        config_file=config_file,
        reset_file=root / "config" / "reset.yaml",
    )
    paths.ensure_dirs()

    timeout = TimeoutSettings(**raw["net"]["timeout"])
    retry = RetrySettings(**raw["net"]["retry"])

    return AppSettings(
        excel_debug=bool(raw.get("excel_debug", False)),
        excel_debug_exports=list(raw.get("excel_debug_exports", [])),
        net=NetSettings(
            concurrency=int(raw["net"]["concurrency"]),
            timeout=timeout,
            retry=retry,
            cache_ttl_s_default=int(raw["net"]["cache_ttl_s_default"]),
        ),
        db=DbSettings(filename=db_filename),
        stage1=Stage1Settings(ttl_hours=int(raw.get("stage1", {}).get("ttl_hours", 24))),
        paths=paths,
    )


def load_reset_settings(settings: AppSettings) -> ResetSettings:
    raw = _read_yaml(settings.paths.reset_file)
    return ResetSettings(
        reset_mode=list(raw.get("reset_mode", [])),
        cache_clear_all=bool(raw.get("cache", {}).get("clear_all", False)),
        db_delete_db_file=bool(raw.get("db", {}).get("delete_db_file", False)),
        checkpoints_clear_all=bool(raw.get("checkpoints", {}).get("clear_all", False)),
        ttl_reset_fetched_at_tables=list(raw.get("ttl", {}).get("reset_fetched_at_tables", [])),
    )


def reset_settings_to_safe_default(settings: AppSettings) -> None:
    safe_payload = {
        "reset_mode": [],
        "cache": {"clear_all": False},
        "db": {"delete_db_file": False},
        "checkpoints": {"clear_all": False},
        "ttl": {"reset_fetched_at_tables": []},
    }
    with settings.paths.reset_file.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(safe_payload, fh, allow_unicode=True, sort_keys=False)
