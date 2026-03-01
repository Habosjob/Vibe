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
    emitents_page_size: int
    emitents_max_pages: int


@dataclass(frozen=True)
class Stage2Settings:
    dropped_ui_filename: str


@dataclass(frozen=True)
class Stage3BondizationSettings:
    enabled: bool
    include_offers: bool
    from_date: str
    till: str


@dataclass(frozen=True)
class Stage3MoexSettings:
    enabled: bool
    ttl_hours: int
    concurrency: int
    engine: str
    market: str
    boards: list[str]
    page_size: int
    bondization: Stage3BondizationSettings


@dataclass(frozen=True)
class Stage3DohodSettings:
    enabled: bool
    ttl_hours: int
    concurrency: int
    min_delay_s: float
    page_timeout_s: float
    base_url: str
    user_agent: str
    use_playwright: bool


@dataclass(frozen=True)
class Stage3Settings:
    enabled: bool
    run_sources_in_parallel: bool
    ttl_hours: int
    batch_size: int
    moex: Stage3MoexSettings
    dohod: Stage3DohodSettings


@dataclass(frozen=True)
class AppSettings:
    excel_debug: bool
    excel_debug_exports: list[str]
    net: NetSettings
    db: DbSettings
    stage1: Stage1Settings
    stage2: Stage2Settings
    stage3: Stage3Settings
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
        stage1=Stage1Settings(
            ttl_hours=int(raw.get("stage1", {}).get("ttl_hours", 24)),
            emitents_page_size=int(raw.get("stage1", {}).get("emitents_page_size", 100)),
            emitents_max_pages=int(raw.get("stage1", {}).get("emitents_max_pages", 1000)),
        ),
        stage2=Stage2Settings(
            dropped_ui_filename=str(raw.get("stage2", {}).get("dropped_ui_filename", "Dropped_bonds.xlsx")),
        ),
        stage3=Stage3Settings(
            enabled=bool(raw.get("stage3", {}).get("enabled", True)),
            run_sources_in_parallel=bool(raw.get("stage3", {}).get("run_sources_in_parallel", True)),
            ttl_hours=int(raw.get("stage3", {}).get("ttl_hours", 6)),
            batch_size=int(raw.get("stage3", {}).get("batch_size", 200)),
            moex=Stage3MoexSettings(
                enabled=bool(raw.get("stage3", {}).get("moex", {}).get("enabled", True)),
                ttl_hours=int(raw.get("stage3", {}).get("moex", {}).get("ttl_hours", raw.get("stage3", {}).get("ttl_hours", 6))),
                concurrency=int(raw.get("stage3", {}).get("moex", {}).get("concurrency", 20)),
                engine=str(raw.get("stage3", {}).get("moex", {}).get("engine", "stock")),
                market=str(raw.get("stage3", {}).get("moex", {}).get("market", "bonds")),
                boards=list(raw.get("stage3", {}).get("moex", {}).get("boards", [])),
                page_size=int(raw.get("stage3", {}).get("moex", {}).get("page_size", 100)),
                bondization=Stage3BondizationSettings(
                    enabled=bool(raw.get("stage3", {}).get("moex", {}).get("bondization", {}).get("enabled", True)),
                    include_offers=bool(raw.get("stage3", {}).get("moex", {}).get("bondization", {}).get("include_offers", True)),
                    from_date=str(raw.get("stage3", {}).get("moex", {}).get("bondization", {}).get("from", "")),
                    till=str(raw.get("stage3", {}).get("moex", {}).get("bondization", {}).get("till", "")),
                ),
            ),
            dohod=Stage3DohodSettings(
                enabled=bool(raw.get("stage3", {}).get("dohod", {}).get("enabled", True)),
                ttl_hours=int(raw.get("stage3", {}).get("dohod", {}).get("ttl_hours", raw.get("stage3", {}).get("ttl_hours", 6))),
                concurrency=int(raw.get("stage3", {}).get("dohod", {}).get("concurrency", 5)),
                min_delay_s=float(raw.get("stage3", {}).get("dohod", {}).get("min_delay_s", 0.3)),
                page_timeout_s=float(raw.get("stage3", {}).get("dohod", {}).get("page_timeout_s", 30)),
                base_url=str(raw.get("stage3", {}).get("dohod", {}).get("base_url", "https://analytics.dohod.ru/bond/{isin}")),
                user_agent=str(
                    raw.get("stage3", {}).get("dohod", {}).get("user_agent", "Mozilla/5.0 (compatible; bond_screener/1.0)")
                ),
                use_playwright=bool(raw.get("stage3", {}).get("dohod", {}).get("use_playwright", False)),
            ),
        ),
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
