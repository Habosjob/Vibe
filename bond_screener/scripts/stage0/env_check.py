from __future__ import annotations

import platform
import sqlite3
from dataclasses import asdict, dataclass

import pandas as pd

from core.db import init_db
from core.excel_debug import export_dataframe
from core.logging import get_script_logger
from core.settings import load_settings


@dataclass
class EnvCheckResult:
    python_version: str
    platform: str
    sqlite_version: str
    db_file: str
    config_file: str
    status: str


def run() -> str:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage0_env_check.log", "stage0.env_check")

    logger.info("Старт проверки окружения")
    init_db(settings)

    result = EnvCheckResult(
        python_version=platform.python_version(),
        platform=platform.platform(),
        sqlite_version=sqlite3.sqlite_version,
        db_file=str(settings.paths.db_file),
        config_file=str(settings.paths.config_file),
        status="ok",
    )

    df = pd.DataFrame([asdict(result)])
    exported = export_dataframe(settings, "stage0_env_check.xlsx", df, export_name="stage0")
    if exported:
        logger.info("Excel debug выгрузка создана: %s", exported)

    logger.info("Проверка окружения завершена успешно")
    return f"python={result.python_version}, sqlite={result.sqlite_version}"


if __name__ == "__main__":
    print(run())
