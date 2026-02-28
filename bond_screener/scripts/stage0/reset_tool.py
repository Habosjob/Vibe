from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.db import get_connection
from core.excel_debug import export_dataframe
from core.logging import get_script_logger
from core.settings import load_reset_settings, load_settings, reset_settings_to_safe_default
from net.cache import HttpCache


def _safe_unlink(path: Path) -> bool:
    if path.exists() and path.is_file():
        path.unlink()
        return True
    return False


def run() -> str:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage0_reset_tool.log", "stage0.reset_tool")
    reset_cfg = load_reset_settings(settings)

    actions: list[str] = []

    if "cache" in reset_cfg.reset_mode and reset_cfg.cache_clear_all:
        removed = HttpCache(settings.paths.cache_http_dir).clear(clear_all=True)
        actions.append(f"cache_cleared={removed}")
        logger.info("HTTP cache очищен: %s файлов", removed)

    if "checkpoints" in reset_cfg.reset_mode and reset_cfg.checkpoints_clear_all:
        with get_connection(settings.paths.db_file) as conn:
            count = conn.execute("SELECT COUNT(*) AS cnt FROM job_items").fetchone()["cnt"]
            conn.execute("DELETE FROM job_items")
        actions.append(f"checkpoints_cleared={count}")
        logger.info("Чекпоинты очищены: %s", count)

    if "db" in reset_cfg.reset_mode and reset_cfg.db_delete_db_file:
        deleted = _safe_unlink(settings.paths.db_file)
        actions.append(f"db_deleted={deleted}")
        logger.info("Удаление файла БД: %s", deleted)

    if "ttl" in reset_cfg.reset_mode:
        actions.append(f"ttl_tables={','.join(reset_cfg.ttl_reset_fetched_at_tables) or 'none'}")
        logger.info("TTL reset заглушка, таблицы: %s", reset_cfg.ttl_reset_fetched_at_tables)

    if not actions:
        actions.append("no_actions")
        logger.info("Reset действий не запрошено")

    report_df = pd.DataFrame([{"action": action} for action in actions])
    exported = export_dataframe(settings, "stage0_reset_tool.xlsx", report_df, export_name="stage0")
    if exported:
        logger.info("Excel debug выгрузка создана: %s", exported)

    reset_settings_to_safe_default(settings)
    logger.info("reset.yaml сброшен в безопасное состояние")
    return "; ".join(actions)


if __name__ == "__main__":
    print(run())
