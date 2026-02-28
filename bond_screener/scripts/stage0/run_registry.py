from __future__ import annotations

import pandas as pd

from core.db import close_run_success, get_connection, open_run
from core.excel_debug import export_dataframe
from core.logging import get_script_logger
from core.settings import load_settings


def run() -> str:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage0_run_registry.log", "stage0.run_registry")

    run_rec = open_run(settings, stage="stage0", script="run_registry_self_test")
    close_run_success(settings, run_rec.run_id, duration_s=0.0)

    with get_connection(settings.paths.db_file) as conn:
        rows = conn.execute(
            """
            SELECT run_id, stage, script, started_at, finished_at, duration_s, status, error_text
            FROM runs
            ORDER BY rowid DESC
            LIMIT 20
            """
        ).fetchall()

    df = pd.DataFrame([dict(row) for row in rows])
    exported = export_dataframe(settings, "stage0_registry.xlsx", df, export_name="stage0")
    if exported:
        logger.info("Excel debug выгрузка создана: %s", exported)

    logger.info("Run registry self-test выполнен успешно")
    return f"runs_snapshot={len(df)}"


if __name__ == "__main__":
    print(run())
