from __future__ import annotations

import time
import traceback

from core.db import close_run_fail, close_run_success, open_run
from core.logging import get_script_logger
from core.settings import load_settings
from scripts.stage3.moex_export import MoexExporter


def run_stage3() -> None:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage3_run.log", "stage3.run")

    run_meta = open_run(settings, stage="stage3", script="moex_export")
    started = time.perf_counter()
    try:
        stats = MoexExporter(settings).run()
        duration_s = time.perf_counter() - started
        close_run_success(settings, run_meta.run_id, duration_s)
        msg = (
            f"total_candidates={stats.total_candidates}, processed={stats.processed}, "
            f"skipped_fresh={stats.skipped_fresh}, failed={stats.failed}"
        )
        logger.info("%s | duration=%.2fs", msg, duration_s)
        print(f"[STAGE3][moex_export] OK | {duration_s:.2f}s | {msg}")
    except Exception as exc:  # noqa: BLE001
        duration_s = time.perf_counter() - started
        err_text = f"{exc}\n{traceback.format_exc()}"
        close_run_fail(settings, run_meta.run_id, duration_s, err_text)
        logger.exception("Ошибка Stage3: %s", exc)
        print(f"[STAGE3][moex_export] FAIL | {duration_s:.2f}s | error={exc}")
        raise


if __name__ == "__main__":
    run_stage3()
