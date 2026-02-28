from __future__ import annotations

import time
import traceback

from core.db import close_run_fail, close_run_success, open_run
from core.logging import get_script_logger
from core.settings import load_settings
from scripts.stage1.moex_emitents_collector import MoexEmitentsCollector


def run_stage1() -> None:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage1_run.log", "stage1.run")

    run_meta = open_run(settings, stage="stage1", script="run")
    started = time.perf_counter()

    try:
        collector = MoexEmitentsCollector(settings)
        stats = collector.run()
        duration_s = time.perf_counter() - started
        close_run_success(settings, run_meta.run_id, duration_s)

        message = (
            f"эмитентов: {stats.emitents_count}, бумаг: {stats.securities_count}, "
            f"длительность: {duration_s:.2f} сек"
        )
        logger.info(message)
        print(f"[STAGE1][run] OK | {duration_s:.2f}s | {message}")
    except Exception as exc:  # noqa: BLE001
        duration_s = time.perf_counter() - started
        err_text = f"{exc}\n{traceback.format_exc()}"
        close_run_fail(settings, run_meta.run_id, duration_s, err_text)
        logger.exception("Ошибка Stage1: %s", exc)
        print(f"[STAGE1][run] FAIL | {duration_s:.2f}s | error={exc}")
        raise


if __name__ == "__main__":
    run_stage1()
