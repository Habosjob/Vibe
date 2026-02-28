from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from typing import Callable

from core.db import close_run_fail, close_run_success, open_run
from core.logging import get_script_logger
from core.progress import progress_iter
from core.settings import load_settings
from scripts.stage2.dropped_manager import DroppedManager
from scripts.stage2.scoring_selector import ScoringSelector


@dataclass(frozen=True)
class Stage2Step:
    name: str
    func: Callable[[], str]


def _print_status(step: str, status: str, duration_s: float, message: str) -> None:
    print(f"[STAGE2][{step}] {status} | {duration_s:.2f}s | {message}")


def run_stage2() -> None:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage2_run.log", "stage2.run")

    scoring_selector = ScoringSelector(settings)
    dropped_manager = DroppedManager(settings)

    steps = [
        Stage2Step(
            name="scoring_selector",
            func=lambda: (
                lambda s: (
                    f"greenlist_emitents={s.greenlist_emitents}, "
                    f"candidates_before_drop={s.candidates_before_drop}"
                )
            )(scoring_selector.run()),
        ),
        Stage2Step(
            name="dropped_manager",
            func=lambda: (
                lambda s: (
                    f"loaded_manual_rows={s.loaded_manual_rows}, excluded_bonds={s.excluded_bonds}, "
                    f"remaining_candidates={s.remaining_candidates}"
                )
            )(dropped_manager.run()),
        ),
    ]

    for step in progress_iter(steps, desc="Stage2", total=len(steps)):
        run_meta = open_run(settings, stage="stage2", script=step.name)
        started = time.perf_counter()
        try:
            msg = step.func()
            duration_s = time.perf_counter() - started
            close_run_success(settings, run_meta.run_id, duration_s)
            logger.info("%s: OK %.2fs | %s", step.name, duration_s, msg)
            _print_status(step.name, "OK", duration_s, msg)
        except Exception as exc:  # noqa: BLE001
            duration_s = time.perf_counter() - started
            err_text = f"{exc}\n{traceback.format_exc()}"
            close_run_fail(settings, run_meta.run_id, duration_s, err_text)
            logger.exception("%s: FAIL %.2fs", step.name, duration_s)
            _print_status(step.name, "FAIL", duration_s, f"error={exc}")
            raise


if __name__ == "__main__":
    run_stage2()
