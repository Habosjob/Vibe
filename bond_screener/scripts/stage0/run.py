from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from typing import Callable

from core.db import close_run_fail, close_run_success, init_db, open_run
from core.progress import progress_iter
from core.settings import load_settings
from scripts.stage0 import env_check, reset_tool, run_registry


@dataclass(frozen=True)
class StageStep:
    name: str
    func: Callable[[], str]


def _print_status(step: str, status: str, duration_s: float, message: str) -> None:
    print(f"[STAGE0][{step}] {status} | {duration_s:.2f}s | {message}")


def run_stage0() -> None:
    settings = load_settings()
    init_db(settings)

    steps = [
        StageStep(name="env_check", func=env_check.run),
        StageStep(name="reset_tool", func=reset_tool.run),
        StageStep(name="run_registry", func=run_registry.run),
    ]

    for step in progress_iter(steps, desc="Stage0", total=len(steps)):
        run_meta = open_run(settings, stage="stage0", script=step.name)
        started = time.perf_counter()
        try:
            msg = step.func()
            duration_s = time.perf_counter() - started
            close_run_success(settings, run_meta.run_id, duration_s)
            _print_status(step.name, "OK", duration_s, msg)
            if step.name == "reset_tool":
                init_db(settings)
        except Exception as exc:  # noqa: BLE001
            duration_s = time.perf_counter() - started
            err_text = f"{exc}\n{traceback.format_exc()}"
            close_run_fail(settings, run_meta.run_id, duration_s, err_text)
            _print_status(step.name, "FAIL", duration_s, f"error={exc}")
            raise


if __name__ == "__main__":
    run_stage0()
