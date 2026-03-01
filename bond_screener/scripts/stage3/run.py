from __future__ import annotations

import asyncio
import time
import traceback
from dataclasses import dataclass

from core.db import close_run_fail, close_run_success, open_run
from core.logging import get_script_logger
from core.settings import AppSettings, load_settings
from scripts.stage3.dohod_export import DohodExporter
from scripts.stage3.moex_export import MoexExporter


@dataclass(frozen=True)
class SourceResult:
    script: str
    ok: bool
    duration_s: float
    message: str


def _run_source_sync(settings: AppSettings, stage: str, script: str, runner: callable) -> SourceResult:
    logger = get_script_logger(settings.paths.logs_dir / f"stage3_{script}.log", f"stage3.{script}")
    run_meta = open_run(settings, stage=stage, script=script)
    started = time.perf_counter()
    try:
        stats = runner()
        duration_s = time.perf_counter() - started
        close_run_success(settings, run_meta.run_id, duration_s)
        message = ", ".join([f"{k}={v}" for k, v in vars(stats).items() if k != "duration_s"])
        logger.info("%s | duration=%.2fs", message, duration_s)
        return SourceResult(script=script, ok=True, duration_s=duration_s, message=message)
    except Exception as exc:  # noqa: BLE001
        duration_s = time.perf_counter() - started
        err_text = f"{exc}\n{traceback.format_exc()}"
        close_run_fail(settings, run_meta.run_id, duration_s, err_text)
        logger.exception("Ошибка Stage3 %s: %s", script, exc)
        return SourceResult(script=script, ok=False, duration_s=duration_s, message=f"error={exc}")


async def _run_parallel(settings: AppSettings) -> list[SourceResult]:
    return await asyncio.gather(
        asyncio.to_thread(_run_source_sync, settings, "stage3", "moex_export", MoexExporter(settings).run),
        asyncio.to_thread(_run_source_sync, settings, "stage3", "dohod_export", DohodExporter(settings).run),
    )


def run_stage3() -> None:
    settings = load_settings()
    logger = get_script_logger(settings.paths.logs_dir / "stage3_run.log", "stage3.run")

    if not settings.stage3.enabled:
        logger.info("Stage3 отключен")
        print("[STAGE3] SKIP | disabled in config")
        return

    if settings.stage3.run_sources_in_parallel:
        results = asyncio.run(_run_parallel(settings))
    else:
        results = [
            _run_source_sync(settings, "stage3", "moex_export", MoexExporter(settings).run),
            _run_source_sync(settings, "stage3", "dohod_export", DohodExporter(settings).run),
        ]

    ok_count = sum(1 for r in results if r.ok)
    for result in results:
        status = "OK" if result.ok else "FAIL"
        logger.info("source=%s status=%s duration=%.2fs %s", result.script, status, result.duration_s, result.message)
        print(f"[STAGE3][{result.script}] {status} | {result.duration_s:.2f}s | {result.message}")

    if ok_count == 0:
        logger.error("Stage3 завершен с ошибкой: все источники упали")
        raise RuntimeError("Stage3 failed: both sources failed")

    logger.info("Stage3 завершен успешно: ok_sources=%s/%s", ok_count, len(results))
    print(f"[STAGE3] OK | successful_sources={ok_count}/{len(results)}")


if __name__ == "__main__":
    run_stage3()
