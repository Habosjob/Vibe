"""Утилиты отображения прогресса выполнения сценария."""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True)
class ProgressState:
    stage_name: str
    stage_index: int
    stage_total: int


class PipelineProgress:
    """Печатает ход выполнения по этапам + ETA для длинных циклов."""

    def __init__(self, total_stages: int) -> None:
        self.total_stages = max(total_stages, 1)
        self.pipeline_started = time.time()
        self.stage_started = self.pipeline_started
        self.current = ProgressState(stage_name="", stage_index=0, stage_total=self.total_stages)
        self._last_line_at = 0.0

    def start_stage(self, index: int, name: str) -> None:
        self.current = ProgressState(stage_name=name, stage_index=index, stage_total=self.total_stages)
        self.stage_started = time.time()
        stage_percent = (index / self.total_stages) * 100
        print(f"[Этап {index}/{self.total_stages}] {name} ({stage_percent:.1f}%)")

    def tick(self, message: str) -> None:
        print(f"  ↳ {message}")

    def report_fraction(self, done: int, total: int, label: str, min_interval_seconds: float = 0.8) -> None:
        now = time.time()
        if now - self._last_line_at < min_interval_seconds and done < total:
            return
        self._last_line_at = now

        percent = (done / total) * 100 if total else 0.0
        elapsed = now - self.stage_started
        eta_seconds = (elapsed / done * (total - done)) if done > 0 and total > done else 0.0
        eta_text = self._format_duration(eta_seconds)
        print(f"  ↳ {label}: {done}/{total} ({percent:.1f}%), осталось примерно {eta_text}")

    def report_counter(self, done: int, label: str, min_interval_seconds: float = 0.8) -> None:
        now = time.time()
        if now - self._last_line_at < min_interval_seconds:
            return
        self._last_line_at = now
        elapsed = self._format_duration(now - self.stage_started)
        print(f"  ↳ {label}: {done} (время этапа {elapsed})")

    @staticmethod
    def _format_duration(seconds: float) -> str:
        sec = max(int(seconds), 0)
        hours, rem = divmod(sec, 3600)
        minutes, seconds_value = divmod(rem, 60)
        return f"{hours:02d}ч {minutes:02d}м {seconds_value:02d}с"
