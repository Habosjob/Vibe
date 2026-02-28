from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator


@dataclass
class TimerResult:
    started_at: float
    finished_at: float

    @property
    def duration_s(self) -> float:
        return self.finished_at - self.started_at


@contextmanager
def measure_time() -> Iterator[TimerResult]:
    started = time.perf_counter()
    result = TimerResult(started_at=started, finished_at=started)
    try:
        yield result
    finally:
        result.finished_at = time.perf_counter()
