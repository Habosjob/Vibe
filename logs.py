# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class RunTimer:
    """Контекстный таймер для измерения времени выполнения блоков."""
    name: str = "run"
    logger: Optional[logging.Logger] = None
    t0: float = 0.0
    elapsed: float = 0.0

    def __enter__(self) -> "RunTimer":
        self.t0 = time.perf_counter()
        if self.logger:
            self.logger.info("TIMER START | %s", self.name)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.elapsed = time.perf_counter() - self.t0
        if self.logger:
            self.logger.info("TIMER END   | %s | elapsed=%.3fs", self.name, self.elapsed)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def setup_logging(
    log_dir: str | Path = "logs",
    log_file: str = "Moex_API.log",
    level: str = "INFO",
    clear_previous: bool = True,
    also_console: bool = True,
) -> Path:
    """Настраивает логирование. По умолчанию очищает предыдущий лог при старте."""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    if clear_previous and log_path.exists():
        log_path.unlink()

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handlers = [logging.FileHandler(log_path, mode="w", encoding="utf-8")]
    if also_console:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        handlers=handlers,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return log_path


def safe_filename(s: str) -> str:
    keep = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)


def dump_json(
    payload: Dict[str, Any],
    out_dir: str | Path,
    tag: str,
    logger: Optional[logging.Logger] = None,
) -> Path:
    """Разово сохраняет JSON на диск (удобно для RAW дебага)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = safe_filename(f"{ts}_{tag}.json")
    p = out_dir / fname
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if logger:
        logger.debug("RAW saved: %s", p.resolve())
    return p


def json_dumps_compact(obj: Any) -> str:
    """Компактный JSON для логов/SQLite."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)