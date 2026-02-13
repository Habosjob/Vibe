# logs.py
from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class LogPaths:
    logs_dir: Path
    logfile: Path


def ensure_logs_dir(logs_dir: str | Path = "logs") -> LogPaths:
    logs_path = Path(logs_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    logfile = logs_path / "Moex_API.log"
    return LogPaths(logs_dir=logs_path, logfile=logfile)


def clear_log_file(logfile: str | Path) -> None:
    p = Path(logfile)
    p.parent.mkdir(parents=True, exist_ok=True)
    # newline="\n" важно, чтобы не было CR-only
    with p.open("w", encoding="utf-8", newline="\n") as f:
        f.write("")


class SafeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except Exception:
            return f"{record.levelname} {record.name}: {record.getMessage()}"


def setup_logger(
    name: str,
    log_file: str | Path = "logs/Moex_API.log",
    level: int = logging.INFO,
    clear: bool = True,
    also_console: bool = True,
) -> logging.Logger:
    """
    Единая точка настройки логов.
    Важно: terminator = "\\n" на обоих handler'ах, чтобы GitHub не склеивал в 1 строку.
    """
    log_file = Path(log_file)
    if clear:
        clear_log_file(log_file)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = SafeFormatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(
        log_file,
        mode="a",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
        delay=False,
    )
    fh.setLevel(level)
    fh.setFormatter(fmt)
    try:
        fh.terminator = "\n"
    except Exception:
        pass
    logger.addHandler(fh)

    if also_console:
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(fmt)
        try:
            ch.terminator = "\n"
        except Exception:
            pass
        logger.addHandler(ch)

    return logger


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Timer:
    def __init__(self, logger: logging.Logger, label: str):
        self.logger = logger
        self.label = label
        self._t0: Optional[float] = None

    def __enter__(self):
        import time
        self._t0 = time.perf_counter()
        self.logger.info(f"TIMER START | {self.label}")
        return self

    def __exit__(self, exc_type, exc, tb):
        import time
        if self._t0 is None:
            return
        elapsed = time.perf_counter() - self._t0
        self.logger.info(f"TIMER END | {self.label} | elapsed={elapsed:.3f}s")