from __future__ import annotations

import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from core.settings import AppSettings

SQLITE_LOCKED_ERRORS = ("database is locked", "database table is locked")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_connection(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _is_locked_error(error: sqlite3.OperationalError) -> bool:
    msg = str(error).lower()
    return any(token in msg for token in SQLITE_LOCKED_ERRORS)


def _sleep_backoff(attempt: int, base_s: float = 0.1, max_s: float = 1.0) -> None:
    delay = min(max_s, base_s * (2 ** max(0, attempt - 1)))
    time.sleep(delay + random.uniform(0, min(0.05, delay / 2)))


def execute_with_retry(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] | list[Any] | None = None,
    *,
    max_attempts: int = 6,
) -> sqlite3.Cursor:
    for attempt in range(1, max_attempts + 1):
        try:
            if params is None:
                return conn.execute(query)
            return conn.execute(query, params)
        except sqlite3.OperationalError as exc:
            if attempt >= max_attempts or not _is_locked_error(exc):
                raise
            _sleep_backoff(attempt)
    raise RuntimeError("unreachable")


def executemany_with_retry(
    conn: sqlite3.Connection,
    query: str,
    params_seq: Iterable[tuple[Any, ...]],
    *,
    max_attempts: int = 6,
) -> sqlite3.Cursor:
    payload = list(params_seq)
    for attempt in range(1, max_attempts + 1):
        try:
            return conn.executemany(query, payload)
        except sqlite3.OperationalError as exc:
            if attempt >= max_attempts or not _is_locked_error(exc):
                raise
            _sleep_backoff(attempt)
    raise RuntimeError("unreachable")


def init_db(settings: AppSettings) -> None:
    with get_connection(settings.paths.db_file) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                stage TEXT NOT NULL,
                script TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_s REAL,
                status TEXT,
                error_text TEXT
            );

            CREATE TABLE IF NOT EXISTS job_items (
                job_name TEXT NOT NULL,
                item_key TEXT NOT NULL,
                status TEXT NOT NULL,
                error_text TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (job_name, item_key)
            );
            """
        )


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    stage: str
    script: str
    started_at: str


def open_run(settings: AppSettings, stage: str, script: str) -> RunRecord:
    run = RunRecord(run_id=str(uuid.uuid4()), stage=stage, script=script, started_at=utc_now_iso())
    with get_connection(settings.paths.db_file) as conn:
        execute_with_retry(
            conn,
            """
            INSERT INTO runs (run_id, stage, script, started_at, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run.run_id, run.stage, run.script, run.started_at, "running"),
        )
    return run


def close_run_success(settings: AppSettings, run_id: str, duration_s: float) -> None:
    with get_connection(settings.paths.db_file) as conn:
        execute_with_retry(
            conn,
            """
            UPDATE runs
            SET finished_at = ?, duration_s = ?, status = ?, error_text = NULL
            WHERE run_id = ?
            """,
            (utc_now_iso(), duration_s, "ok", run_id),
        )


def close_run_fail(settings: AppSettings, run_id: str, duration_s: float, error_text: str) -> None:
    with get_connection(settings.paths.db_file) as conn:
        execute_with_retry(
            conn,
            """
            UPDATE runs
            SET finished_at = ?, duration_s = ?, status = ?, error_text = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), duration_s, "fail", error_text[:4000], run_id),
        )
