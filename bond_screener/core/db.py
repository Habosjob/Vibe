from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.settings import AppSettings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_connection(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    return conn


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
        conn.execute(
            """
            INSERT INTO runs (run_id, stage, script, started_at, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run.run_id, run.stage, run.script, run.started_at, "running"),
        )
    return run


def close_run_success(settings: AppSettings, run_id: str, duration_s: float) -> None:
    with get_connection(settings.paths.db_file) as conn:
        conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, duration_s = ?, status = ?, error_text = NULL
            WHERE run_id = ?
            """,
            (utc_now_iso(), duration_s, "ok", run_id),
        )


def close_run_fail(settings: AppSettings, run_id: str, duration_s: float, error_text: str) -> None:
    with get_connection(settings.paths.db_file) as conn:
        conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, duration_s = ?, status = ?, error_text = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), duration_s, "fail", error_text[:4000], run_id),
        )
