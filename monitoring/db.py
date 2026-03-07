from __future__ import annotations

import json
import sqlite3
from typing import Any

from . import config
from .helpers import now_iso

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS company_map (
    inn TEXT PRIMARY KEY,
    company_id TEXT,
    company_name TEXT,
    company_url TEXT,
    last_checked_at TEXT
);
CREATE TABLE IF NOT EXISTS report_events (
    event_hash TEXT PRIMARY KEY,
    inn TEXT,
    company_name TEXT,
    scoring_date TEXT,
    event_date TEXT,
    event_type TEXT,
    event_url TEXT,
    source TEXT,
    payload_json TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS emitents_snapshot (
    inn TEXT PRIMARY KEY,
    company_name TEXT,
    scoring TEXT,
    scoring_date TEXT,
    nra_rate TEXT,
    acra_rate TEXT,
    nkr_rate TEXT,
    raex_rate TEXT,
    snapshot_at TEXT
);
CREATE TABLE IF NOT EXISTS news_events (
    event_hash TEXT PRIMARY KEY,
    instrument_type TEXT,
    instrument_code TEXT,
    inn TEXT,
    company_name TEXT,
    news_date TEXT,
    title TEXT,
    url TEXT,
    source TEXT,
    first_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS portfolio_items (
    instrument_type TEXT,
    instrument_code TEXT,
    inn TEXT,
    company_name TEXT,
    source_file TEXT,
    loaded_at TEXT,
    PRIMARY KEY (instrument_type, instrument_code)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def connect() -> sqlite3.Connection:
    config.DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def bootstrap(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def get_company_map(conn: sqlite3.Connection, inn: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM company_map WHERE inn = ?", (inn,)).fetchone()


def upsert_company_map(conn: sqlite3.Connection, inn: str, company: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO company_map (inn, company_id, company_name, company_url, last_checked_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(inn) DO UPDATE SET
            company_id=excluded.company_id,
            company_name=excluded.company_name,
            company_url=excluded.company_url,
            last_checked_at=excluded.last_checked_at
        """,
        (
            inn,
            company.get("id", ""),
            company.get("name", ""),
            company.get("url", ""),
            now_iso(),
        ),
    )
    conn.commit()


def upsert_report_event(conn: sqlite3.Connection, row: dict[str, Any]) -> bool:
    existing = conn.execute("SELECT event_hash FROM report_events WHERE event_hash = ?", (row["event_hash"],)).fetchone()
    if existing:
        conn.execute(
            "UPDATE report_events SET last_seen_at = ? WHERE event_hash = ?",
            (now_iso(), row["event_hash"]),
        )
        conn.commit()
        return False
    conn.execute(
        """
        INSERT INTO report_events (
            event_hash, inn, company_name, scoring_date, event_date,
            event_type, event_url, source, payload_json, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["event_hash"],
            row.get("inn", ""),
            row.get("company_name", ""),
            row.get("scoring_date", ""),
            row.get("event_date", ""),
            row.get("event_type", ""),
            row.get("event_url", ""),
            row.get("source", ""),
            json.dumps(row.get("payload", {}), ensure_ascii=False),
            now_iso(),
            now_iso(),
        ),
    )
    conn.commit()
    return True


def list_report_events(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT event_hash, inn, company_name, scoring_date, event_date, event_type, event_url, first_seen_at FROM report_events"
    ).fetchall()


def read_emitents_snapshot(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = conn.execute("SELECT * FROM emitents_snapshot").fetchall()
    return {row["inn"]: row for row in rows if row["inn"]}


def replace_emitents_snapshot(conn: sqlite3.Connection, rows: list[dict[str, str]]) -> None:
    conn.execute("DELETE FROM emitents_snapshot")
    snap_at = now_iso()
    for row in rows:
        conn.execute(
            """
            INSERT INTO emitents_snapshot (
                inn, company_name, scoring, scoring_date, nra_rate, acra_rate, nkr_rate, raex_rate, snapshot_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("inn", ""),
                row.get("company_name", ""),
                row.get("scoring", ""),
                row.get("scoring_date", ""),
                row.get("nra_rate", ""),
                row.get("acra_rate", ""),
                row.get("nkr_rate", ""),
                row.get("raex_rate", ""),
                snap_at,
            ),
        )
    conn.commit()


def save_portfolio_items(conn: sqlite3.Connection, rows: list[dict[str, str]], source_file: str) -> None:
    conn.execute("DELETE FROM portfolio_items")
    loaded_at = now_iso()
    for row in rows:
        conn.execute(
            """
            INSERT INTO portfolio_items (instrument_type, instrument_code, inn, company_name, source_file, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_type, instrument_code) DO UPDATE SET
                inn=excluded.inn,
                company_name=excluded.company_name,
                source_file=excluded.source_file,
                loaded_at=excluded.loaded_at
            """,
            (
                row.get("instrument_type", ""),
                row.get("instrument_code", ""),
                row.get("inn", ""),
                row.get("company_name", ""),
                source_file,
                loaded_at,
            ),
        )
    conn.commit()


def list_portfolio_items(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM portfolio_items ORDER BY instrument_type, instrument_code").fetchall()


def save_news_event(conn: sqlite3.Connection, row: dict[str, Any]) -> bool:
    exists = conn.execute("SELECT event_hash FROM news_events WHERE event_hash = ?", (row["event_hash"],)).fetchone()
    if exists:
        return False
    conn.execute(
        """
        INSERT INTO news_events (
            event_hash, instrument_type, instrument_code, inn, company_name,
            news_date, title, url, source, first_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["event_hash"],
            row.get("instrument_type", ""),
            row.get("instrument_code", ""),
            row.get("inn", ""),
            row.get("company_name", ""),
            row.get("news_date", ""),
            row.get("title", ""),
            row.get("url", ""),
            row.get("source", ""),
            now_iso(),
        ),
    )
    conn.commit()
    return True


def list_news_events(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM news_events ORDER BY news_date DESC").fetchall()
