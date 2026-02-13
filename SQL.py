# SQL.py

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any


# ============================================================
# Helpers
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# Data Models
# ============================================================

@dataclass(frozen=True)
class RawRow:
    id: int
    secid: str
    kind: str
    asof_date: str
    payload: str
    created_at: str


@dataclass(frozen=True)
class RequestLogRow:
    id: int
    endpoint: str
    secid: Optional[str]
    status_code: Optional[int]
    created_at: str


# ============================================================
# SQLiteCache
# ============================================================

class SQLiteCache:

    def __init__(self, db_path: str = "moex_cache.sqlite3", logger=None):
        self.logger = logger
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

        if self.logger:
            self.logger.info("SQLiteCache initialized")

    # ============================================================
    # DB INIT
    # ============================================================

    def _init_db(self):
        cur = self.conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bond_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            secid TEXT NOT NULL,
            kind TEXT NOT NULL,
            asof_date TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup
        ON bond_raw (secid, kind, asof_date)
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS request_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            secid TEXT,
            status_code INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_request_log_lookup
        ON request_log (endpoint, secid)
        """)

        self.conn.commit()

    # ============================================================
    # bond_raw
    # ============================================================

    def get_bond_raw(self, secid: str, kind: str, asof_date: str) -> Optional[RawRow]:

        cur = self.conn.cursor()
        cur.execute("""
            SELECT *
            FROM bond_raw
            WHERE secid=? AND kind=? AND asof_date=?
            ORDER BY id DESC
            LIMIT 1
        """, (secid, kind, asof_date))

        row = cur.fetchone()

        if not row:
            return None

        return RawRow(
            id=row["id"],
            secid=row["secid"],
            kind=row["kind"],
            asof_date=row["asof_date"],
            payload=row["payload"],
            created_at=row["created_at"],
        )

    def save_bond_raw(self, secid: str, kind: str, asof_date: str, payload: Any):

        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False)

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO bond_raw (secid, kind, asof_date, payload, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (secid, kind, asof_date, payload, utc_now()))

        self.conn.commit()

        if self.logger:
            self.logger.debug(f"Saved bond_raw {secid} {kind}")

    # ============================================================
    # request_log
    # ============================================================

    def save_request_log(self, endpoint: str, secid: Optional[str], status_code: Optional[int]):

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO request_log (endpoint, secid, status_code, created_at)
            VALUES (?, ?, ?, ?)
        """, (endpoint, secid, status_code, utc_now()))

        self.conn.commit()

        if self.logger:
            self.logger.debug(f"Logged request {endpoint} {secid} {status_code}")

    def get_last_request(self, endpoint: str, secid: Optional[str]) -> Optional[RequestLogRow]:

        cur = self.conn.cursor()
        cur.execute("""
            SELECT *
            FROM request_log
            WHERE endpoint=? AND secid=?
            ORDER BY id DESC
            LIMIT 1
        """, (endpoint, secid))

        row = cur.fetchone()

        if not row:
            return None

        return RequestLogRow(
            id=row["id"],
            endpoint=row["endpoint"],
            secid=row["secid"],
            status_code=row["status_code"],
            created_at=row["created_at"],
        )

    # ============================================================
    # Close
    # ============================================================

    def close(self):
        self.conn.close()
        if self.logger:
            self.logger.info("SQLiteCache closed")