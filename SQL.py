# SQL.py
from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_iso_days_ago(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=int(days))
    return dt.isoformat(timespec="seconds")


def safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return {r[1] for r in cur.fetchall()}


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


@dataclass(frozen=True)
class RequestLogRow:
    url: str
    params_json: str
    status: Optional[int]
    elapsed_ms: Optional[int]
    size_bytes: Optional[int]
    created_utc: str
    error: Optional[str] = None


class SQLiteCache:
    """
    Thread-safe cache через thread-local соединения.
    """

    def __init__(self, db_path: str = "moex_cache.sqlite", logger=None):
        self.logger = logger
        self.db_path = db_path

        self._tls = threading.local()
        self._conns_lock = threading.Lock()
        self._conns: List[sqlite3.Connection] = []

        conn = self._connect()
        self._ensure_schema(conn)
        conn.commit()

        if self.logger:
            self.logger.info(f"SQLiteCache initialized | db={db_path}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=8000;")
        except Exception:
            pass

        with self._conns_lock:
            self._conns.append(conn)
        return conn

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = self._connect()
            self._tls.conn = conn
        return conn

    def _execute(self, sql: str, params: tuple = (), commit: bool = False) -> sqlite3.Cursor:
        conn = self._get_conn()
        last = None
        for attempt in range(1, 7):
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                if commit:
                    conn.commit()
                return cur
            except sqlite3.OperationalError as e:
                last = e
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(min(0.5, 0.06 * attempt))
                    continue
                raise
        raise last  # type: ignore[misc]

    # -------------------------
    # schema / migration
    # -------------------------
    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds_list (
                asof_date TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_utc TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bonds_list_created ON bonds_list(created_utc)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bond_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                secid TEXT NOT NULL,
                kind TEXT NOT NULL,
                asof_date TEXT NOT NULL,
                url TEXT,
                params_json TEXT,
                status INTEGER,
                elapsed_ms INTEGER,
                size_bytes INTEGER,
                response_text TEXT,
                created_utc TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup ON bond_raw(secid, kind, asof_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_created ON bond_raw(created_utc)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS emitents (
                emitter_id INTEGER PRIMARY KEY,
                inn TEXT,
                title TEXT,
                short_title TEXT,
                ogrn TEXT,
                okpo TEXT,
                kpp TEXT,
                okved TEXT,
                address TEXT,
                phone TEXT,
                site TEXT,
                email TEXT,
                raw_json TEXT,
                updated_utc TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_emitents_updated ON emitents(updated_utc)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS requests_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                params_json TEXT NOT NULL,
                status INTEGER,
                elapsed_ms INTEGER,
                size_bytes INTEGER,
                created_utc TEXT NOT NULL,
                error TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_log_created ON requests_log(created_utc)")

        # --- NEW: progress table for detail-all ---
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS detail_progress (
                run_id TEXT NOT NULL,
                secid TEXT NOT NULL,
                mode TEXT NOT NULL,           -- random/static/all
                status TEXT NOT NULL,         -- pending/done/error
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_utc TEXT NOT NULL,
                error TEXT,
                PRIMARY KEY(run_id, secid)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_detail_progress_status ON detail_progress(run_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_detail_progress_updated ON detail_progress(updated_utc)")

        conn.commit()

        self._migrate_emitents_if_needed(conn)

    def _migrate_emitents_if_needed(self, conn: sqlite3.Connection) -> None:
        if not table_exists(conn, "emitents"):
            return
        have = cols(conn, "emitents")
        desired = [
            ("kpp", "TEXT"),
            ("okved", "TEXT"),
            ("address", "TEXT"),
            ("phone", "TEXT"),
            ("site", "TEXT"),
            ("email", "TEXT"),
        ]
        cur = conn.cursor()
        changed = False
        for col, typ in desired:
            if col not in have:
                cur.execute(f"ALTER TABLE emitents ADD COLUMN {col} {typ}")
                changed = True
        if changed:
            conn.commit()
            if self.logger:
                self.logger.info("emitents schema upgraded (added extra columns)")

    # -------------------------
    # bonds list
    # -------------------------
    def get_bonds_list(self, asof_date: str) -> Optional[List[dict]]:
        cur = self._execute("SELECT payload_json FROM bonds_list WHERE asof_date=?", (asof_date,))
        row = cur.fetchone()
        if not row:
            return None
        data = safe_json_loads(row["payload_json"])
        return data if isinstance(data, list) else None

    def set_bonds_list(self, bonds: List[dict], asof_date: str) -> None:
        self._execute(
            """
            INSERT INTO bonds_list(asof_date, payload_json, created_utc)
            VALUES(?, ?, ?)
            ON CONFLICT(asof_date) DO UPDATE SET
                payload_json=excluded.payload_json,
                created_utc=excluded.created_utc
            """,
            (asof_date, safe_json_dumps(bonds), utc_iso()),
            commit=True,
        )

    # -------------------------
    # bond_raw
    # -------------------------
    def get_bond_raw(self, secid: str, kind: str, asof_date: str) -> Optional[Dict[str, Any]]:
        cur = self._execute(
            """
            SELECT *
            FROM bond_raw
            WHERE secid=? AND kind=? AND asof_date=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (secid, kind, asof_date),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def set_bond_raw(
        self,
        *,
        secid: str,
        kind: str,
        asof_date: str,
        url: str,
        params: Optional[dict],
        status: Optional[int],
        elapsed_ms: Optional[int],
        size_bytes: Optional[int],
        response_text: Optional[str],
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO bond_raw(
                secid, kind, asof_date,
                url, params_json, status,
                elapsed_ms, size_bytes, response_text,
                created_utc
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                secid,
                kind,
                asof_date,
                url,
                safe_json_dumps(params or {}),
                status,
                elapsed_ms,
                size_bytes,
                response_text,
                utc_iso(),
            ),
            commit=True,
        )
        return int(cur.lastrowid)

    # -------------------------
    # emitents
    # -------------------------
    def upsert_emitent(
        self,
        *,
        emitter_id: int,
        inn: Optional[str],
        title: Optional[str],
        short_title: Optional[str],
        ogrn: Optional[str],
        okpo: Optional[str],
        kpp: Optional[str] = None,
        okved: Optional[str] = None,
        address: Optional[str] = None,
        phone: Optional[str] = None,
        site: Optional[str] = None,
        email: Optional[str] = None,
        raw_json: Optional[str] = None,
        updated_utc: Optional[str] = None,
    ) -> None:
        self._execute(
            """
            INSERT INTO emitents(
                emitter_id,
                inn, title, short_title,
                ogrn, okpo,
                kpp, okved, address, phone, site, email,
                raw_json,
                updated_utc
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(emitter_id) DO UPDATE SET
                inn=excluded.inn,
                title=excluded.title,
                short_title=excluded.short_title,
                ogrn=excluded.ogrn,
                okpo=excluded.okpo,
                kpp=excluded.kpp,
                okved=excluded.okved,
                address=excluded.address,
                phone=excluded.phone,
                site=excluded.site,
                email=excluded.email,
                raw_json=excluded.raw_json,
                updated_utc=excluded.updated_utc
            """,
            (
                int(emitter_id),
                inn,
                title,
                short_title,
                ogrn,
                okpo,
                kpp,
                okved,
                address,
                phone,
                site,
                email,
                raw_json,
                updated_utc or utc_iso(),
            ),
            commit=True,
        )

    def get_emitent(self, emitter_id: int) -> Optional[Dict[str, Any]]:
        cur = self._execute("SELECT * FROM emitents WHERE emitter_id=?", (int(emitter_id),))
        row = cur.fetchone()
        return dict(row) if row else None

    # -------------------------
    # requests log
    # -------------------------
    def log_request(self, row: RequestLogRow) -> int:
        cur = self._execute(
            """
            INSERT INTO requests_log(url, params_json, status, elapsed_ms, size_bytes, created_utc, error)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.url,
                row.params_json,
                row.status,
                row.elapsed_ms,
                row.size_bytes,
                row.created_utc,
                row.error,
            ),
            commit=True,
        )
        return int(cur.lastrowid)

    def requests_summary(self, since_created_utc: str) -> Dict[str, int]:
        cur = self._execute("SELECT COUNT(*) AS n FROM requests_log WHERE created_utc >= ?", (since_created_utc,))
        total = int(cur.fetchone()["n"])
        cur = self._execute(
            """
            SELECT COUNT(*) AS n
            FROM requests_log
            WHERE created_utc >= ?
              AND (status IS NULL OR status >= 400 OR error IS NOT NULL)
            """,
            (since_created_utc,),
        )
        errors = int(cur.fetchone()["n"])
        return {"total": total, "errors": errors}

    # -------------------------
    # detail progress
    # -------------------------
    def progress_seed(self, run_id: str, secids: List[str], mode: str) -> int:
        """
        Засеивает pending для тех, кого ещё нет.
        """
        now = utc_iso()
        rows = [(run_id, s, mode, "pending", 0, now, None) for s in secids]
        # INSERT OR IGNORE
        cur = self._get_conn().cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO detail_progress(run_id, secid, mode, status, attempts, updated_utc, error)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._get_conn().commit()
        return int(cur.rowcount or 0)

    def progress_take_batch(self, run_id: str, batch: int) -> List[str]:
        """
        Берём batch secid со статусом pending.
        """
        cur = self._execute(
            """
            SELECT secid
            FROM detail_progress
            WHERE run_id=? AND status='pending'
            ORDER BY secid
            LIMIT ?
            """,
            (run_id, int(batch)),
        )
        return [r["secid"] for r in cur.fetchall()]

    def progress_mark_done(self, run_id: str, secid: str) -> None:
        self._execute(
            """
            UPDATE detail_progress
            SET status='done', updated_utc=?, error=NULL
            WHERE run_id=? AND secid=?
            """,
            (utc_iso(), run_id, secid),
            commit=True,
        )

    def progress_mark_error(self, run_id: str, secid: str, error: str) -> None:
        self._execute(
            """
            UPDATE detail_progress
            SET status='error', attempts=attempts+1, updated_utc=?, error=?
            WHERE run_id=? AND secid=?
            """,
            (utc_iso(), error[:2000], run_id, secid),
            commit=True,
        )

    def progress_counts(self, run_id: str) -> Dict[str, int]:
        out = {"pending": 0, "done": 0, "error": 0, "total": 0}
        cur = self._execute(
            """
            SELECT status, COUNT(*) AS n
            FROM detail_progress
            WHERE run_id=?
            GROUP BY status
            """,
            (run_id,),
        )
        total = 0
        for r in cur.fetchall():
            st = r["status"]
            n = int(r["n"])
            total += n
            if st in out:
                out[st] = n
        out["total"] = total
        return out

    # -------------------------
    # TTL purge
    # -------------------------
    def purge_bond_raw(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM bond_raw WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_requests_log(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM requests_log WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_bonds_list(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM bonds_list WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_emitents(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM emitents WHERE updated_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def close(self) -> None:
        with self._conns_lock:
            conns = list(self._conns)
            self._conns.clear()
        for c in conns:
            try:
                c.close()
            except Exception:
                pass
        if self.logger:
            self.logger.info("SQLiteCache closed")