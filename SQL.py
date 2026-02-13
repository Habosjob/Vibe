# SQL.py
from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _utc_iso_days_ago(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=int(days))
    return dt.isoformat(timespec="seconds")


def _safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return {r[1] for r in cur.fetchall()}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
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

    ВАЖНО: схема/миграции делаются в __init__ на одном соединении,
    дальше каждый поток работает со своим connection.
    """

    def __init__(self, db_path: str = "moex_cache.sqlite", logger=None):
        self.logger = logger
        self.db_path = db_path

        self._tls = threading.local()
        self._conns_lock = threading.Lock()
        self._conns: List[sqlite3.Connection] = []

        # schema / migrations on main connection
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

    # -------------------------
    # execute helpers (lock/retry on "database is locked")
    # -------------------------
    def _execute(self, sql: str, params: tuple = (), commit: bool = False) -> sqlite3.Cursor:
        conn = self._get_conn()
        last = None
        for attempt in range(1, 6):
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
                    time.sleep(min(0.4, 0.05 * attempt))
                    continue
                raise
        raise last  # type: ignore[misc]

    def _executemany(self, sql: str, seq_params: list[tuple], commit: bool = False) -> sqlite3.Cursor:
        conn = self._get_conn()
        last = None
        for attempt in range(1, 6):
            try:
                cur = conn.cursor()
                cur.executemany(sql, seq_params)
                if commit:
                    conn.commit()
                return cur
            except sqlite3.OperationalError as e:
                last = e
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(min(0.4, 0.05 * attempt))
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bonds_list_created ON bonds_list(created_utc)"
        )

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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup ON bond_raw(secid, kind, asof_date)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bond_raw_created ON bond_raw(created_utc)"
        )

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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_emitents_updated ON emitents(updated_utc)"
        )

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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_log_created ON requests_log(created_utc)"
        )

        conn.commit()

        self._migrate_bonds_list_if_needed(conn)
        self._migrate_bond_raw_if_needed(conn)
        self._migrate_emitents_if_needed(conn)

    def _migrate_bonds_list_if_needed(self, conn: sqlite3.Connection) -> None:
        if not _table_exists(conn, "bonds_list"):
            return
        have = _cols(conn, "bonds_list")
        want = {"asof_date", "payload_json", "created_utc"}
        if want.issubset(have):
            return

        if self.logger:
            self.logger.warning(
                f"Schema mismatch: bonds_list columns={sorted(have)} -> recreate/migrate"
            )

        cur = conn.cursor()
        cur.execute("ALTER TABLE bonds_list RENAME TO bonds_list_old;")
        conn.commit()

        cur.execute(
            """
            CREATE TABLE bonds_list (
                asof_date TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_utc TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bonds_list_created ON bonds_list(created_utc)"
        )
        conn.commit()

        old = _cols(conn, "bonds_list_old")
        payload_col = next((c for c in ("payload_json", "payload", "data", "json", "value") if c in old), None)
        created_col = next((c for c in ("created_utc", "created_at", "created", "ts", "timestamp") if c in old), None)

        now = _utc_iso()
        if "asof_date" in old and payload_col:
            if created_col:
                cur.execute(
                    f"""
                    INSERT INTO bonds_list(asof_date, payload_json, created_utc)
                    SELECT asof_date,
                           {payload_col} AS payload_json,
                           COALESCE({created_col}, ?) AS created_utc
                    FROM bonds_list_old
                    """,
                    (now,),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO bonds_list(asof_date, payload_json, created_utc)
                    SELECT asof_date,
                           {payload_col} AS payload_json,
                           ? AS created_utc
                    FROM bonds_list_old
                    """,
                    (now,),
                )
            conn.commit()

        cur.execute("DROP TABLE IF EXISTS bonds_list_old;")
        conn.commit()

    def _migrate_bond_raw_if_needed(self, conn: sqlite3.Connection) -> None:
        if not _table_exists(conn, "bond_raw"):
            return
        have = _cols(conn, "bond_raw")
        want = {
            "id",
            "secid",
            "kind",
            "asof_date",
            "url",
            "params_json",
            "status",
            "elapsed_ms",
            "size_bytes",
            "response_text",
            "created_utc",
        }
        if want.issubset(have):
            return

        if self.logger:
            self.logger.warning(
                f"Schema mismatch: bond_raw columns={sorted(have)} -> recreate/migrate"
            )

        cur = conn.cursor()
        cur.execute("ALTER TABLE bond_raw RENAME TO bond_raw_old;")
        conn.commit()

        cur.execute(
            """
            CREATE TABLE bond_raw (
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup ON bond_raw(secid, kind, asof_date)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bond_raw_created ON bond_raw(created_utc)"
        )
        conn.commit()

        old = _cols(conn, "bond_raw_old")
        response_col = next((c for c in ("response_text", "payload", "payload_json") if c in old), None)
        created_col = next((c for c in ("created_utc", "created_at") if c in old), None)

        can_copy = {"secid", "kind", "asof_date"}.issubset(old)
        now = _utc_iso()
        if can_copy and response_col:
            if created_col:
                cur.execute(
                    f"""
                    INSERT INTO bond_raw(secid, kind, asof_date, response_text, created_utc)
                    SELECT secid, kind, asof_date,
                           {response_col} AS response_text,
                           COALESCE({created_col}, ?) AS created_utc
                    FROM bond_raw_old
                    """,
                    (now,),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO bond_raw(secid, kind, asof_date, response_text, created_utc)
                    SELECT secid, kind, asof_date,
                           {response_col} AS response_text,
                           ? AS created_utc
                    FROM bond_raw_old
                    """,
                    (now,),
                )
            conn.commit()

        cur.execute("DROP TABLE IF EXISTS bond_raw_old;")
        conn.commit()

    def _migrate_emitents_if_needed(self, conn: sqlite3.Connection) -> None:
        if not _table_exists(conn, "emitents"):
            return
        have = _cols(conn, "emitents")
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
        cur = self._execute(
            "SELECT payload_json FROM bonds_list WHERE asof_date=?",
            (asof_date,),
        )
        row = cur.fetchone()
        if not row:
            return None
        data = _safe_json_loads(row["payload_json"])
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
            (asof_date, _safe_json_dumps(bonds), _utc_iso()),
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
                _safe_json_dumps(params or {}),
                status,
                elapsed_ms,
                size_bytes,
                response_text,
                _utc_iso(),
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
        raw_json: Optional[str],
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
                updated_utc or _utc_iso(),
            ),
            commit=True,
        )

    def get_emitent(self, emitter_id: int) -> Optional[Dict[str, Any]]:
        cur = self._execute(
            "SELECT * FROM emitents WHERE emitter_id=?",
            (int(emitter_id),),
        )
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
        cur = self._execute(
            "SELECT COUNT(*) AS n FROM requests_log WHERE created_utc >= ?",
            (since_created_utc,),
        )
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
    # TTL purge
    # -------------------------
    def purge_bond_raw(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM bond_raw WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_requests_log(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM requests_log WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_bonds_list(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM bonds_list WHERE created_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    def purge_emitents(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self._execute("DELETE FROM emitents WHERE updated_utc < ?", (cutoff,), commit=True)
        return int(cur.rowcount or 0)

    # -------------------------
    # close
    # -------------------------
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