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


def utc_iso_seconds_ago(seconds: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(seconds=int(seconds))
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

        # --- NEW: parse diagnostics ---
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS parse_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                content_type TEXT,
                snippet TEXT,
                created_utc TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_parse_errors_created ON parse_errors(created_utc)")

        conn.commit()

        self._migrate_bonds_list_if_needed(conn)
        self._migrate_bond_raw_if_needed(conn)
        self._migrate_requests_log_if_needed(conn)
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
    def _migrate_bonds_list_if_needed(self, conn: sqlite3.Connection) -> None:
        if not table_exists(conn, "bonds_list"):
            return
        have = cols(conn, "bonds_list")
        if "payload_json" in have and "created_utc" in have:
            return

        cur = conn.cursor()

        # Best-effort: try to map legacy column names into payload_json
        legacy_payload_col = None
        for cand in ("payload_json", "payload", "payload_text", "json", "data"):
            if cand in have:
                legacy_payload_col = cand
                break

        if "created_utc" not in have:
            # add created_utc if missing
            try:
                cur.execute("ALTER TABLE bonds_list ADD COLUMN created_utc TEXT")
            except Exception:
                pass

        if "payload_json" not in have:
            try:
                cur.execute("ALTER TABLE bonds_list ADD COLUMN payload_json TEXT")
            except Exception:
                pass

        if legacy_payload_col and legacy_payload_col != "payload_json":
            try:
                cur.execute(f"UPDATE bonds_list SET payload_json = COALESCE(payload_json, {legacy_payload_col})")
            except Exception:
                pass

        # fill any remaining NULLs
        try:
            cur.execute("UPDATE bonds_list SET payload_json = COALESCE(payload_json, '{}')")
        except Exception:
            pass
        try:
            cur.execute("UPDATE bonds_list SET created_utc = COALESCE(created_utc, ?)", (utc_iso(),))
        except Exception:
            pass

        conn.commit()

    def _migrate_bond_raw_if_needed(self, conn: sqlite3.Connection) -> None:
        if not table_exists(conn, "bond_raw"):
            return
        have = cols(conn, "bond_raw")
        # if schema is already ok
        if "id" in have and "response_text" in have and "created_utc" in have:
            return

        cur = conn.cursor()

        # If id missing, safest is a rebuild preserving rowid order.
        needs_rebuild = "id" not in have
        if not needs_rebuild:
            # add missing columns via ALTER TABLE
            for col, decl in [
                ("url", "TEXT"),
                ("params_json", "TEXT"),
                ("status", "INTEGER"),
                ("elapsed_ms", "INTEGER"),
                ("size_bytes", "INTEGER"),
                ("response_text", "TEXT"),
                ("created_utc", "TEXT"),
            ]:
                if col not in have:
                    try:
                        cur.execute(f"ALTER TABLE bond_raw ADD COLUMN {col} {decl}")
                    except Exception:
                        pass
            conn.commit()
            return

        # rebuild table
        cur.execute("ALTER TABLE bond_raw RENAME TO bond_raw_old")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup ON bond_raw(secid, kind, asof_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_created ON bond_raw(created_utc)")

        # copy best-effort
        have_old = cols(conn, "bond_raw_old")
        # minimal required
        secid_col = "secid" if "secid" in have_old else "SECID"
        kind_col = "kind" if "kind" in have_old else "KIND"
        asof_col = "asof_date" if "asof_date" in have_old else ("asof_" if "asof_" in have_old else "ASOF_DATE")
        created_col = "created_utc" if "created_utc" in have_old else None

        def pick(cands):
            for c in cands:
                if c in have_old:
                    return c
            return None

        url_col = pick(["url", "URL"])
        params_col = pick(["params_json", "PARAMS_JSON", "params"])
        status_col = pick(["status", "STATUS"])
        elapsed_col = pick(["elapsed_ms", "ELAPSED_MS"])
        size_col = pick(["size_bytes", "SIZE_BYTES"])
        resp_col = pick(["response_text", "payload_text", "payload", "RESPONSE_TEXT"])

        select_parts = [
            f"{secid_col} AS secid",
            f"{kind_col} AS kind",
            f"{asof_col} AS asof_date",
            f"{url_col} AS url" if url_col else "NULL AS url",
            f"{params_col} AS params_json" if params_col else "NULL AS params_json",
            f"{status_col} AS status" if status_col else "NULL AS status",
            f"{elapsed_col} AS elapsed_ms" if elapsed_col else "NULL AS elapsed_ms",
            f"{size_col} AS size_bytes" if size_col else "NULL AS size_bytes",
            f"{resp_col} AS response_text" if resp_col else "NULL AS response_text",
            f"{created_col} AS created_utc" if created_col else f"'{utc_iso()}' AS created_utc",
        ]

        cur.execute(
            f"""
            INSERT INTO bond_raw(secid, kind, asof_date, url, params_json, status, elapsed_ms, size_bytes, response_text, created_utc)
            SELECT {', '.join(select_parts)}
            FROM bond_raw_old
            """
        )

        cur.execute("DROP TABLE bond_raw_old")
        conn.commit()

    def _migrate_requests_log_if_needed(self, conn: sqlite3.Connection) -> None:
        if not table_exists(conn, "requests_log"):
            return
        have = cols(conn, "requests_log")
        # ensure id exists (older versions might not)
        if "id" in have:
            return

        cur = conn.cursor()
        cur.execute("ALTER TABLE requests_log RENAME TO requests_log_old")
        cur.execute(
            """
            CREATE TABLE requests_log (
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

        have_old = cols(conn, "requests_log_old")
        def col(name):
            return name if name in have_old else None
        url_col = col("url") or col("URL")
        params_col = col("params_json") or col("PARAMS_JSON")
        status_col = col("status") or col("STATUS")
        elapsed_col = col("elapsed_ms") or col("ELAPSED_MS")
        size_col = col("size_bytes") or col("SIZE_BYTES")
        created_col = col("created_utc") or col("CREATED_UTC")
        error_col = col("error") or col("ERROR")

        cur.execute(
            f"""
            INSERT INTO requests_log(url, params_json, status, elapsed_ms, size_bytes, created_utc, error)
            SELECT
                {url_col or "''"},
                {params_col or "'{}'"},
                {status_col or "NULL"},
                {elapsed_col or "NULL"},
                {size_col or "NULL"},
                {created_col or f"'{utc_iso()}'"},
                {error_col or "NULL"}
            FROM requests_log_old
            """
        )
        cur.execute("DROP TABLE requests_log_old")
        conn.commit()

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

    def log_parse_error(self, url: str, content_type: str, snippet: str) -> int:
        cur = self._execute(
            """
            INSERT INTO parse_errors(url, content_type, snippet, created_utc)
            VALUES(?, ?, ?, ?)
            """,
            (str(url or ""), str(content_type or ""), str(snippet or "")[:8000], utc_iso()),
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

    def progress_take_batch(
        self,
        run_id: str,
        batch: int,
        *,
        include_errors: bool = False,
        max_attempts: int = 2,
        error_retry_after_sec: int = 60,
    ) -> List[str]:
        """
        Берём batch secid со статусом pending.
        Если include_errors=True — также берём status='error' с attempts < max_attempts
        и только если последняя ошибка была достаточно давно (error_retry_after_sec).
        """
        params = [run_id]
        where = "run_id=? AND status='pending'"

        if include_errors and int(max_attempts) > 0:
            cutoff = utc_iso_seconds_ago(int(error_retry_after_sec)) if int(error_retry_after_sec) > 0 else None
            if cutoff:
                where = where + " OR (run_id=? AND status='error' AND attempts < ? AND updated_utc <= ?)"
                params.extend([run_id, int(max_attempts), cutoff])
            else:
                where = where + " OR (run_id=? AND status='error' AND attempts < ?)"
                params.extend([run_id, int(max_attempts)])

        sql = f"""
            SELECT secid
            FROM detail_progress
            WHERE {where}
            ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, secid
            LIMIT ?
            """
        params.append(int(batch))
        cur = self._execute(sql, tuple(params))
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