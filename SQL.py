# SQL.py
from __future__ import annotations

import json
import sqlite3
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
    return {r[1] for r in cur.fetchall()}  # r[1] = name


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


@dataclass(frozen=True)
class RequestLogRow:
    # То, что логирует Moex_API.py на каждый HTTP запрос
    url: str
    params_json: str
    status: Optional[int]
    elapsed_ms: Optional[int]
    size_bytes: Optional[int]
    created_utc: str
    error: Optional[str] = None


class SQLiteCache:
    """
    Контракт класса задаётся Moex_API.py.

    Должны существовать:
      - get_bonds_list / set_bonds_list
      - get_bond_raw / set_bond_raw
      - get_emitent / upsert_emitent
      - log_request / requests_summary

    + добавлены purge_* для TTL.
    """

    def __init__(self, db_path: str = "moex_cache.sqlite", logger=None):
        self.logger = logger
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._ensure_schema()
        if self.logger:
            self.logger.info(f"SQLiteCache initialized | db={db_path}")

    # -------------------------
    # Init / schema / migration
    # -------------------------
    def _apply_pragmas(self) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=5000;")
        except Exception:
            pass

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()

        # ---- bonds_list (daily) ----
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
            """
            CREATE INDEX IF NOT EXISTS idx_bonds_list_created
            ON bonds_list(created_utc)
            """
        )

        # ---- bond_raw ----
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
            """
            CREATE INDEX IF NOT EXISTS idx_bond_raw_lookup
            ON bond_raw(secid, kind, asof_date)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bond_raw_created
            ON bond_raw(created_utc)
            """
        )

        # ---- emitents ---- (расширили поля)
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
            """
            CREATE INDEX IF NOT EXISTS idx_emitents_updated
            ON emitents(updated_utc)
            """
        )

        # ---- requests_log ----
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
            """
            CREATE INDEX IF NOT EXISTS idx_requests_log_created
            ON requests_log(created_utc)
            """
        )

        self.conn.commit()

        # миграции старых схем
        self._migrate_bonds_list_if_needed()
        self._migrate_bond_raw_if_needed()
        self._migrate_emitents_if_needed()

    def _migrate_bonds_list_if_needed(self) -> None:
        """
        Если bonds_list уже существует, но колонка payload_json называется иначе,
        пересоздаём таблицу и переносим данные best-effort.
        """
        if not _table_exists(self.conn, "bonds_list"):
            return
        have = _cols(self.conn, "bonds_list")
        want = {"asof_date", "payload_json", "created_utc"}
        if want.issubset(have):
            return

        if self.logger:
            self.logger.warning(
                f"Schema mismatch: bonds_list columns={sorted(have)} -> recreate/migrate"
            )

        cur = self.conn.cursor()
        cur.execute("ALTER TABLE bonds_list RENAME TO bonds_list_old;")
        self.conn.commit()

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
        self.conn.commit()

        old = _cols(self.conn, "bonds_list_old")

        payload_col = None
        for cand in ("payload_json", "payload", "data", "json", "value", "bonds_json"):
            if cand in old:
                payload_col = cand
                break

        created_col = None
        for cand in ("created_utc", "created_at", "created", "ts", "timestamp"):
            if cand in old:
                created_col = cand
                break

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
            self.conn.commit()

        cur.execute("DROP TABLE IF EXISTS bonds_list_old;")
        self.conn.commit()

    def _migrate_bond_raw_if_needed(self) -> None:
        """
        Если у пользователя была старая schema bond_raw, делаем rebuild.
        created_utc NOT NULL -> COALESCE(created_col, now).
        """
        if not _table_exists(self.conn, "bond_raw"):
            return
        have = _cols(self.conn, "bond_raw")
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

        cur = self.conn.cursor()
        cur.execute("ALTER TABLE bond_raw RENAME TO bond_raw_old;")
        self.conn.commit()

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
        self.conn.commit()

        old = _cols(self.conn, "bond_raw_old")

        response_col = None
        for cand in ("response_text", "payload", "payload_json"):
            if cand in old:
                response_col = cand
                break

        created_col = None
        for cand in ("created_utc", "created_at"):
            if cand in old:
                created_col = cand
                break

        can_copy_keys = {"secid", "kind", "asof_date"}.issubset(old)
        now = _utc_iso()
        if can_copy_keys and response_col:
            if created_col:
                cur.execute(
                    f"""
                    INSERT INTO bond_raw(secid, kind, asof_date, response_text, created_utc)
                    SELECT secid,
                           kind,
                           asof_date,
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
                    SELECT secid,
                           kind,
                           asof_date,
                           {response_col} AS response_text,
                           ? AS created_utc
                    FROM bond_raw_old
                    """,
                    (now,),
                )
            self.conn.commit()

        cur.execute("DROP TABLE IF EXISTS bond_raw_old;")
        self.conn.commit()

    def _migrate_emitents_if_needed(self) -> None:
        """
        Добавляем новые колонки в emitents, если база была старой.
        """
        if not _table_exists(self.conn, "emitents"):
            return
        have = _cols(self.conn, "emitents")

        # если старый emitents без новых полей — ALTER TABLE ADD COLUMN
        desired = [
            ("kpp", "TEXT"),
            ("okved", "TEXT"),
            ("address", "TEXT"),
            ("phone", "TEXT"),
            ("site", "TEXT"),
            ("email", "TEXT"),
        ]

        cur = self.conn.cursor()
        changed = False
        for col, typ in desired:
            if col not in have:
                cur.execute(f"ALTER TABLE emitents ADD COLUMN {col} {typ}")
                changed = True

        if changed:
            self.conn.commit()
            if self.logger:
                self.logger.info("emitents schema upgraded (added extra columns)")

    # -------------------------
    # bonds list daily cache
    # -------------------------
    def get_bonds_list(self, asof_date: str) -> Optional[List[dict]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT payload_json
            FROM bonds_list
            WHERE asof_date=?
            """,
            (asof_date,),
        )
        row = cur.fetchone()
        if not row:
            return None
        data = _safe_json_loads(row["payload_json"])
        return data if isinstance(data, list) else None

    def set_bonds_list(self, bonds: List[dict], asof_date: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO bonds_list(asof_date, payload_json, created_utc)
            VALUES(?, ?, ?)
            ON CONFLICT(asof_date) DO UPDATE SET
                payload_json=excluded.payload_json,
                created_utc=excluded.created_utc
            """,
            (asof_date, _safe_json_dumps(bonds), _utc_iso()),
        )
        self.conn.commit()

    # -------------------------
    # bond_raw (detail raw store)
    # -------------------------
    def get_bond_raw(self, secid: str, kind: str, asof_date: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
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
        cur = self.conn.cursor()
        cur.execute(
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
        )
        self.conn.commit()
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
    ) -> None:
        cur = self.conn.cursor()
        cur.execute(
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
                _utc_iso(),
            ),
        )
        self.conn.commit()

    def get_emitent(self, emitter_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM emitents
            WHERE emitter_id=?
            """,
            (int(emitter_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    # -------------------------
    # requests log
    # -------------------------
    def log_request(self, row: RequestLogRow) -> int:
        cur = self.conn.cursor()
        cur.execute(
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
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def requests_summary(self, since_created_utc: str) -> Dict[str, int]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM requests_log
            WHERE created_utc >= ?
            """,
            (since_created_utc,),
        )
        total = int(cur.fetchone()["n"])

        cur.execute(
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
        cur = self.conn.cursor()
        cur.execute("DELETE FROM bond_raw WHERE created_utc < ?", (cutoff,))
        n = cur.rowcount if cur.rowcount is not None else 0
        self.conn.commit()
        return int(n)

    def purge_requests_log(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self.conn.cursor()
        cur.execute("DELETE FROM requests_log WHERE created_utc < ?", (cutoff,))
        n = cur.rowcount if cur.rowcount is not None else 0
        self.conn.commit()
        return int(n)

    def purge_bonds_list(self, keep_days: int) -> int:
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self.conn.cursor()
        cur.execute("DELETE FROM bonds_list WHERE created_utc < ?", (cutoff,))
        n = cur.rowcount if cur.rowcount is not None else 0
        self.conn.commit()
        return int(n)

    def purge_emitents(self, keep_days: int) -> int:
        """
        Обычно эмитенты почти статичны, поэтому purge лучше делать редко/осторожно.
        """
        if int(keep_days) <= 0:
            return 0
        cutoff = _utc_iso_days_ago(int(keep_days))
        cur = self.conn.cursor()
        cur.execute("DELETE FROM emitents WHERE updated_utc < ?", (cutoff,))
        n = cur.rowcount if cur.rowcount is not None else 0
        self.conn.commit()
        return int(n)

    # -------------------------
    # close
    # -------------------------
    def close(self) -> None:
        try:
            self.conn.close()
        finally:
            if self.logger:
                self.logger.info("SQLiteCache closed")