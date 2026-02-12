# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


@dataclass
class CacheInfo:
    asof_date_utc: str
    rows: int
    created_utc: str


@dataclass
class RequestsLogSummary:
    total: int
    errors: int


class SQLiteCache:
    """
    SQLite кэш и склад:
    - schema_version: версионирование схемы
    - meta/bonds: снимок списка бондов за дату
    - requests_log: лог всех HTTP-запросов
    - bond_raw: RAW JSON детализации по (asof_date, secid, kind)
    - bond_description: нормализованная таблица description
    - bond_events: универсальный склад табличных блоков
    - bond_coupons / bond_offers / bond_amortizations: нормализованные таблицы из bondization
    """

    LATEST_SCHEMA_VERSION = 3

    def __init__(self, db_path: str | Path = "moex_cache.sqlite", logger: Optional[logging.Logger] = None):
        self.db_path = Path(db_path)
        self.logger = logger or logging.getLogger("SQLiteCache")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    # ---------- Schema versioning & migrations ----------

    def _get_schema_version(self, con: sqlite3.Connection) -> int:
        row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';"
        ).fetchone()
        if not row:
            return 0
        v = con.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1;").fetchone()
        return int(v[0]) if v else 0

    def _set_schema_version(self, con: sqlite3.Connection, version: int, applied_utc: str) -> None:
        con.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                applied_utc TEXT NOT NULL
            );
        """)
        con.execute(
            "INSERT OR REPLACE INTO schema_version(version, applied_utc) VALUES (?, ?);",
            (version, applied_utc),
        )

    def _migrate_0_to_1(self, con: sqlite3.Connection, applied_utc: str) -> None:
        con.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                asof_date_utc TEXT PRIMARY KEY,
                created_utc   TEXT NOT NULL,
                rows          INTEGER NOT NULL
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS bonds (
                asof_date_utc   TEXT NOT NULL,
                secid           TEXT,
                boardid         TEXT,
                shortname       TEXT,
                name            TEXT,
                isin            TEXT,
                regnumber       TEXT,
                status          TEXT,
                listlevel       TEXT,
                issuedate       TEXT,
                matdate         TEXT,
                facevalue       REAL,
                faceunit        TEXT,
                lotsize         REAL,
                couponpercent   REAL,
                couponvalue     REAL,
                couponperiod    REAL,
                is_active_status INTEGER,
                PRIMARY KEY (asof_date_utc, secid, boardid)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_bonds_asof ON bonds(asof_date_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bonds_isin ON bonds(isin);")
        self._set_schema_version(con, 1, applied_utc)

    def _migrate_1_to_2(self, con: sqlite3.Connection, applied_utc: str) -> None:
        con.execute("""
            CREATE TABLE IF NOT EXISTS requests_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                created_utc   TEXT NOT NULL,
                url           TEXT NOT NULL,
                params_json   TEXT,
                status_code   INTEGER,
                elapsed_ms    REAL,
                response_size INTEGER,
                error         TEXT
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_requests_created ON requests_log(created_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_requests_url ON requests_log(url);")

        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_raw (
                asof_date_utc TEXT NOT NULL,
                secid         TEXT NOT NULL,
                fetched_utc   TEXT NOT NULL,
                url           TEXT NOT NULL,
                params_json   TEXT,
                payload_json  TEXT NOT NULL,
                PRIMARY KEY (asof_date_utc, secid)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_date ON bond_raw(asof_date_utc);")

        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_description (
                asof_date_utc TEXT NOT NULL,
                secid         TEXT NOT NULL,
                name          TEXT,
                title         TEXT,
                value         TEXT,
                type          TEXT,
                sort_order    INTEGER,
                is_hidden     INTEGER,
                PRIMARY KEY (asof_date_utc, secid, name)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_desc_date ON bond_description(asof_date_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_desc_secid ON bond_description(secid);")

        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                asof_date_utc TEXT NOT NULL,
                secid         TEXT NOT NULL,
                block         TEXT NOT NULL,
                row_json      TEXT NOT NULL
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_events_date ON bond_events(asof_date_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_events_secid ON bond_events(secid);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_events_block ON bond_events(block);")

        self._set_schema_version(con, 2, applied_utc)

    def _migrate_2_to_3(self, con: sqlite3.Connection, applied_utc: str) -> None:
        # 1) апгрейд bond_raw -> теперь можно хранить несколько kinds (security/bondization)
        # создадим новую таблицу и переложим данные
        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_raw_new (
                asof_date_utc TEXT NOT NULL,
                secid         TEXT NOT NULL,
                kind          TEXT NOT NULL,  -- security | bondization | ...
                fetched_utc   TEXT NOT NULL,
                url           TEXT NOT NULL,
                params_json   TEXT,
                payload_json  TEXT NOT NULL,
                PRIMARY KEY (asof_date_utc, secid, kind)
            );
        """)

        # если старая bond_raw существует и в ней есть данные — перенесём как kind='security'
        row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bond_raw';"
        ).fetchone()
        if row:
            con.execute("""
                INSERT OR REPLACE INTO bond_raw_new(asof_date_utc,secid,kind,fetched_utc,url,params_json,payload_json)
                SELECT asof_date_utc,secid,'security',fetched_utc,url,params_json,payload_json
                FROM bond_raw;
            """)
            con.execute("DROP TABLE bond_raw;")

        con.execute("ALTER TABLE bond_raw_new RENAME TO bond_raw;")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_date ON bond_raw(asof_date_utc);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bond_raw_kind ON bond_raw(kind);")

        # 2) нормализованные таблицы bondization
        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_coupons (
                asof_date_utc  TEXT NOT NULL,
                secid          TEXT NOT NULL,
                coupondate     TEXT,
                startdate      TEXT,
                enddate        TEXT,
                value          REAL,
                percent        REAL,
                currency       TEXT,
                row_json       TEXT NOT NULL,
                PRIMARY KEY (asof_date_utc, secid, coupondate, startdate, enddate)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_coupons_secid ON bond_coupons(secid);")

        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_offers (
                asof_date_utc  TEXT NOT NULL,
                secid          TEXT NOT NULL,
                offerdate      TEXT,
                offertype      TEXT,
                price          REAL,
                currency       TEXT,
                row_json       TEXT NOT NULL,
                PRIMARY KEY (asof_date_utc, secid, offerdate, offertype)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_offers_secid ON bond_offers(secid);")

        con.execute("""
            CREATE TABLE IF NOT EXISTS bond_amortizations (
                asof_date_utc  TEXT NOT NULL,
                secid          TEXT NOT NULL,
                amortdate      TEXT,
                value          REAL,
                percent        REAL,
                currency       TEXT,
                row_json       TEXT NOT NULL,
                PRIMARY KEY (asof_date_utc, secid, amortdate)
            );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_amort_secid ON bond_amortizations(secid);")

        self._set_schema_version(con, 3, applied_utc)

    def _init_db(self) -> None:
        from logs import utc_now_iso  # локально, чтобы не ловить циклический импорт

        applied_utc = utc_now_iso()
        with self._connect() as con:
            cur_ver = self._get_schema_version(con)

            if cur_ver == 0:
                self.logger.info("DB migration: 0 -> 1")
                self._migrate_0_to_1(con, applied_utc)
                cur_ver = 1

            if cur_ver == 1:
                self.logger.info("DB migration: 1 -> 2")
                self._migrate_1_to_2(con, applied_utc)
                cur_ver = 2

            if cur_ver == 2:
                self.logger.info("DB migration: 2 -> 3")
                self._migrate_2_to_3(con, applied_utc)
                cur_ver = 3

            if cur_ver != self.LATEST_SCHEMA_VERSION:
                raise RuntimeError(f"Unsupported schema version: {cur_ver}")

            con.commit()

    # ---------- Requests log ----------

    def log_request(
        self,
        created_utc: str,
        url: str,
        params_json: str | None,
        status_code: int | None,
        elapsed_ms: float | None,
        response_size: int | None,
        error: str | None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO requests_log(created_utc,url,params_json,status_code,elapsed_ms,response_size,error)
                VALUES (?,?,?,?,?,?,?);
                """,
                (created_utc, url, params_json, status_code, elapsed_ms, response_size, error),
            )
            con.commit()

    def requests_summary_since(self, since_utc: str) -> RequestsLogSummary:
        with self._connect() as con:
            total = con.execute(
                "SELECT COUNT(*) FROM requests_log WHERE created_utc >= ?;",
                (since_utc,),
            ).fetchone()[0]
            errors = con.execute(
                "SELECT COUNT(*) FROM requests_log WHERE created_utc >= ? AND error IS NOT NULL;",
                (since_utc,),
            ).fetchone()[0]
        return RequestsLogSummary(total=int(total), errors=int(errors))

    # ---------- Bonds snapshot cache ----------

    def has_snapshot(self, asof_date_utc: str) -> bool:
        with self._connect() as con:
            row = con.execute("SELECT 1 FROM meta WHERE asof_date_utc = ? LIMIT 1;", (asof_date_utc,)).fetchone()
            return row is not None

    def get_snapshot_info(self, asof_date_utc: str) -> Optional[CacheInfo]:
        with self._connect() as con:
            row = con.execute(
                "SELECT asof_date_utc, rows, created_utc FROM meta WHERE asof_date_utc = ?;",
                (asof_date_utc,),
            ).fetchone()
            if not row:
                return None
            return CacheInfo(asof_date_utc=row[0], rows=int(row[1]), created_utc=row[2])

    def load_bonds(self, asof_date_utc: str) -> pd.DataFrame:
        with self._connect() as con:
            df = pd.read_sql_query(
                "SELECT * FROM bonds WHERE asof_date_utc = ?;",
                con,
                params=(asof_date_utc,),
            )
        return df

    def save_bonds_snapshot(self, asof_date_utc: str, created_utc: str, df: pd.DataFrame) -> None:
        df2 = df.copy()
        df2.columns = [c.lower() for c in df2.columns]

        if "is_active_status" not in df2.columns and "status" in df2.columns:
            df2["is_active_status"] = (df2["status"].astype(str).str.upper() == "A").astype(int)

        for c in ("issuedate", "matdate"):
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], errors="coerce").dt.date.astype("string")

        df2["asof_date_utc"] = asof_date_utc

        cols = [
            "asof_date_utc",
            "secid", "boardid", "shortname", "name", "isin", "regnumber",
            "status", "listlevel", "issuedate", "matdate",
            "facevalue", "faceunit", "lotsize",
            "couponpercent", "couponvalue", "couponperiod",
            "is_active_status",
        ]
        for c in cols:
            if c not in df2.columns:
                df2[c] = None
        df2 = df2[cols]

        with self._connect() as con:
            con.execute("DELETE FROM bonds WHERE asof_date_utc = ?;", (asof_date_utc,))
            con.execute("DELETE FROM meta  WHERE asof_date_utc = ?;", (asof_date_utc,))
            df2.to_sql("bonds", con, if_exists="append", index=False)
            con.execute(
                "INSERT INTO meta(asof_date_utc, created_utc, rows) VALUES (?, ?, ?);",
                (asof_date_utc, created_utc, int(len(df2))),
            )
            con.commit()

        self.logger.info("SQLite snapshot saved | date=%s | rows=%d | db=%s",
                         asof_date_utc, len(df2), self.db_path.resolve())

    # ---------- Detail storage ----------

    def save_bond_raw(
        self,
        asof_date_utc: str,
        secid: str,
        kind: str,
        fetched_utc: str,
        url: str,
        params_json: str | None,
        payload_json: str,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO bond_raw(asof_date_utc,secid,kind,fetched_utc,url,params_json,payload_json)
                VALUES (?,?,?,?,?,?,?);
                """,
                (asof_date_utc, secid, kind, fetched_utc, url, params_json, payload_json),
            )
            con.commit()

    def replace_bond_description(self, asof_date_utc: str, secid: str, df_desc: pd.DataFrame) -> None:
        df = df_desc.copy()
        df.columns = [c.lower() for c in df.columns]
        df["asof_date_utc"] = asof_date_utc
        df["secid"] = secid

        need = ["asof_date_utc", "secid", "name", "title", "value", "type", "sort_order", "is_hidden"]
        for c in need:
            if c not in df.columns:
                df[c] = None
        df = df[need]

        if "is_hidden" in df.columns:
            df["is_hidden"] = df["is_hidden"].map(lambda x: 1 if str(x).lower() in ("1", "true", "t", "yes") else 0)

        with self._connect() as con:
            con.execute("DELETE FROM bond_description WHERE asof_date_utc=? AND secid=?;", (asof_date_utc, secid))
            df.to_sql("bond_description", con, if_exists="append", index=False)
            con.commit()

    def replace_bond_events(self, asof_date_utc: str, secid: str, block: str, rows_json: list[str]) -> None:
        with self._connect() as con:
            con.execute(
                "DELETE FROM bond_events WHERE asof_date_utc=? AND secid=? AND block=?;",
                (asof_date_utc, secid, block),
            )
            con.executemany(
                "INSERT INTO bond_events(asof_date_utc,secid,block,row_json) VALUES (?,?,?,?);",
                [(asof_date_utc, secid, block, rj) for rj in rows_json],
            )
            con.commit()

    def replace_bond_coupons(self, asof_date_utc: str, secid: str, rows: list[dict], rows_json: list[str]) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM bond_coupons WHERE asof_date_utc=? AND secid=?;", (asof_date_utc, secid))
            con.executemany(
                """
                INSERT OR REPLACE INTO bond_coupons(
                    asof_date_utc, secid, coupondate, startdate, enddate, value, percent, currency, row_json
                ) VALUES (?,?,?,?,?,?,?,?,?);
                """,
                [
                    (
                        asof_date_utc, secid,
                        r.get("coupondate"), r.get("startdate"), r.get("enddate"),
                        r.get("value"), r.get("percent"), r.get("currency"),
                        rj
                    )
                    for r, rj in zip(rows, rows_json)
                ],
            )
            con.commit()

    def replace_bond_offers(self, asof_date_utc: str, secid: str, rows: list[dict], rows_json: list[str]) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM bond_offers WHERE asof_date_utc=? AND secid=?;", (asof_date_utc, secid))
            con.executemany(
                """
                INSERT OR REPLACE INTO bond_offers(
                    asof_date_utc, secid, offerdate, offertype, price, currency, row_json
                ) VALUES (?,?,?,?,?,?,?);
                """,
                [
                    (
                        asof_date_utc, secid,
                        r.get("offerdate"), r.get("offertype"),
                        r.get("price"), r.get("currency"),
                        rj
                    )
                    for r, rj in zip(rows, rows_json)
                ],
            )
            con.commit()

    def replace_bond_amortizations(self, asof_date_utc: str, secid: str, rows: list[dict], rows_json: list[str]) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM bond_amortizations WHERE asof_date_utc=? AND secid=?;", (asof_date_utc, secid))
            con.executemany(
                """
                INSERT OR REPLACE INTO bond_amortizations(
                    asof_date_utc, secid, amortdate, value, percent, currency, row_json
                ) VALUES (?,?,?,?,?,?,?);
                """,
                [
                    (
                        asof_date_utc, secid,
                        r.get("amortdate"),
                        r.get("value"), r.get("percent"), r.get("currency"),
                        rj
                    )
                    for r, rj in zip(rows, rows_json)
                ],
            )
            con.commit()

    # ---------- Generic query ----------

    def query(self, sql: str, params: Iterable | None = None) -> pd.DataFrame:
        with self._connect() as con:
            return pd.read_sql_query(sql, con, params=params)