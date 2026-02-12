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


class SQLiteCache:
    """
    Простая БД-кэш:
    - meta: хранит снимки (asof_date_utc) и статистику
    - bonds: хранит данные облигаций, привязанные к asof_date_utc (снимок)
    """

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

    def _init_db(self) -> None:
        with self._connect() as con:
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
            con.commit()

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
        """
        Перезаписывает снимок за дату asof_date_utc.
        """
        # приведение колонок к ожидаемым именам
        df2 = df.copy()

        # нормализуем имена в lowercase под схему
        df2.columns = [c.lower() for c in df2.columns]

        # гарантируем наличие is_active_status
        if "is_active_status" not in df2.columns and "status" in df2.columns:
            df2["is_active_status"] = (df2["status"].astype(str).str.upper() == "A").astype(int)

        # строки дат -> ISO (SQLite хранит TEXT)
        for c in ("issuedate", "matdate"):
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], errors="coerce").dt.date.astype("string")

        df2["asof_date_utc"] = asof_date_utc

        # Подмножество колонок под таблицу
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
            # удаляем старый снимок (если был)
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

    def query(self, sql: str, params: Iterable | None = None) -> pd.DataFrame:
        with self._connect() as con:
            return pd.read_sql_query(sql, con, params=params)