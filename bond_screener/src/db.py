from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pandas as pd


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")

    def write_df(self, table: str, df: pd.DataFrame, if_exists: str = "replace") -> None:
        if df is None or df.columns.empty:
            return
        df.to_sql(table, self.conn, if_exists=if_exists, index=False)

    def read_df(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn)

    def ensure_source_tables(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS moex_coupons (
              secid TEXT,
              coupondate TEXT,
              value REAL,
              rate REAL,
              currencyid TEXT,
              fetched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS moex_amortizations (
              secid TEXT,
              amortdate TEXT,
              value REAL,
              currencyid TEXT,
              fetched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS moex_amort_agg (
              secid TEXT PRIMARY KEY,
              first_amort_date TEXT,
              has_amortization INTEGER,
              fetched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS smartlab_bond (
              secid TEXT PRIMARY KEY,
              sl_price_rub REAL,
              sl_price_pct REAL,
              sl_ytm REAL,
              sl_nkd_rub REAL,
              sl_coupon_rub REAL,
              sl_coupon_rate_pct REAL,
              sl_coupon_freq_per_year REAL,
              sl_next_coupon_date_ddmmyyyy TEXT,
              sl_maturity_date_ddmmyyyy TEXT,
              sl_offer_date_ddmmyyyy TEXT,
              sl_is_qual TEXT,
              sl_credit_rating TEXT,
              sl_rating_source TEXT,
              fetched_at TEXT,
              source_hash TEXT,
              warning_text TEXT
            );
            CREATE TABLE IF NOT EXISTS dropped_bonds (
              key TEXT,
              key_type TEXT,
              reason_code TEXT,
              reason_text TEXT,
              dropped_at TEXT,
              until TEXT,
              is_permanent INTEGER,
              updated_at TEXT,
              PRIMARY KEY (key, key_type, reason_code)
            );
            """
        )
        self.conn.commit()

    def _build_upsert_sql(self, table: str, cols: list[str]) -> str:
        placeholders = ",".join(["?"] * len(cols))
        col_sql = ",".join(cols)
        if table == "smartlab_bond":
            update = ",".join([f"{c}=excluded.{c}" for c in cols if c != "secid"])
            return f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT(secid) DO UPDATE SET {update}"
        if table == "moex_amort_agg":
            update = ",".join([f"{c}=excluded.{c}" for c in cols if c != "secid"])
            return f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT(secid) DO UPDATE SET {update}"
        if table == "dropped_bonds":
            update = ",".join([f"{c}=excluded.{c}" for c in cols if c not in {"key", "key_type", "reason_code"}])
            return (
                f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) "
                "ON CONFLICT(key,key_type,reason_code) DO UPDATE SET " + update
            )
        return f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

    def upsert_many(self, table: str, rows: list[dict], retries: int = 5, commit: bool = True) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        sql = self._build_upsert_sql(table, cols)
        vals = [tuple(r.get(c) for c in cols) for r in rows]
        for i in range(retries):
            try:
                self.conn.executemany(sql, vals)
                if commit:
                    self.conn.commit()
                return len(rows)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and i < retries - 1:
                    time.sleep(min(0.5, 0.05 * (2**i)))
                    continue
                raise
        return 0

    def commit(self, retries: int = 5) -> None:
        for i in range(retries):
            try:
                self.conn.commit()
                return
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and i < retries - 1:
                    time.sleep(min(0.5, 0.05 * (2**i)))
                    continue
                raise

    def close(self) -> None:
        self.conn.close()
