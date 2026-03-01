from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def write_df(self, conn: sqlite3.Connection, table: str, df: pd.DataFrame, if_exists: str = "replace") -> None:
        df.to_sql(table, conn, if_exists=if_exists, index=False)
