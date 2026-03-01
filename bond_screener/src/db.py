from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)

    def write_df(self, table: str, df: pd.DataFrame, if_exists: str = "replace") -> None:
        if df is None:
            raise ValueError(f"df for table {table} is None")
        if df.columns.empty:
            # Avoid sqlite syntax error on CREATE TABLE ... () for empty schema.
            return
        df.to_sql(table, self.conn, if_exists=if_exists, index=False)

    def read_df(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn)

    def close(self) -> None:
        self.conn.close()
