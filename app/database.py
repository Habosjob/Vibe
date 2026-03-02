from __future__ import annotations

import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds_snapshot (
                secid TEXT PRIMARY KEY,
                last_price REAL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                fetched_count INTEGER NOT NULL DEFAULT 0,
                selected_count INTEGER NOT NULL DEFAULT 0,
                saved_count INTEGER NOT NULL DEFAULT 0,
                errors_count INTEGER NOT NULL DEFAULT 0,
                from_cache_count INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self.conn.commit()

    def fetch_previous_prices(self) -> dict[str, float]:
        rows = self.conn.execute("SELECT secid, last_price FROM bonds_snapshot;").fetchall()
        return {str(row["secid"]): float(row["last_price"]) for row in rows if row["last_price"] is not None}

    def upsert_snapshot(self, rows: list[tuple[str, float | None, str]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO bonds_snapshot (secid, last_price, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    last_price=excluded.last_price,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def close(self) -> None:
        self.conn.close()
