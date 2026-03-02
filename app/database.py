from __future__ import annotations

import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        self._create_schema()

    def _create_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds (
                secid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                coupon_rate REAL NOT NULL,
                maturity_years INTEGER NOT NULL,
                rating TEXT NOT NULL,
                source_hash TEXT NOT NULL,
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

    def upsert_bonds(self, rows: list[tuple[str, str, float, int, str, str, str]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO bonds (secid, name, coupon_rate, maturity_years, rating, source_hash, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    name=excluded.name,
                    coupon_rate=excluded.coupon_rate,
                    maturity_years=excluded.maturity_years,
                    rating=excluded.rating,
                    source_hash=excluded.source_hash,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def fetch_selected(self) -> list[tuple[str, str, float, int, str]]:
        return self.conn.execute(
            """
            SELECT secid, name, coupon_rate, maturity_years, rating
            FROM bonds
            ORDER BY secid;
            """
        ).fetchall()

    def close(self) -> None:
        self.conn.close()
