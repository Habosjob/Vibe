from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


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
            CREATE TABLE IF NOT EXISTS bond_description_cache (
                secid TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bond_amortization_cache (
                secid TEXT PRIMARY KEY,
                amortization_start TEXT,
                updated_at INTEGER NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emitter_cache (
                emitter_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                inn TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS corpbonds_cache (
                secid TEXT PRIMARY KEY,
                price TEXT NOT NULL,
                credit_rating TEXT NOT NULL,
                coupon_type TEXT NOT NULL,
                coupon_formula TEXT NOT NULL,
                nearest_offer_date TEXT NOT NULL,
                ladder_coupon TEXT NOT NULL,
                updated_at INTEGER NOT NULL
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

    def get_cached_descriptions(self, secids: list[str], min_ts: int) -> tuple[dict[str, str], list[str]]:
        if not secids:
            return {}, []
        placeholders = ",".join("?" for _ in secids)
        rows = self.conn.execute(
            f"SELECT secid, payload_json, updated_at FROM bond_description_cache WHERE secid IN ({placeholders});",
            secids,
        ).fetchall()
        cached: dict[str, str] = {}
        for row in rows:
            if int(row["updated_at"]) >= min_ts:
                cached[str(row["secid"])] = str(row["payload_json"])
        missing = [secid for secid in secids if secid not in cached]
        return cached, missing

    def upsert_descriptions(self, rows: list[tuple[str, str, int]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO bond_description_cache (secid, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def get_cached_amortizations(self, secids: list[str], min_ts: int) -> tuple[dict[str, str], list[str]]:
        if not secids:
            return {}, []
        placeholders = ",".join("?" for _ in secids)
        rows = self.conn.execute(
            f"SELECT secid, amortization_start, updated_at FROM bond_amortization_cache WHERE secid IN ({placeholders});",
            secids,
        ).fetchall()
        cached: dict[str, str] = {}
        for row in rows:
            if int(row["updated_at"]) >= min_ts:
                cached[str(row["secid"])] = str(row["amortization_start"] or "")
        missing = [secid for secid in secids if secid not in cached]
        return cached, missing

    def upsert_amortizations(self, rows: list[tuple[str, str, int]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO bond_amortization_cache (secid, amortization_start, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    amortization_start=excluded.amortization_start,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def get_cached_emitters(self, emitter_ids: list[int], min_ts: int) -> tuple[dict[int, dict[str, str]], list[int]]:
        if not emitter_ids:
            return {}, []
        placeholders = ",".join("?" for _ in emitter_ids)
        rows = self.conn.execute(
            f"SELECT emitter_id, name, inn, updated_at FROM emitter_cache WHERE emitter_id IN ({placeholders});",
            emitter_ids,
        ).fetchall()
        cached: dict[int, dict[str, str]] = {}
        for row in rows:
            emitter_id = int(row["emitter_id"])
            if int(row["updated_at"]) >= min_ts:
                cached[emitter_id] = {"name": str(row["name"]), "inn": str(row["inn"])}
        missing = [emitter_id for emitter_id in emitter_ids if emitter_id not in cached]
        return cached, missing

    def upsert_emitters(self, rows: list[tuple[int, str, str, int]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO emitter_cache (emitter_id, name, inn, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(emitter_id) DO UPDATE SET
                    name=excluded.name,
                    inn=excluded.inn,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def get_cached_corpbonds(self, secids: list[str], min_ts: int) -> tuple[dict[str, dict[str, str]], list[str]]:
        if not secids:
            return {}, []
        placeholders = ",".join("?" for _ in secids)
        rows = self.conn.execute(
            f"SELECT secid, price, credit_rating, coupon_type, coupon_formula, nearest_offer_date, ladder_coupon, updated_at "
            f"FROM corpbonds_cache WHERE secid IN ({placeholders});",
            secids,
        ).fetchall()
        cached: dict[str, dict[str, str]] = {}
        for row in rows:
            secid = str(row["secid"])
            if int(row["updated_at"]) < min_ts:
                continue
            cached[secid] = {
                "price": str(row["price"]),
                "credit_rating": str(row["credit_rating"]),
                "coupon_type": str(row["coupon_type"]),
                "coupon_formula": str(row["coupon_formula"]),
                "nearest_offer_date": str(row["nearest_offer_date"]),
                "ladder_coupon": str(row["ladder_coupon"]),
            }
        missing = [secid for secid in secids if secid not in cached]
        return cached, missing

    def upsert_corpbonds(self, rows: list[tuple[str, str, str, str, str, str, str, int]]) -> int:
        if not rows:
            return 0
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO corpbonds_cache (
                    secid,
                    price,
                    credit_rating,
                    coupon_type,
                    coupon_formula,
                    nearest_offer_date,
                    ladder_coupon,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    price=excluded.price,
                    credit_rating=excluded.credit_rating,
                    coupon_type=excluded.coupon_type,
                    coupon_formula=excluded.coupon_formula,
                    nearest_offer_date=excluded.nearest_offer_date,
                    ladder_coupon=excluded.ladder_coupon,
                    updated_at=excluded.updated_at;
                """,
                rows,
            )
        return len(rows)

    def close(self) -> None:
        self.conn.close()
