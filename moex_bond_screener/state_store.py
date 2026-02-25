"""Работа с инкрементальным состоянием скринера: JSON/SQLite бэкенды."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class IncrementalStats:
    inserted: int
    updated: int
    unchanged: int
    removed: int


class ScreenerStateStore:
    def __init__(self, base_dir: str = "state", storage_backend: str = "json", sqlite_db_path: str = "screener_state.db") -> None:
        self.base_path = Path(base_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.storage_backend = (storage_backend or "json").strip().lower()
        if self.storage_backend not in {"json", "sqlite"}:
            self.storage_backend = "json"

        self.exclusions_path = self.base_path / "exclusions.json"
        self.eligible_bonds_path = self.base_path / "eligible_bonds.json"
        self.checkpoints_dir = self.base_path / "checkpoints"
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        self.emitents_registry_path = self.base_path / "emitents_registry.json"
        self.secid_to_emitter_path = self.base_path / "secid_to_emitter.json"

        self.db_path = self.base_path / sqlite_db_path
        if self.storage_backend == "sqlite":
            self._init_sqlite()

    def load_exclusions(self) -> dict[str, dict[str, str]]:
        if self.storage_backend == "sqlite":
            return self._load_exclusions_sqlite()

        payload = self._load_json(self.exclusions_path)
        exclusions = payload.get("exclusions", {})
        if isinstance(exclusions, dict):
            return {
                str(secid): {
                    "rule": str(details.get("rule", "manual")),
                    "exclude_until": str(details.get("exclude_until", "")),
                }
                for secid, details in exclusions.items()
                if isinstance(details, dict)
            }
        return {}

    def save_exclusions(self, exclusions: dict[str, dict[str, str]]) -> None:
        if self.storage_backend == "sqlite":
            self._save_exclusions_sqlite(exclusions)
            return
        self._save_json(self.exclusions_path, {"exclusions": exclusions})

    def update_eligible_bonds(self, bonds: list[dict[str, Any]]) -> IncrementalStats:
        if self.storage_backend == "sqlite":
            return self._update_eligible_bonds_sqlite(bonds)

        stored_bonds = self._load_eligible_bonds_map_json()
        return self._update_eligible_bonds_common(bonds, stored_bonds, self._save_eligible_bonds_json)

    def load_eligible_bonds(self) -> list[dict[str, Any]]:
        if self.storage_backend == "sqlite":
            query = "SELECT payload FROM eligible_bonds"
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(query).fetchall()
            bonds: list[dict[str, Any]] = []
            for (payload,) in rows:
                try:
                    parsed = json.loads(str(payload))
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    bonds.append(parsed)
            return bonds

        return list(self._load_eligible_bonds_map_json().values())

    def load_checkpoint(self, name: str) -> dict[str, Any]:
        if self.storage_backend == "sqlite":
            safe_name = self._safe_checkpoint_name(name)
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute("SELECT payload FROM checkpoints WHERE name = ?", (safe_name,)).fetchone()
            if not row:
                return {}
            try:
                payload = json.loads(str(row[0]))
            except json.JSONDecodeError:
                return {}
            return payload if isinstance(payload, dict) else {}

        return self._load_json(self._checkpoint_path(name))

    def save_checkpoint(self, name: str, payload: dict[str, Any]) -> None:
        if self.storage_backend == "sqlite":
            safe_name = self._safe_checkpoint_name(name)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO checkpoints(name, payload, updated_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at",
                    (safe_name, json.dumps(payload, ensure_ascii=False), datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            return

        self._save_json(self._checkpoint_path(name), payload)

    def clear_checkpoint(self, name: str) -> None:
        if self.storage_backend == "sqlite":
            safe_name = self._safe_checkpoint_name(name)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM checkpoints WHERE name = ?", (safe_name,))
                conn.commit()
            return

        path = self._checkpoint_path(name)
        if path.exists():
            path.unlink()

    def load_emitents_registry(self) -> dict[str, dict[str, str]]:
        if self.storage_backend == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute("SELECT emitter_id, full_name, inn FROM emitents_registry").fetchall()
            return {str(eid): {"full_name": str(name or ""), "inn": str(inn or "")} for eid, name, inn in rows}

        payload = self._load_json(self.emitents_registry_path)
        emitents = payload.get("emitents", {}) if isinstance(payload, dict) else {}
        if not isinstance(emitents, dict):
            return {}

        normalized: dict[str, dict[str, str]] = {}
        for emitter_id, details in emitents.items():
            if not isinstance(details, dict):
                continue
            normalized[str(emitter_id)] = {
                "full_name": str(details.get("full_name") or ""),
                "inn": str(details.get("inn") or ""),
            }
        return normalized

    def save_emitents_registry(self, emitents: dict[str, dict[str, str]]) -> None:
        if self.storage_backend == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM emitents_registry")
                conn.executemany(
                    "INSERT INTO emitents_registry(emitter_id, full_name, inn) VALUES (?, ?, ?)",
                    [
                        (str(emitter_id), str(details.get("full_name") or ""), str(details.get("inn") or ""))
                        for emitter_id, details in emitents.items()
                    ],
                )
                conn.commit()
            return

        self._save_json(self.emitents_registry_path, {"emitents": emitents})

    def load_secid_to_emitter_map(self) -> dict[str, str]:
        if self.storage_backend == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute("SELECT secid, emitter_id FROM secid_to_emitter").fetchall()
            return {str(secid): str(emitter_id) for secid, emitter_id in rows if str(secid).strip() and str(emitter_id).strip()}

        payload = self._load_json(self.secid_to_emitter_path)
        mappings = payload.get("mappings", {}) if isinstance(payload, dict) else {}
        if not isinstance(mappings, dict):
            return {}
        return {
            str(secid): str(emitter_id)
            for secid, emitter_id in mappings.items()
            if str(secid).strip() and str(emitter_id).strip()
        }

    def save_secid_to_emitter_map(self, mappings: dict[str, str]) -> None:
        if self.storage_backend == "sqlite":
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM secid_to_emitter")
                conn.executemany(
                    "INSERT INTO secid_to_emitter(secid, emitter_id) VALUES (?, ?)",
                    [(str(secid), str(emitter_id)) for secid, emitter_id in mappings.items()],
                )
                conn.commit()
            return

        self._save_json(self.secid_to_emitter_path, {"mappings": mappings})

    def load_market_cache(self, market: str, max_age_hours: int = 24) -> list[dict[str, Any]] | None:
        checkpoint = self.load_checkpoint(f"market_cache_{market}")
        updated_at_raw = checkpoint.get("updated_at")
        if not isinstance(updated_at_raw, str):
            return None
        try:
            updated_at = datetime.fromisoformat(updated_at_raw)
        except ValueError:
            return None
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - updated_at
        if age.total_seconds() > max_age_hours * 3600:
            return None

        rows = checkpoint.get("rows")
        if not isinstance(rows, list):
            return None
        return [row for row in rows if isinstance(row, dict)]

    def save_market_cache(self, market: str, rows: list[dict[str, Any]]) -> None:
        self.save_checkpoint(
            f"market_cache_{market}",
            {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "rows": rows,
            },
        )

    def save_run_metrics(self, payload: dict[str, Any]) -> None:
        if self.storage_backend != "sqlite":
            return
        started_at = str(payload.get("started_at") or "")
        finished_at = str(payload.get("finished_at") or "")
        elapsed_seconds = float(payload.get("elapsed_seconds") or 0.0)
        bonds_processed = int(payload.get("bonds_processed") or 0)
        bonds_filtered = int(payload.get("bonds_filtered") or 0)
        errors_count = int(payload.get("errors_count") or 0)
        backend = str(payload.get("backend") or self.storage_backend)
        notes = json.dumps(payload.get("notes") or {}, ensure_ascii=False)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO runs(started_at, finished_at, elapsed_seconds, bonds_processed, bonds_filtered, errors_count, backend, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (started_at, finished_at, elapsed_seconds, bonds_processed, bonds_filtered, errors_count, backend, notes),
            )
            conn.commit()

    def _load_eligible_bonds_map_json(self) -> dict[str, dict[str, Any]]:
        current = self._load_json(self.eligible_bonds_path)
        stored_bonds = current.get("bonds", {}) if isinstance(current, dict) else {}
        return stored_bonds if isinstance(stored_bonds, dict) else {}

    def _save_eligible_bonds_json(self, bonds_by_secid: dict[str, dict[str, Any]]) -> None:
        self._save_json(self.eligible_bonds_path, {"bonds": bonds_by_secid})

    def _update_eligible_bonds_common(
        self,
        bonds: list[dict[str, Any]],
        stored_bonds: dict[str, dict[str, Any]],
        saver: Any,
    ) -> IncrementalStats:
        next_bonds: dict[str, dict[str, Any]] = {}
        inserted = 0
        updated = 0
        unchanged = 0

        for bond in bonds:
            secid = str(bond.get("SECID") or "").strip()
            if not secid:
                continue
            prev = stored_bonds.get(secid)
            if prev is None:
                inserted += 1
            elif prev == bond:
                unchanged += 1
            else:
                updated += 1
            next_bonds[secid] = bond

        removed = max(len(stored_bonds) - len(next_bonds), 0)
        saver(next_bonds)
        return IncrementalStats(inserted=inserted, updated=updated, unchanged=unchanged, removed=removed)

    def _update_eligible_bonds_sqlite(self, bonds: list[dict[str, Any]]) -> IncrementalStats:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT secid, payload FROM eligible_bonds").fetchall()
        stored_bonds: dict[str, dict[str, Any]] = {}
        for secid, payload in rows:
            try:
                parsed = json.loads(str(payload))
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                stored_bonds[str(secid)] = parsed

        def _save_sqlite(next_bonds: dict[str, dict[str, Any]]) -> None:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM eligible_bonds")
                conn.executemany(
                    "INSERT INTO eligible_bonds(secid, payload) VALUES (?, ?)",
                    [(secid, json.dumps(payload, ensure_ascii=False)) for secid, payload in next_bonds.items()],
                )
                conn.commit()

        return self._update_eligible_bonds_common(bonds, stored_bonds, _save_sqlite)

    def _load_exclusions_sqlite(self) -> dict[str, dict[str, str]]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT secid, rule, exclude_until FROM exclusions").fetchall()
        return {
            str(secid): {"rule": str(rule or "manual"), "exclude_until": str(exclude_until or "")}
            for secid, rule, exclude_until in rows
        }

    def _save_exclusions_sqlite(self, exclusions: dict[str, dict[str, str]]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM exclusions")
            conn.executemany(
                "INSERT INTO exclusions(secid, rule, exclude_until) VALUES (?, ?, ?)",
                [
                    (str(secid), str(details.get("rule") or "manual"), str(details.get("exclude_until") or ""))
                    for secid, details in exclusions.items()
                ],
            )
            conn.commit()

    def _init_sqlite(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS exclusions(" 
                "secid TEXT PRIMARY KEY, rule TEXT NOT NULL, exclude_until TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS eligible_bonds(" 
                "secid TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS checkpoints(" 
                "name TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS emitents_registry(" 
                "emitter_id TEXT PRIMARY KEY, full_name TEXT NOT NULL, inn TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS secid_to_emitter(" 
                "secid TEXT PRIMARY KEY, emitter_id TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS runs(" 
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "started_at TEXT NOT NULL, finished_at TEXT NOT NULL, elapsed_seconds REAL NOT NULL, "
                "bonds_processed INTEGER NOT NULL, bonds_filtered INTEGER NOT NULL, errors_count INTEGER NOT NULL, "
                "backend TEXT NOT NULL, notes TEXT NOT NULL)"
            )
            conn.commit()

    def _checkpoint_path(self, name: str) -> Path:
        return self.checkpoints_dir / f"{self._safe_checkpoint_name(name)}.json"

    @staticmethod
    def _safe_checkpoint_name(name: str) -> str:
        safe_name = "".join(ch for ch in name if ch.isalnum() or ch in {"_", "-"}).strip("_")
        if not safe_name:
            safe_name = "checkpoint"
        return safe_name

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
        if isinstance(payload, dict):
            return payload
        return {}

    @staticmethod
    def _save_json(path: Path, payload: dict[str, Any]) -> None:
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
