"""Работа с инкрементальным состоянием скринера: исключения и кэш итоговых бумаг."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class IncrementalStats:
    inserted: int
    updated: int
    unchanged: int
    removed: int


class ScreenerStateStore:
    def __init__(self, base_dir: str = "state") -> None:
        self.base_path = Path(base_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.exclusions_path = self.base_path / "exclusions.json"
        self.eligible_bonds_path = self.base_path / "eligible_bonds.json"

    def load_exclusions(self) -> dict[str, dict[str, str]]:
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
        self._save_json(self.exclusions_path, {"exclusions": exclusions})

    def update_eligible_bonds(self, bonds: list[dict[str, Any]]) -> IncrementalStats:
        current = self._load_json(self.eligible_bonds_path)
        stored_bonds = current.get("bonds", {}) if isinstance(current, dict) else {}
        if not isinstance(stored_bonds, dict):
            stored_bonds = {}

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
        self._save_json(self.eligible_bonds_path, {"bonds": next_bonds})
        return IncrementalStats(inserted=inserted, updated=updated, unchanged=unchanged, removed=removed)

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
