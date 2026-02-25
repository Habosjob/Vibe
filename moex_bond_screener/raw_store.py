"""Работа с raw-дампами и автоочисткой."""

from __future__ import annotations

import time
from pathlib import Path


class RawStore:
    def __init__(self, root: str = "raw") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def dump_json(self, filename: str, payload: str) -> None:
        (self.root / filename).write_text(payload, encoding="utf-8")

    def dump_html(self, filename: str, payload: str) -> None:
        (self.root / filename).write_text(payload, encoding="utf-8")

    def cleanup(self, ttl_hours: int, max_size_mb: int) -> None:
        files = sorted([f for f in self.root.glob("*.json") if f.is_file()], key=lambda f: f.stat().st_mtime)
        now = time.time()
        ttl_seconds = ttl_hours * 3600

        for file in files:
            if now - file.stat().st_mtime > ttl_seconds:
                file.unlink(missing_ok=True)

        files = sorted([f for f in self.root.glob("*.json") if f.is_file()], key=lambda f: f.stat().st_mtime)
        max_bytes = max_size_mb * 1024 * 1024
        total = sum(file.stat().st_size for file in files)

        while total > max_bytes and files:
            oldest = files.pop(0)
            total -= oldest.stat().st_size
            oldest.unlink(missing_ok=True)
