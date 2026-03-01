from __future__ import annotations

import json
from pathlib import Path


class CheckpointStore:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, secid: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in secid)
        return self.base_dir / f"{safe}.json"

    def is_done(self, secid: str) -> bool:
        return self._path(secid).exists()

    def mark_done(self, secid: str, payload: dict) -> None:
        self._path(secid).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
