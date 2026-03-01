from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from .utils import file_is_fresh


class HTTPCache:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str, suffix: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.base_dir / f"{digest}.{suffix}"

    def get_bytes(self, key: str, ttl_hours: int) -> Optional[bytes]:
        path = self._key_to_path(key, "bin")
        if not file_is_fresh(path, ttl_hours):
            return None
        return path.read_bytes()

    def set_bytes(self, key: str, data: bytes) -> Path:
        path = self._key_to_path(key, "bin")
        path.write_bytes(data)
        return path

    def get_json(self, key: str, ttl_hours: int) -> Optional[dict[str, Any]]:
        path = self._key_to_path(key, "json")
        if not file_is_fresh(path, ttl_hours):
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set_json(self, key: str, data: dict[str, Any]) -> Path:
        path = self._key_to_path(key, "json")
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
