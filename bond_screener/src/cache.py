from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Optional

import httpx


class HTTPCache:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.base_dir / f"{digest}.bin"

    def is_fresh(self, key: str, ttl_hours: float) -> bool:
        path = self._path(key)
        if not path.exists():
            return False
        age_h = (time.time() - path.stat().st_mtime) / 3600
        return age_h <= ttl_hours

    def get(self, key: str) -> Optional[bytes]:
        path = self._path(key)
        return path.read_bytes() if path.exists() else None

    def put(self, key: str, data: bytes) -> Path:
        path = self._path(key)
        path.write_bytes(data)
        return path

    def fetch(self, key: str, url: str, ttl_hours: float, timeout_s: int = 60) -> bytes:
        if self.is_fresh(key, ttl_hours):
            data = self.get(key)
            if data is not None:
                return data
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.content
        self.put(key, data)
        return data
