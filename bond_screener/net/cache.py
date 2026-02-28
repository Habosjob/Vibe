from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CacheEntry:
    key: str
    payload_file: Path
    meta_file: Path
    fetched_at: datetime
    ttl_s: int
    content_type: str

    def is_expired(self) -> bool:
        return _utc_now() > self.fetched_at + timedelta(seconds=self.ttl_s)


class HttpCache:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def make_key(url: str, params: dict[str, Any] | None, headers_subset: dict[str, str] | None) -> str:
        payload = {
            "url": url,
            "params": params or {},
            "headers_subset": headers_subset or {},
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _payload_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.bin"

    def _meta_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json"

    def set(self, key: str, content: bytes, ttl_s: int, content_type: str = "application/octet-stream") -> None:
        payload_path = self._payload_path(key)
        meta_path = self._meta_path(key)

        payload_path.write_bytes(content)
        meta = {
            "key": key,
            "fetched_at": _utc_now().isoformat(timespec="seconds"),
            "ttl_s": ttl_s,
            "content_type": content_type,
            "payload_file": payload_path.name,
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, key: str) -> CacheEntry | None:
        meta_path = self._meta_path(key)
        if not meta_path.exists():
            return None
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        payload_path = self.cache_dir / meta["payload_file"]
        if not payload_path.exists():
            return None
        return CacheEntry(
            key=meta["key"],
            payload_file=payload_path,
            meta_file=meta_path,
            fetched_at=datetime.fromisoformat(meta["fetched_at"]),
            ttl_s=int(meta["ttl_s"]),
            content_type=meta.get("content_type", "application/octet-stream"),
        )

    def clear(self, clear_all: bool = True, prefix: str | None = None, older_than_seconds: int | None = None) -> int:
        removed = 0
        now = _utc_now()
        for meta_file in self.cache_dir.glob("*.json"):
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            key = meta["key"]
            payload_file = self.cache_dir / meta["payload_file"]

            if prefix and not key.startswith(prefix):
                continue

            if older_than_seconds is not None:
                fetched_at = datetime.fromisoformat(meta["fetched_at"])
                if (now - fetched_at).total_seconds() <= older_than_seconds:
                    continue

            if clear_all or prefix or older_than_seconds is not None:
                if meta_file.exists():
                    meta_file.unlink()
                if payload_file.exists():
                    payload_file.unlink()
                removed += 1
        return removed
