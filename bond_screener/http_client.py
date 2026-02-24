from __future__ import annotations

import asyncio
import hashlib
import json
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import urlparse

import httpx


@dataclass(slots=True)
class DomainPolicy:
    rate_limit_per_sec: float = 2.0
    max_concurrency: int = 2


class _DomainGuard:
    def __init__(self, policy: DomainPolicy) -> None:
        self.policy = policy
        self.semaphore = asyncio.Semaphore(max(1, policy.max_concurrency))
        self._lock = asyncio.Lock()
        self._last_request_monotonic = 0.0

    async def acquire_rate_slot(self) -> None:
        if self.policy.rate_limit_per_sec <= 0:
            return
        min_interval = 1.0 / self.policy.rate_limit_per_sec
        async with self._lock:
            now = time.monotonic()
            wait_for = self._last_request_monotonic + min_interval - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
                now = time.monotonic()
            self._last_request_monotonic = now


class HttpCache:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS http_cache (
                cache_key TEXT PRIMARY KEY,
                status_code INTEGER NOT NULL,
                headers_json TEXT NOT NULL,
                body BLOB NOT NULL,
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def get(self, cache_key: str) -> tuple[int, dict[str, str], bytes] | None:
        now = time.time()
        row = self._conn.execute(
            "SELECT status_code, headers_json, body, expires_at FROM http_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None

        status_code, headers_json, body, expires_at = row
        if expires_at < now:
            self._conn.execute("DELETE FROM http_cache WHERE cache_key = ?", (cache_key,))
            self._conn.commit()
            return None
        return status_code, json.loads(headers_json), body

    def set(self, cache_key: str, status_code: int, headers: dict[str, str], body: bytes, ttl_seconds: int) -> None:
        now = time.time()
        expires_at = now + max(ttl_seconds, 0)
        self._conn.execute(
            """
            INSERT INTO http_cache (cache_key, status_code, headers_json, body, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                status_code = excluded.status_code,
                headers_json = excluded.headers_json,
                body = excluded.body,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at
            """,
            (cache_key, status_code, json.dumps(headers, ensure_ascii=False), body, expires_at, now),
        )
        self._conn.commit()


class AsyncHttpClient:
    def __init__(
        self,
        *,
        cache_db_path: Path,
        cache_ttl_seconds: int = 300,
        domain_policies: dict[str, DomainPolicy] | None = None,
        default_policy: DomainPolicy | None = None,
        timeout_seconds: float = 15.0,
        max_connections: int = 20,
        max_keepalive_connections: int = 10,
        max_retries: int = 3,
        backoff_base_seconds: float = 0.3,
        backoff_max_seconds: float = 5.0,
        jitter_seconds: float = 0.2,
        debug_raw_enabled: bool = False,
        raw_dir: Path | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.jitter_seconds = jitter_seconds
        self.debug_raw_enabled = debug_raw_enabled
        self.raw_dir = raw_dir or Path("raw")

        self.cache = HttpCache(cache_db_path)
        self.domain_policies = domain_policies or {}
        self.default_policy = default_policy or DomainPolicy()
        self._guards: dict[str, _DomainGuard] = {}

        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        )
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            limits=limits,
            transport=transport,
        )

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()
        self.cache.close()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: Any | None = None,
        data: dict[str, Any] | None = None,
        content: bytes | None = None,
        cache_ttl_seconds: int | None = None,
        provider: str = "default",
    ) -> httpx.Response:
        cache_key = self._build_cache_key(method, url, params, json_body, data, content)
        cached = self.cache.get(cache_key)
        if cached:
            status_code, cached_headers, cached_body = cached
            return httpx.Response(
                status_code=status_code,
                headers=self._headers_for_cached_response(cached_headers),
                content=cached_body,
                request=httpx.Request(method, url),
            )

        domain = urlparse(url).netloc
        guard = self._guards.setdefault(domain, _DomainGuard(self.domain_policies.get(domain, self.default_policy)))

        async with guard.semaphore:
            await guard.acquire_rate_slot()
            response = await self._request_with_retries(
                method,
                url,
                params=params,
                headers=headers,
                json_body=json_body,
                data=data,
                content=content,
            )

        ttl = self.cache_ttl_seconds if cache_ttl_seconds is None else cache_ttl_seconds
        if ttl > 0 and response.status_code < 500:
            self.cache.set(
                cache_key,
                response.status_code,
                self._headers_for_cached_response(dict(response.headers)),
                response.content,
                ttl,
            )

        if self.debug_raw_enabled:
            self._dump_raw(provider=provider, url=url, body=response.content)

        return response

    async def _request_with_retries(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client.request(
                    method,
                    url,
                    params=kwargs.get("params"),
                    headers=kwargs.get("headers"),
                    json=kwargs.get("json_body"),
                    data=kwargs.get("data"),
                    content=kwargs.get("content"),
                )
            except httpx.TimeoutException:
                if attempt >= self.max_retries:
                    raise
                await self._backoff(attempt)
                continue

            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt >= self.max_retries:
                    return response
                await self._backoff(attempt)
                continue

            return response

        raise RuntimeError("Unexpected retry loop termination")

    async def _backoff(self, attempt: int) -> None:
        backoff = min(self.backoff_base_seconds * (2**attempt), self.backoff_max_seconds)
        jitter = random.uniform(0, self.jitter_seconds)
        await asyncio.sleep(backoff + jitter)

    def _dump_raw(self, *, provider: str, url: str, body: bytes) -> None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        folder = self.raw_dir / provider / day
        folder.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
        filename = f"{datetime.now(timezone.utc).strftime('%H%M%S_%f')}_{digest}.bin"
        (folder / filename).write_bytes(body)

    @staticmethod
    def _build_cache_key(
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: Any | None,
        data: dict[str, Any] | None,
        content: bytes | None,
    ) -> str:
        serialized_params = json.dumps(params or {}, ensure_ascii=False, sort_keys=True, default=str)
        if json_body is not None:
            payload = json.dumps(json_body, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        elif data is not None:
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        else:
            payload = content or b""
        body_hash = hashlib.sha256(payload).hexdigest()
        normalized = f"{method.upper()}::{url}::{serialized_params}::{body_hash}"
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _headers_for_cached_response(headers: dict[str, str]) -> dict[str, str]:
        sanitized = {k: v for k, v in headers.items()}
        # Мы кэшируем уже декодированное тело response.content.
        # Поэтому удаляем заголовки, которые описывают исходное wire-представление.
        for key in ["content-encoding", "content-length", "transfer-encoding"]:
            sanitized.pop(key, None)
        return sanitized
