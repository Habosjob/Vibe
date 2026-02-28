from __future__ import annotations

from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from core.settings import AppSettings
from net.cache import HttpCache


class HttpClient:
    def __init__(self, settings: AppSettings, cache: HttpCache) -> None:
        self.settings = settings
        self.cache = cache
        self.timeout = httpx.Timeout(
            connect=settings.net.timeout.connect_s,
            read=settings.net.timeout.read_s,
            write=settings.net.timeout.write_s,
            pool=settings.net.timeout.pool_s,
        )
        self._client = httpx.AsyncClient(timeout=self.timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        ttl_s: int | None = None,
    ) -> bytes:
        retry_cfg = self.settings.net.retry

        decorated_call = retry(
            stop=stop_after_attempt(retry_cfg.max_attempts),
            wait=wait_exponential_jitter(
                initial=retry_cfg.backoff_initial_s,
                max=retry_cfg.backoff_max_s,
                jitter=retry_cfg.jitter_s,
            ),
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
            reraise=True,
        )(self._get_uncached)

        cache_key = self.cache.make_key(url, params, headers)
        cached = self.cache.get(cache_key)
        if cached and not cached.is_expired():
            return cached.payload_file.read_bytes()

        response = await decorated_call(url=url, params=params, headers=headers)
        response.raise_for_status()
        payload = response.content
        self.cache.set(
            cache_key,
            payload,
            ttl_s=ttl_s or self.settings.net.cache_ttl_s_default,
            content_type=response.headers.get("content-type", "application/octet-stream"),
        )
        return payload

    async def _get_uncached(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        return await self._client.get(url, params=params, headers=headers)
