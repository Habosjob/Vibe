import asyncio
from pathlib import Path

import httpx
import pytest

from bond_screener.http_client import AsyncHttpClient, DomainPolicy


@pytest.mark.asyncio
async def test_cache_hit_before_ttl_expiry(tmp_path: Path) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True, "calls": calls})

    transport = httpx.MockTransport(handler)

    async with AsyncHttpClient(
        cache_db_path=tmp_path / "cache.sqlite",
        cache_ttl_seconds=60,
        transport=transport,
    ) as client:
        first = await client.request("GET", "https://example.com/data", params={"q": 1})
        second = await client.request("GET", "https://example.com/data", params={"q": 1})

    assert first.json()["calls"] == 1
    assert second.json()["calls"] == 1
    assert calls == 1


@pytest.mark.asyncio
async def test_per_domain_max_concurrency_limit(tmp_path: Path) -> None:
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)

    async with AsyncHttpClient(
        cache_db_path=tmp_path / "cache.sqlite",
        cache_ttl_seconds=0,
        domain_policies={"example.com": DomainPolicy(rate_limit_per_sec=1000, max_concurrency=2)},
        transport=transport,
    ) as client:
        await asyncio.gather(
            *[client.request("GET", f"https://example.com/resource/{idx}") for idx in range(8)]
        )

    assert peak <= 2
