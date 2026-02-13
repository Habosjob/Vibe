# iss_client.py
from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from SQL import SQLiteCache, RequestLogRow


@dataclass
class HttpResult:
    status: Optional[int]
    elapsed_ms: Optional[int]
    size_bytes: Optional[int]
    text: Optional[str]
    url: str
    params: dict
    error: Optional[str] = None
    headers: Optional[dict] = None


class RateLimiter:
    def __init__(self, requests_per_sec: float):
        self.rps = float(requests_per_sec)
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def acquire(self) -> None:
        if self.rps <= 0:
            return
        min_interval = 1.0 / self.rps
        with self._lock:
            now = time.perf_counter()
            if now < self._next_ts:
                time.sleep(self._next_ts - now)
            self._next_ts = max(self._next_ts, time.perf_counter()) + min_interval


class IssClient:
    def __init__(
        self,
        cache: SQLiteCache,
        logger,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_base: float = 0.8,
        rate_limiter: RateLimiter | None = None,
    ):
        self.cache = cache
        self.logger = logger
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.rate_limiter = rate_limiter

        # thread-local session (важно для ThreadPool)
        self._tls = threading.local()

    def _get_session(self) -> requests.Session:
        s = getattr(self._tls, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update(
                {
                    "User-Agent": "Vibe-MOEX-ISS/1.4",
                    "Accept": "application/json,text/plain,*/*",
                }
            )
            self._tls.session = s
        return s

    def _sleep_for_retry(self, attempt: int, resp: requests.Response | None) -> None:
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    sec = float(ra)
                    time.sleep(min(60.0, max(0.0, sec)))
                    return
                except Exception:
                    pass

        base = self.backoff_base * (2 ** (attempt - 1))
        jitter = random.random() * 0.25 * base
        time.sleep(min(30.0, base + jitter))

    def get(self, path: str, params: dict | None = None) -> HttpResult:
        params = params or {}
        url = f"{self.base_url}{path}"
        retry_statuses = {429, 500, 502, 503, 504}
        last_err: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            t0 = time.perf_counter()
            resp: requests.Response | None = None
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire()

                session = self._get_session()
                resp = session.get(url, params=params, timeout=self.timeout)

                status = int(resp.status_code)
                headers = dict(resp.headers)
                text = resp.text if resp.text is not None else ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                size_bytes = len((text or "").encode("utf-8", errors="ignore"))

                self.cache.log_request(
                    RequestLogRow(
                        url=str(resp.url),
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=status,
                        elapsed_ms=elapsed_ms,
                        size_bytes=size_bytes,
                        created_utc=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        error=None,
                    )
                )

                if status in retry_statuses and attempt < self.max_retries:
                    self.logger.warning(
                        f"HTTP {status} retryable | attempt {attempt}/{self.max_retries} | {resp.url}"
                    )
                    self._sleep_for_retry(attempt, resp)
                    continue

                return HttpResult(
                    status=status,
                    elapsed_ms=elapsed_ms,
                    size_bytes=size_bytes,
                    text=text,
                    url=str(resp.url),
                    params=params,
                    error=None,
                    headers=headers,
                )

            except Exception as e:
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                last_err = repr(e)

                self.cache.log_request(
                    RequestLogRow(
                        url=url,
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=None,
                        elapsed_ms=elapsed_ms,
                        size_bytes=0,
                        created_utc=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        error=last_err,
                    )
                )

                self.logger.warning(
                    f"HTTP exception retryable | attempt {attempt}/{self.max_retries} | {url} | {last_err}"
                )
                if attempt < self.max_retries:
                    self._sleep_for_retry(attempt, resp)
                    continue

                return HttpResult(
                    status=None,
                    elapsed_ms=elapsed_ms,
                    size_bytes=0,
                    text=None,
                    url=url,
                    params=params,
                    error=last_err,
                    headers=None,
                )

        return HttpResult(
            status=None,
            elapsed_ms=None,
            size_bytes=None,
            text=None,
            url=url,
            params=params,
            error=last_err,
            headers=None,
        )