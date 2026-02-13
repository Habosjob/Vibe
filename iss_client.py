# iss_client.py
from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from SQL import SQLiteCache, RequestLogRow, utc_iso


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
        self._tls = threading.local()

    def _get_session(self) -> requests.Session:
        s = getattr(self._tls, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update(
                {
                    "User-Agent": "Vibe-MOEX-ISS/1.7",
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
                    time.sleep(max(0.0, min(30.0, sec)))
                    return
                except Exception:
                    pass

        # exponential backoff + jitter
        base = float(self.backoff_base) * (2 ** max(0, attempt - 1))
        jitter = random.uniform(0.0, 0.4)
        time.sleep(min(30.0, base + jitter))

    @staticmethod
    def _iss_payload_status_is_error(text: str) -> bool:
        """MOEX ISS sometimes returns HTTP 200 with JSON like {'status':'error', ...}."""
        if not text:
            return False
        try:
            obj = json.loads(text)
        except Exception:
            return False
        if isinstance(obj, dict) and str(obj.get("status") or "").lower() == "error":
            return True
        return False

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

                # log request (always)
                self.cache.log_request(
                    RequestLogRow(
                        url=str(resp.url),
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=status,
                        elapsed_ms=elapsed_ms,
                        size_bytes=size_bytes,
                        created_utc=utc_iso(),
                        error=None,
                    )
                )

                # Retry when ISS returns application-level error with HTTP 200
                if status == 200 and self._iss_payload_status_is_error(text) and attempt < self.max_retries:
                    self.logger.warning(
                        f"ISS payload status=error | retrying | attempt {attempt}/{self.max_retries} | {resp.url}"
                    )
                    self._sleep_for_retry(attempt, resp)
                    continue

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

                # log as error (best-effort)
                try:
                    self.cache.log_request(
                        RequestLogRow(
                            url=url,
                            params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                            status=None,
                            elapsed_ms=elapsed_ms,
                            size_bytes=None,
                            created_utc=utc_iso(),
                            error=last_err,
                        )
                    )
                except Exception:
                    pass

                if attempt < self.max_retries:
                    self.logger.warning(f"HTTP exception retryable | attempt {attempt}/{self.max_retries} | {url} | err={last_err}")
                    self._sleep_for_retry(attempt, resp)
                    continue

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
