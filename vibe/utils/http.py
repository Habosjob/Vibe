from __future__ import annotations

import time
from dataclasses import dataclass

import requests


class HTTPRequestError(RuntimeError):
    """Raised when HTTP request fails after retries."""


@dataclass
class HTTPResponse:
    content: bytes
    elapsed_seconds: float
    status_code: int
    headers: dict[str, str]
    final_url: str | None = None


def get_with_retries(
    url: str,
    *,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    request_headers: dict[str, str] | None = None,
) -> HTTPResponse:
    last_error: Exception | None = None
    headers = {
        "User-Agent": "VibeBot/1.0 (+https://github.com/Habosjob/Vibe)",
        "Accept": "application/json,text/plain,*/*",
    }
    if request_headers:
        headers.update(request_headers)

    for attempt in range(1, retries + 1):
        start = time.perf_counter()
        try:
            response = requests.get(url, timeout=timeout, headers=headers)
            elapsed = time.perf_counter() - start
            if response.status_code >= 500:
                raise HTTPRequestError(
                    f"Server error {response.status_code} on attempt {attempt}/{retries} for {url}"
                )
            response.raise_for_status()
            return HTTPResponse(
                content=response.content,
                elapsed_seconds=elapsed,
                status_code=response.status_code,
                headers=dict(response.headers),
                final_url=(getattr(response, "url", None) or None),
            )
        except (requests.RequestException, HTTPRequestError) as exc:
            last_error = exc
            if attempt == retries:
                break
            sleep_time = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(sleep_time)

    raise HTTPRequestError(
        f"Failed to GET {url} after {retries} attempts. Last error: {last_error}"
    )
