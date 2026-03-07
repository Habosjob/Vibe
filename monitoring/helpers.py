from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import requests

from . import config


def ensure_dirs() -> None:
    for path in [
        config.CACHE_DIR,
        config.RAW_DIR,
        config.DB_DIR,
        config.LOGS_DIR,
        config.BASE_SNAPSHOTS_DIR,
        config.CACHE_DIR / "edisclosure",
        config.CACHE_DIR / "news",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dirs()
    logger = logging.getLogger("monitoring")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(config.LOG_FILE, mode="w", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.propagate = False
    return logger


def sanitize_str(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_iso() -> str:
    return date.today().isoformat()


def parse_date(value: Any) -> datetime | None:
    text = sanitize_str(value)
    if not text:
        return None
    candidates = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%y",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def to_iso_date_str(value: Any) -> str:
    dt = parse_date(value)
    return dt.date().isoformat() if dt else ""


def md5_short(value: str, size: int = 16) -> str:
    return hashlib.md5(value.encode("utf-8", errors="ignore")).hexdigest()[:size]


def json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def json_load(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def is_cache_fresh(path: Path, ttl_hours: int) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds <= ttl_hours * 3600


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    logger: logging.Logger,
    timeout: float | None = None,
    **kwargs: Any,
) -> requests.Response:
    timeout = timeout or config.REQUEST_TIMEOUT_SECONDS
    last_error: Exception | None = None
    for attempt in range(config.HTTP_RETRIES + 1):
        try:
            response = session.request(method=method, url=url, timeout=timeout, **kwargs)
            if response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}: {url}")
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= config.HTTP_RETRIES:
                break
            sleep_seconds = config.BACKOFF_BASE_SECONDS * (attempt + 1)
            logger.warning("Retry %s for %s %s due to %s", attempt + 1, method, url, exc)
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Request failed for {method} {url}: {last_error}")


def timed(stage_name: str, func: Callable[[], Any]) -> tuple[Any, float]:
    started = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - started
    return result, elapsed
