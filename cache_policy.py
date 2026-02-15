from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CachePolicy:
    details_ttl_hours: int
    intraday_snapshot_interval_minutes: int
    discovery_max_attempts: int
    discovery_backoff_base_seconds: float
    discovery_backoff_max_seconds: float
    cb_failure_threshold: int
    cb_cooldown_seconds: int
    health_retention_days: int


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except (TypeError, ValueError):
        return default


CACHE_POLICY = CachePolicy(
    details_ttl_hours=max(_env_int("DETAILS_TTL_HOURS", 24 * 7), 1),
    intraday_snapshot_interval_minutes=max(_env_int("INTRADAY_SNAPSHOT_INTERVAL_MINUTES", 10), 1),
    discovery_max_attempts=max(_env_int("DISCOVERY_MAX_ATTEMPTS", 4), 1),
    discovery_backoff_base_seconds=max(_env_float("DISCOVERY_BACKOFF_BASE_SECONDS", 0.7), 0.05),
    discovery_backoff_max_seconds=max(_env_float("DISCOVERY_BACKOFF_MAX_SECONDS", 8.0), 0.1),
    cb_failure_threshold=max(_env_int("CB_FAILURE_THRESHOLD", 3), 1),
    cb_cooldown_seconds=max(_env_int("CB_COOLDOWN_SECONDS", 180), 1),
    health_retention_days=max(_env_int("HEALTH_RETENTION_DAYS", 14), 1),
)
