from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


DATE_FORMATS = ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]


def ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def parse_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def normalize_decimal(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(" ", "")
    if not text or text.lower() in {"nan", "none", "null", "-"}:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def to_upper(value: object) -> Optional[str]:
    text = normalize_str(value)
    return text.upper() if text else None


def rolling_value(values: list[float], target_date: date, today: date) -> float:
    if not values:
        raise ValueError("values must not be empty")
    offset_years = target_date.year - today.year
    offset_years = max(0, min(offset_years, len(values) - 1))
    return values[offset_years]


def file_is_fresh(path: Path, ttl_hours: int) -> bool:
    if not path.exists():
        return False
    age_seconds = datetime.now().timestamp() - path.stat().st_mtime
    return age_seconds <= ttl_hours * 3600
