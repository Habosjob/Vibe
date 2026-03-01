from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

import pandas as pd


def parse_date(value: Any) -> Optional[date]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.date()


def date_ddmmyyyy(d: Optional[date]) -> Optional[str]:
    return d.strftime("%d.%m.%Y") if d else None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    s = str(value).strip().replace(" ", "")
    if not s or s.lower() in {"nan", "none", "-", "—"}:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def normalize_isin(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    return s if len(s) >= 6 else None


def coalesce(*values: Any) -> Any:
    for v in values:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None
