"""Расчет YTM для облигаций на основе RealPrice и ACCRUEDINT."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass
class YtmStats:
    calculated: int = 0
    skipped: int = 0


def enrich_ytm(bonds: list[dict[str, Any]], today: date | None = None) -> YtmStats:
    """Добавляет поле YTM (в процентах годовых) в каждую бумагу, где достаточно данных."""
    stats = YtmStats()
    calc_date = today or date.today()

    for bond in bonds:
        ytm = _calculate_bond_ytm(bond, calc_date)
        if ytm is None:
            stats.skipped += 1
            continue
        bond["YTM"] = ytm
        stats.calculated += 1

    return stats


def _calculate_bond_ytm(bond: dict[str, Any], today: date) -> float | None:
    real_price_pct = _resolve_price_for_ytm(bond)
    if real_price_pct is None:
        return None

    face_value = _as_float_or_none(bond.get("FACEVALUE"))
    if face_value is None or face_value <= 0:
        face_value = 1000.0

    accruedint = _as_float_or_none(bond.get("ACCRUEDINT"))
    if accruedint is None:
        accruedint = 0.0

    target_date = _resolve_target_date_for_ytm(bond)
    if target_date is None or target_date <= today:
        return None

    years = (target_date - today).days / 365.0
    if years <= 0:
        return None

    dirty_price = face_value * real_price_pct / 100.0 + accruedint
    if dirty_price <= 0:
        return None

    coupon_percent = _as_float_or_none(bond.get("COUPONPERCENT"))
    coupon_percent = coupon_percent if coupon_percent is not None else 0.0

    if coupon_percent < 1.0:
        ytm = ((face_value / dirty_price) ** (1.0 / years) - 1.0) * 100.0
        return round(ytm, 4)

    annual_coupon = face_value * coupon_percent / 100.0
    approximate_ytm = (
        (annual_coupon + (face_value - dirty_price) / years)
        / ((face_value + dirty_price) / 2.0)
    ) * 100.0
    return round(approximate_ytm, 4)


def _resolve_target_date_for_ytm(bond: dict[str, Any]) -> date | None:
    offer_date = _parse_iso_date(str(bond.get("OFFERDATE") or "").strip())
    if offer_date is not None:
        return offer_date
    return _parse_iso_date(str(bond.get("MATDATE") or "").strip())


def _resolve_price_for_ytm(bond: dict[str, Any]) -> float | None:
    prevwaprice = _as_float_or_none(bond.get("PREVWAPRICE"))
    if prevwaprice is not None and prevwaprice > 0:
        return prevwaprice

    for field_name in ("ASK", "LAST", "BID"):
        value = _as_float_or_none(bond.get(field_name))
        if value is not None and value > 0:
            bond["RealPrice"] = value
            return value

    real_price = _as_float_or_none(bond.get("RealPrice"))
    if real_price is not None and real_price > 0:
        return real_price
    return None


def _parse_iso_date(raw: str) -> date | None:
    if not raw or raw == "0000-00-00":
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _as_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
