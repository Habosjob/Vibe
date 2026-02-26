"""Расчет YTM для облигаций на основе RealPrice и ACCRUEDINT."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Any


@dataclass
class YtmStats:
    calculated: int = 0
    skipped: int = 0


@dataclass(slots=True)
class FloaterForecastConfig:
    cb_rate_current_year: float = 14.0
    cb_rate_next_year: float = 8.5
    cb_rate_plus_one_year: float = 8.0
    linker_inflation_current_year: float = 5.0
    linker_inflation_next_year: float = 4.0
    linker_inflation_plus_one_year: float = 4.0
    ruonia_spread_from_cb_rate: float = -0.5
    z_curve_spread_from_cb_rate: float = -1.0
    cbr_rate_spread_from_cb_rate: float = 0.0


def enrich_ytm(bonds: list[dict[str, Any]], today: date | None = None, config: Any | None = None) -> YtmStats:
    """Добавляет поле YTM (в процентах годовых) в каждую бумагу, где достаточно данных."""
    stats = YtmStats()
    calc_date = today or date.today()
    floater_config = _resolve_floater_forecast_config(config)

    for bond in bonds:
        _sanitize_realprice_artifact(bond)
        ytm = _calculate_bond_ytm(bond, calc_date, floater_config)
        if ytm is None:
            stats.skipped += 1
            continue
        bond["YTM"] = ytm
        bond["_YTM_FORECAST"] = bool(_is_floater_coupon_type(bond) or _is_ofz_linker(bond))
        stats.calculated += 1

    return stats




def _sanitize_realprice_artifact(bond: dict[str, Any]) -> None:
    parsed_real_price = _as_float_or_none(bond.get("RealPrice"))
    if parsed_real_price is None:
        return
    if parsed_real_price <= 0:
        bond.pop("RealPrice", None)

def _calculate_bond_ytm(bond: dict[str, Any], today: date, config: FloaterForecastConfig) -> float | None:
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

    is_linker = _is_ofz_linker(bond)
    if _is_floater_coupon_type(bond):
        coupon_percent = _forecast_floater_coupon_percent(bond, today, target_date, config)
    projected_face_value = face_value
    average_face_value = face_value
    if is_linker:
        projected_face_value = _projected_face_value_with_inflation(face_value=face_value, today=today, target_date=target_date, config=config)
        average_face_value = (face_value + projected_face_value) / 2.0

    if coupon_percent < 1.0:
        ytm = ((projected_face_value / dirty_price) ** (1.0 / years) - 1.0) * 100.0
        return round(ytm, 4)

    annual_coupon = average_face_value * coupon_percent / 100.0
    approximate_ytm = (
        (annual_coupon + (projected_face_value - dirty_price) / years)
        / ((projected_face_value + dirty_price) / 2.0)
    ) * 100.0
    return round(approximate_ytm, 4)


def _resolve_floater_forecast_config(config: Any | None) -> FloaterForecastConfig:
    if config is None:
        return FloaterForecastConfig()
    defaults = FloaterForecastConfig()
    return FloaterForecastConfig(
        cb_rate_current_year=_as_float_or_none(getattr(config, "floater_cb_rate_current_year", defaults.cb_rate_current_year)) or defaults.cb_rate_current_year,
        cb_rate_next_year=_as_float_or_none(getattr(config, "floater_cb_rate_next_year", defaults.cb_rate_next_year)) or defaults.cb_rate_next_year,
        cb_rate_plus_one_year=_as_float_or_none(getattr(config, "floater_cb_rate_plus_one_year", defaults.cb_rate_plus_one_year)) or defaults.cb_rate_plus_one_year,
        linker_inflation_current_year=_as_float_or_none(getattr(config, "linker_inflation_current_year", defaults.linker_inflation_current_year)) or defaults.linker_inflation_current_year,
        linker_inflation_next_year=_as_float_or_none(getattr(config, "linker_inflation_next_year", defaults.linker_inflation_next_year)) or defaults.linker_inflation_next_year,
        linker_inflation_plus_one_year=_as_float_or_none(getattr(config, "linker_inflation_plus_one_year", defaults.linker_inflation_plus_one_year)) or defaults.linker_inflation_plus_one_year,
        ruonia_spread_from_cb_rate=_as_float_or_none(getattr(config, "floater_ruonia_spread_from_cb_rate", defaults.ruonia_spread_from_cb_rate)) or defaults.ruonia_spread_from_cb_rate,
        z_curve_spread_from_cb_rate=_as_float_or_none(getattr(config, "floater_z_curve_spread_from_cb_rate", defaults.z_curve_spread_from_cb_rate)) or defaults.z_curve_spread_from_cb_rate,
        cbr_rate_spread_from_cb_rate=_as_float_or_none(getattr(config, "floater_cbr_rate_spread_from_cb_rate", defaults.cbr_rate_spread_from_cb_rate)) or defaults.cbr_rate_spread_from_cb_rate,
    )


def _is_floater_coupon_type(bond: dict[str, Any]) -> bool:
    coupon_type = str(bond.get("CouponType") or "").strip().lower()
    return coupon_type in {"флоатер", "float", "floater"}


def _is_ofz_linker(bond: dict[str, Any]) -> bool:
    ids = [
        str(bond.get("SECID") or ""),
        str(bond.get("ISIN") or ""),
        str(bond.get("REGNUMBER") or ""),
        str(bond.get("SHORTNAME") or ""),
        str(bond.get("SECNAME") or ""),
    ]
    for raw in ids:
        if not raw:
            continue
        normalized = raw.upper()
        for linker_code in ("52002", "52003", "52004", "52005"):
            if re.search(rf"(?:^|\D){linker_code}(?:\D|$)", normalized):
                return True
    return False


def _forecast_floater_coupon_percent(bond: dict[str, Any], today: date, target_date: date, config: FloaterForecastConfig) -> float:
    index_name = str(bond.get("_INDEX_NAME") or "").strip().upper()
    spread = _as_float_or_none(bond.get("_INDEX_SPREAD"))
    spread = spread if spread is not None else 0.0
    base_rate = _average_projected_index_rate(today=today, target_date=target_date, index_name=index_name, config=config)
    return round(base_rate + spread, 4)


def _average_projected_index_rate(today: date, target_date: date, index_name: str, config: FloaterForecastConfig) -> float:
    total_days = (target_date - today).days
    if total_days <= 0:
        return config.cb_rate_current_year

    weighted_rate_sum = 0.0
    segment_start = today
    while segment_start < target_date:
        year_end = date(segment_start.year, 12, 31)
        segment_end = min(target_date, year_end.fromordinal(year_end.toordinal() + 1))
        segment_days = (segment_end - segment_start).days
        offset = segment_start.year - today.year
        cb_rate = _cb_rate_for_year_offset(offset, config)
        weighted_rate_sum += _projected_index_from_cb_rate(index_name, cb_rate, config) * segment_days
        segment_start = segment_end
    return weighted_rate_sum / total_days


def _cb_rate_for_year_offset(offset: int, config: FloaterForecastConfig) -> float:
    if offset <= 0:
        return config.cb_rate_current_year
    if offset == 1:
        return config.cb_rate_next_year
    return config.cb_rate_plus_one_year


def _inflation_for_year_offset(offset: int, config: FloaterForecastConfig) -> float:
    if offset <= 0:
        return config.linker_inflation_current_year
    if offset == 1:
        return config.linker_inflation_next_year
    return config.linker_inflation_plus_one_year


def _projected_face_value_with_inflation(face_value: float, today: date, target_date: date, config: FloaterForecastConfig) -> float:
    total_days = (target_date - today).days
    if total_days <= 0:
        return face_value

    projected_face_value = face_value
    segment_start = today
    while segment_start < target_date:
        year_end = date(segment_start.year, 12, 31)
        segment_end = min(target_date, year_end.fromordinal(year_end.toordinal() + 1))
        segment_days = (segment_end - segment_start).days
        offset = segment_start.year - today.year
        annual_inflation_rate = _inflation_for_year_offset(offset, config)
        segment_factor = (1.0 + annual_inflation_rate / 100.0) ** (segment_days / 365.0)
        projected_face_value *= segment_factor
        segment_start = segment_end
    return projected_face_value


def _projected_index_from_cb_rate(index_name: str, cb_rate: float, config: FloaterForecastConfig) -> float:
    if index_name == "RUONIA":
        return cb_rate + config.ruonia_spread_from_cb_rate
    if index_name.startswith("Z_CURVE_RUS"):
        return cb_rate + config.z_curve_spread_from_cb_rate
    if index_name in {"CBR_RATE", "KEY_RATE"}:
        return cb_rate + config.cbr_rate_spread_from_cb_rate
    return cb_rate


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
