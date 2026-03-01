from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Dict, List, Tuple

import pandas as pd

from .market_indices import interpolate_zcyc, rolling_value
from .utils import coalesce


def _xnpv(rate: float, flows: List[Tuple[date, float]], start: date) -> float:
    return sum(v / ((1 + rate) ** ((d - start).days / 365.0)) for d, v in flows)


def _xirr(flows: List[Tuple[date, float]], start: date) -> float | None:
    if not flows:
        return None
    lo, hi = -0.99, 5.0
    for _ in range(100):
        mid = (lo + hi) / 2
        val = _xnpv(mid, flows, start)
        if abs(val) < 1e-8:
            return mid
        if val > 0:
            lo = mid
        else:
            hi = mid
    return mid


def _safe_zero_coupon_yield(outstanding: float, dirty: float, years: float) -> float | None:
    if dirty <= 0 or outstanding <= 0:
        return None
    years = max(years, 1.0 / 365.0)
    ratio = outstanding / dirty
    if ratio <= 0:
        return None
    try:
        return math.exp(math.log(ratio) / years) - 1.0
    except (OverflowError, ValueError, ZeroDivisionError):
        return None


def normalize_price(clean_price_raw: float | None, nominal: float | None) -> tuple[float | None, str | None]:
    if clean_price_raw is None or pd.isna(clean_price_raw):
        return None, None
    clean_price = float(clean_price_raw)
    if nominal is not None and nominal > 0 and 0 < clean_price <= 200:
        return (clean_price / 100.0) * nominal, "percent_of_nominal"
    return clean_price, "currency"


def _normalize_floater_base(*texts: str) -> str:
    txt = " ".join(str(t or "") for t in texts).upper()
    if "RUONIA" in txt:
        return "RUONIA"
    if any(x in txt for x in ["КБД", "ZCYC", "G-CURVE", "ОФЗ"]):
        return "ZCYC"
    return "UNKNOWN"


def _bond_kind(row: pd.Series) -> str:
    txt = " ".join(
        str(coalesce(row.get("dohod_dohod_base_index"), ""))
        + " "
        + str(coalesce(row.get("dohod_NAME"), row.get("moex_norm_name"), ""))
    ).lower()
    if any(x in txt for x in ["веч", "perp", "субор", "subord"]):
        return "perpetual"
    if any(x in txt for x in ["офз-ин", "линкер", "инфляц"]):
        return "linker"
    if any(x in txt for x in ["ruonia", "zcyc", "кбд", "g-curve", "плава"]):
        return "floater"
    return "fixed"


def _payments_per_year(row: pd.Series, coupons: pd.DataFrame) -> float:
    freq = row.get("dohod_coupon_freq_per_year")
    if pd.notna(freq) and freq and float(freq) > 0:
        return float(freq)
    if len(coupons) >= 2:
        date_col = "parsed_coupondate" if "parsed_coupondate" in coupons.columns else "coupondate"
        dates = sorted(coupons[date_col].dropna())
        if len(dates) >= 2:
            avg_days = sum((dates[i] - dates[i - 1]).days for i in range(1, len(dates))) / (len(dates) - 1)
            if avg_days > 0:
                return 365.0 / avg_days
    return 2.0


def _normalize_to_date(value) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def compute_ytm(
    merged: pd.DataFrame,
    coupons_df: pd.DataFrame,
    amort_df: pd.DataFrame,
    market_ruonia: pd.DataFrame,
    market_zcyc: pd.DataFrame,
    config: Dict,
    logger=None,
) -> pd.DataFrame:
    today = date.today()
    ruonia_today = float(market_ruonia["ruonia_percent"].iloc[0]) if not market_ruonia.empty else None
    key_vals = config["scenario"]["key_rate_avg_percent"]
    key_today = float(config["scenario"]["key_rate_today_percent"])

    out = merged.copy()
    out["ytm_calc"] = None
    out["ytm_zero_coupon"] = None
    out["yield_perpetual_compounded"] = None
    out["ytm_method"] = "unknown"
    out["warning_text"] = ""
    out["warnings"] = ""
    out["nominal_used"] = None
    out["price_unit"] = None
    out["clean_price_amt"] = None
    out["nkd_amt"] = None
    out["dirty_price_amt"] = None
    out["ytm_is_outlier"] = False
    out["floater_base"] = "UNKNOWN"

    cpn_all = coupons_df.copy() if not coupons_df.empty else pd.DataFrame(columns=["secid", "coupondate", "value", "rate"])
    amo_all = amort_df.copy() if not amort_df.empty else pd.DataFrame(columns=["secid", "amortdate", "value"])
    cpn_all["parsed_coupondate"] = pd.to_datetime(cpn_all.get("coupondate"), dayfirst=True, errors="coerce").dt.date
    amo_all["parsed_amortdate"] = pd.to_datetime(amo_all.get("amortdate"), dayfirst=True, errors="coerce").dt.date

    unparsable_coupon = int((cpn_all.get("coupondate").notna() & cpn_all["parsed_coupondate"].isna()).sum()) if not cpn_all.empty else 0
    unparsable_amort = int((amo_all.get("amortdate").notna() & amo_all["parsed_amortdate"].isna()).sum()) if not amo_all.empty else 0
    if logger and (unparsable_coupon > 0 or unparsable_amort > 0):
        logger.warning(
            "Unparsable dates detected before YTM export: coupons=%s amortizations=%s",
            unparsable_coupon,
            unparsable_amort,
        )

    out["unparsable_coupon_dates_count"] = unparsable_coupon
    out["unparsable_amort_dates_count"] = unparsable_amort

    for idx, row in out.iterrows():
        secid = row.get("secid")
        horizon = _normalize_to_date(row.get("target_date"))
        warn: list[str] = []

        if horizon is None:
            warn.append("no_horizon_date")
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue
        out.at[idx, "horizon_date"] = horizon.isoformat()
        if horizon <= today:
            warn.append("horizon_in_past")
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        nominal_src = coalesce(row.get("dohod_dohod_current_nominal"), row.get("moex_norm_facevalue"))
        nominal = float(nominal_src) if nominal_src is not None and pd.notna(nominal_src) else 1000.0
        if nominal_src is None or pd.isna(nominal_src):
            warn.append("nominal_defaulted_1000")
        out.at[idx, "nominal_used"] = nominal

        clean = coalesce(row.get("dohod_dohod_price"), row.get("moex_moex_price"))
        if clean is None or pd.isna(clean):
            warn.append("missing price")
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        clean_amt, price_unit = normalize_price(float(clean), nominal)
        if clean_amt is None:
            warn.append("bad_clean_price")
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        nkd_raw = coalesce(row.get("dohod_dohod_nkd"), row.get("moex_moex_nkd"))
        if nkd_raw is None or pd.isna(nkd_raw):
            nkd_amt = 0.0
            warn.append("nkd_defaulted_zero")
        else:
            nkd_amt = float(nkd_raw)
            if price_unit == "percent_of_nominal" and nkd_amt <= 20:
                warn.append("warning_if_suspicious_nkd")

        dirty = float(clean_amt) + float(nkd_amt)
        out.at[idx, "price_unit"] = price_unit
        out.at[idx, "clean_price_amt"] = clean_amt
        out.at[idx, "nkd_amt"] = nkd_amt
        out.at[idx, "dirty_price_amt"] = dirty

        if nominal > 0 and dirty < 0.05 * nominal:
            warn.append("dirty_price_too_low")

        if dirty <= 0:
            warn.append("bad_dirty_price")
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        out.at[idx, "floater_base"] = _normalize_floater_base(
            row.get("dohod_dohod_base_index"),
            row.get("dohod_NAME"),
            row.get("moex_norm_name"),
        )

        cpn = cpn_all[cpn_all["secid"] == secid].copy() if not cpn_all.empty else pd.DataFrame(columns=["parsed_coupondate", "value", "rate"])
        amo = amo_all[amo_all["secid"] == secid].copy() if not amo_all.empty else pd.DataFrame(columns=["parsed_amortdate", "value"])

        outstanding = nominal - amo[(amo["parsed_amortdate"].notna()) & (amo["parsed_amortdate"] < horizon)]["value"].fillna(0).sum()
        if outstanding < 0:
            outstanding = 0.0

        kind = _bond_kind(row)
        flows = [(today, -dirty)]

        if kind == "perpetual":
            ppy = _payments_per_year(row, cpn)
            coupon_rate = coalesce(row.get("dohod_dohod_coupon_rate"), cpn["rate"].dropna().mean())
            if coupon_rate is None:
                warn.append("perpetual without coupon rate")
                out.at[idx, "ytm_method"] = "perpetual_compounded"
                out.at[idx, "warning_text"] = "; ".join(warn)
                out.at[idx, "warnings"] = out.at[idx, "warning_text"]
                continue
            periodic = (outstanding * float(coupon_rate) / 100.0 / ppy) / dirty
            out.at[idx, "yield_perpetual_compounded"] = (1 + periodic) ** ppy - 1
            out.at[idx, "ytm_method"] = "perpetual_compounded"
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        if cpn.empty or cpn["value"].fillna(0).sum() == 0:
            if outstanding <= 0:
                warn.append("bad_redemption")
            years = max((horizon - today).days / 365.0, 1.0 / 365.0)
            y = _safe_zero_coupon_yield(outstanding, dirty, years)
            if y is None:
                warn.append("zero-coupon formula invalid or overflow")
            out.at[idx, "ytm_zero_coupon"] = y
            out.at[idx, "ytm_calc"] = y
            out.at[idx, "ytm_method"] = "zero_coupon"
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        if kind == "floater":
            spread = row.get("dohod_dohod_spread")
            if spread is None or pd.isna(spread):
                warn.append("missing floater spread")
                out.at[idx, "ytm_method"] = "floater_scenario"
                out.at[idx, "warning_text"] = "; ".join(warn)
                out.at[idx, "warnings"] = out.at[idx, "warning_text"]
                continue
            spread = float(spread)
            future_dates = sorted([d for d in cpn["parsed_coupondate"].dropna() if d is not None and d > today and d <= horizon])
            if not future_dates:
                ppy = _payments_per_year(row, cpn)
                step = max(int(365 / ppy), 1)
                d = today + timedelta(days=step)
                while d <= horizon:
                    future_dates.append(d)
                    d += timedelta(days=step)
            prev = today
            for d in future_dates:
                tenor = max(0.5, min(10.0, (horizon - today).days / 365.0))
                base = None
                base_idx = str(coalesce(row.get("dohod_dohod_base_index"), "")).lower()
                if "ruonia" in base_idx and ruonia_today is not None:
                    base = rolling_value(key_vals, d, today) + (ruonia_today - key_today)
                else:
                    z = interpolate_zcyc(market_zcyc, tenor)
                    if z is not None:
                        base = z + (rolling_value(key_vals, d, today) - key_today)
                if base is None:
                    continue
                rate = (base + spread) / 100.0
                cf = outstanding * rate * max((d - prev).days, 1) / 365.0
                flows.append((d, cf))
                prev = d
            flows.append((horizon, outstanding))
            out.at[idx, "ytm_calc"] = _xirr(flows, today)
            out.at[idx, "ytm_method"] = "floater_scenario"
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        if kind == "linker":
            infl_vals = config["scenario"]["linker_inflation_percent"]
            prev = today
            for d in sorted([d for d in cpn["parsed_coupondate"].dropna() if d is not None and d > today and d <= horizon]):
                infl = rolling_value(infl_vals, d, today) / 100.0
                years = max((d - today).days / 365.0, 0)
                indexed = outstanding * ((1 + infl) ** years)
                cpn_rate = coalesce(row.get("dohod_dohod_coupon_rate"), cpn[cpn["parsed_coupondate"] == d]["rate"].iloc[0], 0.0)
                cf = indexed * float(cpn_rate) / 100.0 * max((d - prev).days, 1) / 365.0
                flows.append((d, cf))
                prev = d
            if len(flows) <= 1:
                warn.append("insufficient linker data")
            flows.append((horizon, outstanding))
            out.at[idx, "ytm_calc"] = _xirr(flows, today) if len(flows) > 1 else None
            out.at[idx, "ytm_method"] = "linker_scenario"
            out.at[idx, "warning_text"] = "; ".join(warn)
            out.at[idx, "warnings"] = out.at[idx, "warning_text"]
            continue

        for _, r in cpn.iterrows():
            d = r.get("parsed_coupondate")
            v = r.get("value")
            if d is not None and pd.notna(v) and d > today and d <= horizon:
                flows.append((d, float(v)))
        for _, r in amo.iterrows():
            d = r.get("parsed_amortdate")
            v = r.get("value")
            if d is not None and pd.notna(v) and d > today and d <= horizon:
                flows.append((d, float(v)))
        flows.append((horizon, outstanding))
        out.at[idx, "ytm_calc"] = _xirr(flows, today)
        out.at[idx, "ytm_method"] = "fixed_cashflow"
        out.at[idx, "warning_text"] = "; ".join(warn)
        out.at[idx, "warnings"] = out.at[idx, "warning_text"]

    outlier_mask = out["ytm_calc"].notna() & ((out["ytm_calc"] > 2.0) | (out["ytm_calc"] < -0.5))
    out.loc[outlier_mask, "ytm_is_outlier"] = True
    out.loc[outlier_mask, "warning_text"] = out.loc[outlier_mask, "warning_text"].map(
        lambda v: "; ".join([w for w in [str(v).strip("; "), "ytm_outlier"] if w and w != "nan"])
    )
    out.loc[outlier_mask, "warnings"] = out.loc[outlier_mask, "warning_text"]

    return out
