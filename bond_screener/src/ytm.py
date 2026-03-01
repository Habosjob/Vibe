from __future__ import annotations

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
        dates = sorted(coupons["coupondate"].dropna())
        if len(dates) >= 2:
            avg_days = sum((dates[i] - dates[i - 1]).days for i in range(1, len(dates))) / (len(dates) - 1)
            if avg_days > 0:
                return 365.0 / avg_days
    return 2.0


def compute_ytm(merged: pd.DataFrame, coupons_df: pd.DataFrame, amort_df: pd.DataFrame, market_ruonia: pd.DataFrame, market_zcyc: pd.DataFrame, config: Dict) -> pd.DataFrame:
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

    for idx, row in out.iterrows():
        secid = row.get("secid")
        horizon = row.get("target_date")
        if pd.isna(horizon):
            out.at[idx, "warning_text"] = "missing target date"
            continue
        if isinstance(horizon, pd.Timestamp):
            horizon = horizon.date()

        clean = coalesce(row.get("dohod_dohod_price"), row.get("moex_moex_price"))
        nkd = coalesce(row.get("dohod_dohod_nkd"), row.get("moex_moex_nkd"), 0.0)
        warn = []
        if clean is None:
            warn.append("missing price")
            out.at[idx, "warning_text"] = "; ".join(warn)
            continue
        if row.get("dohod_dohod_nkd") is None and row.get("moex_moex_nkd") is None:
            warn.append("nkd defaulted to zero")
        dirty = float(clean) + float(nkd or 0.0)

        nominal = float(coalesce(row.get("dohod_dohod_current_nominal"), row.get("moex_norm_facevalue"), 1000.0))
        if row.get("dohod_dohod_current_nominal") is None and row.get("moex_norm_facevalue") is None:
            warn.append("nominal defaulted to 1000")

        cpn = coupons_df[coupons_df["secid"] == secid].copy() if not coupons_df.empty else pd.DataFrame(columns=["coupondate", "value", "rate"])
        amo = amort_df[amort_df["secid"] == secid].copy() if not amort_df.empty else pd.DataFrame(columns=["amortdate", "value"])

        outstanding = nominal - amo[(amo["amortdate"].notna()) & (amo["amortdate"] < horizon)]["value"].fillna(0).sum()
        if outstanding < 0:
            outstanding = 0.0

        kind = _bond_kind(row)
        flows = [(today, -dirty)]

        if kind == "perpetual":
            ppy = _payments_per_year(row, cpn)
            coupon_rate = coalesce(row.get("dohod_dohod_coupon_rate"), cpn["rate"].dropna().mean())
            if coupon_rate is None:
                out.at[idx, "warning_text"] = "perpetual without coupon rate"
                out.at[idx, "ytm_method"] = "perpetual_compounded"
                continue
            periodic = (outstanding * float(coupon_rate) / 100.0 / ppy) / dirty
            out.at[idx, "yield_perpetual_compounded"] = (1 + periodic) ** ppy - 1
            out.at[idx, "ytm_method"] = "perpetual_compounded"
            out.at[idx, "warning_text"] = "; ".join(warn)
            continue

        if cpn.empty or cpn["value"].fillna(0).sum() == 0:
            years = max((horizon - today).days / 365.0, 1e-6)
            y = (outstanding / dirty) ** (1 / years) - 1 if dirty > 0 else None
            out.at[idx, "ytm_zero_coupon"] = y
            out.at[idx, "ytm_calc"] = y
            out.at[idx, "ytm_method"] = "zero_coupon"
            out.at[idx, "warning_text"] = "; ".join(warn)
            continue

        if kind == "floater":
            spread = row.get("dohod_dohod_spread")
            if spread is None or pd.isna(spread):
                warn.append("missing floater spread")
                out.at[idx, "ytm_method"] = "floater_scenario"
                out.at[idx, "warning_text"] = "; ".join(warn)
                continue
            spread = float(spread)
            future_dates = sorted([d for d in cpn["coupondate"].dropna() if d <= horizon and d > today])
            if not future_dates:
                ppy = _payments_per_year(row, cpn)
                step = int(365 / ppy)
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
            continue

        if kind == "linker":
            infl_vals = config["scenario"]["linker_inflation_percent"]
            prev = today
            for d in sorted([d for d in cpn["coupondate"].dropna() if d <= horizon and d > today]):
                infl = rolling_value(infl_vals, d, today) / 100.0
                years = max((d - today).days / 365.0, 0)
                indexed = outstanding * ((1 + infl) ** years)
                cpn_rate = coalesce(row.get("dohod_dohod_coupon_rate"), cpn[cpn["coupondate"] == d]["rate"].iloc[0], 0.0)
                cf = indexed * float(cpn_rate) / 100.0 * max((d - prev).days, 1) / 365.0
                flows.append((d, cf))
                prev = d
            if len(flows) <= 1:
                warn.append("insufficient linker data")
            flows.append((horizon, outstanding))
            out.at[idx, "ytm_calc"] = _xirr(flows, today) if len(flows) > 1 else None
            out.at[idx, "ytm_method"] = "linker_scenario"
            out.at[idx, "warning_text"] = "; ".join(warn)
            continue

        for _, r in cpn.iterrows():
            d = r.get("coupondate")
            v = r.get("value")
            if pd.notna(d) and pd.notna(v) and d > today and d <= horizon:
                flows.append((d, float(v)))
        for _, r in amo.iterrows():
            d = r.get("amortdate")
            v = r.get("value")
            if pd.notna(d) and pd.notna(v) and d > today and d <= horizon:
                flows.append((d, float(v)))
        flows.append((horizon, outstanding))
        out.at[idx, "ytm_calc"] = _xirr(flows, today)
        out.at[idx, "ytm_method"] = "fixed_cashflow"
        out.at[idx, "warning_text"] = "; ".join(warn)

    return out
