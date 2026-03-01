from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .utils import normalize_decimal, parse_date, rolling_value


@dataclass
class YTMResult:
    ytm_calc: float | None
    warning_text: str | None


def _xnpv(rate: float, cashflows: list[tuple[date, float]], start_date: date) -> float:
    total = 0.0
    for dt, amount in cashflows:
        t = (dt - start_date).days / 365.0
        total += amount / ((1 + rate) ** t)
    return total


def _xirr(cashflows: list[tuple[date, float]], start_date: date) -> float | None:
    lo, hi = -0.95, 5.0
    f_lo = _xnpv(lo, cashflows, start_date)
    f_hi = _xnpv(hi, cashflows, start_date)
    if f_lo * f_hi > 0:
        return None
    for _ in range(120):
        mid = (lo + hi) / 2
        f_mid = _xnpv(mid, cashflows, start_date)
        if abs(f_mid) < 1e-7:
            return mid
        if f_lo * f_mid <= 0:
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid
    return (lo + hi) / 2


def _pick(row: pd.Series, *cols: str, default: float | None = None) -> float | None:
    for col in cols:
        if col in row and pd.notna(row[col]):
            val = normalize_decimal(row[col])
            if val is not None:
                return val
    return default


def _build_cashflows(
    row: pd.Series,
    coupons: pd.DataFrame,
    amortizations: pd.DataFrame,
    today: date,
    key_rate: list[float],
    linker_inf: list[float],
) -> tuple[list[tuple[date, float]], list[str]]:
    warnings: list[str] = []
    isin = row.get("isin")
    secid = row.get("secid")

    sub_c = coupons[(coupons.get("isin") == isin) | (coupons.get("secid") == secid)].copy() if not coupons.empty else pd.DataFrame()
    sub_a = amortizations[(amortizations.get("isin") == isin) | (amortizations.get("secid") == secid)].copy() if not amortizations.empty else pd.DataFrame()

    cashflows: list[tuple[date, float]] = []

    if not sub_c.empty:
        for _, cr in sub_c.iterrows():
            cdate = parse_date(cr.get("coupondate"))
            val = normalize_decimal(cr.get("value"))
            if cdate and cdate > today and val is not None:
                amount = val
                if bool(row.get("dohod_is_floater_norm", False)):
                    margin = normalize_decimal(row.get("dohod_frn_margin_norm")) or 0.0
                    amount = amount * (rolling_value(key_rate, cdate, today) + margin) / max(0.01, key_rate[0])
                if bool(row.get("dohod_is_linker_norm", False)):
                    amount = amount * (1 + rolling_value(linker_inf, cdate, today) / 100)
                cashflows.append((cdate, amount))
    else:
        warnings.append("no_coupons")

    amort_sum = 0.0
    if not sub_a.empty:
        for _, ar in sub_a.iterrows():
            adate = parse_date(ar.get("amortdate"))
            val = normalize_decimal(ar.get("value"))
            if adate and adate > today and val is not None and val > 0:
                amort_sum += val
                cashflows.append((adate, val))

    matdate = parse_date(row.get("dohod_matdate_norm") or row.get("moex_matdate"))
    face = _pick(row, "moex_facevalue", default=1000.0) or 1000.0
    residual = max(0.0, face - amort_sum)
    if matdate and matdate > today and residual > 0:
        cashflows.append((matdate, residual))

    cashflows.sort(key=lambda x: x[0])
    return cashflows, warnings


def calc_ytm_for_row(
    row: pd.Series,
    coupons: pd.DataFrame,
    amortizations: pd.DataFrame,
    today: date,
    key_rate: list[float],
    linker_inf: list[float],
) -> YTMResult:
    clean = _pick(row, "dohod_price_norm", "moex_price")
    nkd = _pick(row, "dohod_nkd_norm", "moex_nkd", default=0.0)
    if clean is None:
        return YTMResult(None, "no_price")
    dirty = clean + (nkd or 0.0)

    cashflows, warnings = _build_cashflows(row, coupons, amortizations, today, key_rate, linker_inf)
    if not cashflows:
        warnings.append("no_future_cashflows")
        return YTMResult(None, ";".join(warnings))

    flows = [(today, -dirty)] + cashflows
    irr = _xirr(flows, today)
    if irr is None:
        warnings.append("irr_failed")
        return YTMResult(None, ";".join(warnings))
    return YTMResult(round(irr * 100, 6), ";".join(warnings) or None)
