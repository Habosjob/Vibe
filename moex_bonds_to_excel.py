#!/usr/bin/env python3
"""Выгружает облигации Московской биржи (MOEX) в Excel-файл."""

from __future__ import annotations

import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

MOEX_BONDS_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
MOEX_SECURITY_URL = "https://iss.moex.com/iss/securities/{secid}.json"
MOEX_BONDIZATION_URL = "https://iss.moex.com/iss/securities/{secid}/bondization.json"

OUTPUT_XLSX = "moex_bonds.xlsx"
ENRICH_ENABLE = True
ENRICH_LIMIT = 20
ENRICH_SECIDS: list[str] = []
CACHE_ENABLE = True
CACHE_TTL_HOURS = 24
INCLUDE_DETAIL_SHEETS = True
CALC_YTM_SIMPLE = True
INCLUDE_INACTIVE = False
BASE_CACHE_FILE = Path(".cache/moex_bonds_payload.json")

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

NUMERIC_COLUMNS = {
    "FACEVALUE": "#,##0.00",
    "COUPONVALUE": "#,##0.00",
    "LAST": "#,##0.00",
    "WAPRICE": "#,##0.00",
    "YIELD": "0.00",
    "VALUE": "#,##0.00",
    "VOLRUR": "#,##0.00",
    "NUMTRADES": "#,##0",
    "COUPONPERIOD": "0",
    "ACCRUEDINT_RUB": "#,##0.00",
    "PRICE_RUB": "#,##0.00",
    "PRICE_RUB_WA": "#,##0.00",
    "DIRTY_PRICE_RUB": "#,##0.00",
    "OFFER_PRICE_PCT": "0.00",
    "OFFER_PRICE_RUB": "#,##0.00",
    "NEXT_AMORT_VALUE": "#,##0.00",
    "NEXT_COUPON_VALUE": "#,##0.00",
    "YTM_SIMPLE": "0.00%",
}

COLUMN_WIDTHS = {
    "SECID": 14,
    "SHORTNAME": 20,
    "FACEVALUE": 14,
    "FACEUNIT": 10,
    "COUPONVALUE": 14,
    "COUPONPERIOD": 13,
    "MATDATE": 12,
    "LAST": 10,
    "WAPRICE": 10,
    "YIELD": 10,
    "VALUE": 14,
    "VOLRUR": 14,
    "NUMTRADES": 12,
    "ISSUER_NAME": 24,
    "ISSUER_INN": 14,
    "CREDIT_RATING": 16,
    "BOND_TYPE": 12,
    "COUPON_FORMULA": 24,
}

CENTER_COLUMNS = {
    "FACEUNIT",
    "MATDATE",
    "NEXT_OFFER_DATE",
    "NEXT_AMORT_DATE",
    "NEXT_COUPON_DATE",
    "YTM_SIMPLE_OK",
    "HAS_OFFER",
    "HAS_AMORTIZATION",
}

EXPECTED_COLUMNS = [
    "SECID",
    "SHORTNAME",
    "FACEVALUE",
    "FACEUNIT",
    "COUPONVALUE",
    "COUPONPERIOD",
    "MATDATE",
    "LAST",
    "WAPRICE",
    "YIELD",
    "VALUE",
    "VOLRUR",
    "NUMTRADES",
    "ISSUER_NAME",
    "ISSUER_INN",
    "CREDIT_RATING",
    "BOND_TYPE",
    "COUPON_FORMULA",
    "ACCRUEDINT_RUB",
    "PRICE_RUB",
    "PRICE_RUB_WA",
    "DIRTY_PRICE_RUB",
    "HAS_OFFER",
    "NEXT_OFFER_DATE",
    "OFFER_TYPE",
    "OFFER_PRICE_PCT",
    "OFFER_PRICE_RUB",
    "HAS_AMORTIZATION",
    "AMORT_START_DATE",
    "NEXT_AMORT_DATE",
    "NEXT_AMORT_VALUE",
    "NEXT_COUPON_DATE",
    "NEXT_COUPON_VALUE",
    "YTM_SIMPLE",
    "YTM_SIMPLE_OK",
]


def log_step(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def _to_dataframe(payload: dict[str, Any], block: str) -> pd.DataFrame:
    section = payload.get(block) or {}
    columns = section.get("columns") or []
    data = section.get("data") or []
    if not columns:
        return pd.DataFrame()
    return pd.DataFrame(data, columns=columns)


def _safe_upper(value: Any) -> str:
    return str(value or "").upper()


def _find_column(df: pd.DataFrame, tokens: list[str]) -> str | None:
    for col in df.columns:
        col_u = _safe_upper(col)
        if any(token in col_u for token in tokens):
            return col
    return None


def _col_idx(columns: list[str], name: str) -> int | None:
    target = name.casefold()
    for idx, col in enumerate(columns):
        if str(col).casefold() == target:
            return idx
    return None


def _get(row: Any, idx: int | None, default: Any = None) -> Any:
    if idx is None:
        return default
    if isinstance(row, (list, tuple)):
        if idx < 0 or idx >= len(row):
            return default
        value = row[idx]
    else:
        try:
            value = row.iloc[idx]
        except Exception:  # noqa: BLE001
            return default
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    return value


def _pick_money_per_bond(columns: list[str], row: Any, facevalue: float | None, *, prefer_rub: bool = True) -> tuple[float | None, str]:
    ordered_names = ["value_rub", "value"] if prefer_rub else ["value", "value_rub"]
    for name in ordered_names:
        idx = _col_idx(columns, name)
        value = pd.to_numeric(_get(row, idx), errors="coerce")
        if pd.notna(value):
            return float(value), name

    valueprc = pd.to_numeric(_get(row, _col_idx(columns, "valueprc")), errors="coerce")
    if pd.notna(valueprc) and facevalue is not None and pd.notna(facevalue):
        return float(facevalue) * float(valueprc) / 100.0, "valueprc"

    return None, "missing"


def _as_float(value: Any) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    return float(numeric) if pd.notna(numeric) else None


def _sanitize_per_bond_value(value: float | None, facevalue: float | None, source_tag: str) -> tuple[float | None, str]:
    if value is None:
        return None, source_tag
    if source_tag in {"missing", "issuevalue"}:
        return None, source_tag
    if facevalue is not None and pd.notna(facevalue) and facevalue > 0 and value > float(facevalue) * 10:
        return None, "suspicious"
    return value, source_tag


def _detail_value_per_bond_rub(row: pd.Series, columns: list[str], facevalue: float | None) -> float | None:
    value_raw, value_scale = _pick_money_per_bond(columns, row, facevalue, prefer_rub=True)
    value, _ = _sanitize_per_bond_value(value_raw, facevalue, value_scale)
    return value


def _load_json_cache(cache_file: Path, ttl_seconds: int) -> dict[str, Any] | None:
    if not CACHE_ENABLE or not cache_file.exists():
        return None
    age = time.time() - cache_file.stat().st_mtime
    if age > ttl_seconds:
        return None
    with cache_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_json_cache(cache_file: Path, payload: dict[str, Any]) -> None:
    if not CACHE_ENABLE:
        return
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with cache_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)


def _cached_get_json(session: requests.Session, url: str, cache_file: Path, ttl_seconds: int) -> dict[str, Any]:
    cached = _load_json_cache(cache_file, ttl_seconds)
    if cached is not None:
        print(f"cache hit: {cache_file}")
        return cached
    print(f"cache miss: {cache_file}")
    response = session.get(url, params={"iss.meta": "off"}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    _save_json_cache(cache_file, payload)
    return payload


def fetch_moex_bonds(session: requests.Session, cache_ttl_seconds: int) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata",
        "securities.columns": (
            "SECID,SHORTNAME,FACEUNIT,FACEVALUE,COUPONVALUE,COUPONPERIOD,MATDATE,STATUS"
        ),
        "marketdata.columns": "SECID,LAST,WAPRICE,YIELD,VALUE,VOLRUR,NUMTRADES,ACCRUEDINT",
    }
    source = "cache"
    payload = _load_json_cache(BASE_CACHE_FILE, cache_ttl_seconds)
    if payload is None:
        source = "api"
        response = session.get(MOEX_BONDS_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        _save_json_cache(BASE_CACHE_FILE, payload)

    securities = _to_dataframe(payload, "securities")
    marketdata = _to_dataframe(payload, "marketdata")
    return securities, marketdata, source


def fetch_security_description(session: requests.Session, secid: str, cache_ttl_seconds: int) -> dict[str, str]:
    cache_file = Path(f".cache/moex/description/{secid}.json")
    payload = _cached_get_json(
        session,
        MOEX_SECURITY_URL.format(secid=secid) + "?iss.only=description",
        cache_file,
        cache_ttl_seconds,
    )
    description = _to_dataframe(payload, "description")
    if description.empty:
        return {}

    key_col = _find_column(description, ["NAME"])
    value_col = _find_column(description, ["VALUE"])
    title_col = _find_column(description, ["TITLE"])
    if not key_col or not value_col:
        return {}

    items: list[tuple[str, str, str]] = []
    for _, row in description.iterrows():
        key = str(row.get(key_col) or "").strip()
        title = str(row.get(title_col) or "").strip() if title_col else ""
        value = str(row.get(value_col) or "").strip()
        if value:
            items.append((title, key, value))

    def pick_by_tokens(tokens: list[str]) -> str:
        folded = [token.casefold() for token in tokens]
        for title, key, value in items:
            title_key = f"{title} {key}".casefold()
            name_key = f"{key} {title}".casefold()
            if any(token in title_key or token in name_key for token in folded):
                return value
        return ""

    coupon_type_raw = pick_by_tokens(
        [
            "coupon type",
            "rate type",
            "floating",
            "float",
            "фикс",
            "плава",
            "тип купона",
            "тип ставки",
        ]
    )
    bond_type = ""
    coupon_type_u = _safe_upper(coupon_type_raw)
    if coupon_type_u:
        if any(token in coupon_type_u for token in ["FLOAT", "ПЛАВА", "VARIABLE", "FRN"]):
            bond_type = "флоатер"
        elif any(token in coupon_type_u for token in ["FIX", "ФИКС", "ПОСТОЯН"]):
            bond_type = "фикс"
        else:
            bond_type = "проч"

    return {
        "ISSUER_NAME": pick_by_tokens(["emitent", "emitter", "issuer", "эмитент", "наименование эмитента"]),
        "ISSUER_INN": pick_by_tokens(["inn", "инн", "tax id", "идентификационный номер"]),
        "BOND_TYPE": bond_type,
        "COUPON_FORMULA": pick_by_tokens(["formula", "формул", "купон", "coupon", "ставк"]),
        "CREDIT_RATING": pick_by_tokens(["rating", "рейтинг", "кредитный рейтинг"]),
    }


def fetch_bondization(session: requests.Session, secid: str, cache_ttl_seconds: int) -> dict[str, pd.DataFrame]:
    cache_file = Path(f".cache/moex/bondization/{secid}.json")
    payload = _cached_get_json(
        session,
        MOEX_BONDIZATION_URL.format(secid=secid),
        cache_file,
        cache_ttl_seconds,
    )
    return {
        "coupons": _to_dataframe(payload, "coupons"),
        "amortizations": _to_dataframe(payload, "amortizations"),
        "offers": _to_dataframe(payload, "offers"),
    }


def _pick_next_event(df: pd.DataFrame, date_tokens: list[str], value_tokens: list[str]) -> tuple[pd.Timestamp | None, float | None]:
    if df.empty:
        return None, None
    date_col = _find_column(df, date_tokens)
    value_col = _find_column(df, value_tokens)
    if not date_col:
        return None, None
    local = df.copy()
    local[date_col] = pd.to_datetime(local[date_col], errors="coerce")
    local = local[local[date_col] >= pd.Timestamp.today().normalize()].sort_values(date_col)
    if local.empty:
        return None, None
    row = local.iloc[0]
    value = pd.to_numeric(row[value_col], errors="coerce") if value_col else None
    return row[date_col], (float(value) if pd.notna(value) else None)


def _aggregate_bondization(
    bondization: dict[str, pd.DataFrame],
    facevalue: float | None,
    faceunit: str,
) -> dict[str, Any]:
    offers = bondization.get("offers", pd.DataFrame())
    coupons = bondization.get("coupons", pd.DataFrame())
    amortizations = bondization.get("amortizations", pd.DataFrame())
    today = pd.Timestamp.today().normalize()

    next_offer_date: pd.Timestamp | None = None
    offer_price_rub: float | None = None
    offer_price_pct: float | None = None
    offer_type = ""

    if not offers.empty:
        offers_local = offers.copy()
        offer_cols = offers_local.columns.tolist()
        offer_date_col = _find_column(offers_local, ["DATE", "OFFER", "PUT"])
        offer_type_col = _find_column(offers_local, ["TYPE", "KIND"])
        if offer_date_col:
            offers_local[offer_date_col] = pd.to_datetime(offers_local[offer_date_col], errors="coerce")
            offers_local = offers_local[offers_local[offer_date_col] >= today].sort_values(offer_date_col)
            if not offers_local.empty:
                row = offers_local.iloc[0]
                next_offer_date = row[offer_date_col]
                if offer_type_col:
                    offer_type = str(row.get(offer_type_col) or "")

                price = _as_float(_get(row, _col_idx(offer_cols, "price")))
                if price is not None:
                    offer_price_pct = price
                    if faceunit.upper() in {"RUB", "SUR", "RUR"} and facevalue is not None:
                        offer_price_rub = facevalue * price / 100.0

    next_coupon_date: pd.Timestamp | None = None
    next_coupon_value: float | None = None
    if not coupons.empty:
        coupons_local = coupons.copy()
        coupon_cols = coupons_local.columns.tolist()
        coupon_date_col = _find_column(coupons_local, ["COUPON", "DATE"])
        if coupon_date_col:
            coupons_local[coupon_date_col] = pd.to_datetime(coupons_local[coupon_date_col], errors="coerce")
            coupons_local = coupons_local[coupons_local[coupon_date_col] >= today].sort_values(coupon_date_col)
            if not coupons_local.empty:
                row = coupons_local.iloc[0]
                next_coupon_date = row[coupon_date_col]
                picked_coupon, coupon_scale = _pick_money_per_bond(coupon_cols, row, facevalue, prefer_rub=True)
                next_coupon_value, _ = _sanitize_per_bond_value(picked_coupon, facevalue, coupon_scale)

    amort_start_date: pd.Timestamp | None = None
    next_amort_date: pd.Timestamp | None = None
    next_amort_value: float | None = None
    if not amortizations.empty:
        amort_local = amortizations.copy()
        amort_cols = amort_local.columns.tolist()
        amort_date_col = _find_column(amort_local, ["AMORT", "DATE"])
        if amort_date_col:
            amort_local[amort_date_col] = pd.to_datetime(amort_local[amort_date_col], errors="coerce")
            amort_local = amort_local[amort_local[amort_date_col] >= today].sort_values(amort_date_col)
            if not amort_local.empty:
                amort_start_date = amort_local[amort_date_col].min()
                row = amort_local.iloc[0]
                next_amort_date = row[amort_date_col]
                picked_amort, amort_scale = _pick_money_per_bond(amort_cols, row, facevalue, prefer_rub=True)
                next_amort_value, _ = _sanitize_per_bond_value(picked_amort, facevalue, amort_scale)

    return {
        "HAS_OFFER": bool(next_offer_date is not None),
        "NEXT_OFFER_DATE": next_offer_date,
        "OFFER_TYPE": offer_type,
        "OFFER_PRICE_PCT": offer_price_pct,
        "OFFER_PRICE_RUB": offer_price_rub,
        "HAS_AMORTIZATION": bool(next_amort_date is not None),
        "AMORT_START_DATE": amort_start_date,
        "NEXT_AMORT_DATE": next_amort_date,
        "NEXT_AMORT_VALUE": next_amort_value,
        "NEXT_COUPON_DATE": next_coupon_date,
        "NEXT_COUPON_VALUE": next_coupon_value,
    }


def _xnpv(rate: float, dates: list[pd.Timestamp], cashflows: list[float]) -> float:
    t0 = dates[0]
    return sum(cf / ((1 + rate) ** ((dt - t0).days / 365.0)) for dt, cf in zip(dates, cashflows))


def _xirr(dates: list[pd.Timestamp], cashflows: list[float]) -> float | None:
    if len(dates) < 2:
        return None
    if not (any(cf < 0 for cf in cashflows) and any(cf > 0 for cf in cashflows)):
        return None

    low, high = -0.95, 1.0
    f_low = _xnpv(low, dates, cashflows)
    f_high = _xnpv(high, dates, cashflows)
    expand_steps = 0
    while f_low * f_high > 0 and expand_steps < 20:
        high *= 2
        if high > 100:
            return None
        f_high = _xnpv(high, dates, cashflows)
        expand_steps += 1

    if f_low * f_high > 0:
        return None

    for _ in range(120):
        mid = (low + high) / 2
        f_mid = _xnpv(mid, dates, cashflows)
        if abs(f_mid) < 1e-8:
            return mid
        if f_low * f_mid <= 0:
            high, f_high = mid, f_mid
        else:
            low, f_low = mid, f_mid
    return (low + high) / 2


def _build_cashflows_for_ytm(row: pd.Series, bondization: dict[str, pd.DataFrame]) -> tuple[list[pd.Timestamp], list[float]]:
    dirty_price = pd.to_numeric(row.get("DIRTY_PRICE_RUB"), errors="coerce")
    if pd.isna(dirty_price) or float(dirty_price) <= 0:
        return [], []

    today = pd.Timestamp.today().normalize()
    dates = [today]
    cashflows = [-float(dirty_price)]

    facevalue = pd.to_numeric(row.get("FACEVALUE"), errors="coerce")
    facevalue_float = float(facevalue) if pd.notna(facevalue) else None

    coupons = bondization.get("coupons", pd.DataFrame()).copy()
    if not coupons.empty:
        date_col = _find_column(coupons, ["COUPON", "DATE"])
        if date_col:
            coupon_cols = coupons.columns.tolist()
            coupons[date_col] = pd.to_datetime(coupons[date_col], errors="coerce")
            coupons = coupons[coupons[date_col] > today].sort_values(date_col)
            for _, c in coupons.iterrows():
                coupon_value_raw, coupon_scale = _pick_money_per_bond(coupon_cols, c, facevalue_float, prefer_rub=True)
                coupon_value, _ = _sanitize_per_bond_value(coupon_value_raw, facevalue_float, coupon_scale)
                if coupon_value is not None and coupon_value > 0:
                    dates.append(c[date_col])
                    cashflows.append(float(coupon_value))

    amortizations = bondization.get("amortizations", pd.DataFrame()).copy()
    amort_sum = 0.0
    has_future_amortizations = False
    if not amortizations.empty:
        date_col = _find_column(amortizations, ["AMORT", "DATE"])
        if date_col:
            amort_cols = amortizations.columns.tolist()
            amortizations[date_col] = pd.to_datetime(amortizations[date_col], errors="coerce")
            amortizations = amortizations[amortizations[date_col] > today].sort_values(date_col)
            has_future_amortizations = not amortizations.empty
            for _, a in amortizations.iterrows():
                amort_value_raw, amort_scale = _pick_money_per_bond(amort_cols, a, facevalue_float, prefer_rub=True)
                amort_value, _ = _sanitize_per_bond_value(amort_value_raw, facevalue_float, amort_scale)
                if amort_value is not None and amort_value > 0:
                    amort_sum += float(amort_value)
                    dates.append(a[date_col])
                    cashflows.append(float(amort_value))

    facevalue = pd.to_numeric(row.get("FACEVALUE"), errors="coerce")
    matdate = pd.to_datetime(row.get("MATDATE"), errors="coerce")
    if pd.notna(facevalue) and pd.notna(matdate) and matdate > today:
        remainder = float(facevalue) - amort_sum
        if remainder > 0.01:
            dates.append(matdate)
            cashflows.append(remainder)
        elif has_future_amortizations and abs(remainder) <= 0.01:
            pass

    pairs = sorted([(d, cf) for d, cf in zip(dates, cashflows) if pd.notna(d) and pd.notna(cf)], key=lambda x: x[0])
    if len(pairs) < 2:
        return [], []
    return [p[0] for p in pairs], [p[1] for p in pairs]


def build_report_dataframe(securities: pd.DataFrame, marketdata: pd.DataFrame, only_active: bool) -> pd.DataFrame:
    report = securities.merge(marketdata, on="SECID", how="left")

    if only_active and "STATUS" in report.columns:
        report = report[report["STATUS"] == "A"].copy()

    if "MATDATE" in report.columns:
        report["MATDATE"] = pd.to_datetime(report["MATDATE"], errors="coerce")
        min_maturity_date = pd.Timestamp.today().normalize() + pd.DateOffset(years=1)
        report = report[(report["MATDATE"].isna()) | (report["MATDATE"] >= min_maturity_date)].copy()

    report["ACCRUEDINT_RUB"] = pd.to_numeric(report.get("ACCRUEDINT"), errors="coerce")
    is_rub = report.get("FACEUNIT", pd.Series(index=report.index)).astype(str).str.upper().isin(["RUB", "SUR", "RUR"])
    report["PRICE_RUB"] = pd.NA
    report["PRICE_RUB_WA"] = pd.NA

    last = pd.to_numeric(report.get("LAST"), errors="coerce")
    waprice = pd.to_numeric(report.get("WAPRICE"), errors="coerce")
    facevalue = pd.to_numeric(report.get("FACEVALUE"), errors="coerce")

    report.loc[is_rub, "PRICE_RUB"] = (facevalue[is_rub] * last[is_rub] / 100).astype(float)
    report.loc[is_rub, "PRICE_RUB_WA"] = (facevalue[is_rub] * waprice[is_rub] / 100).astype(float)

    report["DIRTY_PRICE_RUB"] = pd.to_numeric(report["PRICE_RUB"], errors="coerce") + pd.to_numeric(
        report["ACCRUEDINT_RUB"], errors="coerce"
    )
    report.loc[report["ACCRUEDINT_RUB"].isna(), "DIRTY_PRICE_RUB"] = pd.to_numeric(
        report.loc[report["ACCRUEDINT_RUB"].isna(), "PRICE_RUB"], errors="coerce"
    )
    report.loc[~is_rub, "PRICE_RUB"] = pd.NA
    report.loc[~is_rub, "DIRTY_PRICE_RUB"] = pd.NA

    for col in EXPECTED_COLUMNS:
        if col not in report.columns:
            report[col] = pd.NA

    report = report[EXPECTED_COLUMNS].sort_values(by=["MATDATE", "SECID"], na_position="last")
    return report


def enrich_report(
    session: requests.Session,
    report: pd.DataFrame,
    cache_ttl_seconds: int,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    detail_offers: list[pd.DataFrame] = []
    detail_coupons: list[pd.DataFrame] = []
    detail_amortizations: list[pd.DataFrame] = []

    target = report.copy()
    secids = target["SECID"].astype(str).tolist()
    secids = list(dict.fromkeys(secids))
    if ENRICH_SECIDS:
        selected = {sid.upper() for sid in ENRICH_SECIDS}
        secids = [sid for sid in secids if sid.upper() in selected]
    if ENRICH_LIMIT and ENRICH_LIMIT > 0:
        secids = secids[:ENRICH_LIMIT]

    log_step(f"В enrichment ушло бумаг: {len(secids)}")

    ytm_ok = 0
    for secid in secids:
        idx = target.index[target["SECID"] == secid]
        if idx.empty:
            continue
        i = idx[0]

        try:
            desc = fetch_security_description(session, secid, cache_ttl_seconds)
            for key, value in desc.items():
                if value:
                    target.at[i, key] = value
        except Exception as exc:  # noqa: BLE001
            print(f"description error [{secid}]: {exc}")

        bondization = {"coupons": pd.DataFrame(), "amortizations": pd.DataFrame(), "offers": pd.DataFrame()}
        try:
            bondization = fetch_bondization(session, secid, cache_ttl_seconds)
            facevalue = _as_float(target.at[i, "FACEVALUE"])
            faceunit = str(target.at[i, "FACEUNIT"] or "")
            agg = _aggregate_bondization(bondization, facevalue=facevalue, faceunit=faceunit)
            for key, value in agg.items():
                if value is not None and value != "":
                    target.at[i, key] = value
                elif key in {"HAS_OFFER", "HAS_AMORTIZATION"}:
                    target.at[i, key] = bool(value)
        except Exception as exc:  # noqa: BLE001
            print(f"bondization error [{secid}]: {exc}")

        if INCLUDE_DETAIL_SHEETS:
            for block_name, collector in [
                ("offers", detail_offers),
                ("coupons", detail_coupons),
                ("amortizations", detail_amortizations),
            ]:
                block_df = bondization.get(block_name, pd.DataFrame())
                if not block_df.empty:
                    copy_df = block_df.copy()
                    block_cols = copy_df.columns.tolist()
                    facevalue = _as_float(target.at[i, "FACEVALUE"])
                    copy_df["value_per_bond_rub"] = copy_df.apply(
                        lambda r: _detail_value_per_bond_rub(r, block_cols, facevalue),
                        axis=1,
                    )
                    copy_df.insert(0, "SECID", secid)
                    collector.append(copy_df)

        if CALC_YTM_SIMPLE:
            try:
                dates, cashflows = _build_cashflows_for_ytm(target.loc[i], bondization)
                ytm_value = _xirr(dates, cashflows) if dates else None
                if ytm_value is not None and math.isfinite(ytm_value):
                    target.at[i, "YTM_SIMPLE"] = ytm_value
                    target.at[i, "YTM_SIMPLE_OK"] = True
                    ytm_ok += 1
                else:
                    target.at[i, "YTM_SIMPLE_OK"] = False
            except Exception:  # noqa: BLE001
                target.at[i, "YTM_SIMPLE_OK"] = False

    detail_offers = [df for df in detail_offers if not df.empty]
    detail_coupons = [df for df in detail_coupons if not df.empty]
    detail_amortizations = [df for df in detail_amortizations if not df.empty]

    detail_sheets = {
        "offers": pd.concat(detail_offers, ignore_index=True) if detail_offers else pd.DataFrame(),
        "coupons": pd.concat(detail_coupons, ignore_index=True) if detail_coupons else pd.DataFrame(),
        "amortizations": pd.concat(detail_amortizations, ignore_index=True) if detail_amortizations else pd.DataFrame(),
    }

    has_offer_count = int(pd.to_numeric(target["HAS_OFFER"], errors="coerce").fillna(0).astype(bool).sum())
    has_rating_count = int(target["CREDIT_RATING"].fillna("").astype(str).str.len().gt(0).sum())
    has_inn_count = int(target["ISSUER_INN"].fillna("").astype(str).str.len().gt(0).sum())
    has_type_count = int(target["BOND_TYPE"].fillna("").astype(str).str.len().gt(0).sum())

    log_step(
        f"С офертами: {has_offer_count}; с ИНН: {has_inn_count}; с рейтингом: {has_rating_count}; с типом: {has_type_count}"
    )
    log_step(f"YTM_SIMPLE рассчитан для: {ytm_ok}")
    return target, detail_sheets


def _ensure_expected_columns(df: pd.DataFrame) -> pd.DataFrame:
    target = df.copy()
    for col in EXPECTED_COLUMNS:
        if col not in target.columns:
            target[col] = pd.NA
    return target[EXPECTED_COLUMNS].sort_values(by=["MATDATE", "SECID"], na_position="last")


def save_to_excel(df: pd.DataFrame, output_path: Path, detail_sheets: dict[str, pd.DataFrame]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl", datetime_format="yyyy-mm-dd") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        worksheet = writer.sheets["MOEX_BONDS"]

        worksheet.freeze_panes = "A2"
        worksheet.sheet_view.zoomScale = 110
        worksheet.row_dimensions[1].height = 22

        for idx, column_name in enumerate(df.columns, start=1):
            column_letter = get_column_letter(idx)
            header_cell = worksheet.cell(row=1, column=idx)
            header_cell.fill = HEADER_FILL
            header_cell.font = HEADER_FONT
            header_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=False)
            header_cell.border = BORDER

            worksheet.column_dimensions[column_letter].width = COLUMN_WIDTHS.get(column_name, 14)

        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for cell in row:
                column_name = df.columns[cell.column - 1]
                cell.border = BORDER

                if column_name in NUMERIC_COLUMNS and isinstance(cell.value, (int, float)):
                    cell.number_format = NUMERIC_COLUMNS[column_name]
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif "DATE" in column_name:
                    if cell.value:
                        cell.number_format = "yyyy-mm-dd"
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                elif column_name in CENTER_COLUMNS:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center")

        if worksheet.max_row >= 2 and worksheet.max_column >= 1:
            table = Table(displayName="MOEX_BONDS_TABLE", ref=worksheet.dimensions)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False,
            )
            worksheet.add_table(table)

        if INCLUDE_DETAIL_SHEETS:
            for sheet_name, sheet_df in detail_sheets.items():
                if sheet_df.empty:
                    continue
                sheet_df.to_excel(writer, index=False, sheet_name=sheet_name[:31])


def main() -> None:
    started_at = time.perf_counter()
    cache_ttl_seconds = int(CACHE_TTL_HOURS * 3600)

    log_step("Запускаю выгрузку облигаций MOEX...")
    with requests.Session() as session:
        session.headers.update({"User-Agent": "moex-bonds-export-script/2.0"})
        securities, marketdata, source = fetch_moex_bonds(session=session, cache_ttl_seconds=cache_ttl_seconds)
        log_step(f"Базовые данные получены ({source}): securities={len(securities)}, marketdata={len(marketdata)}")
        report = build_report_dataframe(
            securities=securities,
            marketdata=marketdata,
            only_active=not INCLUDE_INACTIVE,
        )
        log_step(f"Базовая таблица сформирована: {len(report)} бумаг")

        detail_sheets = {"offers": pd.DataFrame(), "coupons": pd.DataFrame(), "amortizations": pd.DataFrame()}
        if ENRICH_ENABLE:
            report, detail_sheets = enrich_report(session, report, cache_ttl_seconds)

    report = _ensure_expected_columns(report)
    log_step(f"Сохраняю Excel-файл: {OUTPUT_XLSX}")
    save_to_excel(report, Path(OUTPUT_XLSX), detail_sheets)
    log_step(f"Готово. Сохранено строк: {len(report)}")
    log_step(f"Файл: {Path(OUTPUT_XLSX).resolve()}")
    log_step(f"Общее время выполнения: {time.perf_counter() - started_at:.2f} сек.")


if __name__ == "__main__":
    main()
