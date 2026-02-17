#!/usr/bin/env python3
"""Выгружает облигации Московской биржи (MOEX) в Excel-файл."""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
MOEX_EMITTER_URL = "https://iss.moex.com/iss/emitters/{emitter_id}.json"
MOEX_SECURITIES_SEARCH_URL = "https://iss.moex.com/iss/securities.json"

OUTPUT_XLSX = "moex_bonds.xlsx"
ENRICH_ENABLE = True
ENRICH_LIMIT = 40
ENRICH_SECIDS: list[str] = []
CACHE_ENABLE = True
CACHE_TTL_HOURS = 24
INCLUDE_DETAIL_SHEETS = True
CALC_YTM_SIMPLE = True
INCLUDE_INACTIVE = False
VERBOSE = False
MAX_WORKERS = 8
REQUEST_PAUSE_SECONDS = 0.05
FX_ENABLE = True
FX_CACHE_TTL_HOURS = 6

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
    "TURNOVER_RUB": "#,##0.00",
    "NUMTRADES": "#,##0",
    "COUPONPERIOD": "0",
    "ACCRUEDINT_NATIVE": "#,##0.00",
    "ACCRUEDINT_RUB_TODAY": "#,##0.00",
    "PRICE_NATIVE": "#,##0.00",
    "PRICE_RUB_TODAY": "#,##0.00",
    "PRICE_RUB_WA": "#,##0.00",
    "DIRTY_PRICE_NATIVE": "#,##0.00",
    "DIRTY_PRICE_RUB_TODAY": "#,##0.00",
    "FX_RATE_TO_RUB_TODAY": "#,##0.0000",
    "OFFER_PRICE_PCT": "0.00",
    "OFFER_PRICE_NATIVE": "#,##0.00",
    "OFFER_PRICE_RUB": "#,##0.00",
    "NEXT_AMORT_VALUE": "#,##0.00",
    "NEXT_AMORT_VALUE_RUB_TODAY": "#,##0.00",
    "NEXT_COUPON_VALUE": "#,##0.00",
    "NEXT_COUPON_VALUE_RUB_TODAY": "#,##0.00",
    "YTM_SIMPLE": "0.00%",
    "YTM_SIMPLE_RUB_ASSUMING_FX_TODAY": "0.00%",
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
    "TURNOVER_RUB": 14,
    "NUMTRADES": 12,
    "ISSUER_NAME": 24,
    "ISSUER_INN": 14,
    "ISSUER_NAME_FALLBACK": 24,
    "BOND_TYPE_RAW": 20,
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

BOOL_COLUMNS = {"HAS_OFFER", "HAS_AMORTIZATION", "YTM_SIMPLE_OK"}

CACHE_STATS = {
    "description_hits": 0,
    "description_misses": 0,
    "bondization_hits": 0,
    "bondization_misses": 0,
    "emitter_hits": 0,
    "emitter_misses": 0,
}

_CACHE_LOCK = threading.Lock()
_WARNING_FLAGS: set[str] = set()


def log_step(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def _warn_once(flag: str, message: str) -> None:
    if flag in _WARNING_FLAGS:
        return
    _WARNING_FLAGS.add(flag)
    log_step(f"WARNING: {message}")


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
    folded = [token.upper() for token in tokens]
    for col in df.columns:
        col_u = _safe_upper(col)
        if any(token in col_u for token in folded):
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
        except Exception:
            return default
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    return value


def _as_float(value: Any) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    return float(numeric) if pd.notna(numeric) else None


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


def _sanitize_per_bond_value(value: float | None, facevalue: float | None, source_tag: str) -> tuple[float | None, str]:
    if value is None:
        return None, source_tag
    if source_tag in {"missing", "issuevalue"}:
        return None, source_tag
    if facevalue is not None and pd.notna(facevalue) and facevalue > 0 and value > float(facevalue) * 10:
        return None, "suspicious"
    return value, source_tag


def _value_from_col(columns: list[str], row: Any, name: str) -> float | None:
    return _as_float(_get(row, _col_idx(columns, name)))


def _normalize_offer_kind(raw: str) -> str:
    raw_u = _safe_upper(raw)
    if "PUT" in raw_u or "ПУТ" in raw_u:
        return "PUT"
    if "CALL" in raw_u or "КОЛЛ" in raw_u:
        return "CALL"
    return "UNKNOWN" if raw_u else ""


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


def _cached_get_json(
    session: requests.Session,
    url: str,
    cache_file: Path,
    ttl_seconds: int,
    stat_hit_key: str | None = None,
    stat_miss_key: str | None = None,
) -> dict[str, Any]:
    cached = _load_json_cache(cache_file, ttl_seconds)
    if cached is not None:
        if stat_hit_key:
            with _CACHE_LOCK:
                CACHE_STATS[stat_hit_key] += 1
        if VERBOSE:
            print(f"cache hit: {cache_file}")
        return cached

    if stat_miss_key:
        with _CACHE_LOCK:
            CACHE_STATS[stat_miss_key] += 1
    if VERBOSE:
        print(f"cache miss: {cache_file}")

    response = session.get(url, params={"iss.meta": "off"}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    _save_json_cache(cache_file, payload)
    return payload


def _hash_cache_name(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _list_cached_ids(path: Path, suffix: str = ".json") -> set[str]:
    if not path.exists():
        return set()
    return {p.stem for p in path.glob(f"*{suffix}") if p.is_file()}


def fetch_moex_bonds(session: requests.Session, cache_ttl_seconds: int) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata",
        "securities.columns": "SECID,SHORTNAME,FACEUNIT,FACEVALUE,COUPONVALUE,COUPONPERIOD,MATDATE,STATUS",
        "marketdata.columns": "SECID,LAST,WAPRICE,YIELD,VALUE,VOLRUR,VOLTODAY,VALTODAY,VALRUR,VOLUME,NUMTRADES,ACCRUEDINT,ACCRUEDINTRUB,ACCRUEDINT_RUB,ACCRUEDINT2",
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


def fetch_security_description(session: requests.Session, secid: str, cache_ttl_seconds: int) -> dict[str, Any]:
    cache_file = Path(f".cache/moex/description/{secid}.json")
    payload = _cached_get_json(
        session,
        MOEX_SECURITY_URL.format(secid=secid) + "?iss.only=description",
        cache_file,
        cache_ttl_seconds,
        stat_hit_key="description_hits",
        stat_miss_key="description_misses",
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
        if key and value:
            items.append((title, key, value))

    def pick_by_tokens(tokens: list[str]) -> str:
        folded = [token.casefold() for token in tokens]
        for title, key, value in items:
            probe = f"{title} {key}".casefold()
            if any(token in probe for token in folded):
                return value
        return ""

    emitter_id = _as_float(pick_by_tokens(["emitter_id", "emitent_id", "код эмитента"]))
    return {
        "EMITTER_ID": int(emitter_id) if emitter_id is not None else None,
        "NAME": pick_by_tokens(["полное наименование", "name"]),
        "LATNAME": pick_by_tokens(["latname", "английское наименование"]),
        "BOND_TYPE_RAW": pick_by_tokens(["вид облигац", "тип облигац", "тип бумаги", "groupname", "тип инструмента"]),
        "COUPON_FORMULA": pick_by_tokens(["formula", "формул", "привяз", "индекс", "спред"]),
        "COUPON_FORMULA_SOURCE": "description",
    }


def _extract_issuer_fields_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    for block_name, block in payload.items():
        if not isinstance(block, dict):
            continue
        columns = [str(c).upper() for c in block.get("columns") or []]
        data = block.get("data") or []
        if not columns or not data:
            continue
        inn_idx = next((i for i, c in enumerate(columns) if "INN" in c), None)
        name_idx = next((i for i, c in enumerate(columns) if any(t in c for t in ["TITLE", "NAME", "SHORTNAME"])), None)
        if inn_idx is None and name_idx is None:
            continue
        row = data[0]
        name = str(_get(row, name_idx, "") or "").strip()
        inn = str(_get(row, inn_idx, "") or "").strip()
        if name or inn:
            return {"ISSUER_NAME": name, "ISSUER_INN": inn}
    return {}


def fetch_emitter_info(session: requests.Session, emitter_id: int, secid: str, cache_ttl_seconds: int) -> dict[str, str]:
    direct_cache = Path(f".cache/moex/emitter/{emitter_id}.json")
    payload = _cached_get_json(
        session,
        MOEX_EMITTER_URL.format(emitter_id=emitter_id),
        direct_cache,
        cache_ttl_seconds,
        stat_hit_key="emitter_hits",
        stat_miss_key="emitter_misses",
    )
    direct = _extract_issuer_fields_from_payload(payload)
    if direct.get("ISSUER_NAME") and direct.get("ISSUER_INN"):
        return direct

    alternative_urls = [
        f"{MOEX_SECURITIES_SEARCH_URL}?q={secid}",
        f"{MOEX_SECURITIES_SEARCH_URL}?q={emitter_id}",
        f"{MOEX_SECURITIES_SEARCH_URL}?iss.only=securities&q={secid}&securities.columns=secid,emitent_id,emitent_title,emitent_inn,name,shortname",
    ]
    merged = {"ISSUER_NAME": direct.get("ISSUER_NAME", ""), "ISSUER_INN": direct.get("ISSUER_INN", "")}
    for url in alternative_urls:
        alt_cache = Path(f".cache/moex/emitter_alt/{_hash_cache_name(url)}.json")
        alt_payload = _cached_get_json(session, url, alt_cache, cache_ttl_seconds)
        block = _to_dataframe(alt_payload, "securities")
        if not block.empty:
            for _, row in block.iterrows():
                row_emitter = _as_float(row.get("emitent_id") or row.get("EMITENT_ID") or row.get("EMITTER_ID"))
                row_secid = str(row.get("secid") or row.get("SECID") or "")
                if (row_emitter is not None and int(row_emitter) == int(emitter_id)) or row_secid.upper() == secid.upper():
                    if not merged["ISSUER_NAME"]:
                        merged["ISSUER_NAME"] = str(row.get("emitent_title") or row.get("emitent_name") or row.get("name") or "").strip()
                    if not merged["ISSUER_INN"]:
                        merged["ISSUER_INN"] = str(row.get("emitent_inn") or row.get("inn") or "").strip()
                    break
        if merged["ISSUER_NAME"] and merged["ISSUER_INN"]:
            break
        extracted = _extract_issuer_fields_from_payload(alt_payload)
        if not merged["ISSUER_NAME"] and extracted.get("ISSUER_NAME"):
            merged["ISSUER_NAME"] = extracted["ISSUER_NAME"]
        if not merged["ISSUER_INN"] and extracted.get("ISSUER_INN"):
            merged["ISSUER_INN"] = extracted["ISSUER_INN"]
        if merged["ISSUER_NAME"] and merged["ISSUER_INN"]:
            break
    return merged


def fetch_bondization(session: requests.Session, secid: str, cache_ttl_seconds: int) -> dict[str, pd.DataFrame]:
    cache_file = Path(f".cache/moex/bondization/{secid}.json")
    payload = _cached_get_json(
        session,
        MOEX_BONDIZATION_URL.format(secid=secid),
        cache_file,
        cache_ttl_seconds,
        stat_hit_key="bondization_hits",
        stat_miss_key="bondization_misses",
    )
    return {
        "coupons": _to_dataframe(payload, "coupons"),
        "amortizations": _to_dataframe(payload, "amortizations"),
        "offers": _to_dataframe(payload, "offers"),
    }


def _is_real_amortization(amortizations: pd.DataFrame, matdate: pd.Timestamp | None) -> bool:
    if amortizations.empty:
        return False
    if len(amortizations) > 1:
        return True

    date_col = _find_column(amortizations, ["AMORT", "DATE"])
    source_col = _find_column(amortizations, ["DATA_SOURCE", "SOURCE"])
    valueprc_col = _find_column(amortizations, ["VALUEPRC", "PRC"])
    for _, row in amortizations.iterrows():
        ds = str(row.get(source_col) or "").strip().lower() if source_col else ""
        amortdate = pd.to_datetime(row.get(date_col), errors="coerce") if date_col else pd.NaT
        valueprc = pd.to_numeric(row.get(valueprc_col), errors="coerce") if valueprc_col else pd.NA
        cond = (
            ds != "maturity"
            or (pd.notna(amortdate) and matdate is not None and pd.notna(matdate) and amortdate < matdate)
            or pd.notna(valueprc) and float(valueprc) < 100
        )
        if cond:
            return True
    return False


def _aggregate_bondization(bondization: dict[str, pd.DataFrame], facevalue: float | None, faceunit: str, matdate: Any) -> dict[str, Any]:
    offers = bondization.get("offers", pd.DataFrame())
    coupons = bondization.get("coupons", pd.DataFrame())
    amortizations = bondization.get("amortizations", pd.DataFrame())
    today = pd.Timestamp.today().normalize()
    is_rub = faceunit.upper() in {"RUB", "SUR", "RUR"}
    matdate_ts = pd.to_datetime(matdate, errors="coerce")

    next_offer_date = None
    offer_price_rub = None
    offer_price_native = None
    offer_price_pct = None
    offer_type = ""

    if not offers.empty:
        offers_local = offers.copy()
        offer_cols = offers_local.columns.tolist()
        offer_date_col = _find_column(offers_local, ["DATE", "OFFER", "PUT"])
        offer_type_col = _find_column(offers_local, ["OFFERTYPE", "TYPE", "KIND"])
        if offer_date_col:
            offers_local[offer_date_col] = pd.to_datetime(offers_local[offer_date_col], errors="coerce")
            offers_local = offers_local[offers_local[offer_date_col] >= today].sort_values(offer_date_col)
            if not offers_local.empty:
                row = offers_local.iloc[0]
                next_offer_date = row[offer_date_col]
                if offer_type_col:
                    offer_type = str(row.get(offer_type_col) or "")
                price = _value_from_col(offer_cols, row, "price")
                if price is not None:
                    offer_price_pct = price
                    if facevalue is not None:
                        offer_price_native = facevalue * price / 100.0
                        if is_rub:
                            offer_price_rub = offer_price_native

    next_coupon_date = None
    next_coupon_value = None
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
                picked_coupon, coupon_scale = _pick_money_per_bond(coupon_cols, row, facevalue, prefer_rub=False)
                next_coupon_value, _ = _sanitize_per_bond_value(picked_coupon, facevalue, coupon_scale)

    has_amortization = _is_real_amortization(amortizations, matdate_ts if pd.notna(matdate_ts) else None)
    next_amort_date = None
    next_amort_value = None
    if has_amortization and not amortizations.empty:
        amort_local = amortizations.copy()
        amort_cols = amort_local.columns.tolist()
        amort_date_col = _find_column(amort_local, ["AMORT", "DATE"])
        if amort_date_col:
            amort_local[amort_date_col] = pd.to_datetime(amort_local[amort_date_col], errors="coerce")
            amort_local = amort_local[amort_local[amort_date_col] >= today].sort_values(amort_date_col)
            if not amort_local.empty:
                row = amort_local.iloc[0]
                next_amort_date = row[amort_date_col]
                picked_amort, amort_scale = _pick_money_per_bond(amort_cols, row, facevalue, prefer_rub=False)
                next_amort_value, _ = _sanitize_per_bond_value(picked_amort, facevalue, amort_scale)

    return {
        "HAS_OFFER": bool(next_offer_date is not None),
        "NEXT_OFFER_DATE": next_offer_date,
        "OFFER_TYPE": offer_type,
        "OFFER_KIND": _normalize_offer_kind(offer_type),
        "OFFER_PRICE_PCT": offer_price_pct,
        "OFFER_PRICE_NATIVE": offer_price_native,
        "OFFER_PRICE_RUB": offer_price_rub,
        "HAS_AMORTIZATION": bool(has_amortization),
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
    steps = 0
    while f_low * f_high > 0 and steps < 20:
        high *= 2
        if high > 100:
            return None
        f_high = _xnpv(high, dates, cashflows)
        steps += 1
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


def _build_cashflows_for_ytm(row: pd.Series, bondization: dict[str, pd.DataFrame], fx_rate: float | None = None) -> tuple[list[pd.Timestamp], list[float]]:
    dirty_price = pd.to_numeric(row.get("DIRTY_PRICE_NATIVE"), errors="coerce")
    if pd.isna(dirty_price) or float(dirty_price) <= 0:
        return [], []
    mult = fx_rate if (fx_rate is not None and pd.notna(fx_rate) and fx_rate > 0) else 1.0

    today = pd.Timestamp.today().normalize()
    dates = [today]
    cashflows = [-float(dirty_price) * mult]

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
                coupon_value_raw, coupon_scale = _pick_money_per_bond(coupon_cols, c, facevalue_float, prefer_rub=False)
                coupon_value, _ = _sanitize_per_bond_value(coupon_value_raw, facevalue_float, coupon_scale)
                if coupon_value is not None and coupon_value > 0:
                    dates.append(c[date_col])
                    cashflows.append(float(coupon_value) * mult)

    amortizations = bondization.get("amortizations", pd.DataFrame()).copy()
    amort_sum = 0.0
    if not amortizations.empty:
        date_col = _find_column(amortizations, ["AMORT", "DATE"])
        if date_col:
            amort_cols = amortizations.columns.tolist()
            amortizations[date_col] = pd.to_datetime(amortizations[date_col], errors="coerce")
            amortizations = amortizations[amortizations[date_col] > today].sort_values(date_col)
            for _, a in amortizations.iterrows():
                amort_value_raw, amort_scale = _pick_money_per_bond(amort_cols, a, facevalue_float, prefer_rub=False)
                amort_value, _ = _sanitize_per_bond_value(amort_value_raw, facevalue_float, amort_scale)
                if amort_value is not None and amort_value > 0:
                    amort_sum += float(amort_value)
                    dates.append(a[date_col])
                    cashflows.append(float(amort_value) * mult)

    matdate = pd.to_datetime(row.get("MATDATE"), errors="coerce")
    if pd.notna(facevalue) and pd.notna(matdate) and matdate > today:
        remainder = float(facevalue) - amort_sum
        if remainder > 0.01:
            dates.append(matdate)
            cashflows.append(remainder * mult)

    pairs = sorted([(d, cf) for d, cf in zip(dates, cashflows) if pd.notna(d) and pd.notna(cf)], key=lambda x: x[0])
    if len(pairs) < 2:
        return [], []
    return [p[0] for p in pairs], [p[1] for p in pairs]


def _fx_symbol_for_faceunit(faceunit: str) -> str | None:
    mapping = {
        "USD": "USDRUB_TOM",
        "EUR": "EURRUB_TOM",
        "CNY": "CNYRUB_TOM",
        "RUB": None,
        "SUR": None,
        "RUR": None,
    }
    return mapping.get(faceunit.upper())


def _fetch_fx_rate(session: requests.Session, faceunit: str) -> float | None:
    symbol = _fx_symbol_for_faceunit(faceunit)
    if symbol is None:
        if faceunit.upper() in {"RUB", "SUR", "RUR"}:
            return 1.0
        return None
    cache_file = Path(f".cache/moex/fx/{symbol}.json")
    ttl = int(FX_CACHE_TTL_HOURS * 3600)
    url = f"https://iss.moex.com/iss/engines/currency/markets/selt/securities/{symbol}.json?iss.only=marketdata"
    payload = _cached_get_json(session, url, cache_file, ttl)
    md = _to_dataframe(payload, "marketdata")
    if md.empty:
        return None
    for col in ["LAST", "LCLOSE", "CLOSE"]:
        if col in md.columns:
            rate = pd.to_numeric(md[col].iloc[0], errors="coerce")
            if pd.notna(rate) and float(rate) > 0:
                return float(rate)
    return None


def _resolve_turnover_column(marketdata: pd.DataFrame) -> str | None:
    cols = {c.upper(): c for c in marketdata.columns}
    if "VOLRUR" in cols:
        return cols["VOLRUR"]
    for alt in ["VALRUR", "VALTODAY"]:
        if alt in cols:
            return cols[alt]
    return None


def build_report_dataframe(securities: pd.DataFrame, marketdata: pd.DataFrame, only_active: bool) -> tuple[pd.DataFrame, str | None, bool]:
    report = securities.merge(marketdata, on="SECID", how="left")
    if only_active and "STATUS" in report.columns:
        report = report[report["STATUS"] == "A"].copy()

    if "MATDATE" in report.columns:
        report["MATDATE"] = pd.to_datetime(report["MATDATE"], errors="coerce")
        min_maturity_date = pd.Timestamp.today().normalize() + pd.DateOffset(years=1)
        report = report[(report["MATDATE"].isna()) | (report["MATDATE"] >= min_maturity_date)].copy()

    turnover_source = _resolve_turnover_column(marketdata)
    turnover_output_col = None
    if "VOLRUR" in marketdata.columns:
        report["VOLRUR"] = pd.to_numeric(report.get("VOLRUR"), errors="coerce")
        turnover_output_col = "VOLRUR"
    elif turnover_source is not None:
        report["TURNOVER_RUB"] = pd.to_numeric(report.get(turnover_source), errors="coerce")
        turnover_output_col = "TURNOVER_RUB"

    is_rub = report.get("FACEUNIT", pd.Series(index=report.index)).astype(str).str.upper().isin(["RUB", "SUR", "RUR"])
    last = pd.to_numeric(report.get("LAST"), errors="coerce")
    waprice = pd.to_numeric(report.get("WAPRICE"), errors="coerce")
    facevalue = pd.to_numeric(report.get("FACEVALUE"), errors="coerce")

    has_accruedint_col = "ACCRUEDINT" in marketdata.columns
    if not has_accruedint_col:
        _warn_once("accruedint_missing", "ACCRUEDINT not present in marketdata response for board/market")
    report["ACCRUEDINT_NATIVE"] = pd.to_numeric(report.get("ACCRUEDINT"), errors="coerce")

    report["PRICE_NATIVE"] = (facevalue * last / 100).astype(float)
    report["PRICE_RUB_WA"] = pd.NA
    report.loc[is_rub, "PRICE_RUB_WA"] = (facevalue[is_rub] * waprice[is_rub] / 100).astype(float)

    report["DIRTY_PRICE_NATIVE"] = pd.to_numeric(report["PRICE_NATIVE"], errors="coerce") + pd.to_numeric(report["ACCRUEDINT_NATIVE"], errors="coerce")
    report.loc[report["ACCRUEDINT_NATIVE"].isna(), "DIRTY_PRICE_NATIVE"] = pd.to_numeric(report.loc[report["ACCRUEDINT_NATIVE"].isna(), "PRICE_NATIVE"], errors="coerce")

    report["ISSUER_NAME"] = report.get("ISSUER_NAME", pd.Series(index=report.index, dtype="object"))
    report["ISSUER_INN"] = report.get("ISSUER_INN", pd.Series(index=report.index, dtype="object"))
    report["ISSUER_NAME_FALLBACK"] = pd.NA

    for col in [
        "COUPON_FORMULA",
        "COUPON_FORMULA_SOURCE",
        "BOND_TYPE_RAW",
        "HAS_OFFER",
        "HAS_AMORTIZATION",
        "YTM_SIMPLE_OK",
        "NEXT_OFFER_DATE",
        "NEXT_AMORT_DATE",
        "NEXT_COUPON_DATE",
        "NEXT_AMORT_VALUE",
        "NEXT_COUPON_VALUE",
        "OFFER_PRICE_PCT",
        "OFFER_PRICE_NATIVE",
        "OFFER_PRICE_RUB",
        "YTM_SIMPLE",
        "YTM_SIMPLE_RUB_ASSUMING_FX_TODAY",
        "OFFER_TYPE",
        "OFFER_KIND",
    ]:
        if col not in report.columns:
            report[col] = pd.NA

    return report, turnover_output_col, has_accruedint_col


def _session_factory() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "moex-bonds-export-script/3.0"})
    return session


def enrich_report(report: pd.DataFrame, cache_ttl_seconds: int) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], list[dict[str, Any]]]:
    target = report.copy()
    secids = list(dict.fromkeys(target["SECID"].astype(str).tolist()))
    if ENRICH_SECIDS:
        selected = {sid.upper() for sid in ENRICH_SECIDS}
        secids = [sid for sid in secids if sid.upper() in selected]
    if ENRICH_LIMIT and ENRICH_LIMIT > 0:
        secids = secids[:ENRICH_LIMIT]

    cached_desc = _list_cached_ids(Path(".cache/moex/description"))
    cached_bond = _list_cached_ids(Path(".cache/moex/bondization"))
    log_step(f"Enrichment: бумаг={len(secids)}, кэш description={len(cached_desc)}, кэш bondization={len(cached_bond)}")

    emitter_misses: list[dict[str, Any]] = []
    detail_offers: list[pd.DataFrame] = []
    detail_coupons: list[pd.DataFrame] = []
    detail_amortizations: list[pd.DataFrame] = []

    semaphore = threading.BoundedSemaphore(MAX_WORKERS)

    def worker(secid: str) -> dict[str, Any]:
        with semaphore:
            session = _session_factory()
            try:
                row_res: dict[str, Any] = {"secid": secid, "updates": {}, "bondization": None, "emitter_miss": None}
                desc = fetch_security_description(session, secid, cache_ttl_seconds)
                emitter_id = desc.get("EMITTER_ID")
                if desc.get("BOND_TYPE_RAW"):
                    row_res["updates"]["BOND_TYPE_RAW"] = desc["BOND_TYPE_RAW"]
                if desc.get("COUPON_FORMULA"):
                    row_res["updates"]["COUPON_FORMULA"] = desc["COUPON_FORMULA"]
                    row_res["updates"]["COUPON_FORMULA_SOURCE"] = desc.get("COUPON_FORMULA_SOURCE")

                issuer_name = ""
                issuer_inn = ""
                if emitter_id is not None:
                    emitter = fetch_emitter_info(session, int(emitter_id), secid, cache_ttl_seconds)
                    issuer_name = str(emitter.get("ISSUER_NAME") or "").strip()
                    issuer_inn = str(emitter.get("ISSUER_INN") or "").strip()
                if issuer_name:
                    row_res["updates"]["ISSUER_NAME"] = issuer_name
                if issuer_inn:
                    row_res["updates"]["ISSUER_INN"] = issuer_inn

                if not issuer_name or not issuer_inn:
                    fallback = desc.get("NAME") or desc.get("LATNAME") or ""
                    if fallback:
                        row_res["updates"]["ISSUER_NAME_FALLBACK"] = fallback
                    if emitter_id is not None:
                        row_res["emitter_miss"] = {"emitter_id": int(emitter_id), "secid": secid}

                bondization = fetch_bondization(session, secid, cache_ttl_seconds)
                row_res["bondization"] = bondization
                time.sleep(REQUEST_PAUSE_SECONDS)
                return row_res
            finally:
                session.close()

    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for secid in secids:
            futures.append(executor.submit(worker, secid))

        ytm_ok = 0
        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as exc:
                log_step(f"WARNING: enrichment worker error: {exc}")
                continue

            secid = res["secid"]
            idx = target.index[target["SECID"] == secid]
            if idx.empty:
                continue
            i = idx[0]
            for key, value in res["updates"].items():
                if value is not None and value != "":
                    target.at[i, key] = value

            bondization = res.get("bondization") or {"coupons": pd.DataFrame(), "amortizations": pd.DataFrame(), "offers": pd.DataFrame()}
            facevalue = _as_float(target.at[i, "FACEVALUE"])
            faceunit = str(target.at[i, "FACEUNIT"] or "")
            matdate = target.at[i, "MATDATE"]
            agg = _aggregate_bondization(bondization, facevalue=facevalue, faceunit=faceunit, matdate=matdate)
            for key, value in agg.items():
                if value is not None and value != "":
                    target.at[i, key] = value
                elif key in {"HAS_OFFER", "HAS_AMORTIZATION"}:
                    target.at[i, key] = bool(value)

            if INCLUDE_DETAIL_SHEETS:
                for block_name, collector in [("offers", detail_offers), ("coupons", detail_coupons), ("amortizations", detail_amortizations)]:
                    block_df = bondization.get(block_name, pd.DataFrame())
                    if not block_df.empty:
                        copy_df = block_df.copy()
                        copy_df.insert(0, "SECID", secid)
                        collector.append(copy_df)

            if CALC_YTM_SIMPLE:
                dates, cashflows = _build_cashflows_for_ytm(target.loc[i], bondization)
                ytm_value = _xirr(dates, cashflows) if dates else None
                if ytm_value is not None and math.isfinite(ytm_value):
                    target.at[i, "YTM_SIMPLE"] = ytm_value
                    target.at[i, "YTM_SIMPLE_OK"] = True
                    ytm_ok += 1
                else:
                    target.at[i, "YTM_SIMPLE_OK"] = False

            if res.get("emitter_miss"):
                emitter_misses.append(res["emitter_miss"])

    detail_sheets = {
        "offers": pd.concat(detail_offers, ignore_index=True) if detail_offers else pd.DataFrame(),
        "coupons": pd.concat(detail_coupons, ignore_index=True) if detail_coupons else pd.DataFrame(),
        "amortizations": pd.concat(detail_amortizations, ignore_index=True) if detail_amortizations else pd.DataFrame(),
    }
    log_step(
        "cache summary: "
        f"desc h/m={CACHE_STATS['description_hits']}/{CACHE_STATS['description_misses']}, "
        f"bond h/m={CACHE_STATS['bondization_hits']}/{CACHE_STATS['bondization_misses']}, "
        f"emitter h/m={CACHE_STATS['emitter_hits']}/{CACHE_STATS['emitter_misses']}"
    )
    log_step(f"YTM_SIMPLE рассчитан для: {ytm_ok}")
    return target, detail_sheets, emitter_misses


def _apply_fx_columns(report: pd.DataFrame) -> pd.DataFrame:
    target = report.copy()
    target["FX_RATE_TO_RUB_TODAY"] = pd.NA
    if not FX_ENABLE:
        return target

    units = sorted(set(target["FACEUNIT"].fillna("").astype(str).str.upper().tolist()))
    rates: dict[str, float | None] = {}
    with _session_factory() as session:
        for unit in units:
            if not unit:
                continue
            rates[unit] = _fetch_fx_rate(session, unit)

    target["FX_RATE_TO_RUB_TODAY"] = target["FACEUNIT"].fillna("").astype(str).str.upper().map(rates)
    target.loc[target["FACEUNIT"].fillna("").astype(str).str.upper().isin(["RUB", "SUR", "RUR"]), "FX_RATE_TO_RUB_TODAY"] = 1.0

    fx = pd.to_numeric(target["FX_RATE_TO_RUB_TODAY"], errors="coerce")
    target["PRICE_RUB_TODAY"] = pd.to_numeric(target.get("PRICE_NATIVE"), errors="coerce") * fx
    target["ACCRUEDINT_RUB_TODAY"] = pd.to_numeric(target.get("ACCRUEDINT_NATIVE"), errors="coerce") * fx
    target["DIRTY_PRICE_RUB_TODAY"] = pd.to_numeric(target.get("DIRTY_PRICE_NATIVE"), errors="coerce") * fx
    target["NEXT_COUPON_VALUE_RUB_TODAY"] = pd.to_numeric(target.get("NEXT_COUPON_VALUE"), errors="coerce") * fx
    target["NEXT_AMORT_VALUE_RUB_TODAY"] = pd.to_numeric(target.get("NEXT_AMORT_VALUE"), errors="coerce") * fx

    target["YTM_SIMPLE_RUB_ASSUMING_FX_TODAY"] = pd.NA
    target.loc[target["YTM_SIMPLE_OK"].fillna(False).astype(bool), "YTM_SIMPLE_RUB_ASSUMING_FX_TODAY"] = pd.to_numeric(
        target.loc[target["YTM_SIMPLE_OK"].fillna(False).astype(bool), "YTM_SIMPLE"], errors="coerce"
    )
    return target


def _get_expected_columns(turnover_col: str | None) -> list[str]:
    cols = [
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
        "NUMTRADES",
        "ISSUER_NAME",
        "ISSUER_INN",
        "ISSUER_NAME_FALLBACK",
        "BOND_TYPE_RAW",
        "COUPON_FORMULA",
        "COUPON_FORMULA_SOURCE",
        "ACCRUEDINT_NATIVE",
        "ACCRUEDINT_RUB_TODAY",
        "PRICE_NATIVE",
        "PRICE_RUB_TODAY",
        "PRICE_RUB_WA",
        "DIRTY_PRICE_NATIVE",
        "DIRTY_PRICE_RUB_TODAY",
        "FX_RATE_TO_RUB_TODAY",
        "HAS_OFFER",
        "NEXT_OFFER_DATE",
        "OFFER_TYPE",
        "OFFER_KIND",
        "OFFER_PRICE_PCT",
        "OFFER_PRICE_NATIVE",
        "OFFER_PRICE_RUB",
        "HAS_AMORTIZATION",
        "NEXT_AMORT_DATE",
        "NEXT_AMORT_VALUE",
        "NEXT_AMORT_VALUE_RUB_TODAY",
        "NEXT_COUPON_DATE",
        "NEXT_COUPON_VALUE",
        "NEXT_COUPON_VALUE_RUB_TODAY",
        "YTM_SIMPLE",
        "YTM_SIMPLE_RUB_ASSUMING_FX_TODAY",
        "YTM_SIMPLE_OK",
    ]
    if turnover_col == "VOLRUR":
        cols.insert(11, "VOLRUR")
    elif turnover_col == "TURNOVER_RUB":
        cols.insert(11, "TURNOVER_RUB")
    return cols


def _ensure_expected_columns(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    target = df.copy()
    for col in expected_columns:
        if col not in target.columns:
            target[col] = pd.NA
    return target[expected_columns].sort_values(by=["MATDATE", "SECID"], na_position="last")


def _to_bool_mark(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if pd.isna(value):
        return ""
    return "✅" if bool(value) else "❌"


def build_column_descriptions(final_columns: list[str], turnover_col: str | None, volrur_all_nan: bool) -> pd.DataFrame:
    desc: dict[str, dict[str, str]] = {
        "SECID": {"meaning_ru": "Код бумаги", "units": "-", "source": "securities", "notes": "Идентификатор ISS"},
        "SHORTNAME": {"meaning_ru": "Короткое имя бумаги", "units": "-", "source": "securities", "notes": ""},
        "FACEVALUE": {"meaning_ru": "Номинал", "units": "faceunit", "source": "securities", "notes": ""},
        "FACEUNIT": {"meaning_ru": "Валюта номинала", "units": "ISO", "source": "securities", "notes": ""},
        "VOLRUR": {"meaning_ru": "Оборот в рублях", "units": "RUB", "source": "marketdata", "notes": "MOEX поле VOLRUR"},
        "TURNOVER_RUB": {"meaning_ru": "Оборот в рублях", "units": "RUB", "source": "marketdata", "notes": "Из альтернативы VALRUR/VALTODAY"},
        "VALUE": {"meaning_ru": "Оборот (native по MOEX)", "units": "var", "source": "marketdata", "notes": "Сырое поле VALUE"},
        "NUMTRADES": {"meaning_ru": "Количество сделок", "units": "шт", "source": "marketdata", "notes": ""},
        "ISSUER_NAME": {"meaning_ru": "Название эмитента", "units": "-", "source": "emitter_lookup", "notes": "Может быть пустым при отсутствии данных по эмитенту в ISS"},
        "ISSUER_INN": {"meaning_ru": "ИНН эмитента", "units": "-", "source": "emitter_lookup", "notes": "Может быть пустым при отсутствии данных по эмитенту в ISS"},
        "ISSUER_NAME_FALLBACK": {"meaning_ru": "Запасной текст вместо эмитента", "units": "-", "source": "description", "notes": "Берётся из NAME/LATNAME, не равен эмитенту"},
        "BOND_TYPE_RAW": {"meaning_ru": "Тип бумаги как в description", "units": "-", "source": "description", "notes": "Сырой текст MOEX"},
        "COUPON_FORMULA": {"meaning_ru": "Формула купона", "units": "-", "source": "description", "notes": ""},
        "COUPON_FORMULA_SOURCE": {"meaning_ru": "Источник формулы", "units": "-", "source": "description", "notes": ""},
        "ACCRUEDINT_NATIVE": {"meaning_ru": "НКД в валюте номинала", "units": "faceunit", "source": "marketdata", "notes": "Только marketdata.ACCRUEDINT"},
        "ACCRUEDINT_RUB_TODAY": {"meaning_ru": "НКД в рублях по FX на сегодня", "units": "RUB", "source": "calculated", "notes": "Для RUB = native"},
        "PRICE_NATIVE": {"meaning_ru": "Чистая цена в валюте номинала", "units": "faceunit", "source": "calculated", "notes": "FACEVALUE*LAST/100"},
        "PRICE_RUB_TODAY": {"meaning_ru": "Чистая цена в RUB по FX на сегодня", "units": "RUB", "source": "calculated", "notes": ""},
        "PRICE_RUB_WA": {"meaning_ru": "Цена по WAPRICE (для RUB)", "units": "RUB", "source": "calculated", "notes": ""},
        "DIRTY_PRICE_NATIVE": {"meaning_ru": "Грязная цена в валюте номинала", "units": "faceunit", "source": "calculated", "notes": "PRICE_NATIVE + ACCRUEDINT_NATIVE"},
        "DIRTY_PRICE_RUB_TODAY": {"meaning_ru": "Грязная цена в RUB по FX на сегодня", "units": "RUB", "source": "calculated", "notes": ""},
        "FX_RATE_TO_RUB_TODAY": {"meaning_ru": "Курс к RUB на сегодня", "units": "RUB/faceunit", "source": "currency marketdata", "notes": "USDRUB_TOM/EURRUB_TOM/CNYRUB_TOM"},
        "HAS_OFFER": {"meaning_ru": "Есть будущая оферта", "units": "✅/❌", "source": "bondization", "notes": ""},
        "NEXT_OFFER_DATE": {"meaning_ru": "Дата ближайшей оферты", "units": "date", "source": "bondization", "notes": ""},
        "OFFER_TYPE": {"meaning_ru": "Тип оферты из MOEX", "units": "-", "source": "bondization", "notes": ""},
        "OFFER_KIND": {"meaning_ru": "Нормализованный тип оферты", "units": "PUT/CALL/UNKNOWN", "source": "bondization", "notes": ""},
        "OFFER_PRICE_PCT": {"meaning_ru": "Цена оферты в %", "units": "% номинала", "source": "bondization", "notes": ""},
        "OFFER_PRICE_NATIVE": {"meaning_ru": "Цена оферты в валюте номинала", "units": "faceunit", "source": "calculated", "notes": ""},
        "OFFER_PRICE_RUB": {"meaning_ru": "Цена оферты в рублях", "units": "RUB", "source": "calculated", "notes": "Для RUB-номинала"},
        "HAS_AMORTIZATION": {"meaning_ru": "Есть амортизация", "units": "✅/❌", "source": "bondization", "notes": "maturity-only не считается"},
        "NEXT_AMORT_DATE": {"meaning_ru": "Дата ближайшей амортизации", "units": "date", "source": "bondization", "notes": "Пусто при maturity-only"},
        "NEXT_AMORT_VALUE": {"meaning_ru": "Ближайшая амортизация", "units": "faceunit", "source": "bondization", "notes": ""},
        "NEXT_AMORT_VALUE_RUB_TODAY": {"meaning_ru": "Ближайшая амортизация в RUB", "units": "RUB", "source": "calculated", "notes": "По FX на сегодня"},
        "NEXT_COUPON_DATE": {"meaning_ru": "Дата ближайшего купона", "units": "date", "source": "bondization", "notes": ""},
        "NEXT_COUPON_VALUE": {"meaning_ru": "Ближайший купон", "units": "faceunit", "source": "bondization", "notes": ""},
        "NEXT_COUPON_VALUE_RUB_TODAY": {"meaning_ru": "Ближайший купон в RUB", "units": "RUB", "source": "calculated", "notes": "По FX на сегодня"},
        "YTM_SIMPLE": {"meaning_ru": "Упрощённая доходность в валюте номинала", "units": "% годовых", "source": "calculated", "notes": "XIRR в native"},
        "YTM_SIMPLE_RUB_ASSUMING_FX_TODAY": {"meaning_ru": "YTM в RUB при фиксированном FX today", "units": "% годовых", "source": "calculated", "notes": "Без прогноза FX"},
        "YTM_SIMPLE_OK": {"meaning_ru": "Флаг успешного расчёта YTM", "units": "✅/❌", "source": "calculated", "notes": ""},
        "COUPONVALUE": {"meaning_ru": "Размер купона", "units": "faceunit", "source": "securities", "notes": ""},
        "COUPONPERIOD": {"meaning_ru": "Период купона", "units": "дней", "source": "securities", "notes": ""},
        "MATDATE": {"meaning_ru": "Дата погашения", "units": "date", "source": "securities", "notes": ""},
        "LAST": {"meaning_ru": "Последняя цена", "units": "% номинала", "source": "marketdata", "notes": ""},
        "WAPRICE": {"meaning_ru": "Средневзвешенная цена", "units": "% номинала", "source": "marketdata", "notes": ""},
        "YIELD": {"meaning_ru": "MOEX yield", "units": "%", "source": "marketdata", "notes": "Сырое поле"},
    }

    if turnover_col == "VOLRUR" and volrur_all_nan:
        desc["VOLRUR"]["notes"] = "MOEX не отдаёт для выбранной board/режима; поле может быть пустым"

    rows = []
    for col in final_columns:
        item = desc.get(col)
        if item is None:
            item = {"meaning_ru": "TODO", "units": "", "source": "unknown", "notes": "auto-added placeholder"}
        rows.append({"column": col, **item})
    return pd.DataFrame(rows)


def save_to_excel(
    df: pd.DataFrame,
    output_path: Path,
    detail_sheets: dict[str, pd.DataFrame],
    emitter_misses: list[dict[str, Any]],
    turnover_col: str | None,
    volrur_all_nan: bool,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    display_df = df.copy()
    for col in BOOL_COLUMNS:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(_to_bool_mark)

    with pd.ExcelWriter(output_path, engine="openpyxl", datetime_format="yyyy-mm-dd") as writer:
        display_df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        worksheet = writer.sheets["MOEX_BONDS"]
        worksheet.freeze_panes = "A2"
        worksheet.sheet_view.zoomScale = 110

        for idx, column_name in enumerate(display_df.columns, start=1):
            letter = get_column_letter(idx)
            header_cell = worksheet.cell(row=1, column=idx)
            header_cell.fill = HEADER_FILL
            header_cell.font = HEADER_FONT
            header_cell.alignment = Alignment(horizontal="center", vertical="center")
            header_cell.border = BORDER
            worksheet.column_dimensions[letter].width = COLUMN_WIDTHS.get(column_name, 14)

        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for cell in row:
                column_name = display_df.columns[cell.column - 1]
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
                if not sheet_df.empty:
                    sheet_df.to_excel(writer, index=False, sheet_name=sheet_name[:31])

        misses_df = pd.DataFrame(emitter_misses).drop_duplicates() if emitter_misses else pd.DataFrame(columns=["emitter_id", "secid"])
        misses_df.to_excel(writer, index=False, sheet_name="EMITTER_LOOKUP_MISSES")

        build_column_descriptions(list(display_df.columns), turnover_col, volrur_all_nan).to_excel(
            writer, index=False, sheet_name="COLUMN_DESCRIPTIONS"
        )


def main() -> None:
    started_at = time.perf_counter()
    cache_ttl_seconds = int(CACHE_TTL_HOURS * 3600)

    log_step("Запускаю выгрузку облигаций MOEX...")
    with _session_factory() as session:
        securities, marketdata, source = fetch_moex_bonds(session=session, cache_ttl_seconds=cache_ttl_seconds)
        log_step(f"Базовые данные получены ({source}): securities={len(securities)}, marketdata={len(marketdata)}")
        report, turnover_col, _ = build_report_dataframe(securities=securities, marketdata=marketdata, only_active=not INCLUDE_INACTIVE)
        log_step(f"Базовая таблица сформирована: {len(report)} бумаг")

    detail_sheets = {"offers": pd.DataFrame(), "coupons": pd.DataFrame(), "amortizations": pd.DataFrame()}
    emitter_misses: list[dict[str, Any]] = []
    if ENRICH_ENABLE:
        report, detail_sheets, emitter_misses = enrich_report(report, cache_ttl_seconds)

    report = _apply_fx_columns(report)
    expected_columns = _get_expected_columns(turnover_col)
    report = _ensure_expected_columns(report, expected_columns)

    volrur_all_nan = bool("VOLRUR" in report.columns and pd.to_numeric(report["VOLRUR"], errors="coerce").isna().all())

    log_step(f"Сохраняю Excel-файл: {OUTPUT_XLSX}")
    save_to_excel(report, Path(OUTPUT_XLSX), detail_sheets, emitter_misses, turnover_col, volrur_all_nan)
    log_step(f"Готово. Сохранено строк: {len(report)}")
    log_step(f"Файл: {Path(OUTPUT_XLSX).resolve()}")
    log_step(f"Общее время выполнения: {time.perf_counter() - started_at:.2f} сек.")


if __name__ == "__main__":
    main()
