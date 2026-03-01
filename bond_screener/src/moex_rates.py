from __future__ import annotations

from io import StringIO
from typing import Dict

import pandas as pd

from .cache import HTTPCache
from .utils import normalize_isin, parse_date, to_float


NUMBER_HINTS = ("price", "yield", "value", "coupon", "accrued", "duration", "vol", "spread", "nkd", "face")


def _normalize_numbers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if any(h in c.lower() for h in NUMBER_HINTS):
            out[c] = out[c].map(to_float)
    return out


def _build_column_map(df: pd.DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for c in df.columns:
        norm = str(c).strip().upper()
        if norm and norm not in mapping:
            mapping[norm] = c
    return mapping


def _detect_moex_header_row(text: str, max_rows: int = 50) -> int:
    lines = text.splitlines()
    for idx, line in enumerate(lines[:max_rows]):
        upper_line = line.upper()
        if "SECID" in upper_line and ("ISIN" in upper_line or ";SECID;" in f";{upper_line};"):
            return idx
    for idx, line in enumerate(lines[:max_rows]):
        if "SECID" in line.upper():
            return idx
    return 0


def load_moex_rates(config: Dict, cache: HTTPCache, logger=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    url = config["sources"]["moex_rates_csv_url"]
    ttl = config["ttl_hours"]["moex_rates"]
    raw_bytes = cache.fetch("moex_rates_csv", url, ttl_hours=ttl)
    text = raw_bytes.decode("cp1251", errors="ignore")

    header_row_idx = _detect_moex_header_row(text)
    raw = pd.read_csv(
        StringIO(text),
        sep=";",
        encoding="cp1251",
        skiprows=header_row_idx,
        header=0,
        skip_blank_lines=True,
        dtype=str,
    )
    norm = _normalize_numbers(raw)
    col_map = _build_column_map(norm)

    def pick(*candidates: str) -> str | None:
        for c in candidates:
            if c in col_map:
                return col_map[c]
        return None

    secid_col = pick("SECID", "SEC_CODE")
    if not secid_col:
        raise ValueError(
            f"MOEX rates CSV parsed without SECID column. header_row_idx={header_row_idx}, first_columns={list(norm.columns)[:30]}"
        )

    isin_col = pick("ISIN", "SECID_ISIN")
    name_col = pick("SHORTNAME", "NAME", "SECNAME")
    mat_col = pick("MATDATE", "MAT_DATE", "MATURITY", "MATURITYDATE")
    face_col = pick("FACEVALUE", "FACEVALUE1", "NOMINAL")
    faceunit_col = pick("FACEUNIT", "CURRENCYID", "CURRENCY")
    last_col = pick("LAST")
    moex_price_col = last_col or pick("PRICE", "MARKETPRICE", "LEGALCLOSEPRICE")
    accrued_col = pick("ACCRUEDINT")

    norm["norm_secid"] = norm[secid_col].map(lambda v: str(v).strip().upper() if pd.notna(v) and str(v).strip() else None)
    norm["norm_isin"] = norm[isin_col].map(normalize_isin) if isin_col else None
    norm["norm_name"] = norm[name_col].map(lambda v: str(v).strip() if pd.notna(v) and str(v).strip() else None) if name_col else None
    norm["norm_maturity_date"] = norm[mat_col].map(parse_date) if mat_col else None
    norm["norm_facevalue"] = norm[face_col].map(to_float) if face_col else None
    norm["norm_faceunit"] = (
        norm[faceunit_col].map(lambda v: str(v).strip().upper() if pd.notna(v) and str(v).strip() else None) if faceunit_col else None
    )
    norm["norm_currency"] = norm["norm_faceunit"]
    norm["moex_price"] = norm[moex_price_col].map(to_float) if moex_price_col else None
    norm["moex_nkd"] = norm[accrued_col].map(to_float) if accrued_col else None

    name_stat_col = name_col if name_col else "norm_name"
    if logger:
        logger.info("MOEX rates detected header_row=%s", header_row_idx)
        logger.info("MOEX rates columns(first30)=%s", list(norm.columns)[:30])
        logger.info(
            "MOEX rates notnull: SECID=%s ISIN=%s NAME=%s",
            int(norm[secid_col].notna().sum()),
            int(norm[isin_col].notna().sum()) if isin_col else 0,
            int(norm[name_stat_col].notna().sum()) if name_stat_col in norm.columns else 0,
        )

    return raw, norm
