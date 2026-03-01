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


def load_moex_rates(config: Dict, cache: HTTPCache) -> tuple[pd.DataFrame, pd.DataFrame]:
    url = config["sources"]["moex_rates_csv_url"]
    ttl = config["ttl_hours"]["moex_rates"]
    raw_bytes = cache.fetch("moex_rates_csv", url, ttl_hours=ttl)
    text = raw_bytes.decode("cp1251", errors="ignore")
    raw = pd.read_csv(StringIO(text), sep=";", dtype=str)
    norm = _normalize_numbers(raw)

    def pick(cols):
        for c in cols:
            if c in norm.columns:
                return c
        return None

    isin_col = pick(["ISIN", "SECID_ISIN", "isin"])
    secid_col = pick(["SECID", "SEC_CODE", "secid"])
    name_col = pick(["SHORTNAME", "SECNAME", "NAME", "shortname"])
    mat_col = pick(["MATDATE", "MAT_DATE", "maturity", "MATURITYDATE"])
    face_col = pick(["FACEVALUE", "FACEVALUE1", "NOMINAL", "facevalue"])
    ccy_col = pick(["FACEUNIT", "CURRENCYID", "CURRENCY", "faceunit"])

    norm["norm_isin"] = norm[isin_col].map(normalize_isin) if isin_col else None
    norm["norm_secid"] = norm[secid_col].astype(str).str.strip().str.upper() if secid_col else None
    norm["norm_name"] = norm[name_col].astype(str).str.strip() if name_col else None
    norm["norm_maturity_date"] = norm[mat_col].map(parse_date) if mat_col else None
    norm["norm_facevalue"] = norm[face_col].map(to_float) if face_col else None
    norm["norm_currency"] = norm[ccy_col].astype(str).str.strip().str.upper() if ccy_col else None
    norm["moex_price"] = norm.get("LAST")
    if "moex_price" not in norm.columns:
        for c in ["PRICE", "MARKETPRICE", "LEGALCLOSEPRICE"]:
            if c in norm.columns:
                norm["moex_price"] = norm[c].map(to_float)
                break
    norm["moex_nkd"] = norm["ACCRUEDINT"].map(to_float) if "ACCRUEDINT" in norm.columns else None
    return raw, norm
