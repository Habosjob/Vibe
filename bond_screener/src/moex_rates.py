from __future__ import annotations

import io
import logging
from dataclasses import dataclass

import pandas as pd
import requests

from .cache import HTTPCache
from .utils import normalize_decimal, parse_date, to_upper

logger = logging.getLogger(__name__)


@dataclass
class MoexRatesResult:
    raw: pd.DataFrame
    norm: pd.DataFrame


def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    lookup = {col.upper(): col for col in df.columns}
    for c in candidates:
        if c.upper() in lookup:
            return lookup[c.upper()]
    return None


def fetch_moex_rates(url: str, ttl_hours: int, cache: HTTPCache) -> MoexRatesResult:
    cache_key = f"moex_rates::{url}"
    payload = cache.get_bytes(cache_key, ttl_hours)
    if payload is None:
        logger.info("Downloading MOEX rates.csv")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        payload = response.content
        cache.set_bytes(cache_key, payload)
    else:
        logger.info("Using cached MOEX rates.csv")

    raw = pd.read_csv(io.BytesIO(payload), encoding="cp1251", sep=";", dtype=str)

    isin_col = _find_col(raw, "ISIN", "ISINCODE")
    secid_col = _find_col(raw, "SECID")
    matdate_col = _find_col(raw, "MATDATE", "REDEMPTIONDATE")
    face_col = _find_col(raw, "FACEVALUE", "FACEUNIT", "LOTVALUE")
    price_col = _find_col(raw, "LAST", "MARKETPRICE", "PRICE")
    nkd_col = _find_col(raw, "ACCRUEDINT", "NKD", "ACCRINT")

    norm = pd.DataFrame()
    norm["isin"] = raw[isin_col].map(to_upper) if isin_col else None
    norm["secid"] = raw[secid_col].map(to_upper) if secid_col else None
    norm["matdate"] = raw[matdate_col].map(parse_date) if matdate_col else None
    norm["facevalue"] = raw[face_col].map(normalize_decimal) if face_col else None
    norm["price"] = raw[price_col].map(normalize_decimal) if price_col else None
    norm["nkd"] = raw[nkd_col].map(normalize_decimal) if nkd_col else None

    norm = norm.dropna(subset=["isin"], how="all")
    norm = norm.drop_duplicates(subset=["isin", "secid"], keep="first")

    return MoexRatesResult(raw=raw, norm=norm)
