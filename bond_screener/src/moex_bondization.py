from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

from .cache import HTTPCache
from .checkpoint import CheckpointStore
from .utils import normalize_decimal, parse_date

logger = logging.getLogger(__name__)


@dataclass
class BondizationResult:
    coupons: pd.DataFrame
    amortizations: pd.DataFrame
    amort_start: pd.DataFrame


def _fetch_single(secid: str, ttl_hours: int, cache: HTTPCache) -> dict[str, Any]:
    url = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json"
    cache_key = f"bondization::{secid}"
    payload = cache.get_json(cache_key, ttl_hours)
    if payload is None:
        response = requests.get(url, params={"iss.meta": "off", "iss.only": "amortizations,coupons"}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        cache.set_json(cache_key, payload)
    return payload


def _parse_block(payload: dict[str, Any], key: str) -> pd.DataFrame:
    if key not in payload:
        return pd.DataFrame()
    block = payload.get(key, {})
    cols = block.get("columns", [])
    data = block.get("data", [])
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(data, columns=cols)


def fetch_bondization(
    universe: pd.DataFrame,
    ttl_hours: int,
    cache: HTTPCache,
    checkpoints: CheckpointStore,
    concurrency: int,
    today: date,
) -> BondizationResult:
    identifiers = []
    for _, row in universe.iterrows():
        secid = row.get("secid") or row.get("isin")
        isin = row.get("isin")
        if secid:
            identifiers.append((str(secid), str(isin) if isin else None))

    coupons_frames: list[pd.DataFrame] = []
    amort_frames: list[pd.DataFrame] = []

    futures = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for secid, isin in identifiers:
            futures[executor.submit(_fetch_single, secid, ttl_hours, cache)] = (secid, isin)

        for future in tqdm(as_completed(futures), total=len(futures), desc="MOEX bondization"):
            secid, isin = futures[future]
            try:
                payload = future.result()
                coupons_df = _parse_block(payload, "coupons")
                amort_df = _parse_block(payload, "amortizations")

                if not coupons_df.empty:
                    coupons_df["secid"] = secid
                    coupons_df["isin"] = isin
                    coupons_frames.append(coupons_df)
                if not amort_df.empty:
                    amort_df["secid"] = secid
                    amort_df["isin"] = isin
                    amort_frames.append(amort_df)

                checkpoints.mark_done(secid, {"ok": True})
            except Exception as exc:
                logger.warning("Failed bondization for %s: %s", secid, exc)
                checkpoints.mark_done(secid, {"ok": False, "error": str(exc)})

    coupons = pd.concat(coupons_frames, ignore_index=True) if coupons_frames else pd.DataFrame()
    amortizations = pd.concat(amort_frames, ignore_index=True) if amort_frames else pd.DataFrame()

    if not coupons.empty:
        coupons["coupondate"] = coupons.get("coupondate", coupons.get("COUPONDATE")).map(parse_date)
        coupons["value"] = coupons.get("value", coupons.get("VALUE")).map(normalize_decimal)

    if not amortizations.empty:
        amortizations["amortdate"] = amortizations.get("amortdate", amortizations.get("AMORTDATE")).map(parse_date)
        amortizations["value"] = amortizations.get("value", amortizations.get("VALUE")).map(normalize_decimal)

    amort_start = pd.DataFrame(columns=["isin", "secid", "amort_start_date", "days_to_amort", "has_amortization"])
    if not amortizations.empty:
        positive = amortizations[amortizations["value"].fillna(0) > 0].copy()
        if not positive.empty:
            grouped = (
                positive.groupby(["isin", "secid"], dropna=False)["amortdate"]
                .min()
                .reset_index()
                .rename(columns={"amortdate": "amort_start_date"})
            )
            grouped["days_to_amort"] = grouped["amort_start_date"].map(lambda d: (d - today).days if d else None)
            grouped["has_amortization"] = True
            amort_start = grouped

    return BondizationResult(coupons=coupons, amortizations=amortizations, amort_start=amort_start)
