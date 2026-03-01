from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict

import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

from .checkpoint import CheckpointStore
from .utils import parse_date, to_float


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def _fetch_bondization(secid: str) -> dict:
    url = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json"
    params = {"iss.meta": "off", "iss.only": "amortizations,coupons"}
    with httpx.Client(timeout=30) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def load_bondization(universe: pd.DataFrame, config: Dict, checkpoints: CheckpointStore, ttl_hours: float, logger):
    now = datetime.utcnow()
    max_workers = config["moex"]["concurrency"]
    coupons_rows, amort_rows = [], []

    def should_skip(secid: str) -> bool:
        st = checkpoints.get(secid)
        if not st or st.get("status") != "done":
            return False
        ts = datetime.fromisoformat(st["updated_at"])
        age_h = (now - ts).total_seconds() / 3600
        return age_h <= ttl_hours

    secids = [str(s) for s in universe["secid"].dropna().astype(str).unique()]
    pending = [s for s in secids if not should_skip(s)]

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_fetch_bondization, secid): secid for secid in pending}
        for fut in tqdm(as_completed(future_map), total=len(future_map), desc="MOEX bondization"):
            secid = future_map[fut]
            try:
                payload = fut.result()
                fetched = datetime.utcnow().isoformat()
                for row in payload.get("coupons", {}).get("data", []):
                    cols = payload["coupons"]["columns"]
                    obj = dict(zip(cols, row))
                    coupons_rows.append(
                        {
                            "secid": secid,
                            "coupondate": parse_date(obj.get("coupondate")),
                            "value": to_float(obj.get("value")),
                            "rate": to_float(obj.get("valueprc")),
                            "currencyid": obj.get("currencyid"),
                            "fetched_at": fetched,
                        }
                    )
                for row in payload.get("amortizations", {}).get("data", []):
                    cols = payload["amortizations"]["columns"]
                    obj = dict(zip(cols, row))
                    amort_rows.append(
                        {
                            "secid": secid,
                            "amortdate": parse_date(obj.get("amortdate")),
                            "value": to_float(obj.get("value")),
                            "currencyid": obj.get("currencyid"),
                            "fetched_at": fetched,
                        }
                    )
                checkpoints.set(secid, {"status": "done", "updated_at": fetched})
            except Exception as exc:
                logger.warning("bondization failed for %s: %s", secid, exc)
                checkpoints.set(secid, {"status": "failed", "updated_at": datetime.utcnow().isoformat()})

    coupons_df = pd.DataFrame(
        coupons_rows,
        columns=["secid", "coupondate", "value", "rate", "currencyid", "fetched_at"],
    )
    amort_df = pd.DataFrame(
        amort_rows,
        columns=["secid", "amortdate", "value", "currencyid", "fetched_at"],
    )
    return coupons_df, amort_df


def build_amort_start(universe: pd.DataFrame, amort_df: pd.DataFrame) -> pd.DataFrame:
    today = datetime.utcnow().date()
    if amort_df.empty:
        out = universe[["isin", "secid"]].copy()
        out["amort_start_date_ddmmyyyy"] = None
        out["amort_start_date_iso"] = None
        out["days_to_amort"] = None
        out["has_amortization"] = False
        return out
    pos = amort_df[(amort_df["value"].fillna(0) > 0) & amort_df["amortdate"].notna()]
    starts = pos.groupby("secid", as_index=False)["amortdate"].min().rename(columns={"amortdate": "amort_start"})
    out = universe[["isin", "secid"]].drop_duplicates().merge(starts, on="secid", how="left")
    out["amort_start_date_ddmmyyyy"] = out["amort_start"].apply(lambda d: d.strftime("%d.%m.%Y") if pd.notna(d) else None)
    out["amort_start_date_iso"] = out["amort_start"].astype(str).where(out["amort_start"].notna(), None)
    out["days_to_amort"] = out["amort_start"].apply(lambda d: (d - today).days if pd.notna(d) else None)
    out["has_amortization"] = out["amort_start"].notna()
    return out.drop(columns=["amort_start"])
