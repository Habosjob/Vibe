from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

import httpx
import pandas as pd
from tqdm.asyncio import tqdm

from .checkpoint import CheckpointStore
from .utils import parse_date, to_float
from .writer_queue import AsyncWriter


async def fetch_bondization_bulk(config: dict, writer: AsyncWriter, checkpoints: CheckpointStore, logger) -> dict[str, int]:
    cfg = config["moex"]["bondization_bulk"]
    if not cfg.get("enabled", True):
        return {"coupons": 0, "amortizations": 0}

    cp = checkpoints.get("bulk")
    start = int(cp.get("last_start", 0))
    page_size = int(cfg["page_size"])
    max_pages = int(cfg["max_pages"])
    today = date.today()
    from_d = (today - timedelta(days=int(cfg["days_back"]))).isoformat()
    till_d = (today + timedelta(days=int(cfg["days_forward"]))).isoformat()

    coupons_total = 0
    amort_total = 0
    first_secid_seen = None

    async with httpx.AsyncClient(timeout=45) as client:
        pbar = tqdm(total=max_pages, desc="MOEX bondization bulk")
        for _ in range(max_pages):
            params = {
                "from": from_d,
                "till": till_d,
                "start": start,
                "limit": page_size,
                "iss.meta": "off",
                "iss.only": "amortizations,coupons",
            }
            resp = await client.get("https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization.json", params=params)
            resp.raise_for_status()
            payload = resp.json()
            fetched = datetime.utcnow().isoformat()

            coupons_blk = payload.get("coupons", {})
            amort_blk = payload.get("amortizations", {})
            cc = coupons_blk.get("columns", [])
            ac = amort_blk.get("columns", [])
            coupon_rows = []
            amort_rows = []

            for row in coupons_blk.get("data", []):
                obj = dict(zip(cc, row))
                coupon_rows.append(
                    {
                        "secid": str(obj.get("secid") or "").upper() or None,
                        "coupondate": parse_date(obj.get("coupondate")).isoformat() if parse_date(obj.get("coupondate")) else None,
                        "value": to_float(obj.get("value")),
                        "rate": to_float(obj.get("valueprc")),
                        "currencyid": obj.get("currencyid"),
                        "fetched_at": fetched,
                    }
                )
            for row in amort_blk.get("data", []):
                obj = dict(zip(ac, row))
                amort_rows.append(
                    {
                        "secid": str(obj.get("secid") or "").upper() or None,
                        "amortdate": parse_date(obj.get("amortdate")).isoformat() if parse_date(obj.get("amortdate")) else None,
                        "value": to_float(obj.get("value")),
                        "currencyid": obj.get("currencyid"),
                        "fetched_at": fetched,
                    }
                )

            if coupon_rows:
                await writer.put("moex_coupons", coupon_rows)
                coupons_total += len(coupon_rows)
            if amort_rows:
                await writer.put("moex_amortizations", amort_rows)
                amort_total += len(amort_rows)

            page_count = max(len(coupon_rows), len(amort_rows))
            first_page_secid = (coupon_rows[0].get("secid") if coupon_rows else (amort_rows[0].get("secid") if amort_rows else None))
            if first_secid_seen and first_page_secid and first_page_secid == first_secid_seen:
                logger.warning("MOEX anti-loop triggered at start=%s", start)
                break
            first_secid_seen = first_page_secid

            checkpoints.set("bulk", {"last_start": start, "last_ok_at": fetched, "last_page_count": page_count})
            start += page_size
            pbar.update(1)

            cursor = payload.get("cursor", {})
            total: Any = None
            if isinstance(cursor, dict):
                cd = cursor.get("data") or []
                ccursor = cursor.get("columns") or []
                if cd and ccursor and "TOTAL" in ccursor:
                    total = cd[0][ccursor.index("TOTAL")]
            if total is not None and start >= int(total):
                break
            if page_count < page_size:
                break
        pbar.close()

    return {"coupons": coupons_total, "amortizations": amort_total}


def build_amort_agg(amort_df: pd.DataFrame) -> pd.DataFrame:
    if amort_df.empty:
        return pd.DataFrame(columns=["secid", "first_amort_date", "has_amortization", "fetched_at"])
    tmp = amort_df.copy()
    tmp["amortdate"] = pd.to_datetime(tmp["amortdate"], errors="coerce").dt.date
    pos = tmp[(tmp["value"].fillna(0) > 0) & tmp["amortdate"].notna()]
    if pos.empty:
        return pd.DataFrame(columns=["secid", "first_amort_date", "has_amortization", "fetched_at"])
    agg = pos.groupby("secid", as_index=False)["amortdate"].min().rename(columns={"amortdate": "first_amort_date"})
    agg["has_amortization"] = 1
    agg["fetched_at"] = datetime.utcnow().isoformat()
    agg["first_amort_date"] = agg["first_amort_date"].astype(str)
    return agg
