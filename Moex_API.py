# Moex_API.py
from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from logs import setup_logger, ensure_logs_dir, Timer
from SQL import SQLiteCache, RequestLogRow


BASE = "https://iss.moex.com/iss"


def today_str() -> str:
    return date.today().isoformat()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class HttpResult:
    status: Optional[int]
    elapsed_ms: Optional[int]
    size_bytes: Optional[int]
    text: Optional[str]
    url: str
    params: dict
    error: Optional[str] = None
    headers: Optional[dict] = None


class IssClient:
    """
    Клиент ISS с retries/backoff на 429/5xx.
    Пишет requests_log в SQLite на КАЖДУЮ попытку.
    """

    def __init__(
        self,
        cache: SQLiteCache,
        logger,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_base: float = 0.8,
    ):
        self.cache = cache
        self.logger = logger
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Vibe-MOEX-ISS/1.2",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    def _sleep_for_retry(self, attempt: int, resp: Optional[requests.Response]) -> None:
        # Respect Retry-After for 429 if present
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    sec = float(ra)
                    time.sleep(min(60.0, max(0.0, sec)))
                    return
                except Exception:
                    pass
        # exponential backoff + jitter
        base = self.backoff_base * (2 ** (attempt - 1))
        jitter = random.random() * 0.25 * base
        time.sleep(min(30.0, base + jitter))

    def get(self, path: str, params: Optional[dict] = None) -> HttpResult:
        params = params or {}
        url = f"{BASE}{path}"

        retry_statuses = {429, 500, 502, 503, 504}
        last_err: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            t0 = time.perf_counter()
            resp: Optional[requests.Response] = None
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                status = int(resp.status_code)
                headers = dict(resp.headers)
                text = resp.text if resp.text is not None else ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                size_bytes = len((text or "").encode("utf-8", errors="ignore"))

                # requests_log per attempt
                self.cache.log_request(
                    RequestLogRow(
                        url=str(resp.url),
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=status,
                        elapsed_ms=elapsed_ms,
                        size_bytes=size_bytes,
                        created_utc=_utc_iso(),
                        error=None,
                    )
                )

                if status in retry_statuses:
                    self.logger.warning(f"HTTP {status} retryable | attempt {attempt}/{self.max_retries} | {resp.url}")
                    if attempt < self.max_retries:
                        self._sleep_for_retry(attempt, resp)
                        continue

                return HttpResult(
                    status=status,
                    elapsed_ms=elapsed_ms,
                    size_bytes=size_bytes,
                    text=text,
                    url=str(resp.url),
                    params=params,
                    error=None,
                    headers=headers,
                )

            except Exception as e:
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                last_err = repr(e)

                self.cache.log_request(
                    RequestLogRow(
                        url=url,
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=None,
                        elapsed_ms=elapsed_ms,
                        size_bytes=0,
                        created_utc=_utc_iso(),
                        error=last_err,
                    )
                )

                self.logger.warning(f"HTTP exception retryable | attempt {attempt}/{self.max_retries} | {url} | {last_err}")
                if attempt < self.max_retries:
                    self._sleep_for_retry(attempt, resp)
                    continue

                return HttpResult(
                    status=None,
                    elapsed_ms=elapsed_ms,
                    size_bytes=0,
                    text=None,
                    url=url,
                    params=params,
                    error=last_err,
                    headers=None,
                )

        return HttpResult(status=None, elapsed_ms=None, size_bytes=None, text=None, url=url, params=params, error=last_err)


def parse_iss_json_tables(payload_text: str) -> Dict[str, pd.DataFrame]:
    obj = json.loads(payload_text)
    out: Dict[str, pd.DataFrame] = {}
    for block, content in obj.items():
        if not isinstance(content, dict):
            continue
        cols = content.get("columns")
        data = content.get("data")
        if isinstance(cols, list) and isinstance(data, list):
            out[block] = pd.DataFrame(data, columns=cols)
    return out


# ---------------------------
# Bonds list (daily cache) with anti-loop paging
# ---------------------------

def _hash_page_secids(secids: List[str]) -> str:
    h = hashlib.sha256()
    for s in secids:
        h.update(s.encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def fetch_all_traded_bonds(
    client: IssClient,
    logger,
    limit: int = 200,
    max_pages: int = 2000,
    min_new_ratio_stop: float = 0.02,
    boardgroup: int = 58,
) -> List[dict]:
    """
    Безопасный сбор списка "торгуемых облигаций" через boardgroup.

    Защиты:
    - boardgroup endpoint (уменьшает "всё подряд")
    - дедуп по SECID
    - стоп при повторе страницы (hash по SECID)
    - стоп при низкой доле новых SECID 3 страницы подряд
    """
    columns = [
        "SECID", "ISIN", "REGNUMBER",
        "SHORTNAME", "NAME",
        "EMITTER_ID",
        "TYPE", "GROUP",
        "PRIMARY_BOARDID",
        "LISTLEVEL",
        "ISSUEDATE", "MATDATE",
        "FACEVALUE", "FACEUNIT",
        "COUPONPERCENT", "COUPONVALUE", "COUPONPERIOD",
    ]

    all_rows: List[dict] = []
    seen_secids: set[str] = set()
    seen_page_hashes: set[str] = set()

    start = 0
    page = 0
    low_new_streak = 0

    path = f"/engines/stock/markets/bonds/boardgroups/{boardgroup}/securities.json"

    with Timer(logger, f"fetch_all_traded_bonds(boardgroup={boardgroup})"):
        while True:
            page += 1
            if page > max_pages:
                logger.warning(f"STOP max_pages reached | page={page} max_pages={max_pages}")
                break

            params = {
                "iss.meta": "off",
                "lang": "ru",
                "is_trading": 1,
                "start": start,
                "limit": limit,
                "securities.columns": ",".join(columns),
            }

            res = client.get(path, params=params)
            if res.status != 200 or not res.text:
                logger.error(f"Failed bonds list | status={res.status} | url={res.url} | err={res.error}")
                break

            tables = parse_iss_json_tables(res.text)
            sec = tables.get("securities")
            if sec is None or sec.empty:
                logger.info(f"Pagination end (empty) | page={page} start={start}")
                break

            rows = sec.to_dict(orient="records")
            page_secids = [str(r.get("SECID")) for r in rows if r.get("SECID") is not None]
            page_secids = [s for s in page_secids if s and s.lower() != "nan"]

            page_hash = _hash_page_secids(page_secids)
            if page_hash in seen_page_hashes:
                logger.warning(f"STOP repeated page hash | page={page} start={start} rows={len(rows)}")
                break
            seen_page_hashes.add(page_hash)

            new = 0
            for r in rows:
                s = r.get("SECID")
                if s is None:
                    continue
                s = str(s)
                if not s or s.lower() == "nan":
                    continue
                if s not in seen_secids:
                    seen_secids.add(s)
                    all_rows.append(r)
                    new += 1

            total = len(all_rows)
            new_ratio = new / max(1, len(rows))

            logger.info(
                f"Page {page} | start={start} | rows={len(rows)} | new={new} | new_ratio={new_ratio:.3f} | total_unique={total}"
            )

            if new_ratio < min_new_ratio_stop:
                low_new_streak += 1
            else:
                low_new_streak = 0

            if low_new_streak >= 3:
                logger.warning(
                    f"STOP low new_ratio streak | streak={low_new_streak} | last_new_ratio={new_ratio:.3f} | total_unique={total}"
                )
                break

            start += len(rows)

    logger.info(f"FETCH DONE | unique_secids={len(seen_secids)} | rows={len(all_rows)}")
    return all_rows


def get_bonds_list_daily(cache: SQLiteCache, client: IssClient, logger, force_refresh: bool) -> List[dict]:
    d = today_str()
    if not force_refresh:
        cached = cache.get_bonds_list(d)
        if cached is not None:
            logger.info(f"CACHE HIT | bonds_list | date={d} | rows={len(cached)}")
            return cached

    logger.info(f"CACHE MISS | bonds_list | date={d} | force_refresh={force_refresh}")
    bonds = fetch_all_traded_bonds(client, logger)
    cache.set_bonds_list(bonds, d)
    logger.info(f"CACHE SAVE | bonds_list | date={d} | rows={len(bonds)}")
    return bonds


# ---------------------------
# TTL for detail endpoints
# ---------------------------

def fetch_bondization_ttl(cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool) -> Dict[str, pd.DataFrame]:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "bondization", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            logger.info(f"TTL HIT | bondization | {secid} | date={d} | bytes={existing.get('size_bytes')}")
            return parse_iss_json_tables(existing["response_text"])

    params = {
        "iss.meta": "off",
        "lang": "ru",
        "limit": "unlimited",
        "iss.only": "coupons,offers,amortizations,events",
    }
    res = client.get(f"/securities/{secid}/bondization.json", params=params)

    cache.set_bond_raw(
        secid=secid,
        kind="bondization",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status != 200 or not res.text:
        logger.warning(f"bondization failed | {secid} | status={res.status} | err={res.error}")
        return {}
    return parse_iss_json_tables(res.text)


def fetch_description_ttl(cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool) -> pd.DataFrame:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "description", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            logger.info(f"TTL HIT | description | {secid} | date={d}")
            tables = parse_iss_json_tables(existing["response_text"])
            return tables.get("description", pd.DataFrame())

    params = {"iss.meta": "off", "lang": "ru"}
    res = client.get(f"/securities/{secid}.json", params=params)

    cache.set_bond_raw(
        secid=secid,
        kind="description",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status != 200 or not res.text:
        logger.warning(f"description failed | {secid} | status={res.status} | err={res.error}")
        return pd.DataFrame()

    tables = parse_iss_json_tables(res.text)
    return tables.get("description", pd.DataFrame())


def try_fetch_emitent(cache: SQLiteCache, client: IssClient, logger, emitter_id: int, force_refresh: bool) -> Optional[dict]:
    if not emitter_id:
        return None

    if not force_refresh:
        existing = cache.get_emitent(emitter_id)
        if existing and (existing.get("inn") or existing.get("title")):
            return existing

    d = today_str()
    fake = f"EMITENT:{emitter_id}"

    if not force_refresh:
        raw_exist = cache.get_bond_raw(fake, "emitent", d)
        if raw_exist and int(raw_exist.get("status") or 0) == 200 and raw_exist.get("response_text"):
            try:
                obj = json.loads(raw_exist["response_text"])
                for block, content in obj.items():
                    if isinstance(content, dict) and isinstance(content.get("columns"), list) and isinstance(content.get("data"), list):
                        df = pd.DataFrame(content["data"], columns=content["columns"])
                        if df.empty:
                            continue
                        row = df.iloc[0].to_dict()
                        inn = row.get("INN") or row.get("inn")
                        title = row.get("TITLE") or row.get("title") or row.get("NAME") or row.get("name")
                        short_title = row.get("SHORT_TITLE") or row.get("short_title")
                        ogrn = row.get("OGRN") or row.get("ogrn")
                        okpo = row.get("OKPO") or row.get("okpo")
                        cache.upsert_emitent(
                            emitter_id=emitter_id,
                            inn=str(inn) if inn else None,
                            title=str(title) if title else None,
                            short_title=str(short_title) if short_title else None,
                            ogrn=str(ogrn) if ogrn else None,
                            okpo=str(okpo) if okpo else None,
                            raw_json=raw_exist["response_text"],
                        )
                        return cache.get_emitent(emitter_id)
            except Exception:
                pass

    params = {"iss.meta": "off", "lang": "ru"}
    res = client.get(f"/emitents/{emitter_id}.json", params=params)

    cache.set_bond_raw(
        secid=fake,
        kind="emitent",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status != 200 or not res.text:
        logger.warning(f"emitent endpoint failed | emitter_id={emitter_id} | status={res.status} | err={res.error}")
        return None

    try:
        obj = json.loads(res.text)
        for block, content in obj.items():
            if isinstance(content, dict) and isinstance(content.get("columns"), list) and isinstance(content.get("data"), list):
                df = pd.DataFrame(content["data"], columns=content["columns"])
                if df.empty:
                    continue
                row = df.iloc[0].to_dict()
                inn = row.get("INN") or row.get("inn")
                title = row.get("TITLE") or row.get("title") or row.get("NAME") or row.get("name")
                short_title = row.get("SHORT_TITLE") or row.get("short_title")
                ogrn = row.get("OGRN") or row.get("ogrn")
                okpo = row.get("OKPO") or row.get("okpo")
                cache.upsert_emitent(
                    emitter_id=emitter_id,
                    inn=str(inn) if inn else None,
                    title=str(title) if title else None,
                    short_title=str(short_title) if short_title else None,
                    ogrn=str(ogrn) if ogrn else None,
                    okpo=str(okpo) if okpo else None,
                    raw_json=res.text,
                )
                return cache.get_emitent(emitter_id)
    except Exception as e:
        logger.warning(f"emitent parse failed | emitter_id={emitter_id} | err={e}")
        return None

    return None


# ---------------------------
# Excel helpers
# ---------------------------

def build_pivot_description(description_df: pd.DataFrame, emitents_df: pd.DataFrame) -> pd.DataFrame:
    if description_df.empty:
        base = pd.DataFrame(columns=["SECID"])
    else:
        df = description_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        if "SECID" not in df.columns:
            df["SECID"] = None

        key_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
        if key_col is None or "VALUE" not in df.columns:
            base = pd.DataFrame({"SECID": sorted(df["SECID"].dropna().unique().tolist())})
        else:
            wide = df.pivot_table(index="SECID", columns=key_col, values="VALUE", aggfunc="first")
            wide.reset_index(inplace=True)
            wide.columns = [str(c) for c in wide.columns]
            base = wide

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        if "SECID" in e.columns:
            keep = [c for c in ["SECID", "EMITTER_ID", "INN", "TITLE", "SHORT_TITLE", "OGRN", "OKPO"] if c in e.columns]
            if keep:
                base = base.merge(e[keep].drop_duplicates(), on="SECID", how="left")
    return base


def _parse_date_safe(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return None


def build_summary(sample_bonds: pd.DataFrame, emitents_df: pd.DataFrame, offers_df: pd.DataFrame, coupons_df: pd.DataFrame) -> pd.DataFrame:
    out = sample_bonds.copy()
    out.columns = [str(c).upper() for c in out.columns]

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        keep = [c for c in ["SECID", "EMITTER_ID", "INN", "TITLE", "SHORT_TITLE", "OGRN", "OKPO"] if c in e.columns]
        if keep:
            out = out.merge(e[keep].drop_duplicates(), on="SECID", how="left")

    next_offer = {}
    if offers_df is not None and not offers_df.empty:
        df = offers_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        date_col = "OFFERDATE" if "OFFERDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(_parse_date_safe)
            now = pd.Timestamp.utcnow()
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                pick = future.iloc[0] if len(future) else g.sort_values("_DT").iloc[-1]
                next_offer[str(secid)] = pick["_DT"].date().isoformat()

    next_coupon = {}
    if coupons_df is not None and not coupons_df.empty:
        df = coupons_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        date_col = "COUPONDATE" if "COUPONDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(_parse_date_safe)
            now = pd.Timestamp.utcnow()
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                if len(future):
                    next_coupon[str(secid)] = future.iloc[0]["_DT"].date().isoformat()

    if "SECID" in out.columns:
        out["NEXT_OFFER_DATE"] = out["SECID"].astype(str).map(next_offer)
        out["NEXT_COUPON_DATE"] = out["SECID"].astype(str).map(next_coupon)

    preferred = [
        "SECID", "ISIN", "REGNUMBER", "SHORTNAME", "NAME",
        "EMITTER_ID", "INN", "TITLE",
        "ISSUEDATE", "MATDATE",
        "FACEVALUE", "FACEUNIT",
        "COUPONPERCENT", "COUPONVALUE", "COUPONPERIOD",
        "NEXT_OFFER_DATE", "NEXT_COUPON_DATE",
        "LISTLEVEL", "PRIMARY_BOARDID",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]


def save_excel_bonds_list(bonds: List[dict], out_path: str | Path, logger) -> None:
    df = pd.DataFrame(bonds)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, index=False, sheet_name="bonds")
        meta = pd.DataFrame([{"created_utc": _utc_iso(), "rows": len(df)}])
        meta.to_excel(w, index=False, sheet_name="meta")
    logger.info(f"Excel saved: {out_path} | rows={len(df)}")


def save_excel_detail(
    bonds_sample: pd.DataFrame,
    description_df: pd.DataFrame,
    events_df: pd.DataFrame,
    coupons_df: pd.DataFrame,
    offers_df: pd.DataFrame,
    amort_df: pd.DataFrame,
    emitents_df: pd.DataFrame,
    out_path: str | Path,
    logger,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pivot_df = build_pivot_description(description_df, emitents_df)
    summary_df = build_summary(bonds_sample, emitents_df, offers_df, coupons_df)

    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        meta = pd.DataFrame(
            [
                {
                    "created_utc": _utc_iso(),
                    "sample_rows": len(bonds_sample),
                    "desc_rows": len(description_df),
                    "events_rows": len(events_df),
                    "coupons_rows": len(coupons_df),
                    "offers_rows": len(offers_df),
                    "amort_rows": len(amort_df),
                    "emitents_rows": len(emitents_df),
                    "pivot_rows": len(pivot_df),
                }
            ]
        )
        meta.to_excel(w, index=False, sheet_name="meta")
        summary_df.to_excel(w, index=False, sheet_name="summary")
        bonds_sample.to_excel(w, index=False, sheet_name="sample_bonds")
        emitents_df.to_excel(w, index=False, sheet_name="emitents")
        pivot_df.to_excel(w, index=False, sheet_name="pivot_description")
        description_df.to_excel(w, index=False, sheet_name="description")
        events_df.to_excel(w, index=False, sheet_name="events")
        coupons_df.to_excel(w, index=False, sheet_name="coupons")
        offers_df.to_excel(w, index=False, sheet_name="offers")
        amort_df.to_excel(w, index=False, sheet_name="amortizations")

    logger.info(
        f"Detail Excel saved: {out_path} | summary={len(summary_df)} | pivot={len(pivot_df)} | "
        f"desc={len(description_df)} | events={len(events_df)} | coupons={len(coupons_df)} | "
        f"offers={len(offers_df)} | amort={len(amort_df)} | emitents={len(emitents_df)}"
    )


# ---------------------------
# CLI
# ---------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX ISS bonds + detail + SQLite cache")
    p.add_argument("--sample-size", type=int, default=10, help="How many random bonds to sample for detail")
    p.add_argument("--seed", type=int, default=42, help="Random seed for sampling")
    p.add_argument("--force-refresh-bonds", action="store_true", help="Ignore daily cache for bonds_list and refetch")
    p.add_argument("--force-refresh-detail", action="store_true", help="Ignore TTL for description/bondization/emitents")
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=5, help="HTTP retries for 429/5xx")
    p.add_argument("--backoff", type=float, default=0.8, help="Backoff base seconds")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    # если boardgroup у тебя отличается — можно переопределить
    p.add_argument("--boardgroup", type=int, default=58, help="MOEX bonds boardgroup id for list endpoint")
    return p.parse_args()


def main():
    args = parse_args()

    lp = ensure_logs_dir("logs")
    import logging
    logger = setup_logger("Moex_API", lp.logfile, level=getattr(logging, args.log_level), clear=True, also_console=True)
    cache_logger = setup_logger("SQLiteCache", lp.logfile, level=getattr(logging, args.log_level), clear=False, also_console=False)

    start_utc = _utc_iso()
    logger.info(f"START | utc={start_utc} | log={lp.logfile.resolve()}")

    cache = SQLiteCache("moex_cache.sqlite", logger=cache_logger)
    client = IssClient(cache, logger, timeout=args.timeout, max_retries=args.retries, backoff_base=args.backoff)

    t0 = time.perf_counter()
    try:
        with Timer(logger, "total"):
            # 1) bonds list (daily cache)
            bonds = get_bonds_list_daily(cache, client, logger, force_refresh=args.force_refresh_bonds)
            # сохраняем общий excel
            save_excel_bonds_list(bonds, "Moex_Bonds.xlsx", logger)

            # 2) sample N random
            df_bonds = pd.DataFrame(bonds)
            if df_bonds.empty or "SECID" not in df_bonds.columns:
                logger.warning("No bonds fetched or missing SECID, stop.")
                return

            df_bonds = df_bonds.dropna(subset=["SECID"]).copy()
            secids = df_bonds["SECID"].astype(str).unique().tolist()
            k = min(max(0, int(args.sample_size)), len(secids))
            if k == 0:
                logger.warning("sample-size=0 or empty list.")
                return

            rnd = random.Random(int(args.seed))
            sample_secids = rnd.sample(secids, k)
            logger.info(f"DETAIL sample | k={k} | seed={args.seed} | force_refresh_detail={args.force_refresh_detail}")

            sample_df = df_bonds[df_bonds["SECID"].astype(str).isin(sample_secids)].copy()
            sample_df = sample_df.sort_values("SECID").reset_index(drop=True)

            # 3) fetch details + emitents
            desc_rows = []
            ev_rows = []
            cp_rows = []
            of_rows = []
            am_rows = []
            em_rows = []

            with Timer(logger, "detail_fetch"):
                for i, secid in enumerate(sample_secids, 1):
                    logger.info(f"DETAIL {i}/{k} | {secid}")

                    desc = fetch_description_ttl(cache, client, logger, secid, force_refresh=args.force_refresh_detail)
                    if not desc.empty:
                        ddf = desc.copy()
                        ddf["SECID"] = secid
                        desc_rows.append(ddf)

                    bz = fetch_bondization_ttl(cache, client, logger, secid, force_refresh=args.force_refresh_detail)

                    for block, sink in [
                        ("events", ev_rows),
                        ("coupons", cp_rows),
                        ("offers", of_rows),
                        ("amortizations", am_rows),
                    ]:
                        df = bz.get(block)
                        if df is not None and not df.empty:
                            x = df.copy()
                            x["SECID"] = secid
                            sink.append(x)

                    # emitent
                    emitter_id = None
                    try:
                        r = sample_df[sample_df["SECID"].astype(str) == str(secid)].iloc[0].to_dict()
                        emitter_id = r.get("EMITTER_ID")
                    except Exception:
                        emitter_id = None

                    emitter_id_int = None
                    if emitter_id is not None and str(emitter_id).strip() != "":
                        try:
                            emitter_id_int = int(emitter_id)
                        except Exception:
                            emitter_id_int = None

                    if emitter_id_int:
                        e = try_fetch_emitent(cache, client, logger, emitter_id_int, force_refresh=args.force_refresh_detail)
                        if e:
                            em_rows.append(
                                {
                                    "SECID": secid,
                                    "EMITTER_ID": emitter_id_int,
                                    "INN": e.get("inn"),
                                    "TITLE": e.get("title"),
                                    "SHORT_TITLE": e.get("short_title"),
                                    "OGRN": e.get("ogrn"),
                                    "OKPO": e.get("okpo"),
                                    "UPDATED_UTC": e.get("updated_utc"),
                                }
                            )
                        else:
                            em_rows.append({"SECID": secid, "EMITTER_ID": emitter_id_int})
                    else:
                        em_rows.append({"SECID": secid, "EMITTER_ID": None})

            # requests summary
            summ = cache.requests_summary(start_utc)
            logger.info(f"REQUESTS summary since start | total={summ['total']} | errors={summ['errors']}")

            desc_df = pd.concat(desc_rows, ignore_index=True) if desc_rows else pd.DataFrame()
            ev_df = pd.concat(ev_rows, ignore_index=True) if ev_rows else pd.DataFrame()
            cp_df = pd.concat(cp_rows, ignore_index=True) if cp_rows else pd.DataFrame()
            of_df = pd.concat(of_rows, ignore_index=True) if of_rows else pd.DataFrame()
            am_df = pd.concat(am_rows, ignore_index=True) if am_rows else pd.DataFrame()
            em_df = pd.DataFrame(em_rows) if em_rows else pd.DataFrame()

            if not desc_df.empty:
                cols = list(desc_df.columns)
                if "SECID" in cols:
                    cols = ["SECID"] + [c for c in cols if c != "SECID"]
                    desc_df = desc_df[cols]

            save_excel_detail(
                bonds_sample=sample_df,
                description_df=desc_df,
                events_df=ev_df,
                coupons_df=cp_df,
                offers_df=of_df,
                amort_df=am_df,
                emitents_df=em_df,
                out_path="Moex_Bonds_Detail.xlsx",
                logger=logger,
            )

    finally:
        cache.close()

    elapsed = time.perf_counter() - t0
    logger.info(f"FINISH | elapsed={elapsed:.3f}s")


if __name__ == "__main__":
    main()