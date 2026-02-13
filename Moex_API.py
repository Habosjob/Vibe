# Moex_API.py
from __future__ import annotations

import argparse
import hashlib
import random
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from logs import setup_logger, ensure_logs_dir, Timer
from SQL import SQLiteCache
from iss_client import IssClient, RateLimiter
from moex_parsers import parse_iss_json_tables_safe
from moex_emitents import try_fetch_emitent
from moex_excel import build_pivot_description, build_summary

BASE = "https://iss.moex.com/iss"


def today_str() -> str:
    return date.today().isoformat()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _hash_page_secids(secids: List[str]) -> str:
    h = hashlib.sha256()
    for s in secids:
        h.update(s.encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def fetch_all_traded_bonds(
    client: IssClient,
    logger,
    *,
    limit: int = 200,
    max_pages: int = 2000,
    min_new_ratio_stop: float = 0.02,
    boardgroup: int = 58,
    snippet_chars: int = 800,
) -> List[dict]:
    columns = [
        "SECID",
        "ISIN",
        "REGNUMBER",
        "SHORTNAME",
        "NAME",
        "EMITTER_ID",
        "TYPE",
        "GROUP",
        "PRIMARY_BOARDID",
        "LISTLEVEL",
        "ISSUEDATE",
        "MATDATE",
        "FACEVALUE",
        "FACEUNIT",
        "COUPONPERCENT",
        "COUPONVALUE",
        "COUPONPERIOD",
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

            ct = (res.headers or {}).get("Content-Type", "")
            tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)

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
                f"Page {page} | start={start} | rows={len(rows)} | new={new} | "
                f"new_ratio={new_ratio:.3f} | total_unique={total}"
            )

            if new_ratio < min_new_ratio_stop:
                low_new_streak += 1
            else:
                low_new_streak = 0
            if low_new_streak >= 3:
                logger.warning(
                    f"STOP low new_ratio streak | streak={low_new_streak} | "
                    f"last_new_ratio={new_ratio:.3f} | total_unique={total}"
                )
                break

            start += len(rows)

    logger.info(f"FETCH DONE | unique_secids={len(seen_secids)} | rows={len(all_rows)}")
    return all_rows


def get_bonds_list_daily(cache: SQLiteCache, client: IssClient, logger, force_refresh: bool, snippet_chars: int) -> List[dict]:
    d = today_str()
    if not force_refresh:
        cached = cache.get_bonds_list(d)
        if cached is not None:
            logger.info(f"CACHE HIT | bonds_list | date={d} | rows={len(cached)}")
            return cached

    logger.info(f"CACHE MISS | bonds_list | date={d} | force_refresh={force_refresh}")
    bonds = fetch_all_traded_bonds(client, logger, snippet_chars=snippet_chars)
    cache.set_bonds_list(bonds, d)
    logger.info(f"CACHE SAVE | bonds_list | date={d} | rows={len(bonds)}")
    return bonds


def fetch_description_ttl(
    cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool, snippet_chars: int
) -> pd.DataFrame:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "description", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            tables = parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)
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

    ct = (res.headers or {}).get("Content-Type", "")
    tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)
    return tables.get("description", pd.DataFrame())


def fetch_bondization_ttl(
    cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool, snippet_chars: int
) -> Dict[str, pd.DataFrame]:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "bondization", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            return parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)

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

    ct = (res.headers or {}).get("Content-Type", "")
    return parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX ISS bonds + detail + SQLite cache")
    p.add_argument("--sample-size", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--force-refresh-bonds", action="store_true")
    p.add_argument("--force-refresh-detail", action="store_true")

    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff", type=float, default=0.8)
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--boardgroup", type=int, default=58)

    p.add_argument("--purge-bond-raw-days", type=int, default=30)
    p.add_argument("--purge-requests-days", type=int, default=30)
    p.add_argument("--purge-bonds-list-days", type=int, default=60)
    p.add_argument("--purge-emitents-days", type=int, default=0)

    p.add_argument("--detail-workers", type=int, default=8)
    p.add_argument("--detail-rps", type=float, default=8.0)

    p.add_argument("--parse-snippet-chars", type=int, default=800)
    p.add_argument("--emitent-ttl-days", type=int, default=90)

    return p.parse_args()


def _process_one_secid(
    secid: str,
    *,
    cache: SQLiteCache,
    client: IssClient,
    logger,
    sample_df: pd.DataFrame,
    force_refresh: bool,
    emitent_ttl_days: int,
    snippet_chars: int,
) -> Tuple[str, List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], Dict[str, Any]]:
    desc_rows: List[pd.DataFrame] = []
    ev_rows: List[pd.DataFrame] = []
    cp_rows: List[pd.DataFrame] = []
    of_rows: List[pd.DataFrame] = []
    am_rows: List[pd.DataFrame] = []

    desc = fetch_description_ttl(cache, client, logger, secid, force_refresh=force_refresh, snippet_chars=snippet_chars)
    if not desc.empty:
        ddf = desc.copy()
        ddf["SECID"] = secid
        desc_rows.append(ddf)

    bz = fetch_bondization_ttl(cache, client, logger, secid, force_refresh=force_refresh, snippet_chars=snippet_chars)
    for block, sink in [("events", ev_rows), ("coupons", cp_rows), ("offers", of_rows), ("amortizations", am_rows)]:
        df = bz.get(block)
        if df is not None and not df.empty:
            x = df.copy()
            x["SECID"] = secid
            sink.append(x)

    emitter_id_int: Optional[int] = None
    try:
        r = sample_df[sample_df["SECID"].astype(str) == str(secid)].iloc[0].to_dict()
        emitter_id = r.get("EMITTER_ID")
        if emitter_id is not None and str(emitter_id).strip() != "":
            emitter_id_int = int(emitter_id)
    except Exception:
        emitter_id_int = None

    emitent_row: Dict[str, Any] = {"SECID": secid, "EMITTER_ID": emitter_id_int}
    if emitter_id_int:
        e = try_fetch_emitent(
            cache,
            client,
            logger,
            emitter_id_int,
            secid_hint=secid,
            force_refresh=force_refresh,
            emitent_ttl_days=emitent_ttl_days,
            snippet_chars=snippet_chars,
        )
        if e:
            emitent_row.update(
                {
                    "INN": e.get("inn"),
                    "TITLE": e.get("title"),
                    "SHORT_TITLE": e.get("short_title"),
                    "OGRN": e.get("ogrn"),
                    "OKPO": e.get("okpo"),
                    "KPP": e.get("kpp"),
                    "OKVED": e.get("okved"),
                    "ADDRESS": e.get("address"),
                    "PHONE": e.get("phone"),
                    "SITE": e.get("site"),
                    "EMAIL": e.get("email"),
                    "UPDATED_UTC": e.get("updated_utc"),
                }
            )

    return secid, desc_rows, ev_rows, cp_rows, of_rows, am_rows, emitent_row


def main():
    args = parse_args()

    lp = ensure_logs_dir("logs")
    import logging

    logger = setup_logger(
        "Moex_API",
        lp.logfile,
        level=getattr(logging, args.log_level),
        clear=True,
        also_console=True,
    )
    cache_logger = setup_logger(
        "SQLiteCache",
        lp.logfile,
        level=getattr(logging, args.log_level),
        clear=False,
        also_console=False,
    )

    start_utc = _utc_iso()
    logger.info(f"START | utc={start_utc} | log={lp.logfile.resolve()}")

    cache = SQLiteCache("moex_cache.sqlite", logger=cache_logger)

    # purge TTL
    try:
        n1 = cache.purge_bond_raw(args.purge_bond_raw_days)
        n2 = cache.purge_requests_log(args.purge_requests_days)
        n3 = cache.purge_bonds_list(args.purge_bonds_list_days)
        n4 = cache.purge_emitents(args.purge_emitents_days)
        if any(x > 0 for x in (n1, n2, n3, n4)):
            logger.info(f"PURGE done | bond_raw={n1} | requests_log={n2} | bonds_list={n3} | emitents={n4}")
    except Exception as e:
        logger.warning(f"PURGE failed | err={e}")

    rate = RateLimiter(args.detail_rps) if args.detail_rps and args.detail_rps > 0 else None
    client = IssClient(
        cache,
        logger,
        base_url=BASE,
        timeout=args.timeout,
        max_retries=args.retries,
        backoff_base=args.backoff,
        rate_limiter=rate,
    )

    t0 = time.perf_counter()
    try:
        with Timer(logger, "total"):
            bonds = get_bonds_list_daily(cache, client, logger, force_refresh=args.force_refresh_bonds, snippet_chars=args.parse_snippet_chars)
            save_excel_bonds_list(bonds, "Moex_Bonds.xlsx", logger)

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

            logger.info(
                f"DETAIL sample | k={k} | seed={args.seed} | force_refresh_detail={args.force_refresh_detail} | "
                f"workers={args.detail_workers} | rps={args.detail_rps} | emitent_ttl_days={args.emitent_ttl_days}"
            )

            sample_df = df_bonds[df_bonds["SECID"].astype(str).isin(sample_secids)].copy()
            sample_df = sample_df.sort_values("SECID").reset_index(drop=True)

            desc_rows_all: List[pd.DataFrame] = []
            ev_rows_all: List[pd.DataFrame] = []
            cp_rows_all: List[pd.DataFrame] = []
            of_rows_all: List[pd.DataFrame] = []
            am_rows_all: List[pd.DataFrame] = []
            em_rows_all: List[Dict[str, Any]] = []

            with Timer(logger, "detail_fetch"):
                from concurrent.futures import ThreadPoolExecutor, as_completed

                workers = max(1, int(args.detail_workers))
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futs = [
                        ex.submit(
                            _process_one_secid,
                            secid,
                            cache=cache,
                            client=client,
                            logger=logger,
                            sample_df=sample_df,
                            force_refresh=args.force_refresh_detail,
                            emitent_ttl_days=args.emitent_ttl_days,
                            snippet_chars=args.parse_snippet_chars,
                        )
                        for secid in sample_secids
                    ]

                    done = 0
                    for fut in as_completed(futs):
                        done += 1
                        try:
                            secid, drows, erows, crows, orows, arows, emrow = fut.result()
                            logger.info(f"DETAIL done {done}/{k} | {secid}")
                            desc_rows_all.extend(drows)
                            ev_rows_all.extend(erows)
                            cp_rows_all.extend(crows)
                            of_rows_all.extend(orows)
                            am_rows_all.extend(arows)
                            em_rows_all.append(emrow)
                        except Exception as e:
                            logger.warning(f"DETAIL task failed | err={e}")

            summ = cache.requests_summary(start_utc)
            logger.info(f"REQUESTS summary since start | total={summ['total']} | errors={summ['errors']}")

            desc_df = pd.concat(desc_rows_all, ignore_index=True) if desc_rows_all else pd.DataFrame()
            ev_df = pd.concat(ev_rows_all, ignore_index=True) if ev_rows_all else pd.DataFrame()
            cp_df = pd.concat(cp_rows_all, ignore_index=True) if cp_rows_all else pd.DataFrame()
            of_df = pd.concat(of_rows_all, ignore_index=True) if of_rows_all else pd.DataFrame()
            am_df = pd.concat(am_rows_all, ignore_index=True) if am_rows_all else pd.DataFrame()
            em_df = pd.DataFrame(em_rows_all) if em_rows_all else pd.DataFrame()

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