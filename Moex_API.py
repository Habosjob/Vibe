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
from SQL import SQLiteCache, utc_iso
from iss_client import IssClient, RateLimiter
from moex_parsers import parse_iss_json_tables_safe
from moex_emitents import try_fetch_emitent
from moex_excel import build_pivot_description, build_summary

BASE = "https://iss.moex.com/iss"


def today_str() -> str:
    return date.today().isoformat()


def hash_page_secids(secids: List[str]) -> str:
    h = hashlib.sha256()
    for s in secids:
        h.update(s.encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def read_or_create_static_secids(all_secids: List[str], path: Path, k: int, logger) -> List[str]:
    """
    10 статичных бумаг для проверки кэша:
    - если static_secids.txt есть -> читаем
    - если нет -> пишем первые k (по сортировке) и используем их
    """
    path = Path(path)
    if path.exists():
        secids = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(secids) >= k:
            return secids[:k]
        # если файл есть, но короткий — добьём из all_secids
        need = k - len(secids)
        tail = [s for s in sorted(all_secids) if s not in secids][:need]
        secids = secids + tail
        path.write_text("\n".join(secids) + "\n", encoding="utf-8")
        return secids[:k]

    secids = sorted(all_secids)[:k]
    path.write_text("\n".join(secids) + "\n", encoding="utf-8")
    logger.info(f"static_secids created: {path} | k={k}")
    return secids


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
        "SECID", "ISIN", "REGNUMBER", "SHORTNAME", "NAME", "EMITTER_ID",
        "TYPE", "GROUP", "PRIMARY_BOARDID", "LISTLEVEL",
        "ISSUEDATE", "MATDATE", "FACEVALUE", "FACEUNIT",
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

            ct = (res.headers or {}).get("Content-Type", "")
            tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)

            sec = tables.get("securities")
            if sec is None or sec.empty:
                logger.info(f"Pagination end (empty) | page={page} start={start}")
                break

            rows = sec.to_dict(orient="records")

            page_secids = [str(r.get("SECID")) for r in rows if r.get("SECID") is not None]
            page_secids = [s for s in page_secids if s and s.lower() != "nan"]
            page_hash = hash_page_secids(page_secids)
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
            logger.info(f"Page {page} | start={start} | rows={len(rows)} | new={new} | new_ratio={new_ratio:.3f} | total_unique={total}")

            if new_ratio < min_new_ratio_stop:
                low_new_streak += 1
            else:
                low_new_streak = 0
            if low_new_streak >= 3:
                logger.warning(f"STOP low new_ratio streak | streak={low_new_streak} | last_new_ratio={new_ratio:.3f} | total_unique={total}")
                break

            start += len(rows)

    logger.info(f"FETCH DONE | unique_secids={len(seen_secids)} | rows={len(all_rows)}")
    return all_rows


def get_bonds_list_daily(cache: SQLiteCache, client: IssClient, logger, force_refresh: bool, boardgroup: int, snippet_chars: int) -> List[dict]:
    d = today_str()
    if not force_refresh:
        cached = cache.get_bonds_list(d)
        if cached is not None:
            logger.info(f"CACHE HIT | bonds_list | date={d} | rows={len(cached)}")
            return cached

    logger.info(f"CACHE MISS | bonds_list | date={d} | force_refresh={force_refresh}")
    bonds = fetch_all_traded_bonds(client, logger, boardgroup=boardgroup, snippet_chars=snippet_chars)
    cache.set_bonds_list(bonds, d)
    logger.info(f"CACHE SAVE | bonds_list | date={d} | rows={len(bonds)}")
    return bonds


def fetch_description_selfheal(
    cache: SQLiteCache,
    client: IssClient,
    logger,
    secid: str,
    *,
    force_refresh: bool,
    snippet_chars: int,
) -> pd.DataFrame:
    """
    Self-heal:
      - если TTL HIT, но парсинг пустой -> делаем запрос и перезаписываем кэш
    """
    d = today_str()

    if not force_refresh:
        existing = cache.get_bond_raw(secid, "description", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            tables = parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)
            df = tables.get("description", pd.DataFrame())
            if df is not None and not df.empty:
                return df
            logger.warning(f"SELF-HEAL description (cache empty after parse) | {secid} | forcing refresh")

    res = client.get(f"/securities/{secid}.json", params={"iss.meta": "off", "lang": "ru"})
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
        return pd.DataFrame()

    ct = (res.headers or {}).get("Content-Type", "")
    tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)
    return tables.get("description", pd.DataFrame())


def fetch_bondization_selfheal(
    cache: SQLiteCache,
    client: IssClient,
    logger,
    secid: str,
    *,
    force_refresh: bool,
    snippet_chars: int,
) -> Dict[str, pd.DataFrame]:
    d = today_str()

    if not force_refresh:
        existing = cache.get_bond_raw(secid, "bondization", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            tables = parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)
            # если совсем пусто — self-heal
            if tables:
                return tables
            logger.warning(f"SELF-HEAL bondization (cache empty after parse) | {secid} | forcing refresh")

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
        return {}

    ct = (res.headers or {}).get("Content-Type", "")
    return parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX ISS bonds + detail + SQLite cache")

    # list
    p.add_argument("--force-refresh-bonds", action="store_true")
    p.add_argument("--boardgroup", type=int, default=58)

    # detail modes
    p.add_argument("--sample-size", type=int, default=10, help="Random sample size (in addition to static 10)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--detail-all", action="store_true", help="Process ALL bonds (uses checkpoint/progress)")
    p.add_argument("--run-id", default=None, help="Run identifier for detail_progress (default: YYYY-MM-DD)")
    p.add_argument("--checkpoint-every", type=int, default=50, help="Log/save checkpoint each N processed secids")

    # static
    p.add_argument("--static-size", type=int, default=10, help="How many static bonds to always include")
    p.add_argument("--static-file", default="static_secids.txt")

    # ttl/refresh
    p.add_argument("--force-refresh-detail", action="store_true")
    p.add_argument("--emitent-ttl-days", type=int, default=90)

    # http
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff", type=float, default=0.8)
    p.add_argument("--detail-workers", type=int, default=8)
    p.add_argument("--detail-rps", type=float, default=8.0)

    # logs/parse
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--parse-snippet-chars", type=int, default=800)

    # purge
    p.add_argument("--purge-bond-raw-days", type=int, default=30)
    p.add_argument("--purge-requests-days", type=int, default=30)
    p.add_argument("--purge-bonds-list-days", type=int, default=60)
    p.add_argument("--purge-emitents-days", type=int, default=0)

    # output
    p.add_argument("--out-detail", default="Moex_Bonds_Detail.xlsx")

    return p.parse_args()


def process_one_secid(
    secid: str,
    *,
    cache: SQLiteCache,
    client: IssClient,
    logger,
    sample_df: pd.DataFrame,
    force_refresh: bool,
    emitent_ttl_days: int,
    snippet_chars: int,
) -> Dict[str, int]:
    """
    Делает self-heal fetch (description + bondization + emitent) и пишет всё в sqlite (bond_raw + emitents).
    Возвращает счётчики для логов.
    """
    c = {"desc": 0, "events": 0, "coupons": 0, "offers": 0, "amort": 0, "emitent": 0}

    desc = fetch_description_selfheal(cache, client, logger, secid, force_refresh=force_refresh, snippet_chars=snippet_chars)
    if desc is not None and not desc.empty:
        c["desc"] = len(desc)

    bz = fetch_bondization_selfheal(cache, client, logger, secid, force_refresh=force_refresh, snippet_chars=snippet_chars)
    for k, key in [("events", "events"), ("coupons", "coupons"), ("offers", "offers"), ("amort", "amortizations")]:
        df = bz.get(key)
        if df is not None and not df.empty:
            c[k] = len(df)

    emitter_id_int: Optional[int] = None
    try:
        r = sample_df[sample_df["SECID"].astype(str) == str(secid)].iloc[0].to_dict()
        emitter_id = r.get("EMITTER_ID")
        if emitter_id is not None and str(emitter_id).strip() != "":
            emitter_id_int = int(emitter_id)
    except Exception:
        emitter_id_int = None

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
        if e and (e.get("inn") or e.get("title")):
            c["emitent"] = 1

    return c


def build_detail_excel_from_cache(
    cache: SQLiteCache,
    logger,
    bonds_df: pd.DataFrame,
    secids: List[str],
    out_path: str,
    snippet_chars: int,
) -> None:
    """
    Собираем Excel из кэша (bond_raw/emitents).
    Для detail-all это может быть ОЧЕНЬ большим, но это честный “экспорт”.
    """
    d = today_str()

    # sample_bonds subset
    sample_df = bonds_df[bonds_df["SECID"].astype(str).isin(secids)].copy()
    sample_df = sample_df.sort_values("SECID").reset_index(drop=True)

    # emitents
    em_rows: List[Dict[str, Any]] = []
    for secid in secids:
        try:
            r = sample_df[sample_df["SECID"].astype(str) == str(secid)].iloc[0].to_dict()
            emitter_id = r.get("EMITTER_ID")
            if emitter_id is None:
                continue
            e = cache.get_emitent(int(emitter_id))
            if not e:
                continue
            em_rows.append(
                {
                    "SECID": secid,
                    "EMITTER_ID": int(emitter_id),
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
        except Exception:
            continue
    emitents_df = pd.DataFrame(em_rows)

    # parse cached description/bondization raw texts
    desc_rows_all: List[pd.DataFrame] = []
    ev_rows_all: List[pd.DataFrame] = []
    cp_rows_all: List[pd.DataFrame] = []
    of_rows_all: List[pd.DataFrame] = []
    am_rows_all: List[pd.DataFrame] = []

    for secid in secids:
        raw_desc = cache.get_bond_raw(secid, "description", d)
        if raw_desc and raw_desc.get("response_text"):
            tables = parse_iss_json_tables_safe(raw_desc["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)
            df = tables.get("description")
            if df is not None and not df.empty:
                x = df.copy()
                x["SECID"] = secid
                desc_rows_all.append(x)

        raw_bz = cache.get_bond_raw(secid, "bondization", d)
        if raw_bz and raw_bz.get("response_text"):
            tables = parse_iss_json_tables_safe(raw_bz["response_text"], logger=logger, url="", content_type="", snippet_chars=snippet_chars)
            for block, sink in [("events", ev_rows_all), ("coupons", cp_rows_all), ("offers", of_rows_all), ("amortizations", am_rows_all)]:
                df = tables.get(block)
                if df is not None and not df.empty:
                    x = df.copy()
                    x["SECID"] = secid
                    sink.append(x)

    description_df = pd.concat(desc_rows_all, ignore_index=True) if desc_rows_all else pd.DataFrame()
    events_df = pd.concat(ev_rows_all, ignore_index=True) if ev_rows_all else pd.DataFrame()
    coupons_df = pd.concat(cp_rows_all, ignore_index=True) if cp_rows_all else pd.DataFrame()
    offers_df = pd.concat(of_rows_all, ignore_index=True) if of_rows_all else pd.DataFrame()
    amort_df = pd.concat(am_rows_all, ignore_index=True) if am_rows_all else pd.DataFrame()

    pivot_df = build_pivot_description(description_df, emitents_df)
    summary_df = build_summary(sample_df, emitents_df)

    out_path = str(Path(out_path))
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        meta = pd.DataFrame(
            [
                {
                    "created_utc": utc_iso(),
                    "secids": len(secids),
                    "sample_rows": len(sample_df),
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
        sample_df.to_excel(w, index=False, sheet_name="sample_bonds")
        emitents_df.to_excel(w, index=False, sheet_name="emitents")
        pivot_df.to_excel(w, index=False, sheet_name="pivot_description")
        description_df.to_excel(w, index=False, sheet_name="description")
        events_df.to_excel(w, index=False, sheet_name="events")
        coupons_df.to_excel(w, index=False, sheet_name="coupons")
        offers_df.to_excel(w, index=False, sheet_name="offers")
        amort_df.to_excel(w, index=False, sheet_name="amortizations")

    logger.info(
        f"Detail Excel saved: {out_path} | secids={len(secids)} | "
        f"desc={len(description_df)} | events={len(events_df)} | coupons={len(coupons_df)} | "
        f"offers={len(offers_df)} | amort={len(amort_df)} | emitents={len(emitents_df)} | pivot={len(pivot_df)}"
    )


def main():
    args = parse_args()

    lp = ensure_logs_dir("logs")
    import logging

    logger = setup_logger("Moex_API", lp.logfile, level=getattr(logging, args.log_level), clear=True, also_console=True)
    cache_logger = setup_logger("SQLiteCache", lp.logfile, level=getattr(logging, args.log_level), clear=False, also_console=False)

    start_utc = utc_iso()
    logger.info(f"START | utc={start_utc} | log={lp.logfile.resolve()}")

    cache = SQLiteCache("moex_cache.sqlite", logger=cache_logger)

    # purge
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

    try:
        with Timer(logger, "total"):
            bonds = get_bonds_list_daily(
                cache,
                client,
                logger,
                force_refresh=args.force_refresh_bonds,
                boardgroup=args.boardgroup,
                snippet_chars=args.parse_snippet_chars,
            )
            bonds_df = pd.DataFrame(bonds)
            if bonds_df.empty or "SECID" not in bonds_df.columns:
                logger.error("No bonds fetched or missing SECID.")
                return
            bonds_df = bonds_df.dropna(subset=["SECID"]).copy()
            all_secids = bonds_df["SECID"].astype(str).unique().tolist()

            # 10 static + N random (или all)
            static_secids = read_or_create_static_secids(all_secids, Path(args.static_file), args.static_size, logger)

            rnd = random.Random(int(args.seed))
            random_k = min(max(0, int(args.sample_size)), len(all_secids))
            random_secids = rnd.sample(all_secids, random_k) if random_k > 0 else []

            if args.detail_all:
                secids = sorted(set(all_secids))  # all
            else:
                secids = sorted(set(static_secids + random_secids))

            run_id = args.run_id or today_str()
            logger.info(
                f"DETAIL plan | run_id={run_id} | detail_all={args.detail_all} | "
                f"secids={len(secids)} | static={len(static_secids)} | random={len(random_secids)}"
            )

            # seed progress
            if args.detail_all:
                cache.progress_seed(run_id, secids, mode="all")
            else:
                # отдельно помечаем статичные/рандомные — удобно для проверки
                cache.progress_seed(run_id, static_secids, mode="static")
                cache.progress_seed(run_id, random_secids, mode="random")

            # process with checkpoint
            processed = 0
            with Timer(logger, "detail_collect"):
                while True:
                    batch_secids = cache.progress_take_batch(run_id, batch=max(1, min(200, args.detail_workers * 10)))
                    if not batch_secids:
                        break

                    # параллельная обработка батча
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    with ThreadPoolExecutor(max_workers=max(1, int(args.detail_workers))) as ex:
                        futs = {
                            ex.submit(
                                process_one_secid,
                                s,
                                cache=cache,
                                client=client,
                                logger=logger,
                                sample_df=bonds_df,  # для EMITTER_ID
                                force_refresh=args.force_refresh_detail,
                                emitent_ttl_days=args.emitent_ttl_days,
                                snippet_chars=args.parse_snippet_chars,
                            ): s
                            for s in batch_secids
                        }

                        for fut in as_completed(futs):
                            s = futs[fut]
                            try:
                                stats = fut.result()
                                cache.progress_mark_done(run_id, s)
                                processed += 1
                                if processed % max(1, int(args.checkpoint_every)) == 0:
                                    cnt = cache.progress_counts(run_id)
                                    logger.info(f"CHECKPOINT | run_id={run_id} | processed={processed} | {cnt}")
                            except Exception as e:
                                cache.progress_mark_error(run_id, s, repr(e))
                                logger.warning(f"detail failed | secid={s} | err={e}")

            cnt = cache.progress_counts(run_id)
            logger.info(f"DETAIL progress done | run_id={run_id} | {cnt}")

            # build Excel from cache:
            # - для НЕ all: сразу строим xlsx на выбранных secids
            # - для all: строим только если пользователь реально хочет (по умолчанию тоже строим, но может быть большим)
            with Timer(logger, "excel_build"):
                # берём только done (и игнорируем error/pending)
                done_secids = []
                cur = cache._execute(
                    "SELECT secid FROM detail_progress WHERE run_id=? AND status='done' ORDER BY secid",
                    (run_id,),
                )
                done_secids = [r["secid"] for r in cur.fetchall()]
                build_detail_excel_from_cache(cache, logger, bonds_df, done_secids, args.out_detail, args.parse_snippet_chars)

            summ = cache.requests_summary(start_utc)
            logger.info(f"REQUESTS summary since start | total={summ['total']} | errors={summ['errors']}")

    finally:
        cache.close()
        logger.info("FINISH")


if __name__ == "__main__":
    main()