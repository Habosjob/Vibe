#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Moex_API.py
- Получаем список облигаций с MOEX ISS.
- Кэшируем в SQLite раз в день (UTC).
- requests_log: лог всех HTTP запросов в SQLite.
- Детализация (sample N):
  1) /iss/securities/{SECID}.json
  2) /iss/securities/{SECID}/bondization.json  (coupons/amortizations/offers)
  Всё сохраняем в SQLite (RAW + нормализация) и временно в Excel:
    - Moex_Bonds.xlsx
    - Moex_Bonds_Detail.xlsx (перезапись)

Запуск:
  python Moex_API.py
  python Moex_API.py --force-refresh
  python Moex_API.py --detail-sample 10 --save-raw --log-level DEBUG
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

from logs import RunTimer, dump_json, json_dumps_compact, setup_logging, utc_now_iso, utc_today_str
from SQL import SQLiteCache


MOEX_BASE_URL = "https://iss.moex.com/iss"
DEFAULT_OUT_XLSX = "Moex_Bonds.xlsx"
DEFAULT_OUT_DETAIL_XLSX = "Moex_Bonds_Detail.xlsx"


@dataclass
class FetchStats:
    http_calls: int = 0
    rows: int = 0


def table_to_df(payload: Dict[str, Any], table_name: str) -> pd.DataFrame:
    if table_name not in payload:
        return pd.DataFrame()
    tbl = payload[table_name]
    return pd.DataFrame(tbl.get("data", []), columns=tbl.get("columns", []))


def moex_get_json_logged(
    session: requests.Session,
    cache: SQLiteCache,
    logger: logging.Logger,
    url: str,
    params: Dict[str, Any],
    timeout: int,
    retries: int,
    backoff: float,
    save_raw: bool,
    raw_dir: Path,
    raw_tag: str,
) -> Tuple[Dict[str, Any], int, float, int]:
    """
    GET JSON с ретраями + запись в requests_log.
    Возвращает: payload, status_code, elapsed_ms, response_size_bytes
    """
    params_json = json_dumps_compact(params) if params else None
    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        status_code: Optional[int] = None
        t0 = time.perf_counter()
        final_url_for_log = url

        try:
            r = session.get(url, params=params, timeout=timeout)
            status_code = r.status_code
            final_url_for_log = r.url
            r.raise_for_status()

            data = r.json()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            try:
                size = int(r.headers.get("Content-Length") or 0)
            except Exception:
                size = 0
            if size <= 0:
                size = len(json.dumps(data, ensure_ascii=False).encode("utf-8"))

            cache.log_request(
                created_utc=utc_now_iso(),
                url=str(final_url_for_log),
                params_json=params_json,
                status_code=int(status_code),
                elapsed_ms=float(elapsed_ms),
                response_size=int(size),
                error=None,
            )

            logger.debug("GET ok | %s | status=%s | %.1f ms | %d bytes", final_url_for_log, status_code, elapsed_ms, size)

            if save_raw:
                dump_json(data, raw_dir, tag=f"{raw_tag}_attempt{attempt}", logger=logger)

            return data, int(status_code), float(elapsed_ms), int(size)

        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            last_exc = e

            cache.log_request(
                created_utc=utc_now_iso(),
                url=str(final_url_for_log),
                params_json=params_json,
                status_code=status_code,
                elapsed_ms=float(elapsed_ms),
                response_size=None,
                error=repr(e),
            )

            logger.warning("HTTP error attempt %d/%d | %s | %r", attempt, retries, url, e)
            if attempt < retries:
                time.sleep(backoff * (2 ** (attempt - 1)))

    raise RuntimeError(f"MOEX request failed after {retries} attempts. Last error: {last_exc!r}")


def fetch_bonds_from_moex(
    session: requests.Session,
    cache: SQLiteCache,
    logger: logging.Logger,
    stats: FetchStats,
    timeout: int,
    retries: int,
    backoff: float,
    save_raw: bool,
    raw_dir: Path,
) -> pd.DataFrame:
    """
    На практике /engines/stock/markets/bonds/securities.json часто отдаёт весь список одним куском.
    Поэтому делаем 1 запрос.
    """
    url = f"{MOEX_BASE_URL}/engines/stock/markets/bonds/securities.json"
    wanted_columns = [
        "SECID", "BOARDID", "SHORTNAME", "NAME", "ISIN", "REGNUMBER",
        "STATUS", "LISTLEVEL",
        "ISSUEDATE", "MATDATE",
        "FACEVALUE", "FACEUNIT",
        "LOTSIZE",
        "COUPONPERCENT", "COUPONVALUE", "COUPONPERIOD",
    ]
    params = {
        "iss.meta": "off",
        "iss.only": "securities",
        "securities.columns": ",".join(wanted_columns),
    }

    payload, _, _, _ = moex_get_json_logged(
        session=session,
        cache=cache,
        logger=logger,
        url=url,
        params=params,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        save_raw=save_raw,
        raw_dir=raw_dir,
        raw_tag="bonds_full",
    )
    stats.http_calls += 1

    df = table_to_df(payload, "securities")
    stats.rows = int(len(df))

    for c in ("ISSUEDATE", "MATDATE"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "STATUS" in df.columns:
        df["IS_ACTIVE_STATUS"] = df["STATUS"].astype(str).str.upper().eq("A")

    if "SECID" in df.columns and "BOARDID" in df.columns:
        df = df.drop_duplicates(subset=["SECID", "BOARDID"])
    elif "SECID" in df.columns:
        df = df.drop_duplicates(subset=["SECID"])

    sort_cols = [c for c in ("SECID", "BOARDID") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


def save_to_excel(df: pd.DataFrame, out_path: Path, logger: logging.Logger, meta: Dict[str, Any]) -> None:
    out_path = out_path.resolve()
    if out_path.exists():
        out_path.unlink()

    meta_df = pd.DataFrame([meta])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta_df.to_excel(writer, index=False, sheet_name="meta")
        df.to_excel(writer, index=False, sheet_name="bonds")
    logger.info("Excel saved: %s | rows=%d", out_path, len(df))


def fetch_security_detail(
    session: requests.Session,
    cache: SQLiteCache,
    logger: logging.Logger,
    asof_date_utc: str,
    secid: str,
    timeout: int,
    retries: int,
    backoff: float,
    save_raw: bool,
    raw_dir: Path,
    lang: str = "ru",
) -> Dict[str, Any]:
    url = f"{MOEX_BASE_URL}/securities/{secid}.json"
    params = {"iss.meta": "off", "lang": lang}

    payload, _, _, _ = moex_get_json_logged(
        session=session,
        cache=cache,
        logger=logger,
        url=url,
        params=params,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        save_raw=save_raw,
        raw_dir=raw_dir,
        raw_tag=f"security_{secid}",
    )

    cache.save_bond_raw(
        asof_date_utc=asof_date_utc,
        secid=secid,
        kind="security",
        fetched_utc=utc_now_iso(),
        url=url,
        params_json=json_dumps_compact(params),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    return payload


def fetch_bondization_detail(
    session: requests.Session,
    cache: SQLiteCache,
    logger: logging.Logger,
    asof_date_utc: str,
    secid: str,
    timeout: int,
    retries: int,
    backoff: float,
    save_raw: bool,
    raw_dir: Path,
) -> Dict[str, Any]:
    url = f"{MOEX_BASE_URL}/securities/{secid}/bondization.json"
    params = {
        "iss.meta": "off",
        "iss.only": "coupons,amortizations,offers",
    }

    payload, _, _, _ = moex_get_json_logged(
        session=session,
        cache=cache,
        logger=logger,
        url=url,
        params=params,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        save_raw=save_raw,
        raw_dir=raw_dir,
        raw_tag=f"bondization_{secid}",
    )

    cache.save_bond_raw(
        asof_date_utc=asof_date_utc,
        secid=secid,
        kind="bondization",
        fetched_utc=utc_now_iso(),
        url=url,
        params_json=json_dumps_compact(params),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    return payload


def persist_security_payload(
    cache: SQLiteCache,
    asof_date_utc: str,
    secid: str,
    payload: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Из /securities/{secid}.json:
    - description -> bond_description
    - часть табличных блоков -> bond_events (универсально)
    Возвращаем DF для Excel: desc_df, events_df
    """
    desc_df = table_to_df(payload, "description")
    if not desc_df.empty:
        cache.replace_bond_description(asof_date_utc, secid, desc_df)

    blocks = ["boards", "marketdata", "securities"]
    events_rows = []
    for block in blocks:
        dfb = table_to_df(payload, block)
        if dfb.empty:
            continue

        rows_json = []
        for _, row in dfb.iterrows():
            d = row.to_dict()
            rows_json.append(json.dumps(d, ensure_ascii=False, separators=(",", ":")))
            events_rows.append({"SECID": secid, "block": block, **d})

        cache.replace_bond_events(asof_date_utc, secid, block, rows_json)

    events_df = pd.DataFrame(events_rows) if events_rows else pd.DataFrame()
    return desc_df, events_df


def _to_iso_date(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    # как правило MOEX отдаёт YYYY-MM-DD; не будем усложнять — просто вернём строку
    return s


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def persist_bondization_payload(
    cache: SQLiteCache,
    asof_date_utc: str,
    secid: str,
    payload: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Из /bondization.json:
    - coupons/offers/amortizations -> отдельные таблицы + дублируем в bond_events
    Возвращаем DF для Excel: coupons_df, offers_df, amort_df, events_extra_df
    """
    coupons_df = table_to_df(payload, "coupons")
    offers_df = table_to_df(payload, "offers")
    amort_df = table_to_df(payload, "amortizations")

    events_extra_rows = []

    # coupons
    if not coupons_df.empty:
        rows = []
        rows_json = []
        for _, row in coupons_df.iterrows():
            d = row.to_dict()
            rj = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
            rows_json.append(rj)

            rows.append({
                "coupondate": _to_iso_date(d.get("COUPONDATE") or d.get("coupondate")),
                "startdate": _to_iso_date(d.get("STARTDATE") or d.get("startdate")),
                "enddate": _to_iso_date(d.get("ENDDATE") or d.get("enddate")),
                "value": _to_float(d.get("VALUE") or d.get("value") or d.get("COUPONVALUE") or d.get("couponvalue")),
                "percent": _to_float(d.get("PERCENT") or d.get("percent") or d.get("COUPONPERCENT") or d.get("couponpercent")),
                "currency": (d.get("CURRENCY") or d.get("currency") or d.get("FACEUNIT") or d.get("faceunit")),
            })

            events_extra_rows.append({"SECID": secid, "block": "coupons", **d})

        cache.replace_bond_coupons(asof_date_utc, secid, rows, rows_json)
        cache.replace_bond_events(asof_date_utc, secid, "coupons", rows_json)

    # offers
    if not offers_df.empty:
        rows = []
        rows_json = []
        for _, row in offers_df.iterrows():
            d = row.to_dict()
            rj = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
            rows_json.append(rj)

            rows.append({
                "offerdate": _to_iso_date(d.get("OFFERDATE") or d.get("offerdate") or d.get("DATE") or d.get("date")),
                "offertype": (d.get("OFFERTYPE") or d.get("offertype") or d.get("TYPE") or d.get("type")),
                "price": _to_float(d.get("PRICE") or d.get("price")),
                "currency": (d.get("CURRENCY") or d.get("currency")),
            })

            events_extra_rows.append({"SECID": secid, "block": "offers", **d})

        cache.replace_bond_offers(asof_date_utc, secid, rows, rows_json)
        cache.replace_bond_events(asof_date_utc, secid, "offers", rows_json)

    # amortizations
    if not amort_df.empty:
        rows = []
        rows_json = []
        for _, row in amort_df.iterrows():
            d = row.to_dict()
            rj = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
            rows_json.append(rj)

            rows.append({
                "amortdate": _to_iso_date(d.get("AMORTDATE") or d.get("amortdate") or d.get("DATE") or d.get("date")),
                "value": _to_float(d.get("VALUE") or d.get("value")),
                "percent": _to_float(d.get("PERCENT") or d.get("percent")),
                "currency": (d.get("CURRENCY") or d.get("currency") or d.get("FACEUNIT") or d.get("faceunit")),
            })

            events_extra_rows.append({"SECID": secid, "block": "amortizations", **d})

        cache.replace_bond_amortizations(asof_date_utc, secid, rows, rows_json)
        cache.replace_bond_events(asof_date_utc, secid, "amortizations", rows_json)

    events_extra_df = pd.DataFrame(events_extra_rows) if events_extra_rows else pd.DataFrame()
    return coupons_df, offers_df, amort_df, events_extra_df


def make_detail_excel(
    out_path: Path,
    logger: logging.Logger,
    meta: Dict[str, Any],
    all_desc: pd.DataFrame,
    all_events: pd.DataFrame,
    all_coupons: pd.DataFrame,
    all_offers: pd.DataFrame,
    all_amort: pd.DataFrame,
) -> None:
    out_path = out_path.resolve()
    if out_path.exists():
        out_path.unlink()

    meta_df = pd.DataFrame([meta])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta_df.to_excel(writer, index=False, sheet_name="meta")
        (all_desc if not all_desc.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="description")
        (all_events if not all_events.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="events")
        (all_coupons if not all_coupons.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="coupons")
        (all_offers if not all_offers.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="offers")
        (all_amort if not all_amort.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="amortizations")

    logger.info(
        "Detail Excel saved: %s | desc=%d | events=%d | coupons=%d | offers=%d | amort=%d",
        out_path, len(all_desc), len(all_events), len(all_coupons), len(all_offers), len(all_amort)
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX bonds -> SQLite cache -> Excel (+ details + bondization)")
    p.add_argument("--out", default=DEFAULT_OUT_XLSX, help="Base Excel output path")
    p.add_argument("--out-detail", default=DEFAULT_OUT_DETAIL_XLSX, help="Detail Excel output path")
    p.add_argument("--db", default="moex_cache.sqlite", help="SQLite DB path")
    p.add_argument("--force-refresh", action="store_true", help="Ignore cache and fetch from MOEX")
    p.add_argument("--log-dir", default="logs", help="Log directory")
    p.add_argument("--log-file", default="Moex_API.log", help="Log file name")
    p.add_argument("--log-level", default="INFO", help="INFO/DEBUG/WARNING/ERROR")
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=4, help="HTTP retries")
    p.add_argument("--backoff", type=float, default=0.7, help="Backoff base seconds")
    p.add_argument("--save-raw", action="store_true", help="Save RAW JSON responses to disk")
    p.add_argument("--raw-dir", default="raw", help="RAW directory")
    p.add_argument("--detail-sample", type=int, default=10, help="How many random bonds to fetch details for")
    p.add_argument("--detail-seed", type=int, default=42, help="Random seed for sampling")
    p.add_argument("--lang", default="ru", help="MOEX ISS language (ru/en)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    log_path = setup_logging(args.log_dir, args.log_file, args.log_level, clear_previous=True, also_console=True)
    logger = logging.getLogger("Moex_API")

    run_started_utc = utc_now_iso()
    logger.info("START | utc=%s | log=%s", run_started_utc, log_path.resolve())

    cache = SQLiteCache(args.db, logger=logging.getLogger("SQLiteCache"))
    asof_date = utc_today_str()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Moex_API.py / moex-iss-client",
        "Accept": "application/json",
    })

    try:
        with RunTimer("total", logger=logger) as tt:
            # --- base list ---
            if (not args.force_refresh) and cache.has_snapshot(asof_date):
                info = cache.get_snapshot_info(asof_date)
                logger.info("CACHE HIT | date=%s | rows=%s | created_utc=%s",
                            asof_date, info.rows if info else "?", info.created_utc if info else "?")
                df_bonds = cache.load_bonds(asof_date)

                for c in ("issuedate", "matdate"):
                    if c in df_bonds.columns:
                        df_bonds[c] = pd.to_datetime(df_bonds[c], errors="coerce")

                source = "sqlite_cache"
                http_calls = 0
            else:
                logger.info("CACHE MISS | date=%s | force_refresh=%s", asof_date, args.force_refresh)
                stats = FetchStats()
                df_bonds = fetch_bonds_from_moex(
                    session=session,
                    cache=cache,
                    logger=logger,
                    stats=stats,
                    timeout=args.timeout,
                    retries=args.retries,
                    backoff=args.backoff,
                    save_raw=bool(args.save_raw),
                    raw_dir=Path(args.raw_dir),
                )
                source = "moex_iss"
                http_calls = stats.http_calls

                cache.save_bonds_snapshot(
                    asof_date_utc=asof_date,
                    created_utc=utc_now_iso(),
                    df=df_bonds,
                )

            base_meta = {
                "generated_utc": utc_now_iso(),
                "asof_date_utc": asof_date,
                "source": source,
                "rows": int(len(df_bonds)),
                "http_calls": int(http_calls),
                "db": str(Path(args.db).resolve()),
            }
            save_to_excel(df_bonds, Path(args.out), logger, meta=base_meta)

            # --- detail sample ---
            if df_bonds.empty:
                logger.warning("bonds DF пустой — detail пропущен.")
            else:
                secid_col = next((c for c in df_bonds.columns if c.lower() == "secid"), None)
                if not secid_col:
                    logger.warning("Нет колонки SECID — detail пропущен.")
                else:
                    secids = [str(x) for x in df_bonds[secid_col].dropna().unique().tolist()]
                    if not secids:
                        logger.warning("Список SECID пуст — detail пропущен.")
                    else:
                        k = min(int(args.detail_sample), len(secids))
                        random.seed(int(args.detail_seed))
                        sample_secids = random.sample(secids, k=k)

                        logger.info("DETAIL sample | k=%d | seed=%s", k, args.detail_seed)

                        desc_frames = []
                        events_frames = []
                        coupons_frames = []
                        offers_frames = []
                        amort_frames = []

                        with RunTimer("detail_fetch", logger=logger):
                            for i, secid in enumerate(sample_secids, start=1):
                                logger.info("DETAIL %d/%d | %s", i, k, secid)

                                # 1) security detail
                                sec_payload = fetch_security_detail(
                                    session=session,
                                    cache=cache,
                                    logger=logger,
                                    asof_date_utc=asof_date,
                                    secid=secid,
                                    timeout=args.timeout,
                                    retries=args.retries,
                                    backoff=args.backoff,
                                    save_raw=bool(args.save_raw),
                                    raw_dir=Path(args.raw_dir),
                                    lang=args.lang,
                                )
                                desc_df, events_df = persist_security_payload(cache, asof_date, secid, sec_payload)
                                if not desc_df.empty:
                                    d = desc_df.copy()
                                    d.insert(0, "SECID", secid)
                                    desc_frames.append(d)
                                if not events_df.empty:
                                    events_frames.append(events_df)

                                # 2) bondization detail
                                bond_payload = fetch_bondization_detail(
                                    session=session,
                                    cache=cache,
                                    logger=logger,
                                    asof_date_utc=asof_date,
                                    secid=secid,
                                    timeout=args.timeout,
                                    retries=args.retries,
                                    backoff=args.backoff,
                                    save_raw=bool(args.save_raw),
                                    raw_dir=Path(args.raw_dir),
                                )
                                cpn_df, off_df, am_df, extra_events_df = persist_bondization_payload(cache, asof_date, secid, bond_payload)

                                if not cpn_df.empty:
                                    d = cpn_df.copy()
                                    d.insert(0, "SECID", secid)
                                    coupons_frames.append(d)
                                if not off_df.empty:
                                    d = off_df.copy()
                                    d.insert(0, "SECID", secid)
                                    offers_frames.append(d)
                                if not am_df.empty:
                                    d = am_df.copy()
                                    d.insert(0, "SECID", secid)
                                    amort_frames.append(d)
                                if not extra_events_df.empty:
                                    events_frames.append(extra_events_df)

                        all_desc = pd.concat(desc_frames, ignore_index=True) if desc_frames else pd.DataFrame()
                        all_events = pd.concat(events_frames, ignore_index=True) if events_frames else pd.DataFrame()
                        all_coupons = pd.concat(coupons_frames, ignore_index=True) if coupons_frames else pd.DataFrame()
                        all_offers = pd.concat(offers_frames, ignore_index=True) if offers_frames else pd.DataFrame()
                        all_amort = pd.concat(amort_frames, ignore_index=True) if amort_frames else pd.DataFrame()

                        # summary по requests_log
                        rq = cache.requests_summary_since(run_started_utc)
                        logger.info("REQUESTS summary since start | total=%d | errors=%d", rq.total, rq.errors)

                        detail_meta = {
                            "generated_utc": utc_now_iso(),
                            "asof_date_utc": asof_date,
                            "detail_sample": int(k),
                            "detail_seed": int(args.detail_seed),
                            "lang": args.lang,
                            "db": str(Path(args.db).resolve()),
                            "requests_total_since_start": rq.total,
                            "requests_errors_since_start": rq.errors,
                        }

                        make_detail_excel(
                            Path(args.out_detail),
                            logger,
                            detail_meta,
                            all_desc,
                            all_events,
                            all_coupons,
                            all_offers,
                            all_amort,
                        )

        logger.info("FINISH | elapsed=%.3fs", tt.elapsed)
        print(f"\nГотово. Время исполнения: {tt.elapsed:.3f} сек\n")
        return 0

    except Exception:
        logger.exception("Критическая ошибка выполнения")
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())