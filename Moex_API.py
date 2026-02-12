#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Moex_API.py
- Получаем список облигаций с MOEX ISS.
- Кэшируем в SQLite раз в день (UTC).
- Пишем requests_log в SQLite по каждому HTTP запросу.
- Детализация: берём 10 рандомных бондов из SQLite и тянем /iss/securities/{SECID}.json
  Сохраняем RAW + description + табличные блоки (coupons/amortizations/offers/...) в SQLite.
- Экспортируем:
  - Moex_Bonds.xlsx (базовый список)
  - Moex_Bonds_Detail.xlsx (детализация, перезапись)

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
    Делает GET JSON с ретраями + пишет запись в requests_log.
    Возвращает: payload, status_code, elapsed_ms, response_size_bytes
    """
    params_json = json_dumps_compact(params) if params else None
    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        status_code: Optional[int] = None
        t0 = time.perf_counter()
        try:
            r = session.get(url, params=params, timeout=timeout)
            status_code = r.status_code
            r.raise_for_status()

            data = r.json()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            # оценка размера
            try:
                size = int(r.headers.get("Content-Length") or 0)
            except Exception:
                size = 0
            if size <= 0:
                # fallback: сериализуем (дороже, но для 10 запросов ок)
                size = len(json.dumps(data, ensure_ascii=False).encode("utf-8"))

            cache.log_request(
                created_utc=utc_now_iso(),
                url=str(r.url),
                params_json=params_json,
                status_code=status_code,
                elapsed_ms=elapsed_ms,
                response_size=size,
                error=None,
            )

            logger.debug("GET ok | %s | status=%s | %.1f ms | %d bytes", r.url, status_code, elapsed_ms, size)

            if save_raw:
                dump_json(data, raw_dir, tag=f"{raw_tag}_attempt{attempt}", logger=logger)

            return data, int(status_code), float(elapsed_ms), int(size)

        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            last_exc = e
            err_text = repr(e)

            cache.log_request(
                created_utc=utc_now_iso(),
                url=url,
                params_json=params_json,
                status_code=status_code,
                elapsed_ms=elapsed_ms,
                response_size=None,
                error=err_text,
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
    На практике этот эндпойнт часто отдаёт весь список облигаций одним куском (~3000 строк).
    Поэтому делаем 1 запрос и сохраняем то, что дали.
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


def fetch_bond_detail(
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
        raw_tag=f"detail_{secid}",
    )

    # RAW в БД
    cache.save_bond_raw(
        asof_date_utc=asof_date_utc,
        secid=secid,
        fetched_utc=utc_now_iso(),
        url=url,
        params_json=json_dumps_compact(params),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )

    return payload


def persist_detail_tables(
    cache: SQLiteCache,
    asof_date_utc: str,
    secid: str,
    payload: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Сохраняем:
      - description -> bond_description
      - табличные блоки (если есть): coupons/amortizations/offers/...) -> bond_events
    Возвращаем DF для Excel:
      desc_df, events_df (flatten)
    """
    desc_df = table_to_df(payload, "description")
    if not desc_df.empty:
        cache.replace_bond_description(asof_date_utc, secid, desc_df)

    # универсально складываем любые известные блоки как JSON-строки
    blocks = ["coupons", "amortizations", "offers", "boards", "marketdata", "securities"]
    events_rows = []
    for block in blocks:
        dfb = table_to_df(payload, block)
        if dfb.empty:
            continue
        # каждую строку - в JSON
        rows_json = []
        for _, row in dfb.iterrows():
            rj = json.dumps(row.to_dict(), ensure_ascii=False, separators=(",", ":"))
            rows_json.append(rj)
            events_rows.append({"secid": secid, "block": block, **row.to_dict()})
        cache.replace_bond_events(asof_date_utc, secid, block, rows_json)

    events_df = pd.DataFrame(events_rows) if events_rows else pd.DataFrame()
    return desc_df, events_df


def make_detail_excel(
    out_path: Path,
    logger: logging.Logger,
    meta: Dict[str, Any],
    all_desc: pd.DataFrame,
    all_events: pd.DataFrame,
) -> None:
    out_path = out_path.resolve()
    if out_path.exists():
        out_path.unlink()

    meta_df = pd.DataFrame([meta])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta_df.to_excel(writer, index=False, sheet_name="meta")
        (all_desc if not all_desc.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="description")
        (all_events if not all_events.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="events")

    logger.info("Detail Excel saved: %s | desc_rows=%d | event_rows=%d",
                out_path, len(all_desc), len(all_events))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX bonds -> SQLite cache -> Excel (+ details sample)")
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

    logger.info("START | utc=%s | log=%s", utc_now_iso(), log_path.resolve())

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
            if df_bonds.empty or "secid" not in [c.lower() for c in df_bonds.columns]:
                logger.warning("Нет данных bonds или нет колонки SECID — detail пропущен.")
            else:
                # secid список
                secid_col = None
                for c in df_bonds.columns:
                    if c.lower() == "secid":
                        secid_col = c
                        break
                assert secid_col is not None

                secids = [str(x) for x in df_bonds[secid_col].dropna().unique().tolist()]
                if not secids:
                    logger.warning("Список SECID пуст — detail пропущен.")
                else:
                    k = min(int(args.detail_sample), len(secids))
                    random.seed(int(args.detail_seed))
                    sample_secids = random.sample(secids, k=k)

                    logger.info("DETAIL sample | k=%d | seed=%s", k, args.detail_seed)
                    all_desc_frames = []
                    all_events_frames = []

                    with RunTimer("detail_fetch", logger=logger):
                        for i, secid in enumerate(sample_secids, start=1):
                            logger.info("DETAIL %d/%d | %s", i, k, secid)
                            payload = fetch_bond_detail(
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
                            desc_df, events_df = persist_detail_tables(cache, asof_date, secid, payload)
                            if not desc_df.empty:
                                desc_df = desc_df.copy()
                                desc_df.insert(0, "SECID", secid)
                                all_desc_frames.append(desc_df)
                            if not events_df.empty:
                                all_events_frames.append(events_df)

                    all_desc = pd.concat(all_desc_frames, ignore_index=True) if all_desc_frames else pd.DataFrame()
                    all_events = pd.concat(all_events_frames, ignore_index=True) if all_events_frames else pd.DataFrame()

                    detail_meta = {
                        "generated_utc": utc_now_iso(),
                        "asof_date_utc": asof_date,
                        "detail_sample": int(k),
                        "detail_seed": int(args.detail_seed),
                        "lang": args.lang,
                        "db": str(Path(args.db).resolve()),
                    }
                    make_detail_excel(Path(args.out_detail), logger, detail_meta, all_desc, all_events)

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