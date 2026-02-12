#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Moex_API.py
- Берём список облигаций с MOEX ISS.
- Кэшируем в SQLite "раз в день" (UTC).
- Пока сохраняем результат в Excel (Moex_Bonds.xlsx).
- Логирование вынесено в logs.py
- SQLite слой вынесен в SQL.py

Запуск:
  python Moex_API.py
  python Moex_API.py --force-refresh
  python Moex_API.py --log-level DEBUG --save-raw
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests

from logs import RunTimer, dump_json, setup_logging, utc_now_iso, utc_today_str
from SQL import SQLiteCache


MOEX_BASE_URL = "https://iss.moex.com/iss"
DEFAULT_OUT_XLSX = "Moex_Bonds.xlsx"


@dataclass
class FetchStats:
    http_calls: int = 0
    rows: int = 0


def moex_get_json(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int,
    retries: int,
    backoff: float,
    logger: logging.Logger,
    save_raw: bool,
    raw_dir: Path,
    raw_tag: str,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            logger.debug("GET %s | status=%s", r.url, r.status_code)
            r.raise_for_status()
            data = r.json()

            if save_raw:
                dump_json(data, raw_dir, tag=f"{raw_tag}_attempt{attempt}", logger=logger)

            return data

        except Exception as e:
            last_exc = e
            logger.warning("HTTP error attempt %d/%d: %r", attempt, retries, e)
            if attempt < retries:
                time.sleep(backoff * (2 ** (attempt - 1)))

    raise RuntimeError(f"MOEX request failed after {retries} attempts. Last error: {last_exc!r}")


def table_to_df(payload: Dict[str, Any], table_name: str) -> pd.DataFrame:
    if table_name not in payload:
        raise KeyError(f"Missing table '{table_name}', got keys={list(payload.keys())}")
    tbl = payload[table_name]
    return pd.DataFrame(tbl.get("data", []), columns=tbl.get("columns", []))


def fetch_bonds_from_moex(
    session: requests.Session,
    logger: logging.Logger,
    stats: FetchStats,
    timeout: int,
    retries: int,
    backoff: float,
    save_raw: bool,
    raw_dir: Path,
) -> pd.DataFrame:
    """
    Важно: MOEX часто отдаёт весь список облигаций одним куском (~3000 строк).
    Пагинация на этом эндпойнте в реальности может игнорироваться.
    Поэтому делаем 1 запрос и берём то, что дали.
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

    payload = moex_get_json(
        session=session,
        url=url,
        params=params,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        logger=logger,
        save_raw=save_raw,
        raw_dir=raw_dir,
        raw_tag="bonds_full",
    )
    stats.http_calls += 1

    df = table_to_df(payload, "securities")
    stats.rows = int(len(df))

    # нормализация дат
    for c in ("ISSUEDATE", "MATDATE"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # флажок по статусу
    if "STATUS" in df.columns:
        df["IS_ACTIVE_STATUS"] = df["STATUS"].astype(str).str.upper().eq("A")

    # дубли
    if "SECID" in df.columns and "BOARDID" in df.columns:
        df = df.drop_duplicates(subset=["SECID", "BOARDID"])
    elif "SECID" in df.columns:
        df = df.drop_duplicates(subset=["SECID"])

    # сортировка
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX bonds -> SQLite cache -> Excel")
    p.add_argument("--out", default=DEFAULT_OUT_XLSX, help="Excel output path")
    p.add_argument("--db", default="moex_cache.sqlite", help="SQLite DB path")
    p.add_argument("--force-refresh", action="store_true", help="Ignore cache and fetch from MOEX")
    p.add_argument("--log-dir", default="logs", help="Log directory")
    p.add_argument("--log-file", default="Moex_API.log", help="Log file name")
    p.add_argument("--log-level", default="INFO", help="INFO/DEBUG/WARNING/ERROR")
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=4, help="HTTP retries")
    p.add_argument("--backoff", type=float, default=0.7, help="Backoff base seconds")
    p.add_argument("--save-raw", action="store_true", help="Save RAW JSON responses")
    p.add_argument("--raw-dir", default="raw", help="RAW directory")
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
            if (not args.force_refresh) and cache.has_snapshot(asof_date):
                info = cache.get_snapshot_info(asof_date)
                logger.info("CACHE HIT | date=%s | rows=%s | created_utc=%s",
                            asof_date, info.rows if info else "?", info.created_utc if info else "?")
                df = cache.load_bonds(asof_date)

                # восстановим типы дат (они хранятся как TEXT)
                for c in ("issuedate", "matdate"):
                    if c in df.columns:
                        df[c] = pd.to_datetime(df[c], errors="coerce")

                source = "sqlite_cache"
                http_calls = 0

            else:
                logger.info("CACHE MISS | date=%s | force_refresh=%s", asof_date, args.force_refresh)
                stats = FetchStats()
                df = fetch_bonds_from_moex(
                    session=session,
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
                    df=df,
                )

            meta = {
                "generated_utc": utc_now_iso(),
                "asof_date_utc": asof_date,
                "source": source,
                "rows": int(len(df)),
                "http_calls": int(http_calls),
                "db": str(Path(args.db).resolve()),
            }

            save_to_excel(df, Path(args.out), logger, meta=meta)

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