#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Moex_API.py
1) Парсит все облигации с MOEX ISS (рынок bonds, engine stock).
2) Сохраняет в Excel (Moex_Bonds.xlsx) с перезаписью.
3) Логирует работу в папку logs (очищает лог при старте).
4) Показывает время исполнения.
5) Дополнительно: ретраи, анти-зацикливание пагинации, метрики, опциональное сохранение RAW-ответов.

Запуск:
  python Moex_API.py
  python Moex_API.py --out Moex_Bonds.xlsx --log-level DEBUG --save-raw

Зависимости:
  pip install requests pandas openpyxl
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


MOEX_BASE_URL = "https://iss.moex.com/iss"
DEFAULT_OUT_XLSX = "Moex_Bonds.xlsx"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILE = "Moex_API.log"


@dataclass
class FetchStats:
    pages: int = 0
    rows: int = 0
    http_calls: int = 0
    started_utc: str = ""
    finished_utc: str = ""


def setup_logging(log_dir: Path, log_file: str, level: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    # очистка предыдущего лога
    if log_path.exists():
        log_path.unlink()

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return log_path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_filename(s: str) -> str:
    keep = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)


def moex_get(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int,
    retries: int,
    backoff: float,
    logger: logging.Logger,
    save_raw_dir: Optional[Path] = None,
    raw_tag: str = "response",
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            logger.debug("GET %s | params=%s | status=%s", r.url, params, r.status_code)
            r.raise_for_status()

            data = r.json()

            if save_raw_dir is not None:
                save_raw_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = safe_filename(f"{ts}_{raw_tag}_attempt{attempt}.json")
                (save_raw_dir / fname).write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            return data
        except Exception as e:
            last_exc = e
            logger.warning("Ошибка запроса (attempt %d/%d): %s", attempt, retries, repr(e))
            if attempt < retries:
                sleep_s = backoff * (2 ** (attempt - 1))
                time.sleep(sleep_s)

    raise RuntimeError(f"Не удалось получить данные после {retries} попыток. Последняя ошибка: {last_exc!r}")


def table_to_df(payload: Dict[str, Any], table_name: str) -> pd.DataFrame:
    """
    MOEX ISS JSON формат:
      { "<table>": { "columns": [...], "data": [[...], ...] }, ... }
    """
    if table_name not in payload:
        raise KeyError(f"В ответе нет таблицы '{table_name}'. Доступно: {list(payload.keys())}")

    tbl = payload[table_name]
    cols = tbl.get("columns", [])
    data = tbl.get("data", [])
    return pd.DataFrame(data, columns=cols)


def fetch_all_bonds(
    session: requests.Session,
    logger: logging.Logger,
    stats: FetchStats,
    page_size: int = 200,
    timeout: int = 30,
    retries: int = 4,
    backoff: float = 0.7,
    save_raw: bool = False,
    raw_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Тянем все бумаги с MOEX:
      /iss/engines/stock/markets/bonds/securities.json

    ВАЖНО: используем табличные параметры пагинации:
      securities.start=<offset>, securities.limit=<page_size>
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

    all_frames: List[pd.DataFrame] = []
    start = 0

    # анти-зацикливание: если снова приходит та же первая запись — пагинация не работает
    prev_first_key = None

    while True:
        params = {
            "iss.meta": "off",
            "iss.only": "securities",
            "securities.columns": ",".join(wanted_columns),
            "securities.start": start,          # <-- ключевое исправление
            "securities.limit": page_size,      # <-- ключевое исправление
        }

        payload = moex_get(
            session=session,
            url=url,
            params=params,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            logger=logger,
            save_raw_dir=(raw_dir if save_raw else None),
            raw_tag=f"bonds_start{start}",
        )
        stats.http_calls += 1

        df = table_to_df(payload, "securities")
        if df.empty:
            logger.info("Пагинация завершена: пустая страница при start=%s", start)
            break

        # анти-зацикливание (по SECID+BOARDID)
        first_key = None
        if "SECID" in df.columns and "BOARDID" in df.columns and len(df) > 0:
            first_key = (str(df.iloc[0]["SECID"]), str(df.iloc[0]["BOARDID"]))
        elif "SECID" in df.columns and len(df) > 0:
            first_key = str(df.iloc[0]["SECID"])

        if prev_first_key is not None and first_key == prev_first_key:
            logger.error(
                "Похоже, пагинация не работает: повтор первой записи %s при start=%s. Останавливаюсь.",
                first_key, start
            )
            break
        prev_first_key = first_key

        all_frames.append(df)
        rows = len(df)
        stats.pages += 1
        stats.rows += rows

        logger.info("Страница %d | start=%d | строк=%d | всего=%d",
                    stats.pages, start, rows, stats.rows)

        # нормальное условие конца: последняя страница меньше limit
        if rows < page_size:
            logger.info("Последняя страница: строк=%d < limit=%d", rows, page_size)
            break

        start += page_size

    if not all_frames:
        return pd.DataFrame()

    out = pd.concat(all_frames, ignore_index=True)

    # нормализация дат
    for c in ["ISSUEDATE", "MATDATE"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    # уберём дубли (на всякий)
    if "SECID" in out.columns and "BOARDID" in out.columns:
        out = out.drop_duplicates(subset=["SECID", "BOARDID"])
    elif "SECID" in out.columns:
        out = out.drop_duplicates(subset=["SECID"])

    # флажок "активный статус" (без жесткой фильтрации)
    if "STATUS" in out.columns:
        out["IS_ACTIVE_STATUS"] = out["STATUS"].astype(str).str.upper().eq("A")

    # сортировка для удобства
    sort_cols = [c for c in ["SECID", "BOARDID"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)

    return out


def save_to_excel(df: pd.DataFrame, out_path: Path, logger: logging.Logger, stats: FetchStats) -> None:
    out_path = out_path.resolve()

    # перезапись всегда
    if out_path.exists():
        out_path.unlink()

    meta = pd.DataFrame(
        [{
            "generated_utc": stats.finished_utc,
            "rows": stats.rows,
            "pages": stats.pages,
            "http_calls": stats.http_calls,
            "source": "MOEX ISS /engines/stock/markets/bonds/securities",
        }]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, index=False, sheet_name="meta")
        df.to_excel(writer, index=False, sheet_name="bonds")

    logger.info("Excel сохранён: %s | rows=%d", out_path, len(df))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX bonds parser -> Excel")
    p.add_argument("--out", default=DEFAULT_OUT_XLSX, help="Путь к Excel файлу")
    p.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Папка логов")
    p.add_argument("--log-file", default=DEFAULT_LOG_FILE, help="Имя лог-файла")
    p.add_argument("--log-level", default="INFO", help="INFO/DEBUG/WARNING/ERROR")
    p.add_argument("--page-size", type=int, default=200, help="Размер страницы MOEX (securities.limit)")
    p.add_argument("--timeout", type=int, default=30, help="Timeout HTTP (сек)")
    p.add_argument("--retries", type=int, default=4, help="Количество ретраев")
    p.add_argument("--backoff", type=float, default=0.7, help="Backoff база (сек)")
    p.add_argument("--save-raw", action="store_true", help="Сохранять RAW ответы MOEX (для отладки)")
    p.add_argument("--raw-dir", default="raw", help="Папка для RAW ответов (если --save-raw)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    log_path = setup_logging(Path(args.log_dir), args.log_file, args.log_level)
    logger = logging.getLogger("Moex_API")

    stats = FetchStats(started_utc=utc_now_iso())

    t0 = time.perf_counter()
    logger.info("START | utc=%s | log=%s", stats.started_utc, log_path.resolve())

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Moex_API.py / moex-iss-client",
        "Accept": "application/json",
    })

    try:
        df = fetch_all_bonds(
            session=session,
            logger=logger,
            stats=stats,
            page_size=args.page_size,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            save_raw=bool(args.save_raw),
            raw_dir=Path(args.raw_dir) if args.save_raw else None,
        )

        stats.finished_utc = utc_now_iso()

        logger.info("Итог: rows=%d | pages=%d | http_calls=%d", stats.rows, stats.pages, stats.http_calls)
        if df.empty:
            logger.warning("ВНИМАНИЕ: DF пустой. Проверь доступность ISS или параметры.")
        else:
            if "BOARDID" in df.columns:
                logger.info("Уникальных BOARDID: %d", df["BOARDID"].nunique(dropna=True))
            if "SECID" in df.columns:
                logger.info("Уникальных SECID: %d", df["SECID"].nunique(dropna=True))

        save_to_excel(df, Path(args.out), logger, stats)

        elapsed = time.perf_counter() - t0
        logger.info("FINISH | utc=%s | elapsed=%.3fs", stats.finished_utc, elapsed)
        print(f"\nГотово. Время исполнения: {elapsed:.3f} сек\n")
        return 0

    except Exception:
        logger.exception("Критическая ошибка выполнения")
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())