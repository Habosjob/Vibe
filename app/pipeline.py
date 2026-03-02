from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from openpyxl import Workbook
from tqdm import tqdm

import config
from app.bootstrap import load_state, save_state
from app.database import Database

LOGGER = logging.getLogger(__name__)


@dataclass
class RunSummary:
    fetched_count: int
    selected_count: int
    saved_count: int
    errors_count: int
    from_cache_count: int
    duration_total: float
    duration_load: float
    duration_calc: float
    duration_save: float
    output_path: Path


async def _fake_fetch(secid: str, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        await asyncio.sleep(0.1)
        return {
            "secid": secid,
            "name": f"Bond {secid}",
            "coupon_rate": 4.0 + (int(secid) % 7),
            "maturity_years": 1 + (int(secid) % 20),
            "rating": "BBB",
        }


def _hash_record(record: dict) -> str:
    data = json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _load_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    age = time.time() - cache_path.stat().st_mtime
    if age > config.CACHE_TTL_SEC:
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache_path: Path, payload: dict) -> None:
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _filter_bonds(rows: list[dict]) -> list[dict]:
    selected: list[dict] = []
    for row in rows:
        if row["coupon_rate"] >= config.MIN_COUPON_RATE and row["maturity_years"] <= config.MAX_MATURITY_YEARS:
            selected.append(row)
    return selected


def _save_excel(rows: list[dict], output_path: Path) -> int:
    wb = Workbook()
    ws = wb.active
    ws.title = config.EXCEL_SHEET_NAME
    ws.append(["SECID", "Название", "Купон", "Срок до погашения (лет)", "Рейтинг"])
    for row in rows:
        ws.append([row["secid"], row["name"], row["coupon_rate"], row["maturity_years"], row["rating"]])
    wb.save(output_path)
    return len(rows)


async def run_pipeline(db: Database) -> RunSummary:
    total_start = time.perf_counter()
    state = load_state()
    cache_path = config.get_cache_file_path()
    cache = _load_cache(cache_path)

    secids = [str(i) for i in range(1, 26)]
    to_process = [x for x in secids if x not in state.get("processed_ids", [])]
    processed = list(state.get("processed_ids", []))

    load_start = time.perf_counter()
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_TASKS)
    fetched_rows: list[dict] = []
    from_cache_count = 0

    tasks = []
    for secid in to_process:
        if secid in cache:
            fetched_rows.append(cache[secid])
            from_cache_count += 1
        else:
            tasks.append(_fake_fetch(secid, semaphore))

    if tasks:
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Загрузка данных", unit="обл"):
            row = await coro
            fetched_rows.append(row)
            processed.append(row["secid"])
            save_state({"processed_ids": processed, "last_stage": "load"})

    for row in fetched_rows:
        cache[row["secid"]] = row
    _save_cache(cache_path, cache)

    duration_load = time.perf_counter() - load_start

    calc_start = time.perf_counter()
    selected = _filter_bonds(fetched_rows)
    now = datetime.now(timezone.utc).isoformat()
    db_rows = [
        (
            row["secid"],
            row["name"],
            float(row["coupon_rate"]),
            int(row["maturity_years"]),
            row["rating"],
            _hash_record(row),
            now,
        )
        for row in selected
    ]
    db.upsert_bonds(db_rows)
    save_state({"processed_ids": processed, "last_stage": "calc"})
    duration_calc = time.perf_counter() - calc_start

    save_start = time.perf_counter()
    output_path = config.get_output_file_path()
    saved_count = _save_excel(selected, output_path)
    save_state({"processed_ids": processed, "last_stage": "done"})
    duration_save = time.perf_counter() - save_start

    duration_total = time.perf_counter() - total_start

    return RunSummary(
        fetched_count=len(fetched_rows),
        selected_count=len(selected),
        saved_count=saved_count,
        errors_count=0,
        from_cache_count=from_cache_count,
        duration_total=duration_total,
        duration_load=duration_load,
        duration_calc=duration_calc,
        duration_save=duration_save,
        output_path=output_path,
    )
