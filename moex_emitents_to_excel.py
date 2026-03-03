from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

BASE_URL = "https://iss.moex.com/iss"
PAGE_LIMIT = 500
TIMEOUT = 30
MAX_WORKERS = 10
OUTPUT_FILE = Path("moex_emitents.xlsx")  # Перезаписываемый файл по требованию.


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_block(payload: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
    block = payload.get(block_name)
    if not isinstance(block, dict):
        return []

    columns = block.get("columns", [])
    rows = block.get("data", [])
    if not columns or not rows:
        return []

    return [dict(zip(columns, row)) for row in rows]


def _extract_cursor(payload: dict[str, Any], block_name: str) -> dict[str, Any] | None:
    cursor_block = payload.get(f"{block_name}.cursor")
    if not isinstance(cursor_block, dict):
        return None

    columns = cursor_block.get("columns", [])
    rows = cursor_block.get("data", [])
    if not columns or not rows:
        return None

    return dict(zip(columns, rows[0]))


def _fetch_json(session: requests.Session, path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(f"{BASE_URL}/{path}", params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_paginated_block(
    session: requests.Session,
    path: str,
    block_name: str,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    start = 0
    params: dict[str, Any] = {"iss.meta": "off", "limit": PAGE_LIMIT}
    if extra_params:
        params.update(extra_params)

    effective_limit = int(params.get("limit", PAGE_LIMIT))

    with tqdm(desc=f"Загрузка {block_name}", unit="строк", leave=False) as progress:
        while True:
            params["start"] = start
            payload = _fetch_json(session, path, params)
            chunk = _get_block(payload, block_name)
            records.extend(chunk)
            progress.update(len(chunk))

            cursor = _extract_cursor(payload, block_name)
            if cursor:
                total = int(cursor.get("TOTAL", 0) or cursor.get("total", 0) or 0)
                pagesize = int(cursor.get("PAGESIZE", 0) or cursor.get("pagesize", 0) or effective_limit)
                if total and progress.total != total:
                    progress.total = total
                    progress.refresh()

                start += pagesize
                if start >= total:
                    break
                continue

            if len(chunk) < effective_limit:
                break

            start += effective_limit

    return records


def is_russian_moex_emitent(row: dict[str, Any]) -> bool:
    """
    Фильтрация под задачу:
    - только реально торгующиеся инструменты MOEX (`is_traded == 1`),
    - только эмитенты с российским ИНН (10 цифр),
    - должен быть emitent_id.
    """
    if row.get("is_traded") != 1:
        return False

    if row.get("emitent_id") is None:
        return False

    inn_raw = row.get("emitent_inn")
    if inn_raw is None:
        return False

    inn = "".join(ch for ch in str(inn_raw) if ch.isdigit())
    if len(inn) != 10:
        return False

    return True


def fetch_emitent_related_blocks(session: requests.Session, emitent_id: int) -> dict[str, list[dict[str, Any]]]:
    block_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    start = 0

    while True:
        params = {
            "iss.meta": "off",
            "limit": PAGE_LIMIT,
            "start": start,
            "emitent_id": emitent_id,
        }
        payload = _fetch_json(session, "securities.json", params)

        for key in payload.keys():
            if key.endswith(".cursor"):
                continue

            rows = _get_block(payload, key)
            if not rows:
                continue

            for row in rows:
                row["emitent_id"] = emitent_id
            block_records[key].extend(rows)

        securities_chunk = _get_block(payload, "securities")
        cursor = _extract_cursor(payload, "securities")

        if cursor:
            total = int(cursor.get("TOTAL", 0) or cursor.get("total", 0) or 0)
            pagesize = int(cursor.get("PAGESIZE", 0) or cursor.get("pagesize", 0) or PAGE_LIMIT)
            start += pagesize
            if start >= total:
                break
            continue

        if len(securities_chunk) < PAGE_LIMIT:
            break

        start += PAGE_LIMIT

    return block_records


def _fetch_one_emitent(emitent_id: int) -> dict[str, list[dict[str, Any]]]:
    worker_session = _build_session()
    try:
        return fetch_emitent_related_blocks(worker_session, emitent_id)
    finally:
        worker_session.close()


def save_to_excel(file_name: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            if df.empty:
                continue
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def main() -> None:
    session = _build_session()
    try:
        print("Шаг 1/4: Загружаем общий список бумаг MOEX...")
        securities = fetch_paginated_block(session, "securities.json", "securities")
        securities_df = pd.DataFrame(securities)
        if securities_df.empty:
            raise RuntimeError("MOEX ISS вернул пустой список securities.")

        print("Шаг 2/4: Оставляем только РФ эмитентов, торгующихся на MOEX...")
        filtered_df = securities_df[securities_df.apply(is_russian_moex_emitent, axis=1)].copy()

        emitents_df = (
            filtered_df.drop_duplicates(subset=["emitent_id"])
            .sort_values(by="emitent_id")
            .reset_index(drop=True)
        )
        emitent_ids = emitents_df["emitent_id"].astype(int).tolist()

        print(f"Всего бумаг: {len(securities_df)}")
        print(f"РФ торгуемых бумаг: {len(filtered_df)}")
        print(f"РФ эмитентов для выгрузки: {len(emitent_ids)}")
        print(f"Шаг 3/4: Собираем дополнительные данные по эмитентам (потоки: {MAX_WORKERS})...")

        emitent_blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_one_emitent, emitent_id): emitent_id for emitent_id in emitent_ids}

            for future in tqdm(as_completed(futures), total=len(futures), desc="Эмитенты", unit="эмитент"):
                emitent_id = futures[future]
                try:
                    blocks = future.result()
                except Exception as exc:
                    print(f"⚠️ Ошибка по эмитенту {emitent_id}: {exc}")
                    continue

                for block_name, rows in blocks.items():
                    emitent_blocks[f"emitent_{block_name}"].extend(rows)

        print("Шаг 4/4: Сохраняем результат в Excel (с перезаписью)...")
        sheets: dict[str, pd.DataFrame] = {
            "securities_all": securities_df,
            "securities_ru_traded": filtered_df,
            "emitents_ru": emitents_df,
        }
        for block_name, rows in emitent_blocks.items():
            if rows:
                sheets[block_name] = pd.DataFrame(rows)

        save_to_excel(OUTPUT_FILE, sheets)
        print(f"Готово. Файл обновлен: {OUTPUT_FILE.resolve()}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
