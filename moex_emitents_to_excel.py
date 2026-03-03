from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
PAGE_LIMIT = 100
TIMEOUT = 30
OUTPUT_FILE = f"moex_emitents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


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

    while True:
        params["start"] = start
        payload = _fetch_json(session, path, params)
        chunk = _get_block(payload, block_name)
        records.extend(chunk)

        cursor = _extract_cursor(payload, block_name)
        if cursor:
            total = int(cursor.get("TOTAL", 0) or cursor.get("total", 0) or 0)
            pagesize = int(cursor.get("PAGESIZE", 0) or cursor.get("pagesize", 0) or effective_limit)
            start += pagesize
            if start >= total:
                break
            continue

        if len(chunk) < effective_limit:
            break

        start += effective_limit

    return records


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


def save_to_excel(file_name: str, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            if df.empty:
                continue
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def main() -> None:
    session = requests.Session()

    print("Шаг 1/4: Загружаем общий список бумаг MOEX...")
    securities = fetch_paginated_block(session, "securities.json", "securities")
    securities_df = pd.DataFrame(securities)
    if securities_df.empty:
        raise RuntimeError("MOEX ISS вернул пустой список securities.")

    print("Шаг 2/4: Формируем список эмитентов...")
    emitents_df = (
        securities_df.dropna(subset=["emitent_id"])
        .drop_duplicates(subset=["emitent_id"])
        .sort_values(by="emitent_id")
        .reset_index(drop=True)
    )
    emitent_ids = emitents_df["emitent_id"].astype(int).tolist()

    print(f"Найдено эмитентов: {len(emitent_ids)}")
    print("Шаг 3/4: Собираем дополнительные данные по эмитентам...")

    emitent_blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for emitent_id in tqdm(emitent_ids, desc="Эмитенты", unit="эмитент"):
        blocks = fetch_emitent_related_blocks(session, emitent_id)
        for block_name, rows in blocks.items():
            emitent_blocks[f"emitent_{block_name}"].extend(rows)

    print("Шаг 4/4: Сохраняем результат в Excel...")
    sheets: dict[str, pd.DataFrame] = {
        "securities_all": securities_df,
        "emitents": emitents_df,
    }
    for block_name, rows in emitent_blocks.items():
        if rows:
            sheets[block_name] = pd.DataFrame(rows)

    save_to_excel(OUTPUT_FILE, sheets)
    print(f"Готово. Файл: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
