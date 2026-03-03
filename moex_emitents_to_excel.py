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

STOCKS_FILE = Path("moex_stocks_ru.xlsx")
BONDS_FILE = Path("moex_bonds_ru.xlsx")
EMITENTS_FILE = Path("moex_emitents_ru.xlsx")


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


def fetch_paginated_block(session: requests.Session, path: str, block_name: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    start = 0
    prev_signature: tuple[Any, Any, int] | None = None

    with tqdm(desc=f"Загрузка {block_name} ({path})", unit="строк", leave=False) as progress:
        while True:
            payload = _fetch_json(
                session,
                path,
                {"iss.meta": "off", "limit": PAGE_LIMIT, "start": start},
            )
            chunk = _get_block(payload, block_name)
            records.extend(chunk)
            progress.update(len(chunk))

            cursor = _extract_cursor(payload, block_name)
            if cursor:
                total = int(cursor.get("TOTAL", 0) or cursor.get("total", 0) or 0)
                pagesize = int(cursor.get("PAGESIZE", 0) or cursor.get("pagesize", 0) or PAGE_LIMIT)
                if total and progress.total != total:
                    progress.total = total
                    progress.refresh()

                start += pagesize
                if start >= total:
                    break
                continue

            if not chunk:
                break

            signature = (chunk[0], chunk[-1], len(chunk))
            if prev_signature == signature:
                break
            prev_signature = signature
            start += len(chunk)

    return records


def fetch_market_dataset(session: requests.Session, market_path: str) -> dict[str, list[dict[str, Any]]]:
    """Собирает все блоки из market endpoint с постраничной загрузкой."""
    collected: dict[str, list[dict[str, Any]]] = defaultdict(list)
    start = 0
    prev_signature: tuple[Any, Any, int] | None = None

    with tqdm(desc=f"Загрузка рынка {market_path}", unit="строк", leave=False) as progress:
        while True:
            payload = _fetch_json(
                session,
                market_path,
                {"iss.meta": "off", "limit": PAGE_LIMIT, "start": start},
            )

            securities_chunk = _get_block(payload, "securities")
            progress.update(len(securities_chunk))

            for key in payload.keys():
                if key.endswith(".cursor"):
                    continue
                rows = _get_block(payload, key)
                if rows:
                    collected[key].extend(rows)

            cursor = _extract_cursor(payload, "securities")
            if cursor:
                total = int(cursor.get("TOTAL", 0) or cursor.get("total", 0) or 0)
                pagesize = int(cursor.get("PAGESIZE", 0) or cursor.get("pagesize", 0) or PAGE_LIMIT)
                if total and progress.total != total:
                    progress.total = total
                    progress.refresh()

                start += pagesize
                if start >= total:
                    break
                continue

            if not securities_chunk:
                break

            signature = (securities_chunk[0], securities_chunk[-1], len(securities_chunk))
            if prev_signature == signature:
                break
            prev_signature = signature
            start += len(securities_chunk)

    return collected


def _secid_from_row(row: dict[str, Any]) -> str | None:
    return row.get("SECID") or row.get("secid")


def is_russian_traded(row: dict[str, Any]) -> bool:
    if row.get("is_traded") != 1:
        return False
    if row.get("emitent_id") is None:
        return False
    inn_raw = row.get("emitent_inn")
    if inn_raw is None:
        return False
    inn = "".join(ch for ch in str(inn_raw) if ch.isdigit())
    return len(inn) == 10


def filter_market_blocks_by_secids(
    blocks: dict[str, list[dict[str, Any]]],
    allowed_secids: set[str],
) -> dict[str, list[dict[str, Any]]]:
    filtered: dict[str, list[dict[str, Any]]] = {}
    for block_name, rows in blocks.items():
        kept: list[dict[str, Any]] = []
        for row in rows:
            secid = _secid_from_row(row)
            if secid is None or secid in allowed_secids:
                kept.append(row)
        filtered[block_name] = kept
    return filtered


def fetch_emitent_related_blocks(session: requests.Session, emitent_id: int) -> dict[str, list[dict[str, Any]]]:
    block_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    start = 0

    while True:
        payload = _fetch_json(
            session,
            "securities.json",
            {"iss.meta": "off", "limit": PAGE_LIMIT, "start": start, "emitent_id": emitent_id},
        )

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


def build_instrument_file(
    session: requests.Session,
    market_path: str,
    output_file: Path,
    metadata_df: pd.DataFrame,
    label: str,
) -> pd.DataFrame:
    print(f"Шаг: Загружаем {label}...")
    market_blocks = fetch_market_dataset(session, market_path)
    market_securities = pd.DataFrame(market_blocks.get("securities", []))
    if market_securities.empty:
        raise RuntimeError(f"MOEX ISS вернул пустой блок securities для {label}.")

    merged = market_securities.merge(
        metadata_df,
        left_on="SECID",
        right_on="secid",
        how="left",
        suffixes=("_market", "_meta"),
    )

    filtered_merged = merged[merged.apply(is_russian_traded, axis=1)].copy()
    allowed_secids = set(filtered_merged["SECID"].astype(str).tolist())

    filtered_blocks = filter_market_blocks_by_secids(market_blocks, allowed_secids)

    sheets: dict[str, pd.DataFrame] = {
        "securities_merged": filtered_merged,
    }
    for block_name, rows in filtered_blocks.items():
        sheets[f"{block_name}_filtered"] = pd.DataFrame(rows)

    save_to_excel(output_file, sheets)
    print(f"Готово: {label} сохранены в {output_file.resolve()}")

    return filtered_merged


def main() -> None:
    session = _build_session()
    try:
        print("Шаг 1/5: Загружаем метаданные всех бумаг MOEX...")
        metadata_rows = fetch_paginated_block(session, "securities.json", "securities")
        metadata_df = pd.DataFrame(metadata_rows)
        if metadata_df.empty:
            raise RuntimeError("MOEX ISS вернул пустые метаданные securities.")

        print("Шаг 2/5: Формируем Excel по акциям РФ (торгуются сейчас)...")
        stocks_df = build_instrument_file(
            session=session,
            market_path="engines/stock/markets/shares/securities.json",
            output_file=STOCKS_FILE,
            metadata_df=metadata_df,
            label="акции",
        )

        print("Шаг 3/5: Формируем Excel по облигациям РФ (торгуются сейчас)...")
        bonds_df = build_instrument_file(
            session=session,
            market_path="engines/stock/markets/bonds/securities.json",
            output_file=BONDS_FILE,
            metadata_df=metadata_df,
            label="облигации",
        )

        print("Шаг 4/5: Собираем уникальных эмитентов из акций и облигаций...")
        emitent_ids = sorted(
            {
                int(x)
                for x in pd.concat([stocks_df["emitent_id"], bonds_df["emitent_id"]], ignore_index=True).dropna().tolist()
            }
        )
        print(f"Уникальных эмитентов: {len(emitent_ids)}")

        print("Шаг 5/5: Выгружаем всю информацию по эмитентам в отдельный Excel...")
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
                    emitent_blocks[block_name].extend(rows)

        emitent_sheets: dict[str, pd.DataFrame] = {
            "emitents_from_stocks_bonds": pd.DataFrame({"emitent_id": emitent_ids}),
        }
        for block_name, rows in emitent_blocks.items():
            emitent_sheets[block_name] = pd.DataFrame(rows)

        save_to_excel(EMITENTS_FILE, emitent_sheets)
        print(f"Готово: эмитенты сохранены в {EMITENTS_FILE.resolve()}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
