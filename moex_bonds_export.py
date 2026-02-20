"""Скрипт выгружает торгуемые облигации MOEX, расширенные данные по выпускам и данные по эмитентам в Excel."""

from __future__ import annotations

import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
RAW_DIR = BASE_DIR / "raw"
CACHE_DIR = BASE_DIR / "cache" / "moex"
OUTPUT_DIR = BASE_DIR / "output"

OUTPUT_FILE = OUTPUT_DIR / "moex_bonds_full_export.xlsx"
LOG_FILE = LOGS_DIR / "moex_bonds_export.log"

# Настройки запуска (редактируются прямо в файле, без argparse).
MAX_WORKERS = 16
REQUEST_TIMEOUT_SECONDS = 30
RETRY_COUNT = 3
CACHE_TTL_HOURS = 24
MAX_BONDS_TO_PROCESS: int | None = None  # Например, 100 для быстрого теста; None = все облигации.

MOEX_BASE_URL = "https://iss.moex.com/iss"


@dataclass
class MoexBlocks:
    descriptions: list[dict[str, Any]]
    boards: list[dict[str, Any]]
    coupons: list[dict[str, Any]]
    amortizations: list[dict[str, Any]]
    offers: list[dict[str, Any]]


def setup_environment() -> None:
    """Готовит папки проекта и очищает временные данные перед запуском."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Каждый запуск затирает предыдущий лог.
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    # Папка raw очищается полностью перед новым отладочным запуском.
    for item in RAW_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def request_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Делает HTTP-запрос к ISS MOEX c повторными попытками при временных сбоях."""
    last_exception: Exception | None = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_exception = exc
            logging.warning("Попытка %s/%s для %s завершилась ошибкой: %s", attempt, RETRY_COUNT, url, exc)
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"Не удалось получить данные: {url}") from last_exception


def fetch_all_pages(url: str, block_name: str, extra_params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Собирает все страницы ISS-блока через параметр start."""
    start = 0
    all_rows: list[list[Any]] = []
    columns: list[str] | None = None
    previous_rows: list[list[Any]] | None = None
    while True:
        params = {"iss.meta": "off", "start": start}
        if extra_params:
            params.update(extra_params)

        payload = request_json(url, params=params)
        block = payload.get(block_name)
        if not block:
            raise RuntimeError(f"В ответе нет блока '{block_name}' для URL {url}")

        if columns is None:
            columns = block["columns"]

        rows = block["data"]
        if not rows:
            break

        if previous_rows is not None and rows == previous_rows:
            logging.warning(
                "Пагинация для %s перестала сдвигаться (start=%s). Останавливаю цикл, чтобы избежать дублей.",
                url,
                start,
            )
            break

        all_rows.extend(rows)
        logging.info("Загружено %s строк из %s (start=%s)", len(rows), block_name, start)

        # Если API возвращает все записи сразу, дополнительный запрос не нужен.
        if len(rows) < 100:
            break

        previous_rows = rows
        start += len(rows)

    return pd.DataFrame(all_rows, columns=columns or [])


def to_records(payload_block: dict[str, Any]) -> list[dict[str, Any]]:
    columns = payload_block.get("columns", [])
    return [dict(zip(columns, row)) for row in payload_block.get("data", [])]


def is_cache_valid(cache_file: Path) -> bool:
    if not cache_file.exists():
        return False
    max_age = timedelta(hours=CACHE_TTL_HOURS)
    age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
    return age <= max_age


def fetch_security_details(secid: str) -> MoexBlocks:
    """Забирает расширенные данные по бумаге с кэшем на диске."""
    cache_file = CACHE_DIR / f"{secid}.json"

    if is_cache_valid(cache_file):
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        details_payload = cached["details"]
        bondization_payload = cached["bondization"]
    else:
        details_payload = request_json(f"{MOEX_BASE_URL}/securities/{secid}.json", params={"iss.meta": "off"})
        bondization_payload = request_json(f"{MOEX_BASE_URL}/securities/{secid}/bondization.json", params={"iss.meta": "off"})
        cache_file.write_text(
            json.dumps({"details": details_payload, "bondization": bondization_payload}, ensure_ascii=False),
            encoding="utf-8",
        )

    description_rows = to_records(details_payload.get("description", {"columns": [], "data": []}))
    descriptions = [{"secid": secid, "field": row.get("name"), "title": row.get("title"), "value": row.get("value")} for row in description_rows]

    boards = to_records(details_payload.get("boards", {"columns": [], "data": []}))
    for row in boards:
        row["secid"] = secid

    coupons = to_records(bondization_payload.get("coupons", {"columns": [], "data": []}))
    amortizations = to_records(bondization_payload.get("amortizations", {"columns": [], "data": []}))
    offers = to_records(bondization_payload.get("offers", {"columns": [], "data": []}))

    return MoexBlocks(
        descriptions=descriptions,
        boards=boards,
        coupons=coupons,
        amortizations=amortizations,
        offers=offers,
    )


def collect_extended_data(secids: list[str]) -> MoexBlocks:
    """Параллельно собирает расширенные блоки по всем облигациям."""
    descriptions: list[dict[str, Any]] = []
    boards: list[dict[str, Any]] = []
    coupons: list[dict[str, Any]] = []
    amortizations: list[dict[str, Any]] = []
    offers: list[dict[str, Any]] = []

    total = len(secids)
    logging.info("Старт параллельной загрузки расширенных данных для %s облигаций", total)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_security_details, secid): secid for secid in secids}
        for index, future in enumerate(as_completed(future_map), start=1):
            secid = future_map[future]
            try:
                blocks = future.result()
                descriptions.extend(blocks.descriptions)
                boards.extend(blocks.boards)
                coupons.extend(blocks.coupons)
                amortizations.extend(blocks.amortizations)
                offers.extend(blocks.offers)
            except Exception as exc:  # noqa: BLE001
                logging.error("Ошибка при загрузке %s: %s", secid, exc)

            if index % 50 == 0 or index == total:
                message = f"Обработано облигаций: {index}/{total}"
                print(message)
                logging.info(message)

    return MoexBlocks(
        descriptions=descriptions,
        boards=boards,
        coupons=coupons,
        amortizations=amortizations,
        offers=offers,
    )


def save_raw(df: pd.DataFrame, file_name: str) -> None:
    file_path = RAW_DIR / file_name
    df.to_json(file_path, orient="records", force_ascii=False, indent=2)


def build_emitents_sheet(traded_bonds: pd.DataFrame) -> pd.DataFrame:
    emitent_cols = [
        "emitent_id",
        "emitent_title",
        "emitent_inn",
        "emitent_okpo",
    ]
    existing_cols = [col for col in emitent_cols if col in traded_bonds.columns]

    emitents = (
        traded_bonds[existing_cols + ["SECID"]]
        .dropna(subset=["emitent_id"], how="all")
        .copy()
    )

    aggregated = emitents.groupby(existing_cols, dropna=False, as_index=False).agg(
        bonds_count=("SECID", "count"),
        secids=("SECID", lambda x: ", ".join(sorted(set(map(str, x))))),
    )
    return aggregated


def main() -> None:
    start_time = time.perf_counter()
    setup_environment()
    setup_logging()

    print("[1/6] Загружаю список облигаций по рынку MOEX...")
    bonds_market_df = fetch_all_pages(
        f"{MOEX_BASE_URL}/engines/stock/markets/bonds/securities.json",
        block_name="securities",
    )

    print("[2/6] Загружаю общие данные по инструментам (включая эмитентов)...")
    all_securities_df = fetch_all_pages(
        f"{MOEX_BASE_URL}/securities.json",
        block_name="securities",
        extra_params={"engine": "stock", "market": "bonds"},
    )

    # Приводим названия столбцов к единому стилю перед merge.
    all_securities_df = all_securities_df.rename(columns=str.upper)

    merged_df = bonds_market_df.merge(
        all_securities_df[[
            "SECID",
            "IS_TRADED",
            "EMITENT_ID",
            "EMITENT_TITLE",
            "EMITENT_INN",
            "EMITENT_OKPO",
            "TYPE",
            "GROUP",
            "PRIMARY_BOARDID",
            "MARKETPRICE_BOARDID",
        ]],
        how="left",
        on="SECID",
    )

    traded_bonds_df = merged_df[merged_df["IS_TRADED"] == 1].copy()
    traded_bonds_df = traded_bonds_df.sort_values(["EMITENT_TITLE", "SECID"], na_position="last")

    if MAX_BONDS_TO_PROCESS:
        traded_bonds_df = traded_bonds_df.head(MAX_BONDS_TO_PROCESS)
        logging.warning("Включен лимит MAX_BONDS_TO_PROCESS=%s", MAX_BONDS_TO_PROCESS)

    secids = traded_bonds_df["SECID"].dropna().astype(str).unique().tolist()
    print(f"Найдено торгуемых облигаций: {len(secids)}")

    print("[3/6] Параллельно собираю расширенные данные по каждой облигации...")
    blocks = collect_extended_data(secids)

    descriptions_df = pd.DataFrame(blocks.descriptions)
    boards_df = pd.DataFrame(blocks.boards)
    coupons_df = pd.DataFrame(blocks.coupons)
    amortizations_df = pd.DataFrame(blocks.amortizations)
    offers_df = pd.DataFrame(blocks.offers)
    emitents_df = build_emitents_sheet(traded_bonds_df)

    print("[4/6] Сохраняю сырые данные для отладки в папку raw...")
    save_raw(traded_bonds_df, "traded_bonds.json")
    save_raw(descriptions_df, "bond_descriptions.json")
    save_raw(boards_df, "bond_boards.json")
    save_raw(coupons_df, "bond_coupons.json")
    save_raw(amortizations_df, "bond_amortizations.json")
    save_raw(offers_df, "bond_offers.json")
    save_raw(emitents_df, "emitents.json")

    print("[5/6] Формирую Excel-файл...")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        traded_bonds_df.to_excel(writer, sheet_name="bonds_traded", index=False)
        emitents_df.to_excel(writer, sheet_name="emitents", index=False)
        descriptions_df.to_excel(writer, sheet_name="bond_descriptions", index=False)
        boards_df.to_excel(writer, sheet_name="bond_boards", index=False)
        coupons_df.to_excel(writer, sheet_name="bond_coupons", index=False)
        amortizations_df.to_excel(writer, sheet_name="bond_amortizations", index=False)
        offers_df.to_excel(writer, sheet_name="bond_offers", index=False)

    elapsed = time.perf_counter() - start_time
    print("[6/6] Готово.")
    print(f"Excel сохранен: {OUTPUT_FILE}")
    print(f"Время выполнения: {elapsed:.2f} сек.")

    logging.info("Excel сохранен: %s", OUTPUT_FILE)
    logging.info("Время выполнения: %.2f сек.", elapsed)


if __name__ == "__main__":
    main()
