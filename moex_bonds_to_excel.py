from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# =========================
# Базовые настройки скрипта
# =========================
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 20
CACHE_TTL_HOURS = 6

ROOT_DIR = Path(__file__).resolve().parent
LOGS_DIR = ROOT_DIR / "logs"
RAW_DIR = ROOT_DIR / "raw"
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"

LOG_FILE = LOGS_DIR / "moex_bonds.log"
RAW_FILE = RAW_DIR / "moex_bonds_raw.json"
CACHE_FILE = CACHE_DIR / "moex_bonds_cache.json"
OUTPUT_FILE = OUTPUT_DIR / "moex_bonds.xlsx"


@dataclass
class IssPage:
    """Одна страница ответа ISS API."""

    start: int
    rows: list[dict[str, Any]]


def setup_folders() -> None:
    """Создаёт рабочие директории и очищает папку raw перед запуском."""
    LOGS_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

    if RAW_DIR.exists():
        shutil.rmtree(RAW_DIR)
    RAW_DIR.mkdir(exist_ok=True)


def setup_logging() -> None:
    """Настраивает логирование в файл, который перезаписывается при каждом запуске."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_cache() -> list[dict[str, Any]] | None:
    """Читает кэш, если он есть и не устарел."""
    if not CACHE_FILE.exists():
        return None

    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        created_at = datetime.fromisoformat(payload["created_at"])
        if datetime.now() - created_at > timedelta(hours=CACHE_TTL_HOURS):
            logging.info("Кэш найден, но устарел. Загружаем свежие данные.")
            return None

        rows = payload.get("rows", [])
        logging.info("Данные загружены из кэша: %s записей.", len(rows))
        return rows
    except Exception as exc:
        logging.warning("Не удалось прочитать кэш: %s", exc)
        return None


def save_cache(rows: list[dict[str, Any]]) -> None:
    """Сохраняет кэш в JSON файл."""
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "rows": rows,
    }
    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_page(session: requests.Session, start: int) -> IssPage:
    """Запрашивает одну страницу облигаций с MOEX ISS."""
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,SHORTNAME,SECNAME,ISIN,FACEVALUE,FACEUNIT,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,LISTLEVEL,STATUS,PRIMARYBOARDID,PREVLEGALCLOSEPRICE,PREVPRICE",
        "start": start,
        "limit": PAGE_SIZE,
    }
    response = session.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    rows = data.get("securities", {}).get("data", [])
    columns = data.get("securities", {}).get("columns", [])
    records = [dict(zip(columns, row)) for row in rows]
    return IssPage(start=start, rows=records)


def fetch_all_bonds() -> list[dict[str, Any]]:
    """Собирает облигации с MOEX ISS API.

    Примечание: на текущем эндпоинте MOEX возвращает полный набор облигаций
    одним ответом, даже если передавать start/limit.
    """
    logging.info("Запрос данных с MOEX ISS...")

    with requests.Session() as session:
        page = fetch_page(session, 0)
        unique = {(row.get("SECID"), row.get("ISIN")): row for row in page.rows}
        rows = list(unique.values())
        logging.info("Получено записей: %s", len(rows))
        return rows


def save_raw(rows: list[dict[str, Any]]) -> None:
    """Сохраняет сырые данные для отладки."""
    RAW_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Сырые данные сохранены: %s", RAW_FILE)


def save_excel(rows: list[dict[str, Any]]) -> None:
    """Сохраняет итоговый набор в Excel."""
    df = pd.DataFrame(rows)
    sort_columns = [col for col in ["SECID", "ISIN"] if col in df.columns]
    if sort_columns:
        df = df.sort_values(by=sort_columns).reset_index(drop=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")

    logging.info("Excel файл сохранён: %s", OUTPUT_FILE)


def main() -> None:
    setup_folders()
    setup_logging()

    logging.info("Старт скрипта: сбор облигаций MOEX")
    rows = load_cache()

    if rows is None:
        rows = fetch_all_bonds()
        save_cache(rows)

    save_raw(rows)
    save_excel(rows)
    logging.info("Готово. Всего облигаций: %s", len(rows))


if __name__ == "__main__":
    main()
