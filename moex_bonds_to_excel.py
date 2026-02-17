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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


# =========================
# Базовые настройки скрипта
# =========================
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 20
CACHE_TTL_HOURS = 6
MAX_RETRIES = 4
BACKOFF_FACTOR = 1.2
MAX_WORKERS = 10

ROOT_DIR = Path(__file__).resolve().parent
LOGS_DIR = ROOT_DIR / "logs"
RAW_DIR = ROOT_DIR / "raw"
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"

LOG_FILE = LOGS_DIR / "moex_bonds.log"
RAW_FILE = RAW_DIR / "moex_bonds_raw.json"
CACHE_FILE = CACHE_DIR / "moex_bonds_cache.json"
OUTPUT_FILE = OUTPUT_DIR / "moex_bonds.xlsx"

# Колонки, которые пользователь попросил убрать из итогового файла
REMOVED_COLUMNS = {"SECNAME", "LISTLEVEL", "STATUS"}
# SECID нужен для технической работы, но в Excel должен быть скрыт
HIDDEN_COLUMN_NAME = "SECID"
ISSUER_COLUMN_NAME = "ISSUER_NAME"
FIRST_COLUMN_NAME = "ISIN"


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


def load_cache(allow_stale: bool = False) -> list[dict[str, Any]] | None:
    """Читает кэш, если он есть; по флагу allow_stale может вернуть и устаревшие данные."""
    if not CACHE_FILE.exists():
        return None

    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        created_at = datetime.fromisoformat(payload["created_at"])
        if datetime.now() - created_at > timedelta(hours=CACHE_TTL_HOURS):
            if allow_stale:
                rows = payload.get("rows", [])
                logging.warning("Кэш устарел, но будет использован как резерв: %s записей.", len(rows))
                return rows
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



def build_session() -> requests.Session:
    """Создаёт HTTP-сессию с ретраями при временных ошибках сети/API."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_page(session: requests.Session, start: int) -> IssPage:
    """Запрашивает одну страницу облигаций с MOEX ISS."""
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,SHORTNAME,ISIN,FACEVALUE,FACEUNIT,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,PRIMARYBOARDID,PREVLEGALCLOSEPRICE,PREVPRICE",
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
    """Собирает облигации с MOEX ISS API."""
    logging.info("Этап 1/6: Запрос данных с MOEX ISS...")

    with build_session() as session:
        page = fetch_page(session, 0)
        unique = {(row.get("SECID"), row.get("ISIN")): row for row in page.rows}
        rows = list(unique.values())
        logging.info("Получено записей: %s", len(rows))
        return rows


def fetch_emitter_id_for_security(session: requests.Session, secid: str) -> int | None:
    """Возвращает ID эмитента по SECID, если доступен."""
    params = {
        "iss.meta": "off",
        "iss.only": "description",
        "description.columns": "name,value",
    }
    response = session.get(f"https://iss.moex.com/iss/securities/{secid}.json", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    rows = data.get("description", {}).get("data", [])
    for name, value in rows:
        if name == "EMITTER_ID" and value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None


def fetch_emitter_name(session: requests.Session, emitter_id: int) -> str | None:
    """Возвращает краткое наименование эмитента по его ID."""
    params = {"iss.meta": "off"}
    response = session.get(f"https://iss.moex.com/iss/emitters/{emitter_id}.json", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    emitter_rows = data.get("emitter", {}).get("data", [])
    if not emitter_rows:
        return None

    columns = data.get("emitter", {}).get("columns", [])
    record = dict(zip(columns, emitter_rows[0]))
    return record.get("SHORT_TITLE") or record.get("TITLE")


def enrich_with_issuer_names(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Добавляет в каждую строку имя эмитента облигации."""
    logging.info("Этап 2/6: Обогащение данных наименованиями эмитентов...")
    secids = sorted({str(row.get("SECID")) for row in rows if row.get("SECID")})

    if not secids:
        for row in rows:
            row[ISSUER_COLUMN_NAME] = ""
        return rows

    secid_to_emitter_id: dict[str, int | None] = {}
    emitter_cache: dict[int, str | None] = {}

    def resolve_emitter(secid: str) -> tuple[str, int | None]:
        with build_session() as local_session:
            return secid, fetch_emitter_id_for_security(local_session, secid)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(resolve_emitter, secid): secid for secid in secids}
        for idx, future in enumerate(as_completed(futures), start=1):
            secid, emitter_id = future.result()
            secid_to_emitter_id[secid] = emitter_id
            if idx % 200 == 0 or idx == len(secids):
                logging.info("Обработано %s/%s бумаг для поиска эмитента.", idx, len(secids))

    unique_emitter_ids = sorted({emitter_id for emitter_id in secid_to_emitter_id.values() if emitter_id is not None})

    def resolve_emitter_name(emitter_id: int) -> tuple[int, str | None]:
        with build_session() as local_session:
            try:
                return emitter_id, fetch_emitter_name(local_session, emitter_id)
            except Exception as exc:
                logging.warning("Не удалось получить наименование эмитента %s: %s", emitter_id, exc)
                return emitter_id, None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(resolve_emitter_name, emitter_id): emitter_id for emitter_id in unique_emitter_ids}
        for idx, future in enumerate(as_completed(futures), start=1):
            emitter_id, emitter_name = future.result()
            emitter_cache[emitter_id] = emitter_name
            if idx % 50 == 0 or idx == len(unique_emitter_ids):
                logging.info("Обработано %s/%s эмитентов.", idx, len(unique_emitter_ids))

    for row in rows:
        secid = str(row.get("SECID", ""))
        emitter_id = secid_to_emitter_id.get(secid)
        row[ISSUER_COLUMN_NAME] = emitter_cache.get(emitter_id) or ""

    return rows


def merge_incremental_data(cached_rows: list[dict[str, Any]], fresh_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    """Инкрементально объединяет кэш и свежие данные (добавляет новые и обновляет изменённые)."""
    merged = {(row.get("SECID"), row.get("ISIN")): row for row in cached_rows}
    new_count = 0
    updated_count = 0

    for row in fresh_rows:
        key = (row.get("SECID"), row.get("ISIN"))
        if key not in merged:
            merged[key] = row
            new_count += 1
        elif merged[key] != row:
            merged[key] = row
            updated_count += 1

    return list(merged.values()), new_count, updated_count


def save_raw(rows: list[dict[str, Any]]) -> None:
    """Сохраняет сырые данные для отладки."""
    RAW_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Этап 4/6: Сырые данные сохранены: %s", RAW_FILE)


def apply_excel_formatting(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    """Применяет форматирование итогового Excel и скрывает техническую колонку SECID."""
    ws = writer.sheets["MOEX_BONDS"]
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    ws.sheet_view.showGridLines = False

    header_fill = PatternFill(fill_type="solid", start_color="1F4E78", end_color="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    even_fill = PatternFill(fill_type="solid", start_color="F5F9FF", end_color="F5F9FF")
    odd_fill = PatternFill(fill_type="solid", start_color="FFFFFF", end_color="FFFFFF")

    ws.row_dimensions[1].height = 22

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    numeric_columns = {"FACEVALUE", "COUPONVALUE", "COUPONPERIOD", "COUPONPERCENT", "PREVLEGALCLOSEPRICE", "PREVPRICE"}

    for row_idx in range(2, ws.max_row + 1):
        row_fill = even_fill if row_idx % 2 == 0 else odd_fill
        for col_idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.fill = row_fill
            cell.alignment = Alignment(vertical="center")
            if col_name in numeric_columns and isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0.00"

    for idx, col_name in enumerate(df.columns, start=1):
        col_letter = get_column_letter(idx)
        max_len = max(
            len(str(col_name)),
            *(len(str(v)) for v in df[col_name].head(500).fillna("")),
        )
        ws.column_dimensions[col_letter].width = min(max_len + 2, 48)

        if col_name == HIDDEN_COLUMN_NAME:
            ws.column_dimensions[col_letter].hidden = True


def add_info_sheet(writer: pd.ExcelWriter) -> None:
    """Добавляет лист INFO с пояснением полей простым языком."""
    info_rows = [
        {"Поле": "SECID", "Описание": "Технический код бумаги на бирже. В основном листе скрыт, но сохранён для аналитики и связок."},
        {"Поле": "ISIN", "Описание": "Международный идентификатор ценной бумаги."},
        {"Поле": "SHORTNAME", "Описание": "Краткое название облигации."},
        {"Поле": "ISSUER_NAME", "Описание": "Наименование эмитента облигации (компании или организации, которая выпустила бумагу)."},
        {"Поле": "FACEVALUE", "Описание": "Номинал облигации."},
        {"Поле": "FACEUNIT", "Описание": "Валюта номинала (например, RUB)."},
        {"Поле": "COUPONVALUE", "Описание": "Размер купонной выплаты."},
        {"Поле": "COUPONPERIOD", "Описание": "Период выплаты купона в днях."},
        {"Поле": "COUPONPERCENT", "Описание": "Купонная ставка в процентах."},
        {"Поле": "PRIMARYBOARDID", "Описание": "Основной торговый режим/секция."},
        {"Поле": "PREVLEGALCLOSEPRICE", "Описание": "Предыдущая официальная цена закрытия."},
        {"Поле": "PREVPRICE", "Описание": "Предыдущая рыночная цена."},
    ]

    info_df = pd.DataFrame(info_rows)
    info_df.to_excel(writer, index=False, sheet_name="INFO")
    info_ws = writer.sheets["INFO"]
    info_ws.freeze_panes = "A2"

    for idx, col_name in enumerate(info_df.columns, start=1):
        col_letter = get_column_letter(idx)
        max_len = max(len(str(col_name)), *(len(str(v)) for v in info_df[col_name]))
        info_ws.column_dimensions[col_letter].width = min(max_len + 2, 80)


def save_excel(rows: list[dict[str, Any]]) -> None:
    """Сохраняет итоговый набор в Excel."""
    logging.info("Этап 5/6: Подготовка итогового Excel...")
    df = pd.DataFrame(rows)

    for col in REMOVED_COLUMNS:
        if col in df.columns:
            df = df.drop(columns=[col])

    sort_columns = [col for col in ["SECID", "ISIN"] if col in df.columns]
    if sort_columns:
        df = df.sort_values(by=sort_columns).reset_index(drop=True)

    if FIRST_COLUMN_NAME in df.columns:
        ordered_columns = [FIRST_COLUMN_NAME] + [col for col in df.columns if col != FIRST_COLUMN_NAME]
        df = df[ordered_columns]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        apply_excel_formatting(writer, df)
        add_info_sheet(writer)

    logging.info("Этап 6/6: Excel файл сохранён: %s", OUTPUT_FILE)


def main() -> None:
    started_at = time.perf_counter()
    setup_folders()
    setup_logging()

    logging.info("Старт скрипта: сбор облигаций MOEX")
    cached_rows = load_cache()

    try:
        if cached_rows is None:
            rows = fetch_all_bonds()
        else:
            fresh_rows = fetch_all_bonds()
            rows, new_count, updated_count = merge_incremental_data(cached_rows, fresh_rows)
            logging.info(
                "Этап 3/6: Инкрементальное обновление завершено. Новых: %s, обновлённых: %s, всего: %s.",
                new_count,
                updated_count,
                len(rows),
            )
    except Exception as exc:
        logging.error("Не удалось получить свежие данные: %s", exc)
        rows = cached_rows or load_cache(allow_stale=True)
        if not rows:
            raise
        logging.warning("Используем резервный кэш из-за недоступности API.")

    rows = enrich_with_issuer_names(rows)
    save_cache(rows)
    save_raw(rows)
    save_excel(rows)

    elapsed = time.perf_counter() - started_at
    logging.info("Готово. Всего облигаций: %s", len(rows))
    logging.info("Время выполнения: %.2f сек.", elapsed)


if __name__ == "__main__":
    main()
