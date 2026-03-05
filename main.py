from __future__ import annotations

import csv
import base64
import html
import io
import json
import logging
import random
import re
import sqlite3
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Font, PatternFill
from playwright.sync_api import Error as PWError
from playwright.sync_api import TimeoutError as PWTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

import config


def progress(total: int, desc: str, unit: str, position: int = 0):
    return tqdm(total=total, desc=desc, unit=unit, position=position, leave=False, dynamic_ncols=True)


def setup_logging() -> logging.Logger:
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("bonds_main")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(config.LOGS_DIR / config.LOG_FILENAME, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def create_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": config.NRA_REQUEST_USER_AGENT})
    return session


def ensure_directories() -> None:
    for folder in (
        config.RAW_DIR,
        config.CACHE_DIR,
        config.DB_DIR,
        config.BASE_SNAPSHOTS_DIR,
        config.LOGS_DIR,
        config.DOCS_DIR,
    ):
        folder.mkdir(parents=True, exist_ok=True)


def migrate_legacy_db_if_needed() -> None:
    legacy_path = config.DB_DIR / config.LEGACY_DB_FILENAME
    target_path = config.DB_DIR / config.DB_FILENAME
    if target_path.exists() or not legacy_path.exists():
        return
    legacy_path.replace(target_path)


def migrate_legacy_rates_table_if_needed(conn: sqlite3.Connection) -> None:
    if config.RATES_TABLE_NAME == config.LEGACY_RATES_TABLE_NAME:
        return
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (config.RATES_TABLE_NAME,),
    ).fetchone()
    if table_exists:
        return
    legacy_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (config.LEGACY_RATES_TABLE_NAME,),
    ).fetchone()
    if legacy_exists:
        conn.execute(
            f'ALTER TABLE "{config.LEGACY_RATES_TABLE_NAME}" RENAME TO "{config.RATES_TABLE_NAME}"'
        )
        conn.commit()


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    conn.execute("PRAGMA mmap_size=1073741824;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {config.META_TABLE_NAME} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()


def get_meta_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        f"SELECT value FROM {config.META_TABLE_NAME} WHERE key = ?",
        (key,),
    ).fetchone()
    return row[0] if row else None


def set_meta_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {config.META_TABLE_NAME}(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )
    conn.commit()


def should_refresh_cache(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    last_refresh_raw = get_meta_value(conn, "last_refresh_utc")
    rows_count_raw = get_meta_value(conn, "last_rows_count")

    if not last_refresh_raw or not rows_count_raw:
        return True

    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
        rows_count = int(rows_count_raw)
    except ValueError:
        return True

    if rows_count <= 0:
        return True

    ttl = timedelta(hours=config.CACHE_TTL_HOURS)
    return now_utc - last_refresh >= ttl


def download_csv(url: str, timeout_seconds: int | float) -> str:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    for encoding in ("utf-8-sig", "cp1251", response.encoding or "utf-8"):
        try:
            return response.content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return response.text


def parse_moex_rates_csv(raw_text: str) -> tuple[list[str], list[list[str]]]:
    lines = raw_text.splitlines()
    header_index = None
    delimiter = ";"

    for idx, line in enumerate(lines):
        if line.startswith("SECID;"):
            header_index = idx
            delimiter = ";"
            break
        if line.startswith("SECID\t"):
            header_index = idx
            delimiter = "\t"
            break

    if header_index is None:
        raise ValueError("Не удалось найти строку заголовков (SECID...) в CSV.")

    data_lines = [line for line in lines[header_index:] if line.strip()]
    reader = csv.reader(data_lines, delimiter=delimiter)

    rows = list(reader)
    headers = [h.strip() for h in rows[0]]
    values: list[list[str]] = []

    for row in rows[1:]:
        if not any(cell.strip() for cell in row):
            continue
        normalized = row + [""] * (len(headers) - len(row))
        values.append([cell.strip() for cell in normalized[: len(headers)]])

    return headers, values


def replace_rates_table(conn: sqlite3.Connection, headers: list[str], rows: list[list[str]]) -> None:
    quoted_columns = ", ".join([f'"{col}" TEXT' for col in headers])
    quoted_headers = ", ".join([f'"{col}"' for col in headers])
    placeholders = ", ".join(["?"] * len(headers))

    conn.execute(f'DROP TABLE IF EXISTS "{config.RATES_TABLE_NAME}"')
    conn.execute(f'CREATE TABLE "{config.RATES_TABLE_NAME}" ({quoted_columns})')

    conn.execute("BEGIN")
    conn.executemany(
        f'INSERT INTO "{config.RATES_TABLE_NAME}" ({quoted_headers}) VALUES ({placeholders})',
        rows,
    )
    conn.commit()


def ensure_emitents_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.EMITENTS_TABLE_NAME}" (
            "INN" TEXT PRIMARY KEY,
            "EMITENTNAME" TEXT NOT NULL,
            "Scoring" TEXT,
            "DateScoring" TEXT,
            "NRA_Rate" TEXT,
            "Acra_Rate" TEXT,
            "NKR_Rate" TEXT,
            "RAEX_Rate" TEXT
        )
        '''
    )
    conn.execute(
        f'ALTER TABLE "{config.EMITENTS_TABLE_NAME}" ADD COLUMN "NRA_Rate" TEXT'
    ) if _column_absent(conn, config.EMITENTS_TABLE_NAME, "NRA_Rate") else None
    conn.execute(
        f'ALTER TABLE "{config.EMITENTS_TABLE_NAME}" ADD COLUMN "Acra_Rate" TEXT'
    ) if _column_absent(conn, config.EMITENTS_TABLE_NAME, "Acra_Rate") else None
    conn.execute(
        f'ALTER TABLE "{config.EMITENTS_TABLE_NAME}" ADD COLUMN "NKR_Rate" TEXT'
    ) if _column_absent(conn, config.EMITENTS_TABLE_NAME, "NKR_Rate") else None
    conn.execute(
        f'ALTER TABLE "{config.EMITENTS_TABLE_NAME}" ADD COLUMN "RAEX_Rate" TEXT'
    ) if _column_absent(conn, config.EMITENTS_TABLE_NAME, "RAEX_Rate") else None
    conn.commit()


def _column_absent(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return column_name not in {row[1] for row in rows}


def ensure_nra_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.NRA_TABLE_NAME}" (
            "id" TEXT,
            "organization_name" TEXT,
            "inn" TEXT,
            "press_release_title" TEXT,
            "press_release_date" TEXT,
            "rating" TEXT,
            "rating_status" TEXT,
            "forecast" TEXT,
            "rating_type" TEXT,
            "organization_sector" TEXT,
            "industry" TEXT,
            "osk" TEXT,
            "isin" TEXT,
            "press_release_link" TEXT,
            "under_watch" TEXT,
            "source_file_name" TEXT,
            "loaded_at_utc" TEXT,
            UNIQUE("id")
        )
        '''
    )
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.NRA_LATEST_TABLE_NAME}" (
            "inn" TEXT PRIMARY KEY,
            "organization_name" TEXT,
            "press_release_date" TEXT,
            "rating" TEXT,
            "rating_status" TEXT,
            "forecast" TEXT
        )
        '''
    )
    conn.commit()


def ensure_nkr_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.NKR_TABLE_NAME}" (
            "id" TEXT,
            "issuer_name" TEXT,
            "rating_date" TEXT,
            "rating" TEXT,
            "outlook" TEXT,
            "tin" TEXT,
            "loaded_at_utc" TEXT,
            UNIQUE("tin", "rating_date", "rating", "outlook")
        )
        '''
    )
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.NKR_LATEST_TABLE_NAME}" (
            "tin" TEXT PRIMARY KEY,
            "issuer_name" TEXT,
            "rating_date" TEXT,
            "rating" TEXT,
            "outlook" TEXT
        )
        '''
    )
    conn.commit()


def ensure_raex_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.RAEX_TABLE_NAME}" (
            "inn" TEXT,
            "company_name" TEXT,
            "rating" TEXT,
            "forecast" TEXT,
            "rating_date" TEXT,
            "company_url" TEXT,
            "loaded_at_utc" TEXT,
            UNIQUE("inn", "rating_date", "rating", "forecast")
        )
        '''
    )
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.RAEX_LATEST_TABLE_NAME}" (
            "inn" TEXT PRIMARY KEY,
            "company_name" TEXT,
            "rating" TEXT,
            "forecast" TEXT,
            "rating_date" TEXT,
            "company_url" TEXT,
            "loaded_at_utc" TEXT
        )
        '''
    )
    conn.commit()


def ensure_dohod_table(conn: sqlite3.Connection, headers: list[str] | None = None) -> None:
    base_columns = ['"ISIN" TEXT PRIMARY KEY']
    if headers:
        for header in headers:
            normalized = header.strip()
            if not normalized or normalized == "ISIN":
                continue
            base_columns.append(f'"{normalized}" TEXT')
    base_columns.append('"loaded_at_utc" TEXT')
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{config.DOHOD_TABLE_NAME}" ({", ".join(base_columns)})'
    )
    conn.commit()


def ensure_table_has_columns(conn: sqlite3.Connection, table_name: str, headers: list[str]) -> None:
    existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
    for header in headers:
        normalized = header.strip()
        if not normalized or normalized in existing:
            continue
        conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{normalized}" TEXT')
    conn.commit()


NRA_HEADERS_MAP = {
    "id": "id",
    "Название организации": "organization_name",
    "ИНН": "inn",
    "Название пресс-релиза": "press_release_title",
    "Дата опубликования пресс-релиза": "press_release_date",
    "Рейтинг": "rating",
    "Статус рейтинга": "rating_status",
    "Прогноз": "forecast",
    "Вид рейтинга": "rating_type",
    "Сектор организации": "organization_sector",
    "Отрасль": "industry",
    "ОСК": "osk",
    "ISIN": "isin",
    "Ссылка на пресс релиз": "press_release_link",
    "Под наблюдением": "under_watch",
}

NKR_HEADERS_MAP = {
    "ID": "id",
    "Issuer Name": "issuer_name",
    "Date": "rating_date",
    "Rating": "rating",
    "Outlook": "outlook",
    "TIN": "tin",
}

RU_MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}


def normalize_date_ru(value: str) -> str:
    date_str = (value or "").strip()
    if not date_str:
        return ""
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", date_str):
        return f"{date_str[6:10]}-{date_str[3:5]}-{date_str[0:2]}"
    match = re.match(r"^(\d{1,2})\s+([А-Яа-я]{3})\s+(\d{4})$", date_str)
    if not match:
        return date_str
    day = int(match.group(1))
    month = RU_MONTHS.get(match.group(2).lower())
    year = int(match.group(3))
    if not month:
        return date_str
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def find_nra_excel_link(page_html: str) -> str:
    marker = re.search(r'Выгрузить\s*в\s*Excel', page_html, flags=re.IGNORECASE)
    if marker:
        idx = marker.start()
        tag_start = page_html.rfind('<a ', 0, idx)
        tag_end = page_html.find('>', tag_start)
        if tag_start != -1 and tag_end != -1:
            tag = page_html[tag_start:tag_end]
            href_match = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.IGNORECASE)
            if href_match:
                return html.unescape(href_match.group(1))

    links = re.findall(r'href=["\']([^"\']+\.(?:xlsx|xls)[^"\']*)["\']', page_html, flags=re.IGNORECASE)
    if not links:
        raise ValueError("На странице НРА не найдена ссылка на Excel-файл.")
    return html.unescape(links[0])


def _normalize_cell(value: object) -> str:
    return "" if value is None else str(value).strip()


def parse_nra_excel(content: bytes) -> list[dict[str, str]]:
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    headers = [_normalize_cell(cell) for cell in rows[0]]
    indexes: dict[str, int] = {}
    for idx, header in enumerate(headers):
        if header in NRA_HEADERS_MAP:
            indexes[NRA_HEADERS_MAP[header]] = idx

    missing = [column for column in NRA_HEADERS_MAP.values() if column not in indexes]
    if missing:
        raise ValueError(f"В выгрузке НРА отсутствуют колонки: {', '.join(missing)}")

    parsed_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        row_dict: dict[str, str] = {}
        for key, idx in indexes.items():
            row_dict[key] = _normalize_cell(row[idx]) if idx < len(row) else ""
        if any(row_dict.values()):
            parsed_rows.append(row_dict)
    return parsed_rows


def _normalize_tin(value: object) -> str:
    raw = _normalize_cell(value)
    if not raw:
        return ""

    compact = raw.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    if re.fullmatch(r"\d+", compact):
        return compact.zfill(10) if len(compact) < 10 else compact

    try:
        as_int = str(int(Decimal(compact)))
        return as_int.zfill(10) if len(as_int) < 10 else as_int
    except (InvalidOperation, ValueError):
        digits = re.sub(r"\D+", "", compact)
        return digits.zfill(10) if digits and len(digits) < 10 else digits


def parse_nkr_excel(content: bytes) -> list[dict[str, str]]:
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    headers = [_normalize_cell(cell) for cell in rows[0]]
    indexes: dict[str, int] = {}
    for idx, header in enumerate(headers):
        if header in NKR_HEADERS_MAP:
            indexes[NKR_HEADERS_MAP[header]] = idx

    missing = [column for column in NKR_HEADERS_MAP.values() if column not in indexes]
    if missing:
        raise ValueError(f"В выгрузке НКР отсутствуют колонки: {', '.join(missing)}")

    parsed_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        row_dict: dict[str, str] = {}
        for key, idx in indexes.items():
            row_dict[key] = _normalize_cell(row[idx]) if idx < len(row) else ""
        row_dict["tin"] = _normalize_tin(row[indexes["tin"]] if indexes["tin"] < len(row) else "")
        row_dict["rating_date"] = normalize_date_ru(row_dict.get("rating_date", ""))
        if any(row_dict.values()):
            parsed_rows.append(row_dict)
    return parsed_rows


def parse_dohod_excel(content: bytes) -> tuple[list[str], list[dict[str, str]]]:
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return [], []

    headers = [_normalize_cell(cell) for cell in rows[0]]
    if "ISIN" not in headers:
        raise ValueError("В выгрузке Доходъ отсутствует колонка ISIN.")

    parsed_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        row_dict: dict[str, str] = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row_dict[header] = _normalize_cell(row[idx]) if idx < len(row) else ""
        if any(row_dict.values()):
            parsed_rows.append(row_dict)
    return headers, parsed_rows


def _normalize_isin(value: str | None) -> str:
    return str(value or "").strip().upper()


def _deduplicate_dohod_rows(rows: list[dict[str, str]], headers: list[str]) -> list[dict[str, str]]:
    """Схлопывает дубли по ISIN, объединяя непустые значения по колонкам."""
    normalized_headers = [header for header in headers if header]
    grouped: dict[str, dict[str, str]] = {}

    for row in rows:
        isin = _normalize_isin(row.get("ISIN", ""))
        if not isin:
            continue

        target = grouped.setdefault(isin, {"ISIN": isin})
        for header in normalized_headers:
            if header == "ISIN":
                continue
            current_value = str(target.get(header, "")).strip()
            next_value = str(row.get(header, "")).strip()
            if not current_value and next_value:
                target[header] = next_value

    return list(grouped.values())


def should_refresh_dohod(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    last_refresh_raw = get_meta_value(conn, "dohod_last_refresh_utc")
    rows_count_raw = get_meta_value(conn, "dohod_last_rows_count")
    if not last_refresh_raw or not rows_count_raw:
        return True
    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
        rows_count = int(rows_count_raw)
    except ValueError:
        return True
    if rows_count <= 0:
        return True
    return now_utc - last_refresh >= timedelta(hours=config.DOHOD_CACHE_TTL_HOURS)


def download_dohod_excel_via_playwright(logger: logging.Logger) -> bytes:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config.DOHOD_HEADLESS, channel=config.DOHOD_BROWSER_CHANNEL)
        context = browser.new_context(locale="ru-RU", timezone_id="Europe/Moscow", accept_downloads=True)
        try:
            page = context.new_page()
            page.goto(config.DOHOD_BONDS_PAGE_URL, wait_until="domcontentloaded", timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))
            button = page.get_by_text("Скачать Excel", exact=False).first
            page.wait_for_timeout(700)

            try:
                with page.expect_download(timeout=12_000) as download_info:
                    button.click(timeout=8_000)
                download = download_info.value
                download_path = download.path()
                bytes_data = Path(download_path).read_bytes() if download_path else b""
                if bytes_data:
                    logger.info("Доходъ: файл получен через expect_download")
                    return bytes_data
            except Exception as exc:
                logger.info("Доходъ: expect_download не сработал (%s)", exc)

            blob_url = page.evaluate(
                """() => {
                    const anchors = [...document.querySelectorAll('a[href^="blob:"]')];
                    if (anchors.length) return anchors[0].getAttribute('href');
                    const anyBlob = [...document.querySelectorAll('[href],[data-href]')]
                        .map(el => el.getAttribute('href') || el.getAttribute('data-href') || '')
                        .find(v => v && v.startsWith('blob:'));
                    return anyBlob || '';
                }"""
            )
            if blob_url:
                content_b64 = page.evaluate(
                    '''async (blobHref) => {
                        const response = await fetch(blobHref);
                        const buffer = await response.arrayBuffer();
                        let binary = '';
                        const bytes = new Uint8Array(buffer);
                        const chunk = 0x8000;
                        for (let i = 0; i < bytes.length; i += chunk) {
                            binary += String.fromCharCode(...bytes.slice(i, i + chunk));
                        }
                        return btoa(binary);
                    }''',
                    blob_url,
                )
                if content_b64:
                    logger.info("Доходъ: файл получен через blob-ссылку")
                    return base64.b64decode(content_b64)

            raise ValueError("На странице Доходъ не удалось получить Excel-файл.")
        finally:
            context.close()
            browser.close()


def should_refresh_nkr(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    last_refresh_raw = get_meta_value(conn, "nkr_last_refresh_utc")
    rows_count_raw = get_meta_value(conn, "nkr_last_rows_count")
    if not last_refresh_raw or not rows_count_raw:
        return True
    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
        rows_count = int(rows_count_raw)
    except ValueError:
        return True
    if rows_count <= 0:
        return True
    return now_utc - last_refresh >= timedelta(hours=config.NKR_CACHE_TTL_HOURS)


def should_refresh_nra(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    last_refresh_raw = get_meta_value(conn, "nra_last_refresh_utc")
    rows_count_raw = get_meta_value(conn, "nra_last_rows_count")
    if not last_refresh_raw or not rows_count_raw:
        return True
    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
        rows_count = int(rows_count_raw)
    except ValueError:
        return True
    if rows_count <= 0:
        return True
    return now_utc - last_refresh >= timedelta(hours=config.NRA_CACHE_TTL_HOURS)


def should_refresh_raex(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    latest_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (config.RAEX_LATEST_TABLE_NAME,),
    ).fetchone()
    if latest_exists:
        latest_rows_count = conn.execute(f'SELECT COUNT(*) FROM "{config.RAEX_LATEST_TABLE_NAME}"').fetchone()
        if not latest_rows_count or int(latest_rows_count[0]) <= 0:
            return True

    last_refresh_raw = get_meta_value(conn, "raex_last_refresh_utc")
    rows_count_raw = get_meta_value(conn, "raex_last_rows_count")
    if not last_refresh_raw or not rows_count_raw:
        return True
    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
        rows_count = int(rows_count_raw)
    except ValueError:
        return True
    if rows_count <= 0:
        return True
    return now_utc - last_refresh >= timedelta(hours=config.RAEX_CACHE_TTL_HOURS)


def get_emitents_inns_for_raex(main_db_path: Path) -> list[str]:
    with connect_db(main_db_path) as main_conn:
        rows = main_conn.execute(
            f'''SELECT DISTINCT TRIM("INN") FROM "{config.EMITENTS_TABLE_NAME}" WHERE TRIM(COALESCE("INN", '')) <> '' '''
        ).fetchall()
    return [row[0] for row in rows if row and row[0]]


def get_raex_company_urls_by_inn(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        f'''
        SELECT "inn", "company_url"
        FROM "{config.RAEX_LATEST_TABLE_NAME}"
        WHERE TRIM(COALESCE("inn", '')) <> ''
          AND TRIM(COALESCE("company_url", '')) <> ''
        '''
    ).fetchall()
    return {str(inn).strip(): str(company_url).strip() for inn, company_url in rows if inn and company_url}


def _extract_raex_csrf_token(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    token_input = soup.select_one('input[name="CSRFToken"]')
    return (token_input.get("value") or "").strip() if token_input else ""


def _extract_raex_company_url(html_text: str) -> str:
    match = re.search(r"href=[\"'](?P<url>/database/companies/[^\"']+/?)[\"']", html_text, flags=re.IGNORECASE)
    return match.group("url").strip() if match else ""


def _looks_like_raex_revoked(value: str) -> bool:
    normalized = (value or "").strip().lower()
    if not normalized:
        return True
    return "отозван" in normalized or normalized in {"-", "—", "n/a"}


def parse_raex_company_page(html_text: str) -> dict[str, str] | None:
    soup = BeautifulSoup(html_text, "lxml")
    heading = soup.find(lambda tag: tag.get_text(" ", strip=True) == "Рейтинги компании")
    if heading is None:
        return None

    rating_header_aliases = (
        "национальная шкала",
        "шкала эксперт ра",
        "шкала эксперт\xa0ра",
    )
    forecast_header_aliases = ("прогноз",)
    date_header_aliases = ("дата",)
    for node in heading.find_all_next():
        node_text = node.get_text(" ", strip=True)
        if node is not heading and "Архив рейтингов" in node_text:
            break
        if getattr(node, "name", "") != "table":
            continue

        headers = [th.get_text(" ", strip=True).lower() for th in node.select("thead th")]
        if not headers:
            first_row = node.select_one("tr")
            headers = [cell.get_text(" ", strip=True).lower() for cell in first_row.select("th, td")] if first_row else []
        rating_index = next((idx for idx, value in enumerate(headers) if value in rating_header_aliases), -1)
        forecast_index = next((idx for idx, value in enumerate(headers) if value in forecast_header_aliases), -1)
        date_index = next((idx for idx, value in enumerate(headers) if value in date_header_aliases), -1)
        if rating_index < 0 or forecast_index < 0 or date_index < 0:
            continue

        for tr in node.select("tbody tr") or node.select("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if not cells:
                continue
            rating_raw = cells[rating_index] if rating_index < len(cells) else ""
            forecast_raw = cells[forecast_index] if forecast_index < len(cells) else ""
            date_raw = cells[date_index] if date_index < len(cells) else ""
            if _looks_like_raex_revoked(rating_raw):
                return None
            rating_clean = re.sub(r"\s+", " ", rating_raw.strip())
            rating_head_match = re.match(r"[A-Za-zА-Яа-я0-9+\-.]+", rating_clean)
            rating = re.sub(r"^ru", "", rating_head_match.group(0) if rating_head_match else rating_clean, flags=re.IGNORECASE)
            if _looks_like_raex_revoked(rating):
                return None
            return {
                "rating": rating,
                "forecast": forecast_raw.strip(),
                "rating_date": date_raw.strip(),
            }
    return None


def fetch_raex_rating_by_inn(
    inn: str,
    timeout_seconds: int | float,
    known_company_url: str = "",
) -> dict[str, str] | None:
    session = requests.Session()
    session.headers.update({"User-Agent": config.NRA_REQUEST_USER_AGENT})

    company_url = (known_company_url or "").strip()
    if company_url:
        company_response = session.get(company_url, timeout=timeout_seconds)
        company_response.raise_for_status()
        parsed = parse_raex_company_page(company_response.text)
        if parsed:
            return {
                "inn": inn,
                "company_name": "",
                "rating": parsed["rating"],
                "forecast": parsed["forecast"],
                "rating_date": parsed["rating_date"],
                "company_url": company_url,
            }

    search_page = session.get(config.RAEX_SEARCH_URL, timeout=timeout_seconds)
    search_page.raise_for_status()
    csrf_token = _extract_raex_csrf_token(search_page.text)

    payload = {"search": inn}
    if csrf_token:
        payload["CSRFToken"] = csrf_token

    search_response = session.post(config.RAEX_SEARCH_URL, data=payload, timeout=timeout_seconds)
    search_response.raise_for_status()
    company_relative_url = _extract_raex_company_url(search_response.text)
    if not company_relative_url:
        return None

    company_url = urljoin(config.RAEX_SEARCH_URL, company_relative_url)
    company_response = session.get(company_url, timeout=timeout_seconds)
    company_response.raise_for_status()
    parsed = parse_raex_company_page(company_response.text)
    if not parsed:
        return None

    return {
        "inn": inn,
        "company_name": "",
        "rating": parsed["rating"],
        "forecast": parsed["forecast"],
        "rating_date": parsed["rating_date"],
        "company_url": company_url,
    }


def refresh_raex_data_if_needed(
    ratings_conn: sqlite3.Connection,
    main_db_path: Path,
    logger: logging.Logger,
    now_utc: datetime,
) -> tuple[bool, int, int, int]:
    ensure_raex_tables(ratings_conn)
    if not should_refresh_raex(ratings_conn, now_utc):
        logger.info("RAEX: кэш актуален, загрузка из сети пропущена.")
        row = ratings_conn.execute(f'SELECT COUNT(*) FROM "{config.RAEX_LATEST_TABLE_NAME}"').fetchone()
        return False, int(row[0]) if row else 0, 0, 0

    inns = get_emitents_inns_for_raex(main_db_path)
    if not inns:
        set_meta_value(ratings_conn, "raex_last_refresh_utc", now_utc.isoformat())
        set_meta_value(ratings_conn, "raex_last_rows_count", "0")
        logger.info("RAEX: в emitents нет ИНН для обработки.")
        return True, 0, 0, 0

    loaded_at = now_utc.isoformat()
    parsed_rows: list[dict[str, str]] = []
    errors_count = 0
    known_company_urls = get_raex_company_urls_by_inn(ratings_conn)
    with progress(total=len(inns), desc="RAEX INN", unit="inn") as raex_pbar:
        with ThreadPoolExecutor(max_workers=max(1, int(config.RAEX_MAX_WORKERS))) as executor:
            futures = {
                executor.submit(
                    fetch_raex_rating_by_inn,
                    inn,
                    config.REQUEST_TIMEOUT_SECONDS,
                    known_company_urls.get(inn, ""),
                ): inn
                for inn in inns
            }
            for future in as_completed(futures):
                inn = futures[future]
                try:
                    parsed = future.result()
                    if parsed:
                        parsed_rows.append(parsed)
                except Exception as exc:
                    errors_count += 1
                    logger.warning("RAEX: ошибка для INN=%s: %s", inn, exc)
                finally:
                    raex_pbar.update(1)

    payload = [
        (
            row["inn"],
            row.get("company_name", ""),
            row["rating"],
            row.get("forecast", ""),
            row.get("rating_date", ""),
            row.get("company_url", ""),
            loaded_at,
        )
        for row in parsed_rows
        if row.get("inn", "").strip() and row.get("rating", "").strip()
    ]

    if payload:
        ratings_conn.executemany(
            f'''
            INSERT INTO "{config.RAEX_TABLE_NAME}" (
                "inn", "company_name", "rating", "forecast", "rating_date", "company_url", "loaded_at_utc"
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT("inn", "rating_date", "rating", "forecast") DO UPDATE SET
                "company_name"=excluded."company_name",
                "company_url"=excluded."company_url",
                "loaded_at_utc"=excluded."loaded_at_utc"
            ''',
            payload,
        )

    ratings_conn.execute(f'DELETE FROM "{config.RAEX_LATEST_TABLE_NAME}"')
    ratings_conn.execute(
        f'''
        INSERT INTO "{config.RAEX_LATEST_TABLE_NAME}" (
            "inn", "company_name", "rating", "forecast", "rating_date", "company_url", "loaded_at_utc"
        )
        SELECT src."inn", src."company_name", src."rating", src."forecast", src."rating_date", src."company_url", src."loaded_at_utc"
        FROM "{config.RAEX_TABLE_NAME}" src
        JOIN (
            SELECT "inn", MAX(
                (CASE
                    WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                    ELSE "rating_date"
                END) || '|' || COALESCE("loaded_at_utc", '')
            ) AS max_key
            FROM "{config.RAEX_TABLE_NAME}"
            WHERE TRIM(COALESCE("inn", '')) <> ''
            GROUP BY "inn"
        ) latest ON latest."inn" = src."inn"
        WHERE
            (CASE
                WHEN instr(src."rating_date", '.') > 0 THEN substr(src."rating_date", 7, 4) || '-' || substr(src."rating_date", 4, 2) || '-' || substr(src."rating_date", 1, 2)
                ELSE src."rating_date"
            END) || '|' || COALESCE(src."loaded_at_utc", '') = latest.max_key
        '''
    )
    ratings_conn.commit()

    set_meta_value(ratings_conn, "raex_last_refresh_utc", loaded_at)
    set_meta_value(ratings_conn, "raex_last_rows_count", str(len(payload)))
    logger.info("RAEX обновление завершено. INN=%s, актуальных=%s, ошибок=%s", len(inns), len(payload), errors_count)
    return True, len(payload), len(inns), errors_count


def ensure_acra_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.ACRA_TABLE_NAME}" (
            "issuer_url" TEXT NOT NULL,
            "issuer_name" TEXT,
            "rating" TEXT,
            "forecast" TEXT,
            "rating_date" TEXT,
            "inn" TEXT,
            "loaded_at_utc" TEXT,
            UNIQUE("issuer_url", "rating_date", "rating")
        )
        '''
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{config.ACRA_TABLE_NAME}_url" ON "{config.ACRA_TABLE_NAME}"("issuer_url")'
    )
    conn.execute(
        f'ALTER TABLE "{config.ACRA_TABLE_NAME}" ADD COLUMN "forecast" TEXT'
    ) if _column_absent(conn, config.ACRA_TABLE_NAME, "forecast") else None
    conn.commit()


def should_refresh_acra(conn: sqlite3.Connection, now_utc: datetime) -> bool:
    last_refresh_raw = get_meta_value(conn, "acra_last_refresh_utc")
    if not last_refresh_raw:
        return True
    try:
        last_refresh = datetime.fromisoformat(last_refresh_raw)
    except ValueError:
        return True
    return now_utc - last_refresh >= timedelta(hours=config.ACRA_CACHE_TTL_HOURS)


def backfill_acra_forecast_from_local_dump(conn: sqlite3.Connection, logger: logging.Logger) -> int:
    list_dump_path = config.ACRA_DUMP_DIR / config.ACRA_LIST_HTML_FILENAME
    if not list_dump_path.exists():
        return 0

    try:
        html_text = list_dump_path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("АКРА backfill прогноза пропущен: не удалось прочитать %s (%s)", list_dump_path, exc)
        return 0

    parsed_rows = parse_acra_list(html_text)
    forecast_by_url = {
        row["issuer_url"]: (row.get("forecast") or "").strip()
        for row in parsed_rows
        if (row.get("issuer_url") or "").strip() and (row.get("forecast") or "").strip()
    }
    if not forecast_by_url:
        return 0

    updated = 0
    for issuer_url, forecast in forecast_by_url.items():
        cursor = conn.execute(
            f'''
            UPDATE "{config.ACRA_TABLE_NAME}"
            SET "forecast" = ?
            WHERE "issuer_url" = ? AND TRIM(COALESCE("forecast", '')) = ''
            ''',
            (forecast, issuer_url),
        )
        if cursor.rowcount and cursor.rowcount > 0:
            updated += int(cursor.rowcount)

    if updated:
        conn.commit()
        logger.info("АКРА backfill прогноза из локального дампа: обновлено строк=%s", updated)
    return updated


def parse_acra_list(html_text: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text, "lxml")
    parsed_rows: list[dict[str, str]] = []
    for row in soup.select("div.emits-row.search-table-row"):
        issuer_link = row.select_one('a.emits-row__item[data-type="ratePerson"]')
        if issuer_link is None:
            continue
        href = (issuer_link.get("href") or "").strip()
        if not href:
            continue
        issuer_url = href if href.startswith("http") else urljoin(config.ACRA_RATINGS_LIST_URL, href)
        issuer_name = issuer_link.get_text(" ", strip=True)

        rating_container = row.select_one('div.emits-row__item[data-type="rate"]')
        rating_raw = rating_container.get_text("\n", strip=True) if rating_container else ""
        rating, forecast = split_acra_rating_and_forecast(rating_raw)

        forecast = ""
        forecast_container = row.select_one('div.emits-row__item[data-type="forecast"]')
        if forecast_container:
            forecast = forecast_container.get_text(" ", strip=True)

        rating_container = row.select_one('div.emits-row__item[data-type="rate"]')
        rating_raw = rating_container.get_text("\n", strip=True) if rating_container else ""
        fallback_rating, fallback_forecast = split_acra_rating_and_forecast(rating_raw)
        if not rating:
            rating = fallback_rating
        if not forecast:
            forecast = fallback_forecast

        date_node = row.select_one('div.emits-row__item[data-type="pressRelease"] a')
        date_raw = date_node.get_text(" ", strip=True) if date_node else ""
        parsed_rows.append(
            {
                "issuer_url": issuer_url,
                "issuer_name": issuer_name,
                "rating": rating,
                "forecast": forecast,
                "rating_date": normalize_date_ru(date_raw) or date_raw,
            }
        )
    return parsed_rows


def split_acra_rating_and_forecast(raw_value: str) -> tuple[str, str]:
    normalized_lines = [line.strip() for line in re.split(r"[\r\n]+", raw_value or "") if line.strip()]
    if not normalized_lines:
        return "", ""

    forecast_line = ""
    rating_line = ""

    for line in normalized_lines:
        if not forecast_line and is_acra_forecast_value(line):
            forecast_line = line
            continue
        if not rating_line and is_acra_rating_value(line):
            rating_line = line

    if rating_line:
        return rating_line, forecast_line

    one_line = normalized_lines[0]
    match = re.match(r"^(.*?)\s*[;,]\s*([^;,]+)$", one_line)
    if match:
        left = match.group(1).strip()
        right = match.group(2).strip()
        if is_acra_forecast_value(right):
            return left, right
    return one_line, forecast_line


def is_acra_rating_value(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    normalized = text.replace(" ", "")
    return bool(re.search(r"[A-ZА-Я][+\-]?(?:\([A-ZА-Я]{2}\))", normalized))


def is_acra_forecast_value(value: str) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {
        "стабильный",
        "позитивный",
        "негативный",
        "развивающийся",
    }


def extract_inn_from_acra_card(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    for info in soup.select("div.info"):
        label = info.find("small")
        if label and label.get_text(" ", strip=True).lower() == "инн":
            value_node = info.find("p")
            raw_value = value_node.get_text(" ", strip=True) if value_node else ""
            return re.sub(r"\D+", "", raw_value)

    fallback = re.search(r"ИНН\D{0,50}(\d[\d\s]{8,14}\d)", soup.get_text(" ", strip=True), flags=re.IGNORECASE)
    return re.sub(r"\D+", "", fallback.group(1)) if fallback else ""


def acra_human_sleep(min_seconds: float = 0.7, max_seconds: float = 1.8) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def acra_ensure_dirs() -> None:
    config.ACRA_DUMP_DIR.mkdir(parents=True, exist_ok=True)
    config.ACRA_ISSUERS_DUMP_DIR.mkdir(parents=True, exist_ok=True)


def acra_safe_filename(name: str, limit: int = 80) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9а-яА-Я_-]+", "_", name or "").strip("_")
    return cleaned[:limit] or "issuer"


def acra_log_progress(payload: dict[str, str]) -> None:
    acra_ensure_dirs()
    progress_log_path = config.ACRA_DUMP_DIR / config.ACRA_PROGRESS_LOG_FILENAME
    with progress_log_path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(json.dumps(payload, ensure_ascii=False) + "\n")


def acra_save_mhtml(page, file_path: Path) -> None:
    cdp = page.context.new_cdp_session(page)
    snapshot = cdp.send("Page.captureSnapshot", {"format": "mhtml"})
    file_path.write_text(snapshot["data"], encoding="utf-8")


def acra_goto_with_retries(page, url: str, logger: logging.Logger, wait_selector: str | None = None, attempts: int = 5) -> bool:
    last_error: Exception | None = None
    for retry in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))
            return True
        except (PWTimeoutError, PWError) as exc:
            last_error = exc
            message = str(exc)
            retryable = any(
                marker in message
                for marker in ("ERR_CONNECTION_CLOSED", "ERR_CONNECTION_RESET", "ERR_EMPTY_RESPONSE", "ERR_TIMED_OUT", "net::")
            )
            if not retryable:
                logger.warning("АКРА goto non-retryable error for %s: %s", url, message)
                return False
            sleep_seconds = min((3.0 * retry) + random.uniform(0.5, 2.0), 20.0)
            logger.warning(
                "АКРА goto retry %s/%s for %s due to %s; sleep %.1fs",
                retry,
                attempts,
                url,
                message[:200],
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
    logger.warning("АКРА goto failed for %s after retries: %s", url, last_error)
    return False


def collect_acra_rows_via_playwright(logger: logging.Logger, inn_cache_by_url: dict[str, str]) -> tuple[dict[str, dict[str, str]], int]:
    acra_ensure_dirs()
    unique_rows: dict[str, dict[str, str]] = {}
    card_fetch_count = 0
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(config.ACRA_PROFILE_DIR),
            channel=config.ACRA_BROWSER_CHANNEL,
            headless=config.ACRA_HEADLESS,
            viewport={"width": 1365, "height": 768},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            args=["--start-maximized"],
        )
        try:
            page = context.new_page()
            list_ok = acra_goto_with_retries(
                page,
                config.ACRA_RATINGS_LIST_URL,
                logger,
                wait_selector='a.emits-row__item[data-type="ratePerson"]',
                attempts=config.ACRA_LIST_GOTO_ATTEMPTS,
            )
            if not list_ok:
                return unique_rows, card_fetch_count

            acra_human_sleep(1.0, 2.0)
            page.mouse.wheel(0, random.randint(500, 1400))
            acra_human_sleep(0.6, 1.2)

            try:
                acra_save_mhtml(page, config.ACRA_DUMP_DIR / config.ACRA_LIST_MHTML_FILENAME)
            except Exception as exc:
                logger.warning("Не удалось сохранить MHTML списка АКРА: %s", exc)

            list_html = page.content()
            (config.ACRA_DUMP_DIR / config.ACRA_LIST_HTML_FILENAME).write_text(list_html, encoding="utf-8")

            parsed_rows = parse_acra_list(list_html)
            for row_data in parsed_rows:
                unique_rows[row_data["issuer_url"]] = row_data

            for index, row_data in enumerate(
                tqdm(list(unique_rows.values()), desc="АКРА карточки", unit="эмитент", leave=False, dynamic_ncols=True),
                start=1,
            ):
                cached_inn = (inn_cache_by_url.get(row_data["issuer_url"]) or "").strip()
                if cached_inn:
                    row_data["inn"] = cached_inn
                    continue
                card_ok = acra_goto_with_retries(
                    page,
                    row_data["issuer_url"],
                    logger,
                    wait_selector=None,
                    attempts=config.ACRA_CARD_GOTO_ATTEMPTS,
                )
                if not card_ok:
                    acra_log_progress(
                        {
                            "url": row_data["issuer_url"],
                            "name": row_data.get("issuer_name", ""),
                            "inn": "",
                            "status": "goto_failed",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    continue
                acra_human_sleep(0.8, 1.8)
                if random.random() < 0.6:
                    page.mouse.wheel(0, random.randint(250, 900))
                    acra_human_sleep(0.3, 0.8)
                card_html = page.content()
                card_filename = f"{index:04d}_{acra_safe_filename(row_data.get('issuer_name', ''))}.html"
                (config.ACRA_ISSUERS_DUMP_DIR / card_filename).write_text(card_html, encoding="utf-8")
                row_data["inn"] = extract_inn_from_acra_card(card_html)
                acra_log_progress(
                    {
                        "url": row_data["issuer_url"],
                        "name": row_data.get("issuer_name", ""),
                        "inn": row_data.get("inn", ""),
                        "status": "ok",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                )
                card_fetch_count += 1
                acra_human_sleep(0.6, 1.6)
        finally:
            context.close()
    return unique_rows, card_fetch_count


def refresh_acra_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int, int]:
    ensure_acra_tables(conn)
    current = conn.execute(f'SELECT COUNT(*) FROM "{config.ACRA_TABLE_NAME}"').fetchone()
    current_total = int(current[0]) if current else 0
    if not should_refresh_acra(conn, now_utc):
        backfill_acra_forecast_from_local_dump(conn, logger)
        return False, current_total, 0

    inn_cache_by_url = {
        row[0]: row[1]
        for row in conn.execute(
            f'''
            SELECT "issuer_url", "inn"
            FROM "{config.ACRA_TABLE_NAME}"
            WHERE TRIM(COALESCE("inn", '')) <> ''
            '''
        ).fetchall()
    }

    try:
        unique_rows, card_requests = collect_acra_rows_via_playwright(logger, inn_cache_by_url)

        loaded_at = now_utc.isoformat()
        changed_rows = 0

        for row_data in unique_rows.values():
            cursor = conn.execute(
                    f'''
                    INSERT INTO "{config.ACRA_TABLE_NAME}" (
                        "issuer_url", "issuer_name", "rating", "forecast", "rating_date", "inn", "loaded_at_utc"
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT("issuer_url", "rating_date", "rating") DO UPDATE SET
                        "issuer_name" = excluded."issuer_name",
                        "forecast" = excluded."forecast",
                        "inn" = CASE
                            WHEN TRIM(COALESCE("{config.ACRA_TABLE_NAME}"."inn", '')) = '' THEN excluded."inn"
                            ELSE "{config.ACRA_TABLE_NAME}"."inn"
                        END,
                        "loaded_at_utc" = excluded."loaded_at_utc"
                    ''',
                    (
                        row_data["issuer_url"],
                        row_data.get("issuer_name", ""),
                        row_data.get("rating", ""),
                        row_data.get("forecast", ""),
                        row_data.get("rating_date", ""),
                        row_data.get("inn", ""),
                        loaded_at,
                    ),
                )
            if cursor.rowcount and cursor.rowcount > 0:
                changed_rows += 1
    except Exception as exc:
        logger.warning("АКРА обновление пропущено из-за сетевой ошибки: %s", exc)
        return False, current_total, 0

    conn.commit()
    backfill_acra_forecast_from_local_dump(conn, logger)
    set_meta_value(conn, "acra_last_refresh_utc", now_utc.isoformat())
    set_meta_value(conn, "acra_last_rows_count", str(len(unique_rows)))
    logger.info(
        "АКРА обновление завершено. В списке=%s, вставлено/обновлено=%s, карточек запрошено=%s",
        len(unique_rows),
        changed_rows,
        card_requests,
    )

    total = conn.execute(f'SELECT COUNT(*) FROM "{config.ACRA_TABLE_NAME}"').fetchone()
    return True, int(total[0]) if total else 0, card_requests


def refresh_nra_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int]:
    ensure_nra_tables(conn)
    if not should_refresh_nra(conn, now_utc):
        row = conn.execute(f'SELECT COUNT(*) FROM "{config.NRA_TABLE_NAME}"').fetchone()
        return False, int(row[0]) if row else 0

    with create_http_session() as session:
        page_response = session.get(config.NRA_RATINGS_PAGE_URL, timeout=config.REQUEST_TIMEOUT_SECONDS)
        page_response.raise_for_status()
        excel_link = find_nra_excel_link(page_response.text)
        excel_url = requests.compat.urljoin(config.NRA_RATINGS_PAGE_URL, excel_link)
        excel_response = session.get(excel_url, timeout=config.REQUEST_TIMEOUT_SECONDS)
        excel_response.raise_for_status()
        content = excel_response.content

    raw_path = config.RAW_DIR / config.NRA_RAW_FILENAME
    if raw_path.exists():
        raw_path.unlink()
    raw_path.write_bytes(content)

    parsed_rows = parse_nra_excel(content)
    loaded_at = now_utc.isoformat()
    payload = [
        (
            row["id"],
            row["organization_name"],
            row["inn"],
            row["press_release_title"],
            row["press_release_date"],
            row["rating"],
            row["rating_status"],
            row["forecast"],
            row["rating_type"],
            row["organization_sector"],
            row["industry"],
            row["osk"],
            row["isin"],
            row["press_release_link"],
            row["under_watch"],
            config.NRA_RAW_FILENAME,
            loaded_at,
        )
        for row in parsed_rows
    ]
    conn.executemany(
        f'''
        INSERT INTO "{config.NRA_TABLE_NAME}" (
            "id", "organization_name", "inn", "press_release_title", "press_release_date", "rating",
            "rating_status", "forecast", "rating_type", "organization_sector", "industry", "osk",
            "isin", "press_release_link", "under_watch", "source_file_name", "loaded_at_utc"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT("id") DO UPDATE SET
            "organization_name"=excluded."organization_name",
            "inn"=excluded."inn",
            "press_release_title"=excluded."press_release_title",
            "press_release_date"=excluded."press_release_date",
            "rating"=excluded."rating",
            "rating_status"=excluded."rating_status",
            "forecast"=excluded."forecast",
            "rating_type"=excluded."rating_type",
            "organization_sector"=excluded."organization_sector",
            "industry"=excluded."industry",
            "osk"=excluded."osk",
            "isin"=excluded."isin",
            "press_release_link"=excluded."press_release_link",
            "under_watch"=excluded."under_watch",
            "source_file_name"=excluded."source_file_name",
            "loaded_at_utc"=excluded."loaded_at_utc"
        ''',
        payload,
    )

    conn.execute(f'DELETE FROM "{config.NRA_LATEST_TABLE_NAME}"')
    conn.execute(
        f'''
        INSERT INTO "{config.NRA_LATEST_TABLE_NAME}" (
            "inn", "organization_name", "press_release_date", "rating", "rating_status", "forecast"
        )
        SELECT src."inn", src."organization_name", src."press_release_date", src."rating", src."rating_status", src."forecast"
        FROM "{config.NRA_TABLE_NAME}" src
        JOIN (
            SELECT "inn", MAX(
                (CASE
                    WHEN instr("press_release_date", '.') > 0 THEN substr("press_release_date", 7, 4) || '-' || substr("press_release_date", 4, 2) || '-' || substr("press_release_date", 1, 2)
                    ELSE "press_release_date"
                END) || '|' || printf('%012d', CAST(COALESCE("id", '0') AS INTEGER))
            ) AS max_key
            FROM "{config.NRA_TABLE_NAME}"
            WHERE TRIM(COALESCE("inn", '')) <> ''
            GROUP BY "inn"
        ) latest ON latest."inn" = src."inn"
        WHERE
            (CASE
                WHEN instr(src."press_release_date", '.') > 0 THEN substr(src."press_release_date", 7, 4) || '-' || substr(src."press_release_date", 4, 2) || '-' || substr(src."press_release_date", 1, 2)
                ELSE src."press_release_date"
            END) || '|' || printf('%012d', CAST(COALESCE(src."id", '0') AS INTEGER)) = latest.max_key
        '''
    )
    conn.commit()

    set_meta_value(conn, "nra_last_refresh_utc", loaded_at)
    set_meta_value(conn, "nra_last_rows_count", str(len(parsed_rows)))
    logger.info("NRA обновление завершено. Загружено строк: %s", len(parsed_rows))
    return True, len(parsed_rows)


def download_nkr_excel_via_playwright(logger: logging.Logger) -> bytes:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config.NKR_HEADLESS, channel=config.NKR_BROWSER_CHANNEL)
        context = browser.new_context(locale="ru-RU", timezone_id="Europe/Moscow", accept_downloads=True)
        page = context.new_page()
        try:
            page.goto(config.NKR_RATINGS_PAGE_URL, wait_until="domcontentloaded", timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))
            page.wait_for_selector(config.NKR_EXPORT_BUTTON_SELECTOR, timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))

            for attempt in range(1, config.NKR_DOWNLOAD_ATTEMPTS + 1):
                logger.info("НКР: попытка скачивания %s/%s", attempt, config.NKR_DOWNLOAD_ATTEMPTS)

                try:
                    with page.expect_download(timeout=8_000) as download_info:
                        page.locator(config.NKR_EXPORT_BUTTON_SELECTOR).first.click(timeout=5_000)
                    download = download_info.value
                    download_path = download.path()
                    bytes_data = Path(download_path).read_bytes() if download_path else b""
                    if bytes_data:
                        logger.info("НКР: файл получен через expect_download")
                        return bytes_data
                except Exception as exc:
                    logger.info("НКР: expect_download не сработал (%s)", exc)

                href_value = page.locator(config.NKR_EXPORT_BUTTON_SELECTOR).first.get_attribute("href") or ""
                if href_value and not href_value.lower().startswith("blob:"):
                    direct_url = urljoin(config.NKR_RATINGS_PAGE_URL, href_value)
                    response = context.request.get(direct_url, timeout=int(config.REQUEST_TIMEOUT_SECONDS * 1000))
                    if response.ok and response.body():
                        logger.info("НКР: файл получен по прямой ссылке %s", direct_url)
                        return response.body()

                blob_url = page.evaluate(
                    '''(selector) => {
                        const node = document.querySelector(selector);
                        const href = (node?.getAttribute('href') || '').trim();
                        if (href.startsWith('blob:')) return href;

                        const asText = (el) => (el?.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
                        const linkByText = [...document.querySelectorAll('a')]
                            .find((el) => asText(el).includes('выгрузить в excel'));
                        const textHref = (linkByText?.getAttribute('href') || '').trim();
                        if (textHref.startsWith('blob:')) return textHref;

                        const links = [...document.querySelectorAll('a[href^="blob:"]')];
                        return links.length > 0 ? links[0].getAttribute('href') : '';
                    }''',
                    config.NKR_EXPORT_BUTTON_SELECTOR,
                )

                if blob_url:
                    payload_b64 = page.evaluate(
                        '''async (blobHref) => {
                            const response = await fetch(blobHref);
                            const buffer = await response.arrayBuffer();
                            const bytes = new Uint8Array(buffer);
                            const chunkSize = 0x8000;
                            let binary = '';
                            for (let i = 0; i < bytes.length; i += chunkSize) {
                                const chunk = bytes.subarray(i, i + chunkSize);
                                binary += String.fromCharCode(...chunk);
                            }
                            return btoa(binary);
                        }''',
                        blob_url,
                    )
                    bytes_data = base64.b64decode(payload_b64)
                    if bytes_data:
                        logger.info("НКР: файл получен через blob-ссылку")
                        return bytes_data

                page.wait_for_timeout(1200)

            raise ValueError("На странице НКР не удалось получить Excel-файл (ни download, ни href, ни blob).")
        except Exception:
            logger.exception("Ошибка скачивания НКР через Playwright")
            raise
        finally:
            context.close()
            browser.close()


def refresh_nkr_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int]:
    ensure_nkr_tables(conn)
    current = conn.execute(f'SELECT COUNT(*) FROM "{config.NKR_TABLE_NAME}"').fetchone()
    current_total = int(current[0]) if current else 0
    if not should_refresh_nkr(conn, now_utc):
        return False, current_total

    try:
        content = download_nkr_excel_via_playwright(logger)
    except Exception as exc:
        logger.warning("НКР обновление пропущено из-за сетевой ошибки: %s", exc)
        return False, current_total
    raw_path = config.RAW_DIR / config.NKR_RAW_FILENAME
    if raw_path.exists():
        raw_path.unlink()
    raw_path.write_bytes(content)

    parsed_rows = parse_nkr_excel(content)
    loaded_at = now_utc.isoformat()
    payload = [
        (
            row.get("id", ""),
            row.get("issuer_name", ""),
            row.get("rating_date", ""),
            row.get("rating", ""),
            row.get("outlook", ""),
            row.get("tin", ""),
            loaded_at,
        )
        for row in parsed_rows
        if row.get("tin", "").strip()
    ]

    conn.executemany(
        f'''
        INSERT INTO "{config.NKR_TABLE_NAME}" (
            "id", "issuer_name", "rating_date", "rating", "outlook", "tin", "loaded_at_utc"
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT("tin", "rating_date", "rating", "outlook") DO UPDATE SET
            "issuer_name"=excluded."issuer_name",
            "id"=excluded."id",
            "loaded_at_utc"=excluded."loaded_at_utc"
        ''',
        payload,
    )

    conn.execute(f'DELETE FROM "{config.NKR_LATEST_TABLE_NAME}"')
    conn.execute(
        f'''
        INSERT INTO "{config.NKR_LATEST_TABLE_NAME}" (
            "tin", "issuer_name", "rating_date", "rating", "outlook"
        )
        SELECT src."tin", src."issuer_name", src."rating_date", src."rating", src."outlook"
        FROM "{config.NKR_TABLE_NAME}" src
        JOIN (
            SELECT "tin", MAX(
                (CASE
                    WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                    ELSE "rating_date"
                END) || '|' || COALESCE("loaded_at_utc", '')
            ) AS max_key
            FROM "{config.NKR_TABLE_NAME}"
            WHERE TRIM(COALESCE("tin", '')) <> ''
            GROUP BY "tin"
        ) latest ON latest."tin" = src."tin"
        WHERE
            (CASE
                WHEN instr(src."rating_date", '.') > 0 THEN substr(src."rating_date", 7, 4) || '-' || substr(src."rating_date", 4, 2) || '-' || substr(src."rating_date", 1, 2)
                ELSE src."rating_date"
            END) || '|' || COALESCE(src."loaded_at_utc", '') = latest.max_key
        '''
    )
    conn.commit()

    set_meta_value(conn, "nkr_last_refresh_utc", loaded_at)
    set_meta_value(conn, "nkr_last_rows_count", str(len(parsed_rows)))
    logger.info("НКР обновление завершено. Загружено строк: %s", len(parsed_rows))
    return True, len(parsed_rows)


def sync_nra_rate_to_emitents(main_conn: sqlite3.Connection, nra_conn: sqlite3.Connection, logger: logging.Logger) -> int:
    rows = nra_conn.execute(
        f'''
        SELECT "inn", "rating", "forecast"
        FROM "{config.NRA_LATEST_TABLE_NAME}"
        WHERE TRIM(COALESCE("inn", '')) <> ''
        '''
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for inn, rating, forecast in rows:
        rating_text = (rating or "").strip()
        forecast_text = (forecast or "").strip().lower()
        if not rating_text:
            continue
        updates.append((f"{rating_text}({forecast_text})" if forecast_text else rating_text, inn.strip()))

    main_conn.executemany(
        f'UPDATE "{config.EMITENTS_TABLE_NAME}" SET "NRA_Rate" = ? WHERE "INN" = ?',
        updates,
    )
    main_conn.commit()
    logger.info("NRA_Rate синхронизирован для INN: %s", len(updates))
    return len(updates)


def sync_acra_rate_to_emitents(main_conn: sqlite3.Connection, ratings_conn: sqlite3.Connection, logger: logging.Logger) -> int:
    rows = ratings_conn.execute(
        f'''
        SELECT src."inn", src."rating", src."forecast"
        FROM "{config.ACRA_TABLE_NAME}" src
        JOIN (
            SELECT "inn", MAX(
                (CASE
                    WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                    ELSE "rating_date"
                END) || '|' || "loaded_at_utc"
            ) AS max_key
            FROM "{config.ACRA_TABLE_NAME}"
            WHERE TRIM(COALESCE("inn", '')) <> ''
            GROUP BY "inn"
        ) latest ON latest."inn" = src."inn"
        WHERE
            (CASE
                WHEN instr(src."rating_date", '.') > 0 THEN substr(src."rating_date", 7, 4) || '-' || substr(src."rating_date", 4, 2) || '-' || substr(src."rating_date", 1, 2)
                ELSE src."rating_date"
            END) || '|' || src."loaded_at_utc" = latest.max_key
        '''
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for inn, rating, forecast in rows:
        rating_text = (rating or "").strip()
        forecast_text = (forecast or "").strip().lower()
        if not rating_text:
            continue

        base_rating = rating_text
        if not forecast_text:
            base_rating, forecast_text = split_acra_rating_and_forecast(rating_text)
            forecast_text = forecast_text.lower()
        if not base_rating:
            continue

        rate_for_showcase = f"{base_rating}({forecast_text})" if forecast_text else base_rating
        updates.append((rate_for_showcase, inn.strip()))

    main_conn.executemany(
        f'UPDATE "{config.EMITENTS_TABLE_NAME}" SET "Acra_Rate" = ? WHERE "INN" = ?',
        updates,
    )
    main_conn.commit()
    logger.info("Acra_Rate синхронизирован для INN: %s", len(updates))
    return len(updates)


def sync_nkr_rate_to_emitents(main_conn: sqlite3.Connection, ratings_conn: sqlite3.Connection, logger: logging.Logger) -> int:
    rows = ratings_conn.execute(
        f'''
        SELECT "tin", "rating", "outlook"
        FROM "{config.NKR_LATEST_TABLE_NAME}"
        WHERE TRIM(COALESCE("tin", '')) <> ''
        '''
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for tin, rating, outlook in rows:
        rating_text = (rating or "").strip()
        outlook_text = (outlook or "").strip().lower()
        if not rating_text:
            continue
        updates.append((f"{rating_text}({outlook_text})" if outlook_text else rating_text, tin.strip()))

    main_conn.executemany(
        f'UPDATE "{config.EMITENTS_TABLE_NAME}" SET "NKR_Rate" = ? WHERE "INN" = ?',
        updates,
    )
    main_conn.commit()
    logger.info("NKR_Rate синхронизирован для INN: %s", len(updates))
    return len(updates)


def sync_raex_rate_to_emitents(main_conn: sqlite3.Connection, ratings_conn: sqlite3.Connection, logger: logging.Logger) -> int:
    rows = ratings_conn.execute(
        f''' 
        SELECT "inn", "rating", "forecast"
        FROM "{config.RAEX_LATEST_TABLE_NAME}"
        WHERE TRIM(COALESCE("inn", '')) <> ''
        '''
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for inn, rating, forecast in rows:
        rating_text = (rating or "").strip()
        forecast_text = (forecast or "").strip().lower()
        if not rating_text:
            continue
        updates.append((f"{rating_text}({forecast_text})" if forecast_text else rating_text, inn.strip()))

    main_conn.executemany(
        f'UPDATE "{config.EMITENTS_TABLE_NAME}" SET "RAEX_Rate" = ? WHERE "INN" = ?',
        updates,
    )
    main_conn.commit()
    logger.info("RAEX_Rate синхронизирован для INN: %s", len(updates))
    return len(updates)


def sync_emitents_from_rates(conn: sqlite3.Connection, logger: logging.Logger) -> int:
    cursor = conn.execute(
        f'''
        INSERT INTO "{config.EMITENTS_TABLE_NAME}" ("INN", "EMITENTNAME")
        SELECT DISTINCT TRIM("INN"), TRIM("EMITENTNAME")
        FROM "{config.RATES_TABLE_NAME}"
        WHERE TRIM(COALESCE("INN", '')) <> ''
          AND TRIM(COALESCE("EMITENTNAME", '')) <> ''
        ON CONFLICT("INN") DO UPDATE SET
            "EMITENTNAME" = excluded."EMITENTNAME"
        '''
    )
    conn.commit()
    affected = cursor.rowcount if cursor.rowcount is not None else 0
    logger.info("Синхронизация эмитентов из moex_bonds завершена. Затронуто строк: %s", affected)
    return max(affected, 0)


def pull_scoring_from_excel(conn: sqlite3.Connection, logger: logging.Logger, today_str: str) -> int:
    excel_path = config.BASE_DIR / config.EMITENTS_XLSX_FILENAME
    if not excel_path.exists():
        logger.info("Файл витрины %s пока отсутствует — перенос оценок из Excel пропущен.", excel_path)
        return 0

    wb = load_workbook(excel_path)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return 0

    headers = [str(cell).strip() if cell is not None else "" for cell in rows[0]]
    required = {"INN", "Scoring", "DateScoring"}
    if not required.issubset(set(headers)):
        logger.warning("В %s не найдены обязательные колонки INN/Scoring/DateScoring.", excel_path)
        return 0

    inn_idx = headers.index("INN")
    scoring_idx = headers.index("Scoring")
    date_idx = headers.index("DateScoring")
    allowed_scoring = set(config.SCORING_ALLOWED_VALUES)

    updates: list[tuple[str | None, str | None, str]] = []
    for row in rows[1:]:
        inn = str(row[inn_idx]).strip() if inn_idx < len(row) and row[inn_idx] is not None else ""
        if not inn:
            continue
        scoring_val = ""
        if scoring_idx < len(row) and row[scoring_idx] is not None:
            scoring_val = str(row[scoring_idx]).strip()
        if scoring_val and scoring_val not in allowed_scoring:
            logger.warning(
                "Пропущено некорректное значение Scoring='%s' для INN=%s. Допустимые значения: %s",
                scoring_val,
                inn,
                ", ".join(config.SCORING_ALLOWED_VALUES),
            )
            scoring_val = ""
        date_val = ""
        if date_idx < len(row) and row[date_idx] is not None:
            date_val = str(row[date_idx]).strip()

        scoring_db = scoring_val or None
        date_db = date_val or (today_str if scoring_db else None)
        updates.append((scoring_db, date_db, inn))

    if not updates:
        return 0

    conn.executemany(
        f'''
        UPDATE "{config.EMITENTS_TABLE_NAME}"
        SET "Scoring" = ?,
            "DateScoring" = ?
        WHERE "INN" = ?
        ''',
        updates,
    )
    conn.commit()
    logger.info("Перенос ручных Scoring из Excel в SQL: обработано строк=%s", len(updates))
    return len(updates)


def ensure_scoring_dates(conn: sqlite3.Connection, logger: logging.Logger, today_str: str) -> int:
    cursor = conn.execute(
        f'''
        UPDATE "{config.EMITENTS_TABLE_NAME}"
        SET "DateScoring" = ?
        WHERE TRIM(COALESCE("Scoring", '')) <> ''
          AND TRIM(COALESCE("DateScoring", '')) = ''
        ''',
        (today_str,),
    )
    conn.commit()
    fixed = cursor.rowcount if cursor.rowcount is not None else 0
    logger.info("Автозаполнение DateScoring выполнено. Добавлено дат: %s", fixed)
    return max(fixed, 0)


def export_emitents_excel(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "EMITENTNAME", "INN", "Scoring", "DateScoring", "NRA_Rate", "Acra_Rate", "NKR_Rate", "RAEX_Rate"
        FROM "{config.EMITENTS_TABLE_NAME}"
        ORDER BY "EMITENTNAME", "INN"
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "emitents"
    ws.append(headers)

    for row in rows:
        ws.append(list(row))

    header_fill = PatternFill(fill_type="solid", fgColor=config.EMITENTS_HEADER_FILL_COLOR)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"

    scoring_column_index = headers.index("Scoring") + 1
    scoring_column_letter = ws.cell(row=1, column=scoring_column_index).column_letter
    validation_values = ",".join(config.SCORING_ALLOWED_VALUES)
    scoring_validation = DataValidation(
        type="list",
        formula1=f'"{validation_values}"',
        allow_blank=True,
        showErrorMessage=True,
        errorStyle="stop",
        errorTitle="Недопустимое значение",
        error=f"Выберите одно из значений: {validation_values}",
        promptTitle="Scoring",
        prompt=f"Доступные значения: {validation_values}",
    )
    ws.add_data_validation(scoring_validation)
    scoring_validation.add(f"{scoring_column_letter}2:{scoring_column_letter}1048576")

    for column_cells in ws.columns:
        max_len = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(value))
        ws.column_dimensions[column_letter].width = min(max_len + 2, 80)

    for rating_column_name in ("NRA_Rate", "Acra_Rate", "NKR_Rate", "RAEX_Rate"):
        if rating_column_name not in headers:
            continue
        rating_column_index = headers.index(rating_column_name) + 1
        rating_column_letter = ws.cell(row=1, column=rating_column_index).column_letter
        ws.column_dimensions[rating_column_letter].width = config.EMITENTS_RATINGS_COLUMN_WIDTH

    excel_path = config.BASE_DIR / config.EMITENTS_XLSX_FILENAME
    wb.save(excel_path)
    return len(rows)


def export_emitents_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "EMITENTNAME", "INN", "Scoring", "DateScoring", "NRA_Rate", "Acra_Rate", "NKR_Rate", "RAEX_Rate"
        FROM "{config.EMITENTS_TABLE_NAME}"
        ORDER BY RANDOM()
        LIMIT 5
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "emitents_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.EMITENTS_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def export_nra_snapshot(conn: sqlite3.Connection) -> int:
    query = f'''
    SELECT "organization_name", "inn", "press_release_date", "rating", "rating_status", "forecast"
    FROM "{config.NRA_LATEST_TABLE_NAME}"
    ORDER BY
        CASE
            WHEN instr("press_release_date", '.') > 0 THEN substr("press_release_date", 7, 4) || '-' || substr("press_release_date", 4, 2) || '-' || substr("press_release_date", 1, 2)
            ELSE "press_release_date"
        END DESC,
        "inn" ASC
    LIMIT 5
    '''
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "nra_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.NRA_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def export_acra_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "issuer_name", "issuer_url", "rating", "forecast", "rating_date", "inn"
        FROM "{config.ACRA_TABLE_NAME}"
        ORDER BY
            CASE
                WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                ELSE "rating_date"
            END DESC,
            "loaded_at_utc" DESC
        LIMIT 5
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "acra_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.ACRA_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def export_nkr_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "issuer_name", "tin", "rating_date", "rating", "outlook"
        FROM "{config.NKR_LATEST_TABLE_NAME}"
        ORDER BY
            CASE
                WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                ELSE "rating_date"
            END DESC,
            "tin" ASC
        LIMIT 5
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "nkr_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.NKR_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def export_raex_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "inn", "company_name", "rating_date", "rating", "forecast", "company_url"
        FROM "{config.RAEX_LATEST_TABLE_NAME}"
        ORDER BY
            CASE
                WHEN instr("rating_date", '.') > 0 THEN substr("rating_date", 7, 4) || '-' || substr("rating_date", 4, 2) || '-' || substr("rating_date", 1, 2)
                ELSE "rating_date"
            END DESC,
            "inn" ASC
        LIMIT 5
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "raex_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.RAEX_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def save_text_file(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def refresh_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int]:
    raw_path = config.RAW_DIR / config.RAW_FILENAME
    cache_path = config.CACHE_DIR / config.CACHE_FILENAME

    if not should_refresh_cache(conn, now_utc):
        logger.info("Кэш актуален: загрузка из сети пропущена.")
        row = conn.execute(f'SELECT COUNT(*) FROM "{config.RATES_TABLE_NAME}"').fetchone()
        return False, int(row[0]) if row else 0

    raw_text = download_csv(config.SOURCE_CSV_URL, config.REQUEST_TIMEOUT_SECONDS)

    if raw_path.exists():
        raw_path.unlink()
    if cache_path.exists():
        cache_path.unlink()

    save_text_file(raw_path, raw_text)
    save_text_file(cache_path, raw_text)

    headers, rows = parse_moex_rates_csv(raw_text)
    replace_rates_table(conn, headers, rows)

    set_meta_value(conn, "last_refresh_utc", now_utc.isoformat())
    set_meta_value(conn, "last_rows_count", str(len(rows)))
    set_meta_value(conn, "last_headers", "|".join(headers))

    logger.info("Данные обновлены: строк=%s, колонок=%s", len(rows), len(headers))
    return True, len(rows)


def refresh_dohod_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int]:
    raw_path = config.RAW_DIR / config.DOHOD_RAW_FILENAME

    ensure_dohod_table(conn)
    if not should_refresh_dohod(conn, now_utc):
        logger.info("Доходъ: кэш актуален, загрузка из сети пропущена.")
        row = conn.execute(f'SELECT COUNT(*) FROM "{config.DOHOD_TABLE_NAME}"').fetchone()
        return False, int(row[0]) if row else 0

    content = download_dohod_excel_via_playwright(logger)
    if raw_path.exists():
        raw_path.unlink()
    raw_path.write_bytes(content)

    headers, rows = parse_dohod_excel(content)
    deduplicated_rows = _deduplicate_dohod_rows(rows, headers)
    ensure_dohod_table(conn, headers)
    ensure_table_has_columns(conn, config.DOHOD_TABLE_NAME, headers)

    insert_columns = [header for header in headers if header]
    quoted_cols = ", ".join([f'"{column}"' for column in insert_columns] + ['"loaded_at_utc"'])
    placeholders = ", ".join(["?"] * (len(insert_columns) + 1))
    update_cols = [column for column in insert_columns if column != "ISIN"]
    update_expr = ", ".join([f'"{column}"=excluded."{column}"' for column in update_cols] + ['"loaded_at_utc"=excluded."loaded_at_utc"'])
    payload = [
        tuple(((_normalize_isin(row.get(column, "")) if column == "ISIN" else row.get(column, "")) for column in insert_columns))
        + (now_utc.isoformat(),)
        for row in deduplicated_rows
        if _normalize_isin(row.get("ISIN", ""))
    ]

    conn.execute("BEGIN")
    conn.executemany(
        f'''
        INSERT INTO "{config.DOHOD_TABLE_NAME}" ({quoted_cols})
        VALUES ({placeholders})
        ON CONFLICT("ISIN") DO UPDATE SET {update_expr}
        ''',
        payload,
    )
    conn.commit()

    set_meta_value(conn, "dohod_last_refresh_utc", now_utc.isoformat())
    set_meta_value(conn, "dohod_last_rows_count", str(len(payload)))
    set_meta_value(conn, "dohod_last_headers", "|".join(insert_columns))
    logger.info(
        "Доходъ: данные обновлены, исходных строк=%s, после дедупликации по ISIN=%s, колонок=%s",
        len(rows),
        len(payload),
        len(insert_columns),
    )
    return True, len(payload)


def export_random_snapshot(conn: sqlite3.Connection) -> int:
    query = f"""
    SELECT *
    FROM "{config.RATES_TABLE_NAME}"
    WHERE rowid IN (
        SELECT MIN(rowid)
        FROM "{config.RATES_TABLE_NAME}"
        GROUP BY "SECID"
        ORDER BY RANDOM()
        LIMIT 5
    )
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def export_dohod_snapshot(conn: sqlite3.Connection) -> int:
    query = f'''
    SELECT *
    FROM "{config.DOHOD_TABLE_NAME}"
    WHERE rowid IN (
        SELECT MIN(rowid)
        FROM "{config.DOHOD_TABLE_NAME}"
        GROUP BY "ISIN"
        ORDER BY RANDOM()
        LIMIT 5
    )
    '''
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "dohod_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.DOHOD_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


MERGE_MOEX_COLUMNS = [
    "SECID",
    "ISIN",
    "FACEVALUE",
    "FACEUNIT",
    "MATDATE",
    "IS_QUALIFIED_INVESTORS",
    "BOND_TYPE",
    "BOND_SUBTYPE",
    "YIELDATWAP",
    "PRICE",
]

MERGE_DOHOD_COLUMNS = [
    "Название",
    "Ближайшая дата погашения/оферты (Дата)",
    "Событие в дату",
    "Коэф. Ликвидности (max=100)",
    "Медиана дневного оборота (млн в валюте торгов)",
    "Цена, % от номинала",
    "НКД",
    "Размер купона",
    "Текущий купон, %",
    "Тип купона",
    "Купон (раз/год)",
    "Субординированная (да/нет)",
    "Базовый индекс (для FRN)",
    "Премия/Дисконт к базовому индексу (для FRN)",
]

CORPBONDS_COLUMNS_MAP = {
    'Corpbonds_Цена последняя': 'Цена последняя',
    'Corpbonds_Тип купона': 'Тип купона',
    'Corpbonds_Формула купона': 'Формула купона',
    'Corpbonds_Дата ближайшего купона': 'Дата ближайшего купона',
    'Corpbonds_Дата ближайшей оферты': 'Дата ближайшей оферты',
    'Corpbonds_Наличие амортизации': 'Наличие амортизации',
    'Corpbonds_Купон лесенкой': 'Купон лесенкой',
}


def ensure_table_columns(conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
    existing_columns = {
        str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    }
    for column in columns:
        if column not in existing_columns:
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" TEXT')


def ensure_merge_table(conn: sqlite3.Connection, table_name: str) -> None:
    columns_sql = ['"ISIN" TEXT PRIMARY KEY']
    for column in MERGE_MOEX_COLUMNS:
        if column == "ISIN":
            continue
        columns_sql.append(f'"{column}" TEXT')
    for column in MERGE_DOHOD_COLUMNS:
        columns_sql.append(f'"{column}" TEXT')
    for column in CORPBONDS_COLUMNS_MAP:
        columns_sql.append(f'"{column}" TEXT')
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_sql)})')
    ensure_table_columns(conn, table_name, list(CORPBONDS_COLUMNS_MAP.keys()))
    conn.commit()


def rebuild_merge_table_by_scoring(conn: sqlite3.Connection, table_name: str, scoring: str) -> int:
    ensure_merge_table(conn, table_name)

    dohod_join_type = "INNER" if getattr(config, "MERGE_REQUIRE_DOHOD_ISIN_MATCH", True) else "LEFT"

    insert_columns = [f'"{column}"' for column in MERGE_MOEX_COLUMNS if column != "ISIN"]
    insert_columns = ['"ISIN"'] + insert_columns + [f'"{column}"' for column in MERGE_DOHOD_COLUMNS]
    insert_columns += [f'"{column}"' for column in CORPBONDS_COLUMNS_MAP]

    selected_columns = [f'm."ISIN"']
    selected_columns.extend(
        f'm."{column}"' for column in MERGE_MOEX_COLUMNS if column != "ISIN"
    )
    selected_columns.extend(f'd."{column}"' for column in MERGE_DOHOD_COLUMNS)
    selected_columns.extend(f"'' AS \"{column}\"" for column in CORPBONDS_COLUMNS_MAP)
    placeholders = ", ".join(["?"] * len(insert_columns))

    rows = conn.execute(
        f'''
        SELECT {", ".join(selected_columns)}
        FROM "{config.RATES_TABLE_NAME}" m
        INNER JOIN "{config.EMITENTS_TABLE_NAME}" e
            ON TRIM(COALESCE(m."INN", '')) = TRIM(COALESCE(e."INN", ''))
        {dohod_join_type} JOIN "{config.DOHOD_TABLE_NAME}" d
            ON TRIM(COALESCE(m."ISIN", '')) = TRIM(COALESCE(d."ISIN", ''))
        WHERE LOWER(TRIM(COALESCE(e."Scoring", ''))) = LOWER(TRIM(?))
          AND TRIM(COALESCE(m."ISIN", '')) <> ''
        ''',
        (scoring,),
    ).fetchall()

    conn.execute("BEGIN")
    conn.execute(f'DELETE FROM "{table_name}"')
    if rows:
        conn.executemany(
            f'INSERT OR REPLACE INTO "{table_name}" ({", ".join(insert_columns)}) VALUES ({placeholders})',
            rows,
        )
    conn.commit()
    return len(rows)


def export_merge_snapshot(conn: sqlite3.Connection, table_name: str, snapshot_filename: str, sheet_name: str) -> int:
    query = f'''
    SELECT *
    FROM "{table_name}"
    WHERE rowid IN (
        SELECT MIN(rowid)
        FROM "{table_name}"
        GROUP BY "ISIN"
        ORDER BY RANDOM()
        LIMIT 5
    )
    '''
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / snapshot_filename
    wb.save(snapshot_path)
    return len(rows)


def _parse_bond_date(raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None

    date_part = value.split()[0].replace("/", ".")
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y", "%Y.%m.%d"):
        try:
            return datetime.strptime(date_part, fmt)
        except ValueError:
            continue
    return None


def _normalize_bond_type(raw_value: str | None) -> str:
    value = str(raw_value or "").replace("\xa0", " ")
    return " ".join(value.split()).casefold()


def presort_merge_table(conn: sqlite3.Connection, table_name: str) -> dict[str, int]:
    min_days = config.PRESORTER_MIN_DAYS_TO_EVENT
    excluded_bond_type = _normalize_bond_type(config.PRESORTER_EXCLUDED_BOND_TYPE)
    use_dohod_nearest_date = bool(getattr(config, "PRESORTER_USE_DOHOD_NEAREST_DATE", True))
    today = datetime.now().date()

    rows_before = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    rows = conn.execute(
        f'SELECT "ISIN", "MATDATE", "Ближайшая дата погашения/оферты (Дата)", "BOND_TYPE" FROM "{table_name}"'
    ).fetchall()

    matdate_rule_isins: set[str] = set()
    dohod_nearest_rule_isins: set[str] = set()
    bond_type_rule_isins: set[str] = set()

    for isin, matdate, nearest_date, bond_type in rows:
        isin_value = str(isin or "").strip()
        if not isin_value:
            continue

        mat_dt = _parse_bond_date(matdate)
        if mat_dt is not None and (mat_dt.date() - today).days < min_days:
            matdate_rule_isins.add(isin_value)

        if use_dohod_nearest_date:
            nearest_dt = _parse_bond_date(nearest_date)
            if nearest_dt is not None and (nearest_dt.date() - today).days < min_days:
                dohod_nearest_rule_isins.add(isin_value)

        if excluded_bond_type and _normalize_bond_type(bond_type) == excluded_bond_type:
            bond_type_rule_isins.add(isin_value)

    isins_to_delete = matdate_rule_isins | dohod_nearest_rule_isins | bond_type_rule_isins
    if isins_to_delete:
        placeholders = ", ".join(["?"] * len(isins_to_delete))
        conn.execute("BEGIN")
        conn.execute(f'DELETE FROM "{table_name}" WHERE "ISIN" IN ({placeholders})', tuple(isins_to_delete))
        conn.commit()

    rows_after = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "excluded_by_matdate_rule": len(matdate_rule_isins),
        "excluded_by_dohod_nearest_rule": len(dohod_nearest_rule_isins),
        "excluded_by_bond_type_rule": len(bond_type_rule_isins),
        "excluded_total": len(isins_to_delete),
    }


def ensure_corpbonds_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.CORPBONDS_TABLE_NAME}" (
            "SECID" TEXT PRIMARY KEY,
            "Цена последняя" TEXT,
            "Тип купона" TEXT,
            "Формула купона" TEXT,
            "Дата ближайшего купона" TEXT,
            "Дата ближайшей оферты" TEXT,
            "Наличие амортизации" TEXT,
            "Купон лесенкой" TEXT,
            "source_url" TEXT,
            "updated_at_utc" TEXT NOT NULL
        )
        '''
    )
    conn.commit()


def collect_merge_secids(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        f'''
        SELECT DISTINCT TRIM(COALESCE("SECID", ''))
        FROM "{config.MERGE_GREEN_TABLE_NAME}"
        WHERE TRIM(COALESCE("SECID", '')) <> ''
        UNION
        SELECT DISTINCT TRIM(COALESCE("SECID", ''))
        FROM "{config.MERGE_YELLOW_TABLE_NAME}"
        WHERE TRIM(COALESCE("SECID", '')) <> ''
        ORDER BY 1
        '''
    ).fetchall()
    return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]


def _normalize_label(raw_label: str) -> str:
    cleaned = str(raw_label or "").replace("\xa0", " ").replace("?", " ")
    return " ".join(cleaned.split()).casefold()


def parse_corpbonds_page_fields(raw_html: str) -> dict[str, str]:
    soup = BeautifulSoup(raw_html, "lxml")
    parsed = {
        "Цена последняя": "",
        "Тип купона": "",
        "Формула купона": "",
        "Дата ближайшего купона": "",
        "Дата ближайшей оферты": "",
        "Наличие амортизации": "",
        "Купон лесенкой": "",
    }
    for row in soup.select("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue
        label = " ".join(tds[0].stripped_strings)
        value = " ".join(tds[-1].stripped_strings)
        if not label:
            continue
        normalized = _normalize_label(label)
        if normalized.startswith("цена последняя"):
            parsed["Цена последняя"] = value
        elif normalized.startswith("тип купона"):
            parsed["Тип купона"] = value
        elif normalized.startswith("формула купона") or normalized.startswith("формула флоатера"):
            parsed["Формула купона"] = value
        elif normalized.startswith("дата ближайшего купона"):
            parsed["Дата ближайшего купона"] = value
        elif normalized.startswith("дата ближайшей оферты"):
            parsed["Дата ближайшей оферты"] = value
        elif normalized.startswith("наличие амортизации"):
            parsed["Наличие амортизации"] = value
        elif normalized.startswith("купон лесенкой"):
            parsed["Купон лесенкой"] = value
    return parsed


def _is_corpbonds_stale(updated_at_raw: str | None, now_utc: datetime) -> bool:
    if not updated_at_raw:
        return True
    try:
        updated_at = datetime.fromisoformat(updated_at_raw)
    except ValueError:
        return True
    return now_utc - updated_at >= timedelta(hours=config.CORPBONDS_CACHE_TTL_HOURS)


def get_stale_corpbonds_secids(conn: sqlite3.Connection, secids: list[str], now_utc: datetime) -> list[str]:
    if not secids:
        return []

    stale: list[str] = []
    chunk_size = 500
    for start in range(0, len(secids), chunk_size):
        chunk = secids[start : start + chunk_size]
        placeholders = ", ".join(["?"] * len(chunk))
        rows = conn.execute(
            f'SELECT "SECID", "updated_at_utc" FROM "{config.CORPBONDS_TABLE_NAME}" WHERE "SECID" IN ({placeholders})',
            tuple(chunk),
        ).fetchall()
        actual = {str(row[0]): str(row[1] or "") for row in rows}
        for secid in chunk:
            if _is_corpbonds_stale(actual.get(secid), now_utc):
                stale.append(secid)

    return stale


def fetch_corpbonds_payload(secid: str) -> dict[str, str]:
    if not hasattr(fetch_corpbonds_payload, "_thread_local"):
        fetch_corpbonds_payload._thread_local = threading.local()  # type: ignore[attr-defined]
    thread_local = fetch_corpbonds_payload._thread_local  # type: ignore[attr-defined]
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()

    url = f"{config.CORPBONDS_BOND_URL_PREFIX}{secid}"
    response = thread_local.session.get(url, timeout=config.CORPBONDS_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = parse_corpbonds_page_fields(response.text)
    data["SECID"] = secid
    data["source_url"] = url
    return data


def refresh_corpbonds_data_if_needed(
    conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime
) -> tuple[int, int, int]:
    ensure_corpbonds_table(conn)
    secids = collect_merge_secids(conn)
    if not secids:
        return 0, 0, 0

    stale_secids = get_stale_corpbonds_secids(conn, secids, now_utc)
    fetched_rows: list[dict[str, str]] = []
    errors = 0

    if stale_secids:
        with ThreadPoolExecutor(max_workers=config.CORPBONDS_MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_corpbonds_payload, secid): secid for secid in stale_secids}
            with progress(total=len(stale_secids), desc="Corpbonds fetch", unit="бумаг", position=1) as pbar:
                for future in as_completed(futures):
                    secid = futures[future]
                    try:
                        fetched_rows.append(future.result())
                    except Exception as exc:
                        errors += 1
                        logger.warning("Corpbonds: ошибка SECID=%s: %s", secid, exc)
                    pbar.update(1)

    if fetched_rows:
        now_iso = now_utc.isoformat()
        payload = [
            (
                row.get("SECID", ""),
                row.get("Цена последняя", ""),
                row.get("Тип купона", ""),
                row.get("Формула купона", ""),
                row.get("Дата ближайшего купона", ""),
                row.get("Дата ближайшей оферты", ""),
                row.get("Наличие амортизации", ""),
                row.get("Купон лесенкой", ""),
                row.get("source_url", ""),
                now_iso,
            )
            for row in fetched_rows
            if row.get("SECID", "")
        ]
        conn.execute("BEGIN")
        conn.executemany(
            f'''
            INSERT INTO "{config.CORPBONDS_TABLE_NAME}" (
                "SECID", "Цена последняя", "Тип купона", "Формула купона",
                "Дата ближайшего купона", "Дата ближайшей оферты", "Наличие амортизации",
                "Купон лесенкой", "source_url", "updated_at_utc"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT("SECID") DO UPDATE SET
                "Цена последняя"=excluded."Цена последняя",
                "Тип купона"=excluded."Тип купона",
                "Формула купона"=excluded."Формула купона",
                "Дата ближайшего купона"=excluded."Дата ближайшего купона",
                "Дата ближайшей оферты"=excluded."Дата ближайшей оферты",
                "Наличие амортизации"=excluded."Наличие амортизации",
                "Купон лесенкой"=excluded."Купон лесенкой",
                "source_url"=excluded."source_url",
                "updated_at_utc"=excluded."updated_at_utc"
            ''',
            payload,
        )
        conn.commit()

    logger.info(
        "Corpbonds: SECID в Merge=%s, запрошено=%s, сохранено=%s, ошибок=%s, TTL=%s ч",
        len(secids),
        len(stale_secids),
        len(fetched_rows),
        errors,
        config.CORPBONDS_CACHE_TTL_HOURS,
    )
    return len(secids), len(stale_secids), len(fetched_rows)


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()]


def apply_corpbonds_inner_join_to_merge_table(conn: sqlite3.Connection, table_name: str) -> int:
    ensure_merge_table(conn, table_name)
    merge_columns = _get_table_columns(conn, table_name)
    require_match = bool(getattr(config, "MERGE_REQUIRE_CORPBONDS_SECID_MATCH", True))
    select_columns: list[str] = []
    for column in merge_columns:
        if column in CORPBONDS_COLUMNS_MAP:
            source_column = CORPBONDS_COLUMNS_MAP[column]
            select_columns.append(f'c."{source_column}" AS "{column}"')
        else:
            select_columns.append(f'm."{column}"')

    join_type = "INNER" if require_match else "LEFT"
    secid_condition = "WHERE TRIM(COALESCE(m.\"SECID\", '')) <> ''" if require_match else ""

    conn.execute("BEGIN")
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}__tmp_corpbonds"')
    conn.execute(
        f'''
        CREATE TABLE "{table_name}__tmp_corpbonds" AS
        SELECT {", ".join(select_columns)}
        FROM "{table_name}" m
        {join_type} JOIN "{config.CORPBONDS_TABLE_NAME}" c
            ON TRIM(COALESCE(m."SECID", '')) = TRIM(COALESCE(c."SECID", ''))
        {secid_condition}
        '''
    )
    conn.execute(f'DELETE FROM "{table_name}"')
    conn.execute(
        f'''
        INSERT OR REPLACE INTO "{table_name}" ({", ".join(f'"{column}"' for column in merge_columns)})
        SELECT {", ".join(f'"{column}"' for column in merge_columns)}
        FROM "{table_name}__tmp_corpbonds"
        '''
    )
    conn.execute(f'DROP TABLE "{table_name}__tmp_corpbonds"')
    rows_after = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    conn.commit()
    return int(rows_after)


def export_corpbonds_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT *
        FROM "{config.CORPBONDS_TABLE_NAME}"
        WHERE rowid IN (
            SELECT MIN(rowid)
            FROM "{config.CORPBONDS_TABLE_NAME}"
            GROUP BY "SECID"
            ORDER BY RANDOM()
            LIMIT 5
        )
        '''
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "corpbonds_snapshot"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))

    snapshot_path = config.BASE_SNAPSHOTS_DIR / config.CORPBONDS_SNAPSHOT_FILENAME
    wb.save(snapshot_path)
    return len(rows)


def main() -> None:
    logger = setup_logging()
    stage_times: dict[str, float] = {}
    presorter_summary: dict[str, dict[str, int]] = {}
    started = perf_counter()

    db_path = config.DB_DIR / config.DB_FILENAME
    ratings_db_path = config.DB_DIR / config.RAITINGS_DB_FILENAME

    try:
        print("=====\nЭтап 1: Подготовка окружения")
        s = perf_counter()
        with progress(total=2, desc="Подготовка", unit="шаг") as pbar:
            ensure_directories()
            migrate_legacy_db_if_needed()
            pbar.update(1)
            pbar.set_description("Подготовка БД")
            with connect_db(db_path) as conn:
                init_meta_table(conn)
                migrate_legacy_rates_table_if_needed(conn)
                ensure_emitents_table(conn)
                ensure_dohod_table(conn)
            pbar.update(1)
        stage_times["Этап 1: Подготовка окружения"] = perf_counter() - s

        print("Этап 2: Проверка TTL кэша и обновление данных")
        s = perf_counter()
        with progress(total=3, desc="Обновление данных", unit="шаг") as pbar:
            now_utc = datetime.now(timezone.utc)
            pbar.update(1)
            pbar.set_description("Работа с SQLite")
            with connect_db(db_path) as conn:
                init_meta_table(conn)
                refreshed, row_count = refresh_data_if_needed(conn, logger, now_utc)
            pbar.update(1)
            pbar.set_description("Финализация")
            logger.info("Режим: %s", "обновлено из сети" if refreshed else "использован локальный кэш")
            logger.info("Количество строк в таблице %s: %s", config.RATES_TABLE_NAME, row_count)
            pbar.update(1)
        stage_times["Этап 2: Проверка TTL кэша и обновление данных"] = perf_counter() - s

        print("Этап 3: Доходъ (Playwright Excel + SQLite + snapshot)")
        s = perf_counter()
        with progress(total=3, desc="Dohod", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                refreshed, dohod_rows = refresh_dohod_data_if_needed(conn, logger, datetime.now(timezone.utc))
                pbar.update(1)
                dohod_snapshot = export_dohod_snapshot(conn)
            pbar.update(1)
            logger.info(
                "Доходъ: режим=%s, строк в таблице=%s, строк в snapshot=%s",
                "обновлено из сети" if refreshed else "использован локальный кэш",
                dohod_rows,
                dohod_snapshot,
            )
            pbar.update(1)
        stage_times["Этап 3: Доходъ (Playwright Excel + SQLite + snapshot)"] = perf_counter() - s

        print("Этап 4: Формирование Excel-среза MOEX")
        s = perf_counter()
        with progress(total=2, desc="MOEX snapshot", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                count = export_random_snapshot(conn)
            pbar.update(1)
            logger.info("Сформирован Excel-срез MOEX: строк=%s", count)
            pbar.update(1)
        stage_times["Этап 4: Формирование Excel-среза MOEX"] = perf_counter() - s

        print("Этап 5: Рейтинги агентств (НРА + АКРА + НКР + RAEX, отдельная SQLite)")
        s = perf_counter()
        with progress(total=10, desc="NRA/ACRA/NKR/RAEX", unit="шаг", position=1) as pbar:
            now_utc = datetime.now(timezone.utc)
            with connect_db(ratings_db_path) as nra_conn:
                init_meta_table(nra_conn)
                ensure_nra_tables(nra_conn)
                ensure_acra_tables(nra_conn)
                ensure_nkr_tables(nra_conn)
                ensure_raex_tables(nra_conn)
                nra_refreshed, nra_rows = refresh_nra_data_if_needed(nra_conn, logger, now_utc)
                nra_snapshot_rows = export_nra_snapshot(nra_conn)
                pbar.update(1)
                acra_refreshed, acra_rows, acra_cards = refresh_acra_data_if_needed(nra_conn, logger, now_utc)
                acra_snapshot_rows = export_acra_snapshot(nra_conn)
                pbar.update(1)
                nkr_refreshed, nkr_rows = refresh_nkr_data_if_needed(nra_conn, logger, now_utc)
                nkr_snapshot_rows = export_nkr_snapshot(nra_conn)
                pbar.update(1)
                raex_refreshed, raex_rows, raex_inns, raex_errors = refresh_raex_data_if_needed(nra_conn, db_path, logger, now_utc)
                raex_snapshot_rows = export_raex_snapshot(nra_conn)
            pbar.update(1)
            pbar.set_description("Фиксация результата")
            logger.info(
                "НРА: режим=%s, строк в источнике=%s, строк в snapshot=%s",
                "обновлено из сети" if nra_refreshed else "использован локальный кэш",
                nra_rows,
                nra_snapshot_rows,
            )
            logger.info(
                "АКРА: режим=%s, строк в базе=%s, карточек запрошено=%s, строк в snapshot=%s",
                "обновлено из сети" if acra_refreshed else "использован локальный кэш",
                acra_rows,
                acra_cards,
                acra_snapshot_rows,
            )
            logger.info(
                "НКР: режим=%s, строк в источнике=%s, строк в snapshot=%s",
                "обновлено из сети" if nkr_refreshed else "использован локальный кэш",
                nkr_rows,
                nkr_snapshot_rows,
            )
            logger.info(
                "RAEX: режим=%s, обработано INN=%s, актуальных=%s, ошибок=%s, строк в snapshot=%s",
                "обновлено из сети" if raex_refreshed else "использован локальный кэш",
                raex_inns,
                raex_rows,
                raex_errors,
                raex_snapshot_rows,
            )
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
        stage_times["Этап 5: Рейтинги агентств (НРА + АКРА + НКР + RAEX, отдельная SQLite)"] = perf_counter() - s

        print("Этап 6: Витрина эмитентов (SQL + Excel)")
        s = perf_counter()
        with progress(total=8, desc="Emitents", unit="шаг") as pbar:
            today_str = datetime.now().strftime(config.DATE_SCORING_FORMAT)
            with connect_db(db_path) as conn:
                ensure_emitents_table(conn)
                pulled = pull_scoring_from_excel(conn, logger, today_str)
                pbar.update(1)
                synced = sync_emitents_from_rates(conn, logger)
                pbar.update(1)
                with connect_db(ratings_db_path) as nra_conn:
                    nra_synced = sync_nra_rate_to_emitents(conn, nra_conn, logger)
                    acra_synced = sync_acra_rate_to_emitents(conn, nra_conn, logger)
                    nkr_synced = sync_nkr_rate_to_emitents(conn, nra_conn, logger)
                    raex_synced = sync_raex_rate_to_emitents(conn, nra_conn, logger)
                pbar.update(1)
                dates_fixed = ensure_scoring_dates(conn, logger, today_str)
                pbar.update(1)
                emitents_count = export_emitents_excel(conn)
                pbar.update(1)
                emitents_snapshot = export_emitents_snapshot(conn)
                pbar.update(1)

            logger.info(
                "Витрина эмитентов: перенос из Excel=%s, upsert из %s=%s, NRA_Rate=%s, Acra_Rate=%s, NKR_Rate=%s, RAEX_Rate=%s, авто-дат=%s, строк в Emitents.xlsx=%s, строк в snapshot=%s",
                pulled,
                config.RATES_TABLE_NAME,
                synced,
                nra_synced,
                acra_synced,
                nkr_synced,
                raex_synced,
                dates_fixed,
                emitents_count,
                emitents_snapshot,
            )
            pbar.update(1)
        stage_times["Этап 6: Витрина эмитентов (SQL + Excel)"] = perf_counter() - s

        print("Этап 7: Merge Green/Yellow (SQL)")
        s = perf_counter()
        with progress(total=2, desc="Merge bonds", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                green_rows = rebuild_merge_table_by_scoring(conn, config.MERGE_GREEN_TABLE_NAME, "Green")
                pbar.update(1)
                yellow_rows = rebuild_merge_table_by_scoring(conn, config.MERGE_YELLOW_TABLE_NAME, "Yellow")
                pbar.update(1)

            logger.info(
                "Merge Green: строк=%s; Merge Yellow: строк=%s",
                green_rows,
                yellow_rows,
            )
        stage_times["Этап 7: Merge Green/Yellow (SQL)"] = perf_counter() - s

        print("Этап 8: Presorter для Merge-таблиц")
        s = perf_counter()
        with progress(total=4, desc="Presorter", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                green_presort = presort_merge_table(conn, config.MERGE_GREEN_TABLE_NAME)
                pbar.update(1)
                yellow_presort = presort_merge_table(conn, config.MERGE_YELLOW_TABLE_NAME)
                pbar.update(1)
                green_snapshot = export_merge_snapshot(
                    conn,
                    config.MERGE_GREEN_TABLE_NAME,
                    config.MERGE_GREEN_SNAPSHOT_FILENAME,
                    "merge_green_snapshot",
                )
                pbar.update(1)
                yellow_snapshot = export_merge_snapshot(
                    conn,
                    config.MERGE_YELLOW_TABLE_NAME,
                    config.MERGE_YELLOW_SNAPSHOT_FILENAME,
                    "merge_yellow_snapshot",
                )
                pbar.update(1)
            presorter_summary["MergeGreen"] = green_presort
            presorter_summary["MergeYellow"] = yellow_presort
            logger.info(
                "Presorter: MergeGreen rows=%s->%s excluded(matdate=%s, dohod_nearest=%s, bond_type=%s, total=%s); "
                "MergeYellow rows=%s->%s excluded(matdate=%s, dohod_nearest=%s, bond_type=%s, total=%s); "
                "dohod_nearest_rule_enabled=%s; post-presort snapshots: green=%s, yellow=%s",
                green_presort["rows_before"],
                green_presort["rows_after"],
                green_presort["excluded_by_matdate_rule"],
                green_presort["excluded_by_dohod_nearest_rule"],
                green_presort["excluded_by_bond_type_rule"],
                green_presort["excluded_total"],
                yellow_presort["rows_before"],
                yellow_presort["rows_after"],
                yellow_presort["excluded_by_matdate_rule"],
                yellow_presort["excluded_by_dohod_nearest_rule"],
                yellow_presort["excluded_by_bond_type_rule"],
                yellow_presort["excluded_total"],
                config.PRESORTER_USE_DOHOD_NEAREST_DATE,
                green_snapshot,
                yellow_snapshot,
            )
        stage_times["Этап 8: Presorter для Merge-таблиц"] = perf_counter() - s

        print("Этап 9: Обогащение Merge* из Corpbonds")
        s = perf_counter()
        with progress(total=4, desc="Corpbonds enrich", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                secids_total, secids_requested, secids_saved = refresh_corpbonds_data_if_needed(
                    conn, logger, datetime.now(timezone.utc)
                )
                pbar.update(1)
                green_rows_after_corpbonds = apply_corpbonds_inner_join_to_merge_table(conn, config.MERGE_GREEN_TABLE_NAME)
                pbar.update(1)
                yellow_rows_after_corpbonds = apply_corpbonds_inner_join_to_merge_table(conn, config.MERGE_YELLOW_TABLE_NAME)
                pbar.update(1)
                corpbonds_snapshot = export_corpbonds_snapshot(conn)
                pbar.update(1)
            logger.info(
                "Corpbonds: SECID в Merge=%s, к запросу по TTL=%s, успешно сохранено=%s, "
                "INNER JOIN обновил MergeGreen=%s, MergeYellow=%s, snapshot=%s",
                secids_total,
                secids_requested,
                secids_saved,
                green_rows_after_corpbonds,
                yellow_rows_after_corpbonds,
                corpbonds_snapshot,
            )
        stage_times["Этап 9: Обогащение Merge* из Corpbonds"] = perf_counter() - s

        print("=====\nГотово")
    except Exception as exc:
        logger.exception("Ошибка выполнения: %s", exc)
        raise
    finally:
        total = perf_counter() - started
        print("=====\nSummary")
        for stage_name, duration in stage_times.items():
            print(f"{stage_name}: {duration:.2f} сек")
        if presorter_summary:
            print("Этап Presorter")
            for merge_name in ("MergeGreen", "MergeYellow"):
                item = presorter_summary.get(merge_name, {})
                print(merge_name)
                print(f"Строк до/после Presorter: {item.get('rows_before', 0)} -> {item.get('rows_after', 0)}")
                print(
                    f"Исключено бумаг по правилу MATDATE < {config.PRESORTER_MIN_DAYS_TO_EVENT} дней: {item.get('excluded_by_matdate_rule', 0)}"
                )
                print(
                    f"Исключено бумаг по правилу Доходъ (ближайшая дата) < {config.PRESORTER_MIN_DAYS_TO_EVENT} дней: {item.get('excluded_by_dohod_nearest_rule', 0)}"
                )
                print(
                    f"Исключено бумаг по правилу Bond_TYPE: {item.get('excluded_by_bond_type_rule', 0)}"
                )
        print(f"Всего: {total:.2f} сек")


if __name__ == "__main__":
    main()
