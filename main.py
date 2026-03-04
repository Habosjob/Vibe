from __future__ import annotations

import csv
import html
import io
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Font, PatternFill
from tqdm import tqdm

import config


def progress(total: int, desc: str, unit: str):
    return tqdm(total=total, desc=desc, unit=unit, position=0, leave=False, dynamic_ncols=True)


def setup_logging() -> logging.Logger:
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("moex_rates_main")
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
            "NRA_Rate" TEXT
        )
        '''
    )
    conn.execute(
        f'ALTER TABLE "{config.EMITENTS_TABLE_NAME}" ADD COLUMN "NRA_Rate" TEXT'
    ) if _column_absent(conn, config.EMITENTS_TABLE_NAME, "NRA_Rate") else None
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


def ensure_acra_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{config.ACRA_TABLE_NAME}" (
            "issuer_url" TEXT NOT NULL,
            "issuer_name" TEXT,
            "rating" TEXT,
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

        rating_node = row.select_one('div.emits-row__item[data-type="rate"] p')
        rating = rating_node.get_text(" ", strip=True) if rating_node else ""

        date_node = row.select_one('div.emits-row__item[data-type="pressRelease"] a')
        date_raw = date_node.get_text(" ", strip=True) if date_node else ""
        parsed_rows.append(
            {
                "issuer_url": issuer_url,
                "issuer_name": issuer_name,
                "rating": rating,
                "rating_date": normalize_date_ru(date_raw) or date_raw,
            }
        )
    return parsed_rows


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


def refresh_acra_data_if_needed(conn: sqlite3.Connection, logger: logging.Logger, now_utc: datetime) -> tuple[bool, int, int]:
    ensure_acra_tables(conn)
    current = conn.execute(f'SELECT COUNT(*) FROM "{config.ACRA_TABLE_NAME}"').fetchone()
    current_total = int(current[0]) if current else 0
    if not should_refresh_acra(conn, now_utc):
        return False, current_total, 0

    try:
        with create_http_session() as session:
            list_response = session.get(config.ACRA_RATINGS_LIST_URL, timeout=config.REQUEST_TIMEOUT_SECONDS)
            list_response.raise_for_status()
            parsed_rows = parse_acra_list(list_response.text)

            unique_rows: dict[str, dict[str, str]] = {}
            for parsed_row in parsed_rows:
                unique_rows[parsed_row["issuer_url"]] = parsed_row

            loaded_at = now_utc.isoformat()
            card_requests = 0
            changed_rows = 0

            for row_data in unique_rows.values():
                issuer_url = row_data["issuer_url"]
                existing = conn.execute(
                    f'''
                    SELECT "inn"
                    FROM "{config.ACRA_TABLE_NAME}"
                    WHERE "issuer_url" = ?
                      AND TRIM(COALESCE("inn", '')) <> ''
                    LIMIT 1
                    ''',
                    (issuer_url,),
                ).fetchone()

                inn_value = existing[0] if existing else ""
                if not inn_value:
                    card_response = session.get(issuer_url, timeout=config.REQUEST_TIMEOUT_SECONDS)
                    card_response.raise_for_status()
                    inn_value = extract_inn_from_acra_card(card_response.text)
                    card_requests += 1

                cursor = conn.execute(
                    f'''
                    INSERT INTO "{config.ACRA_TABLE_NAME}" (
                        "issuer_url", "issuer_name", "rating", "rating_date", "inn", "loaded_at_utc"
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT("issuer_url", "rating_date", "rating") DO UPDATE SET
                        "issuer_name" = excluded."issuer_name",
                        "inn" = CASE
                            WHEN TRIM(COALESCE("{config.ACRA_TABLE_NAME}"."inn", '')) = '' THEN excluded."inn"
                            ELSE "{config.ACRA_TABLE_NAME}"."inn"
                        END,
                        "loaded_at_utc" = excluded."loaded_at_utc"
                    ''',
                    (
                        issuer_url,
                        row_data.get("issuer_name", ""),
                        row_data.get("rating", ""),
                        row_data.get("rating_date", ""),
                        inn_value,
                        loaded_at,
                    ),
                )
                if cursor.rowcount and cursor.rowcount > 0:
                    changed_rows += 1
    except requests.RequestException as exc:
        logger.warning("АКРА обновление пропущено из-за сетевой ошибки: %s", exc)
        return False, current_total, 0

    conn.commit()
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
    logger.info("Синхронизация эмитентов из rates завершена. Затронуто строк: %s", affected)
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
        SELECT "EMITENTNAME", "INN", "Scoring", "DateScoring", "NRA_Rate"
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

    excel_path = config.BASE_DIR / config.EMITENTS_XLSX_FILENAME
    wb.save(excel_path)
    return len(rows)


def export_emitents_snapshot(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f'''
        SELECT "EMITENTNAME", "INN", "Scoring", "DateScoring", "NRA_Rate"
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
        SELECT "issuer_name", "issuer_url", "rating", "rating_date", "inn"
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


def main() -> None:
    logger = setup_logging()
    stage_times: dict[str, float] = {}
    started = perf_counter()

    db_path = config.DB_DIR / config.DB_FILENAME
    ratings_db_path = config.DB_DIR / config.RAITINGS_DB_FILENAME

    try:
        print("=====\nЭтап 1: Подготовка окружения")
        s = perf_counter()
        with progress(total=2, desc="Подготовка", unit="шаг") as pbar:
            ensure_directories()
            pbar.update(1)
            pbar.set_description("Подготовка БД")
            with connect_db(db_path) as conn:
                init_meta_table(conn)
                ensure_emitents_table(conn)
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
            logger.info("Количество строк в таблице rates: %s", row_count)
            pbar.update(1)
        stage_times["Этап 2: Проверка TTL кэша и обновление данных"] = perf_counter() - s

        print("Этап 3: Формирование Excel-среза")
        s = perf_counter()
        with progress(total=2, desc="Excel snapshot", unit="шаг") as pbar:
            with connect_db(db_path) as conn:
                count = export_random_snapshot(conn)
            pbar.update(1)
            logger.info("Сформирован Excel-срез: строк=%s", count)
            pbar.update(1)
        stage_times["Этап 3: Формирование Excel-среза"] = perf_counter() - s

        print("Этап 4: Рейтинги агентств (НРА + АКРА, отдельная SQLite)")
        s = perf_counter()
        with progress(total=6, desc="NRA/ACRA", unit="шаг") as pbar:
            now_utc = datetime.now(timezone.utc)
            with connect_db(ratings_db_path) as nra_conn:
                init_meta_table(nra_conn)
                ensure_nra_tables(nra_conn)
                ensure_acra_tables(nra_conn)
                nra_refreshed, nra_rows = refresh_nra_data_if_needed(nra_conn, logger, now_utc)
                nra_snapshot_rows = export_nra_snapshot(nra_conn)
                pbar.update(1)
                acra_refreshed, acra_rows, acra_cards = refresh_acra_data_if_needed(nra_conn, logger, now_utc)
                acra_snapshot_rows = export_acra_snapshot(nra_conn)
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
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
            pbar.update(1)
        stage_times["Этап 4: Рейтинги агентств (НРА + АКРА, отдельная SQLite)"] = perf_counter() - s

        print("Этап 5: Витрина эмитентов (SQL + Excel)")
        s = perf_counter()
        with progress(total=6, desc="Emitents", unit="шаг") as pbar:
            today_str = datetime.now().strftime(config.DATE_SCORING_FORMAT)
            with connect_db(db_path) as conn:
                ensure_emitents_table(conn)
                pulled = pull_scoring_from_excel(conn, logger, today_str)
                pbar.update(1)
                synced = sync_emitents_from_rates(conn, logger)
                pbar.update(1)
                with connect_db(ratings_db_path) as nra_conn:
                    nra_synced = sync_nra_rate_to_emitents(conn, nra_conn, logger)
                pbar.update(1)
                dates_fixed = ensure_scoring_dates(conn, logger, today_str)
                pbar.update(1)
                emitents_count = export_emitents_excel(conn)
                pbar.update(1)
                emitents_snapshot = export_emitents_snapshot(conn)
                pbar.update(1)

            logger.info(
                "Витрина эмитентов: перенос из Excel=%s, upsert из rates=%s, NRA_Rate=%s, авто-дат=%s, строк в Emitents.xlsx=%s, строк в snapshot=%s",
                pulled,
                synced,
                nra_synced,
                dates_fixed,
                emitents_count,
                emitents_snapshot,
            )
        stage_times["Этап 5: Витрина эмитентов (SQL + Excel)"] = perf_counter() - s

        print("=====\nГотово")
    except Exception as exc:
        logger.exception("Ошибка выполнения: %s", exc)
        raise
    finally:
        total = perf_counter() - started
        print("=====\nSummary")
        for stage_name, duration in stage_times.items():
            print(f"{stage_name}: {duration:.2f} сек")
        print(f"Всего: {total:.2f} сек")


if __name__ == "__main__":
    main()
