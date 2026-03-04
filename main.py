from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter

import requests
from openpyxl import Workbook
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

    try:
        print("=====\nЭтап 1: Подготовка окружения")
        s = perf_counter()
        with progress(total=2, desc="Подготовка", unit="шаг") as pbar:
            ensure_directories()
            pbar.update(1)
            pbar.set_description("Подготовка БД")
            with connect_db(db_path) as conn:
                init_meta_table(conn)
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
