from __future__ import annotations

import io
import csv
import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

MOEX_RATES_URL = (
    "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?"
    "sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,"
    "stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&"
    "iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&"
    "iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&"
    "limit=unlimited&lang=ru"
)

BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "DB"
LOGS_DIR = BASE_DIR / "logs"
CACHE_DB_PATH = DB_DIR / "moex_cache.sqlite3"
RAW_RESPONSE_PATH = LOGS_DIR / "raw_response_latest.csv"
LOG_PATH = LOGS_DIR / "moex_api.log"
EXCEL_PATH = BASE_DIR / "Moex_Bonds.xlsx"


class CacheMissError(RuntimeError):
    """Raised when cache does not contain data for requested day."""


def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("moex_api")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def init_db() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds_cache (
                fetch_date TEXT PRIMARY KEY,
                csv_data TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.commit()


def get_cached_data(target_date: str) -> str:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            "SELECT csv_data FROM bonds_cache WHERE fetch_date = ?",
            (target_date,),
        ).fetchone()

    if row is None:
        raise CacheMissError(f"No cache for {target_date}")

    return row[0]


def get_latest_cached_data() -> tuple[str, str]:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT fetch_date, csv_data
            FROM bonds_cache
            ORDER BY fetch_date DESC
            LIMIT 1
            """
        ).fetchone()

    if row is None:
        raise CacheMissError("No cached data found")

    return row[0], row[1]


def save_to_cache(target_date: str, csv_data: str) -> None:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO bonds_cache(fetch_date, csv_data, created_at)
            VALUES (?, ?, ?)
            """,
            (target_date, csv_data, datetime.now().isoformat(timespec="seconds")),
        )
        connection.commit()


def fetch_moex_csv() -> str:
    response = requests.get(MOEX_RATES_URL, timeout=60)
    response.raise_for_status()
    return response.text


def persist_raw_response(csv_data: str) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_RESPONSE_PATH.write_text(csv_data, encoding="utf-8")


def save_excel(csv_data: str) -> int:
    dataframe = pd.read_csv(io.StringIO(_prepare_csv_for_pandas(csv_data)))
    dataframe.to_excel(EXCEL_PATH, index=False)
    return len(dataframe)


def _prepare_csv_for_pandas(csv_data: str) -> str:
    lines = [line for line in csv_data.splitlines() if line.strip()]
    if not lines:
        raise ValueError("CSV data is empty")

    delimiter = ","
    if sum(";" in line for line in lines[:10]) > sum("," in line for line in lines[:10]):
        delimiter = ";"

    parsed_rows: list[list[str]] = []
    for line in lines:
        parsed_rows.append(next(csv.reader([line], delimiter=delimiter)))

    header_index = next(
        (index for index, row in enumerate(parsed_rows) if len(row) > 1),
        None,
    )
    if header_index is None:
        raise ValueError("Could not find CSV header in response")

    expected_width = len(parsed_rows[header_index])
    filtered_lines = [
        lines[index]
        for index in range(header_index, len(lines))
        if len(parsed_rows[index]) == expected_width
    ]

    if len(filtered_lines) < 2:
        raise ValueError("Could not parse tabular CSV data from response")

    return "\n".join(filtered_lines)


def main() -> int:
    logger = setup_logging()
    init_db()

    start_time = time.perf_counter()
    today = date.today().isoformat()
    data_source = "cache"

    logger.info("MOEX API script started")

    try:
        try:
            csv_data = get_cached_data(today)
            logger.info("Using cached data for %s", today)
        except CacheMissError:
            data_source = "network"
            logger.info("Cache miss for %s. Fetching from MOEX...", today)
            try:
                csv_data = fetch_moex_csv()
                save_to_cache(today, csv_data)
                logger.info("Data fetched and cached")
            except requests.RequestException as error:
                try:
                    cached_date, csv_data = get_latest_cached_data()
                except CacheMissError:
                    elapsed = time.perf_counter() - start_time
                    logger.error("MOEX is unavailable (%s) and cache is empty", error)
                    logger.info("Execution time before failure: %.3f seconds", elapsed)
                    print("MOEX_API failed: MOEX unavailable and cache is empty")
                    return 1

                data_source = f"cache_fallback:{cached_date}"
                logger.warning(
                    "MOEX is unavailable (%s). Falling back to cached data from %s",
                    error,
                    cached_date,
                )

        persist_raw_response(csv_data)
        row_count = save_excel(csv_data)
        elapsed = time.perf_counter() - start_time

        logger.info("Data source: %s", data_source)
        logger.info("Saved %s rows to %s", row_count, EXCEL_PATH)
        logger.info("Raw response saved to %s", RAW_RESPONSE_PATH)
        logger.info("Execution time: %.3f seconds", elapsed)
        print("MOEX_API completed successfully")
        return 0

    except Exception as error:
        elapsed = time.perf_counter() - start_time
        logger.error("Script failed: %s", error)
        logger.info("Execution time before failure: %.3f seconds", elapsed)
        print(f"MOEX_API failed: {error}")
        return 1


if __name__ == "__main__":
    main()
