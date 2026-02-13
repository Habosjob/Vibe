from __future__ import annotations

import io
import csv
import json
import logging
import random
import sqlite3
import time
from datetime import date, datetime, timedelta
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
DETAILS_EXCEL_PATH = BASE_DIR / "Moex_Bonds_Details.xlsx"
DETAILS_TTL_HOURS = 24

DETAILS_ENDPOINTS = [
    (
        "security_overview",
        "https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off",
    ),
    (
        "bondization",
        "https://iss.moex.com/iss/securities/{secid}/bondization.json?iss.meta=off",
    ),
    (
        "bondization_stats",
        "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json?iss.meta=off",
    ),
    (
        "security_events",
        "https://iss.moex.com/iss/securities/{secid}/events.json?iss.meta=off",
    ),
]


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
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS details_cache (
                endpoint TEXT NOT NULL,
                secid TEXT NOT NULL,
                response_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY(endpoint, secid)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS details_rows (
                fetched_at TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                secid TEXT NOT NULL,
                block_name TEXT NOT NULL,
                row_json TEXT NOT NULL
            )
            """
        )
        connection.commit()


def cleanup_details_cache() -> None:
    cutoff = (datetime.now() - timedelta(hours=DETAILS_TTL_HOURS)).isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute("DELETE FROM details_cache WHERE fetched_at < ?", (cutoff,))
        connection.execute("DELETE FROM details_rows WHERE fetched_at < ?", (cutoff,))
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
    dataframe = parse_rates_csv(csv_data)
    dataframe.to_excel(EXCEL_PATH, index=False)
    return len(dataframe)


def parse_rates_csv(csv_data: str) -> pd.DataFrame:
    prepared_csv, delimiter = _prepare_csv_for_pandas(csv_data)
    return pd.read_csv(io.StringIO(prepared_csv), sep=delimiter)


def _extract_blocks(payload: dict) -> dict[str, pd.DataFrame]:
    blocks: dict[str, pd.DataFrame] = {}
    for block_name, block_payload in payload.items():
        if not isinstance(block_payload, dict):
            continue
        columns = block_payload.get("columns")
        data = block_payload.get("data")
        if not isinstance(columns, list) or not isinstance(data, list):
            continue
        blocks[block_name] = pd.DataFrame(data, columns=columns)
    return blocks


def _pick_random_secids(dataframe: pd.DataFrame, sample_size: int = 10) -> list[str]:
    for column in ["SECID", "secid", "SecID"]:
        if column in dataframe.columns:
            series = dataframe[column].dropna().astype(str).str.strip()
            unique_secids = sorted(set(series[series != ""]))
            if not unique_secids:
                return []
            if len(unique_secids) <= sample_size:
                return unique_secids
            rng = random.Random(date.today().isoformat())
            return sorted(rng.sample(unique_secids, sample_size))
    return []


def _fetch_endpoint_payload(endpoint_name: str, secid: str, endpoint_url: str) -> dict:
    response = requests.get(endpoint_url.format(secid=secid), timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected JSON shape for {endpoint_name}:{secid}")
    return payload


def _get_cached_endpoint_payload(endpoint_name: str, secid: str) -> dict | None:
    cutoff = (datetime.now() - timedelta(hours=DETAILS_TTL_HOURS)).isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT response_json
            FROM details_cache
            WHERE endpoint = ? AND secid = ? AND fetched_at >= ?
            """,
            (endpoint_name, secid, cutoff),
        ).fetchone()

    if row is None:
        return None
    return json.loads(row[0])


def _save_endpoint_payload(endpoint_name: str, secid: str, payload: dict) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    serialized_payload = json.dumps(payload, ensure_ascii=False)
    blocks = _extract_blocks(payload)

    rows_to_insert: list[tuple[str, str, str, str, str]] = []
    for block_name, block_df in blocks.items():
        for row in block_df.to_dict(orient="records"):
            rows_to_insert.append(
                (
                    now,
                    endpoint_name,
                    secid,
                    block_name,
                    json.dumps(row, ensure_ascii=False),
                )
            )

    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO details_cache(endpoint, secid, response_json, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            (endpoint_name, secid, serialized_payload, now),
        )
        connection.execute(
            "DELETE FROM details_rows WHERE endpoint = ? AND secid = ?",
            (endpoint_name, secid),
        )
        if rows_to_insert:
            connection.executemany(
                """
                INSERT INTO details_rows(fetched_at, endpoint, secid, block_name, row_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )
        connection.commit()


def _discover_working_endpoints(sample_secid: str, logger: logging.Logger) -> list[tuple[str, str]]:
    working_endpoints: list[tuple[str, str]] = []
    for endpoint_name, endpoint_url in DETAILS_ENDPOINTS:
        try:
            payload = _fetch_endpoint_payload(endpoint_name, sample_secid, endpoint_url)
            blocks = _extract_blocks(payload)
            if blocks:
                logger.info("Endpoint %s is available (blocks: %s)", endpoint_name, ", ".join(blocks.keys()))
                working_endpoints.append((endpoint_name, endpoint_url))
            else:
                logger.warning("Endpoint %s returned no tabular blocks for %s", endpoint_name, sample_secid)
        except requests.RequestException as error:
            logger.warning("Endpoint %s is unavailable for %s: %s", endpoint_name, sample_secid, error)
            response = getattr(error, "response", None)
            if response is not None and response.status_code >= 500:
                logger.warning("MOEX service looks unavailable (HTTP %s). Stopping endpoint discovery early", response.status_code)
                break
        except Exception as error:  # noqa: BLE001
            logger.warning("Endpoint %s is unavailable for %s: %s", endpoint_name, sample_secid, error)
    return working_endpoints


def fetch_and_save_bond_details(dataframe: pd.DataFrame, logger: logging.Logger) -> tuple[int, int]:
    cleanup_details_cache()
    secids = _pick_random_secids(dataframe)
    if not secids:
        logger.warning("Could not find SECID column in rates dataset. Skipping details export")
        return 0, 0

    working_endpoints = _discover_working_endpoints(secids[0], logger)
    if not working_endpoints:
        logger.warning("No working details endpoints found. Skipping details export")
        return len(secids), 0

    endpoint_frames: dict[str, list[pd.DataFrame]] = {name: [] for name, _ in working_endpoints}

    for secid in secids:
        for endpoint_name, endpoint_url in working_endpoints:
            payload = _get_cached_endpoint_payload(endpoint_name, secid)
            source = "cache"
            if payload is None:
                try:
                    payload = _fetch_endpoint_payload(endpoint_name, secid, endpoint_url)
                    _save_endpoint_payload(endpoint_name, secid, payload)
                    source = "network"
                except Exception as error:  # noqa: BLE001
                    logger.warning("Failed to load %s for %s: %s", endpoint_name, secid, error)
                    continue

            blocks = _extract_blocks(payload)
            if not blocks:
                continue

            logger.info("Details %s for %s loaded from %s", endpoint_name, secid, source)
            for block_name, block_df in blocks.items():
                if block_df.empty:
                    continue
                enriched = block_df.copy()
                enriched.insert(0, "block_name", block_name)
                enriched.insert(0, "secid", secid)
                endpoint_frames[endpoint_name].append(enriched)

    non_empty_endpoints = sum(1 for frames in endpoint_frames.values() if frames)
    if non_empty_endpoints > 0:
        with pd.ExcelWriter(DETAILS_EXCEL_PATH, engine="openpyxl") as writer:
            for endpoint_name, frames in endpoint_frames.items():
                if not frames:
                    continue
                merged = pd.concat(frames, ignore_index=True, sort=False)
                sheet_name = endpoint_name[:31]
                merged.to_excel(writer, sheet_name=sheet_name, index=False)

    return len(secids), non_empty_endpoints


def _prepare_csv_for_pandas(csv_data: str) -> tuple[str, str]:
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

    return "\n".join(filtered_lines), delimiter


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

        parse_started_at = time.perf_counter()
        dataframe = parse_rates_csv(csv_data)
        parse_elapsed = time.perf_counter() - parse_started_at

        excel_started_at = time.perf_counter()
        dataframe.to_excel(EXCEL_PATH, index=False)
        excel_elapsed = time.perf_counter() - excel_started_at
        row_count = len(dataframe)

        details_started_at = time.perf_counter()
        secids_count, details_sheets = fetch_and_save_bond_details(dataframe, logger)
        details_elapsed = time.perf_counter() - details_started_at

        persist_raw_response(csv_data)
        elapsed = time.perf_counter() - start_time

        logger.info("Data source: %s", data_source)
        logger.info("CSV parse time: %.3f seconds", parse_elapsed)
        logger.info("Excel export time: %.3f seconds", excel_elapsed)
        logger.info("Details export time: %.3f seconds", details_elapsed)
        logger.info("Saved %s rows to %s", row_count, EXCEL_PATH)
        logger.info(
            "Saved extended details for %s securities into %s endpoint sheets (%s)",
            secids_count,
            details_sheets,
            DETAILS_EXCEL_PATH,
        )
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
