from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import random
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
DETAILS_PARQUET_DIR = BASE_DIR / "details_parquet"
DETAILS_TTL_HOURS = 24
DETAILS_POOL_SIZE = 6
RANDOM_SECID_SAMPLE_SIZE = 10

STATIC_SECIDS = [
    "RU000A1021G3",
    "RU000A107GM1",
    "RU000A109BW7",
    "RU000A10B4W8",
    "RU000A10BFQ1",
    "RU000A10BRN3",
    "RU000A10CPQ8",
    "RU000A10D616",
    "RU000A10DKS3",
    "SU26212RMFS9",
]

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


def build_retry_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Vibe-MOEX-Client/2.0"})
    return session


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
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS endpoint_health_history (
                checked_at TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                secid TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                http_status INTEGER,
                latency_ms REAL,
                blocks TEXT,
                error_text TEXT
            )
            """
        )
        connection.commit()


def cleanup_details_cache() -> None:
    cutoff = (datetime.now() - timedelta(hours=DETAILS_TTL_HOURS)).isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute("DELETE FROM details_cache WHERE fetched_at < ?", (cutoff,))
        connection.execute("DELETE FROM details_rows WHERE fetched_at < ?", (cutoff,))
        connection.execute("DELETE FROM endpoint_health_history WHERE checked_at < ?", (cutoff,))
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


def fetch_moex_csv(session: requests.Session) -> str:
    response = session.get(MOEX_RATES_URL, timeout=60)
    response.raise_for_status()
    return response.text


def persist_raw_response(csv_data: str) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_RESPONSE_PATH.write_text(csv_data, encoding="utf-8")


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


def _pick_secids(dataframe: pd.DataFrame, random_sample_size: int = RANDOM_SECID_SAMPLE_SIZE) -> list[str]:
    for column in ["SECID", "secid", "SecID"]:
        if column in dataframe.columns:
            series = dataframe[column].dropna().astype(str).str.strip()
            unique_secids = sorted(set(series[series != ""]))
            if not unique_secids:
                return STATIC_SECIDS.copy()
            rng = random.Random(date.today().isoformat())
            random_part = unique_secids if len(unique_secids) <= random_sample_size else rng.sample(unique_secids, random_sample_size)
            merged = sorted(set(random_part + STATIC_SECIDS))
            return merged
    return STATIC_SECIDS.copy()


def _fetch_endpoint_payload(session: requests.Session, endpoint_name: str, secid: str, endpoint_url: str) -> tuple[dict, float, int | None]:
    started = time.perf_counter()
    response = session.get(endpoint_url.format(secid=secid), timeout=30)
    latency_ms = (time.perf_counter() - started) * 1000
    status_code = response.status_code
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected JSON shape for {endpoint_name}:{secid}")
    return payload, latency_ms, status_code


def _save_endpoint_health(endpoint_name: str, secid: str, status: str, source: str, http_status: int | None, latency_ms: float | None, blocks: list[str] | None, error_text: str | None) -> None:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT INTO endpoint_health_history(
                checked_at, endpoint, secid, status, source, http_status, latency_ms, blocks, error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                endpoint_name,
                secid,
                status,
                source,
                http_status,
                latency_ms,
                ", ".join(blocks) if blocks else None,
                error_text,
            ),
        )
        connection.commit()


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


def _discover_working_endpoints(session: requests.Session, sample_secid: str, logger: logging.Logger) -> list[tuple[str, str]]:
    working_endpoints: list[tuple[str, str]] = []
    for endpoint_name, endpoint_url in DETAILS_ENDPOINTS:
        try:
            payload, latency_ms, status_code = _fetch_endpoint_payload(session, endpoint_name, sample_secid, endpoint_url)
            blocks = _extract_blocks(payload)
            if blocks:
                block_names = list(blocks.keys())
                logger.info("Endpoint %s is available (blocks: %s)", endpoint_name, ", ".join(block_names))
                _save_endpoint_health(endpoint_name, sample_secid, "ok", "network", status_code, latency_ms, block_names, None)
                working_endpoints.append((endpoint_name, endpoint_url))
            else:
                logger.warning("Endpoint %s returned no tabular blocks for %s", endpoint_name, sample_secid)
                _save_endpoint_health(endpoint_name, sample_secid, "empty", "network", status_code, latency_ms, [], None)
        except requests.RequestException as error:
            response = getattr(error, "response", None)
            status_code = response.status_code if response is not None else None
            logger.warning("Endpoint %s is unavailable for %s: %s", endpoint_name, sample_secid, error)
            _save_endpoint_health(endpoint_name, sample_secid, "error", "network", status_code, None, None, str(error))
            if response is not None and response.status_code >= 500:
                logger.warning("MOEX service looks unavailable (HTTP %s). Stopping endpoint discovery early", response.status_code)
                break
        except Exception as error:  # noqa: BLE001
            logger.warning("Endpoint %s is unavailable for %s: %s", endpoint_name, sample_secid, error)
            _save_endpoint_health(endpoint_name, sample_secid, "error", "network", None, None, None, str(error))
    return working_endpoints


def _normalize_block_frame(secid: str, block_name: str, block_df: pd.DataFrame) -> pd.DataFrame:
    enriched = block_df.copy()
    enriched.insert(0, "secid", secid)
    enriched.insert(1, "block_name", block_name)
    non_meta_cols = [column for column in enriched.columns if column not in {"secid", "block_name"}]
    return enriched[["secid", "block_name", *non_meta_cols]]


def _fetch_details_task(session: requests.Session, secid: str, endpoint_name: str, endpoint_url: str) -> tuple[str, str, str, dict | None]:
    cached_payload = _get_cached_endpoint_payload(endpoint_name, secid)
    if cached_payload is not None:
        return secid, endpoint_name, "cache", cached_payload

    payload, latency_ms, status_code = _fetch_endpoint_payload(session, endpoint_name, secid, endpoint_url)
    _save_endpoint_payload(endpoint_name, secid, payload)
    blocks = list(_extract_blocks(payload).keys())
    _save_endpoint_health(endpoint_name, secid, "ok", "network", status_code, latency_ms, blocks, None)
    return secid, endpoint_name, "network", payload


def _build_sheet_name(endpoint_name: str, block_name: str) -> str:
    base_name = f"{endpoint_name}_{block_name}".replace("/", "_")
    return base_name[:31]


def _autosize_worksheet(worksheet) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    for column_cells in worksheet.columns:
        max_length = 0
        column = column_cells[0].column_letter
        for cell in column_cells[:200]:
            value_length = len(str(cell.value)) if cell.value is not None else 0
            if value_length > max_length:
                max_length = value_length
        worksheet.column_dimensions[column].width = min(max(12, max_length + 2), 60)


def _write_details_excel(endpoint_frames: dict[str, dict[str, pd.DataFrame]]) -> int:
    mode = "a" if DETAILS_EXCEL_PATH.exists() else "w"
    writer_kwargs = {"engine": "openpyxl", "mode": mode}
    if mode == "a":
        writer_kwargs["if_sheet_exists"] = "replace"

    sheet_count = 0
    with pd.ExcelWriter(DETAILS_EXCEL_PATH, **writer_kwargs) as writer:
        summary_rows: list[dict[str, str | int]] = []
        for endpoint_name, block_frames in endpoint_frames.items():
            for block_name, frame in block_frames.items():
                clean_frame = frame.dropna(axis=1, how="all")
                if clean_frame.empty:
                    continue
                sheet_name = _build_sheet_name(endpoint_name, block_name)
                clean_frame.to_excel(writer, sheet_name=sheet_name, index=False)
                summary_rows.append(
                    {
                        "endpoint": endpoint_name,
                        "block_name": block_name,
                        "rows": len(clean_frame),
                        "sheet": sheet_name,
                    }
                )
                sheet_count += 1

        summary_df = pd.DataFrame(summary_rows).sort_values(["endpoint", "block_name"]) if summary_rows else pd.DataFrame(columns=["endpoint", "block_name", "rows", "sheet"])
        summary_df.to_excel(writer, sheet_name="summary", index=False)

        for worksheet in writer.book.worksheets:
            _autosize_worksheet(worksheet)

    return sheet_count


def _update_details_parquet(endpoint_frames: dict[str, dict[str, pd.DataFrame]]) -> int:
    DETAILS_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    updated_files = 0

    for endpoint_name, block_frames in endpoint_frames.items():
        flat_frames = []
        for block_name, frame in block_frames.items():
            if frame.empty:
                continue
            prepared = frame.copy()
            prepared["_row_hash"] = prepared.apply(
                lambda row: hashlib.sha1(json.dumps(row.to_dict(), sort_keys=True, default=str).encode("utf-8")).hexdigest(),
                axis=1,
            )
            prepared["_updated_at"] = datetime.now().isoformat(timespec="seconds")
            prepared["block_name"] = block_name
            flat_frames.append(prepared)

        if not flat_frames:
            continue

        new_data = pd.concat(flat_frames, ignore_index=True, sort=False)
        parquet_path = DETAILS_PARQUET_DIR / f"{endpoint_name}.parquet"
        if parquet_path.exists():
            existing = pd.read_parquet(parquet_path)
            merged = pd.concat([existing, new_data], ignore_index=True, sort=False)
            merged = merged.drop_duplicates(subset=["secid", "block_name", "_row_hash"], keep="last")
        else:
            merged = new_data

        merged.to_parquet(parquet_path, index=False)
        updated_files += 1

    return updated_files


def fetch_and_save_bond_details(dataframe: pd.DataFrame, logger: logging.Logger, session: requests.Session) -> tuple[int, int, int]:
    cleanup_details_cache()
    secids = _pick_secids(dataframe)
    if not secids:
        logger.warning("Could not determine SECID list. Skipping details export")
        return 0, 0, 0

    working_endpoints = _discover_working_endpoints(session, secids[0], logger)
    if not working_endpoints:
        logger.warning("No working details endpoints found. Skipping details export")
        return len(secids), 0, 0

    endpoint_frames: dict[str, dict[str, list[pd.DataFrame]]] = {
        name: {} for name, _ in working_endpoints
    }

    futures = []
    with ThreadPoolExecutor(max_workers=DETAILS_POOL_SIZE) as executor:
        for secid in secids:
            for endpoint_name, endpoint_url in working_endpoints:
                futures.append(executor.submit(_fetch_details_task, session, secid, endpoint_name, endpoint_url))

        for future in as_completed(futures):
            try:
                secid, endpoint_name, source, payload = future.result()
            except requests.RequestException as error:
                logger.warning("Details request failed: %s", error)
                continue
            except Exception as error:  # noqa: BLE001
                logger.warning("Details loading failed: %s", error)
                continue

            if payload is None:
                continue

            blocks = _extract_blocks(payload)
            if not blocks:
                continue

            logger.info("Details %s for %s loaded from %s", endpoint_name, secid, source)
            for block_name, block_df in blocks.items():
                if block_df.empty:
                    continue
                normalized = _normalize_block_frame(secid, block_name, block_df)
                endpoint_frames[endpoint_name].setdefault(block_name, []).append(normalized)

    merged_frames: dict[str, dict[str, pd.DataFrame]] = {}
    for endpoint_name, blocks in endpoint_frames.items():
        merged_frames[endpoint_name] = {}
        for block_name, frames in blocks.items():
            valid_frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]
            if not valid_frames:
                continue
            merged = pd.concat(valid_frames, ignore_index=True, sort=False)
            merged_frames[endpoint_name][block_name] = merged

    excel_sheets = _write_details_excel(merged_frames)
    parquet_files = _update_details_parquet(merged_frames)
    return len(secids), excel_sheets, parquet_files


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
    session = build_retry_session()

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
                csv_data = fetch_moex_csv(session)
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
        secids_count, details_sheets, parquet_files = fetch_and_save_bond_details(dataframe, logger, session)
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
        logger.info("Updated %s parquet detail files in %s", parquet_files, DETAILS_PARQUET_DIR)
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
