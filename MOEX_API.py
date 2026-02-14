from __future__ import annotations

import csv
import argparse
import hashlib
import io
import json
import logging
import os
import random
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
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
FINISH_EXCEL_PATH = BASE_DIR / "Moex_Bonds_Finish.xlsx"
FINISH_BATCH_DIR = BASE_DIR / "finish_batches"
DETAILS_PARQUET_DIR = BASE_DIR / "details_parquet"
DETAILS_TTL_HOURS = 24
DEFAULT_DETAILS_WORKER_PROCESSES = 2
RANDOM_SECID_SAMPLE_SIZE = 10
DETAILS_SAMPLE_ONLY = os.getenv("DETAILS_SAMPLE_ONLY", "0") == "1"
CB_FAILURE_THRESHOLD = 3
CB_COOLDOWN_SECONDS = 180
HEALTH_RETENTION_DAYS = 14
ROW_COUNT_SPIKE_THRESHOLD = 0.30

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
            CREATE TABLE IF NOT EXISTS details_update_watermark (
                endpoint TEXT NOT NULL,
                secid TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(endpoint, secid, fetched_at)
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
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS endpoint_circuit_breaker (
                endpoint TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'closed',
                opened_at TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS endpoint_health_mv (
                window TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                total_requests INTEGER NOT NULL,
                error_requests INTEGER NOT NULL,
                error_rate REAL NOT NULL,
                avg_latency_ms REAL,
                p95_latency_ms REAL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(window, endpoint)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS dq_run_history (
                run_id TEXT PRIMARY KEY,
                run_at TEXT NOT NULL,
                source TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                empty_secid_ratio REAL NOT NULL,
                empty_isin_ratio REAL NOT NULL,
                row_count_delta_ratio REAL,
                notes TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds_enriched_incremental (
                batch_id TEXT NOT NULL,
                export_date TEXT NOT NULL,
                exported_at TEXT NOT NULL,
                source TEXT NOT NULL,
                row_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_endpoint_health_history_checked_endpoint ON endpoint_health_history(checked_at, endpoint)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_details_rows_endpoint_secid_block ON details_rows(endpoint, secid, block_name)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_dq_run_history_run_at ON dq_run_history(run_at)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_bonds_enriched_incremental_export_date_batch ON bonds_enriched_incremental(export_date, batch_id)"
        )
        connection.commit()


def cleanup_details_cache() -> None:
    cutoff = (datetime.now() - timedelta(hours=DETAILS_TTL_HOURS)).isoformat(timespec="seconds")
    health_cutoff = (datetime.now() - timedelta(days=HEALTH_RETENTION_DAYS)).isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute("DELETE FROM details_cache WHERE fetched_at < ?", (cutoff,))
        connection.execute("DELETE FROM details_rows WHERE fetched_at < ?", (cutoff,))
        connection.execute("DELETE FROM endpoint_health_history WHERE checked_at < ?", (health_cutoff,))
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
            if DETAILS_SAMPLE_ONLY:
                rng = random.Random(date.today().isoformat())
                random_part = unique_secids if len(unique_secids) <= random_sample_size else rng.sample(unique_secids, random_sample_size)
                return sorted(set(random_part + STATIC_SECIDS))
            return unique_secids
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




def _is_circuit_open(endpoint_name: str) -> bool:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            "SELECT state, opened_at FROM endpoint_circuit_breaker WHERE endpoint = ?",
            (endpoint_name,),
        ).fetchone()

    if row is None:
        return False

    state, opened_at = row
    if state != "open" or not opened_at:
        return False

    opened_at_dt = datetime.fromisoformat(opened_at)
    if (datetime.now() - opened_at_dt).total_seconds() >= CB_COOLDOWN_SECONDS:
        with sqlite3.connect(CACHE_DB_PATH) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO endpoint_circuit_breaker(endpoint, failure_count, state, opened_at, updated_at)
                VALUES (?, 0, 'half_open', NULL, ?)
                """,
                (endpoint_name, datetime.now().isoformat(timespec="seconds")),
            )
            connection.commit()
        return False

    return True


def _update_circuit_breaker(endpoint_name: str, is_success: bool) -> None:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            "SELECT failure_count, state FROM endpoint_circuit_breaker WHERE endpoint = ?",
            (endpoint_name,),
        ).fetchone()

        failure_count = 0 if row is None else row[0]
        state = "closed" if row is None else row[1]

        now = datetime.now().isoformat(timespec="seconds")
        if is_success:
            connection.execute(
                """
                INSERT OR REPLACE INTO endpoint_circuit_breaker(endpoint, failure_count, state, opened_at, updated_at)
                VALUES (?, 0, 'closed', NULL, ?)
                """,
                (endpoint_name, now),
            )
        else:
            failure_count += 1
            new_state = "open" if failure_count >= CB_FAILURE_THRESHOLD else state
            opened_at = now if new_state == "open" else None
            connection.execute(
                """
                INSERT OR REPLACE INTO endpoint_circuit_breaker(endpoint, failure_count, state, opened_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (endpoint_name, failure_count, new_state, opened_at, now),
            )
        connection.commit()


def refresh_endpoint_health_mv() -> None:
    now = datetime.now().isoformat(timespec="seconds")
    windows = {"1d": datetime.now() - timedelta(days=1), "7d": datetime.now() - timedelta(days=7)}

    with sqlite3.connect(CACHE_DB_PATH) as connection:
        for window_name, cutoff in windows.items():
            df = pd.read_sql_query(
                """
                SELECT endpoint, status, latency_ms
                FROM endpoint_health_history
                WHERE checked_at >= ?
                """,
                connection,
                params=(cutoff.isoformat(timespec="seconds"),),
            )
            if df.empty:
                continue

            rows = []
            for endpoint, group in df.groupby("endpoint"):
                total = len(group)
                errors = int((group["status"] != "ok").sum())
                latencies = group["latency_ms"].dropna()
                rows.append(
                    (
                        window_name,
                        endpoint,
                        total,
                        errors,
                        errors / total if total else 0.0,
                        float(latencies.mean()) if not latencies.empty else None,
                        float(latencies.quantile(0.95)) if not latencies.empty else None,
                        now,
                    )
                )

            connection.executemany(
                """
                INSERT OR REPLACE INTO endpoint_health_mv(
                    window, endpoint, total_requests, error_requests, error_rate, avg_latency_ms, p95_latency_ms, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        connection.commit()

def _get_cached_endpoint_record(endpoint_name: str, secid: str) -> tuple[dict, str] | None:
    cutoff = (datetime.now() - timedelta(hours=DETAILS_TTL_HOURS)).isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT response_json, fetched_at
            FROM details_cache
            WHERE endpoint = ? AND secid = ? AND fetched_at >= ?
            """,
            (endpoint_name, secid, cutoff),
        ).fetchone()

    if row is None:
        return None
    return json.loads(row[0]), row[1]


def _watermark_exists(endpoint_name: str, secid: str, fetched_at: str) -> bool:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM details_update_watermark
            WHERE endpoint = ? AND secid = ? AND fetched_at = ?
            LIMIT 1
            """,
            (endpoint_name, secid, fetched_at),
        ).fetchone()
    return row is not None


def _save_watermark(endpoint_name: str, secid: str, fetched_at: str) -> None:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO details_update_watermark(endpoint, secid, fetched_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (endpoint_name, secid, fetched_at, datetime.now().isoformat(timespec="seconds")),
        )
        connection.commit()


def _save_endpoint_payload(endpoint_name: str, secid: str, payload: dict, fetched_at: str | None = None) -> str:
    now = fetched_at or datetime.now().isoformat(timespec="seconds")
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
    return now


def _discover_working_endpoints(session: requests.Session, sample_secid: str, logger: logging.Logger) -> list[tuple[str, str]]:
    working_endpoints: list[tuple[str, str]] = []
    for endpoint_name, endpoint_url in DETAILS_ENDPOINTS:
        if _is_circuit_open(endpoint_name):
            logger.warning("Endpoint %s skipped during discovery: circuit breaker is open", endpoint_name)
            continue
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
    enriched = enriched.drop(columns=["secid", "block_name"], errors="ignore")
    enriched.insert(0, "secid", secid)
    enriched.insert(1, "block_name", block_name)
    non_meta_cols = [column for column in enriched.columns if column not in {"secid", "block_name"}]
    return enriched[["secid", "block_name", *non_meta_cols]]


def _fetch_details_worker(secid: str, endpoints: list[tuple[str, str]]) -> list[tuple[str, str, dict | None, float | None, int | None, str | None]]:
    session = build_retry_session()
    results: list[tuple[str, str, dict | None, float | None, int | None, str | None]] = []
    for endpoint_name, endpoint_url in endpoints:
        try:
            payload, latency_ms, status_code = _fetch_endpoint_payload(session, endpoint_name, secid, endpoint_url)
            results.append((endpoint_name, secid, payload, latency_ms, status_code, None))
        except Exception as error:  # noqa: BLE001
            status_code = getattr(getattr(error, "response", None), "status_code", None)
            results.append((endpoint_name, secid, None, None, status_code, str(error)))
    return results


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


def _build_enrichment_frame(endpoint_frames: dict[str, dict[str, pd.DataFrame]]) -> pd.DataFrame:
    secid_features: dict[str, dict[str, str | int | float]] = {}
    for endpoint_name, block_frames in endpoint_frames.items():
        for block_name, frame in block_frames.items():
            if frame.empty:
                continue
            for secid, group in frame.groupby("secid"):
                secid_features.setdefault(secid, {})
                secid_features[secid][f"{endpoint_name}_{block_name}_rows"] = len(group)
                for col in group.columns:
                    if col in {"secid", "block_name"}:
                        continue
                    values = [str(v) for v in group[col].dropna().tolist() if str(v).strip()]
                    if not values:
                        continue
                    unique_values = list(dict.fromkeys(values))
                    secid_features[secid][f"{endpoint_name}_{block_name}_{col}"] = " | ".join(unique_values[:5])

    if not secid_features:
        return pd.DataFrame(columns=["secid"])

    rows = [{"secid": secid, **features} for secid, features in secid_features.items()]
    enrichment = pd.DataFrame(rows)
    enrichment = enrichment.loc[:, ~enrichment.columns.duplicated()].copy()
    return enrichment


def _write_details_excel(endpoint_frames: dict[str, dict[str, pd.DataFrame]], enrichment_df: pd.DataFrame) -> int:
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
                summary_rows.append({"endpoint": endpoint_name, "block_name": block_name, "rows": len(clean_frame), "sheet": sheet_name})
                sheet_count += 1

        enrichment_df.to_excel(writer, sheet_name="details_transposed", index=False)
        summary_df = pd.DataFrame(summary_rows).sort_values(["endpoint", "block_name"]) if summary_rows else pd.DataFrame(columns=["endpoint", "block_name", "rows", "sheet"])
        summary_df.to_excel(writer, sheet_name="summary", index=False)

        for worksheet in writer.book.worksheets:
            _autosize_worksheet(worksheet)

    return sheet_count + 1


def _update_details_parquet(endpoint_frames: dict[str, dict[str, pd.DataFrame]]) -> int:
    DETAILS_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    updated_files = 0

    for endpoint_name, block_frames in endpoint_frames.items():
        flat_frames = []
        for block_name, frame in block_frames.items():
            if frame.empty:
                continue
            prepared = frame.copy()
            prepared["_row_hash"] = prepared.apply(lambda row: hashlib.sha1(json.dumps(row.to_dict(), sort_keys=True, default=str).encode("utf-8")).hexdigest(), axis=1)
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


def _merge_base_with_enrichment(base_df: pd.DataFrame, enrichment_df: pd.DataFrame) -> pd.DataFrame:
    secid_column = next((c for c in ["SECID", "secid", "SecID"] if c in base_df.columns), None)
    if secid_column is None:
        return base_df.copy()

    prepared = base_df.copy()
    prepared["secid"] = prepared[secid_column].astype(str).str.strip().str.upper()
    normalized_enrichment = enrichment_df.copy()
    if "secid" in normalized_enrichment.columns:
        normalized_enrichment["secid"] = normalized_enrichment["secid"].astype(str).str.strip().str.upper()

    merged = prepared.merge(normalized_enrichment, on="secid", how="left")
    merged = merged.drop(columns=["secid"]) if secid_column != "secid" else merged
    merged = merged.loc[:, ~merged.columns.duplicated()].copy()
    return merged


def _persist_finish_to_sqlite(finish_df: pd.DataFrame, batch_id: str, export_date: str, source: str) -> None:
    exported_at = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        finish_df.to_sql("bonds_enriched", connection, if_exists="replace", index=False)

        payload_rows = [
            (
                batch_id,
                export_date,
                exported_at,
                source,
                json.dumps(row, ensure_ascii=False, default=str),
            )
            for row in finish_df.to_dict(orient="records")
        ]
        connection.execute("DELETE FROM bonds_enriched_incremental WHERE batch_id = ?", (batch_id,))
        connection.executemany(
            """
            INSERT INTO bonds_enriched_incremental(batch_id, export_date, exported_at, source, row_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            payload_rows,
        )
        connection.commit()


def _export_finish_incremental(finish_df: pd.DataFrame, export_date: str, batch_id: str) -> Path:
    FINISH_BATCH_DIR.mkdir(parents=True, exist_ok=True)
    batch_path = FINISH_BATCH_DIR / f"Moex_Bonds_Finish_{export_date}_{batch_id}.xlsx"
    finish_df.to_excel(batch_path, index=False)
    finish_df.to_excel(FINISH_EXCEL_PATH, index=False)
    return batch_path


def _run_data_quality_checks(dataframe: pd.DataFrame, run_id: str, source: str, logger: logging.Logger) -> list[str]:
    row_count = len(dataframe)
    secid_col = next((c for c in ["SECID", "secid", "SecID"] if c in dataframe.columns), None)
    isin_col = next((c for c in ["ISIN", "isin"] if c in dataframe.columns), None)

    empty_secid_ratio = 1.0
    empty_isin_ratio = 1.0
    if secid_col:
        secid_values = dataframe[secid_col].astype(str).str.strip()
        empty_secid_ratio = float((secid_values == "").mean())
    if isin_col:
        isin_values = dataframe[isin_col].astype(str).str.strip()
        empty_isin_ratio = float((isin_values == "").mean())

    notes: list[str] = []
    row_count_delta_ratio = None

    with sqlite3.connect(CACHE_DB_PATH) as connection:
        prev = connection.execute(
            """
            SELECT row_count
            FROM dq_run_history
            ORDER BY run_at DESC
            LIMIT 1
            """
        ).fetchone()

        if prev and prev[0]:
            previous_count = int(prev[0])
            row_count_delta_ratio = abs(row_count - previous_count) / previous_count
            if row_count_delta_ratio >= ROW_COUNT_SPIKE_THRESHOLD:
                notes.append(
                    f"Резкое изменение row_count: {previous_count} -> {row_count} ({row_count_delta_ratio:.1%})"
                )

        if empty_secid_ratio > 0.01:
            notes.append(f"Высокая доля пустого SECID: {empty_secid_ratio:.2%}")
        if empty_isin_ratio > 0.05:
            notes.append(f"Высокая доля пустого ISIN: {empty_isin_ratio:.2%}")

        connection.execute(
            """
            INSERT OR REPLACE INTO dq_run_history(
                run_id, run_at, source, row_count, empty_secid_ratio, empty_isin_ratio, row_count_delta_ratio, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                datetime.now().isoformat(timespec="seconds"),
                source,
                row_count,
                empty_secid_ratio,
                empty_isin_ratio,
                row_count_delta_ratio,
                " | ".join(notes),
            ),
        )
        connection.commit()

    for note in notes:
        logger.warning("DQ check: %s", note)
    if not notes:
        logger.info("DQ check: ok (row_count=%s, empty SECID %.2f%%, empty ISIN %.2f%%)", row_count, empty_secid_ratio * 100, empty_isin_ratio * 100)

    return notes


def fetch_and_save_bond_details(
    dataframe: pd.DataFrame,
    logger: logging.Logger,
    session: requests.Session,
    export_date: str,
    source: str,
    details_worker_processes: int,
) -> tuple[int, int, int, int, Path]:
    cleanup_details_cache()
    secids = _pick_secids(dataframe)
    if not secids:
        logger.warning("Could not determine SECID list. Skipping details export")
        return 0, 0, 0, 0, FINISH_EXCEL_PATH

    working_endpoints = _discover_working_endpoints(session, secids[0], logger)
    if not working_endpoints:
        logger.warning("No working details endpoints found. Skipping details export")
        return len(secids), 0, 0, 0, FINISH_EXCEL_PATH

    endpoint_frames: dict[str, dict[str, list[pd.DataFrame]]] = {name: {} for name, _ in working_endpoints}

    pending_by_secid: dict[str, list[tuple[str, str]]] = {}
    for secid in secids:
        missing_endpoints: list[tuple[str, str]] = []
        for endpoint_name, endpoint_url in working_endpoints:
            cached_record = _get_cached_endpoint_record(endpoint_name, secid)
            if cached_record is None:
                missing_endpoints.append((endpoint_name, endpoint_url))
                continue
            cached_payload, fetched_at = cached_record
            blocks = _extract_blocks(cached_payload)
            logger.info("Details %s for %s loaded from cache", endpoint_name, secid)
            _save_endpoint_health(endpoint_name, secid, "ok", "cache", None, 0.0, list(blocks.keys()), None)
            _save_watermark(endpoint_name, secid, fetched_at)
            for block_name, block_df in blocks.items():
                if not block_df.empty:
                    endpoint_frames[endpoint_name].setdefault(block_name, []).append(_normalize_block_frame(secid, block_name, block_df))
        if missing_endpoints:
            pending_by_secid[secid] = missing_endpoints

    pending_secids = list(pending_by_secid.keys())

    with ProcessPoolExecutor(max_workers=details_worker_processes) as executor:
        futures = []
        for secid in pending_secids:
            allowed_endpoints = [(n, u) for n, u in pending_by_secid[secid] if not _is_circuit_open(n)]
            if not allowed_endpoints:
                logger.warning("All endpoints are blocked by circuit breaker for %s", secid)
                continue
            futures.append(executor.submit(_fetch_details_worker, secid, allowed_endpoints))

        completed = 0
        total = len(futures)
        progress_started_at = time.perf_counter()
        for future in as_completed(futures):
            completed += 1
            elapsed = time.perf_counter() - progress_started_at
            avg_seconds = elapsed / completed if completed else 0.0
            remaining = max(total - completed, 0)
            eta_seconds = int(avg_seconds * remaining)
            percent = (completed / total * 100) if total else 100.0
            print(f"Details progress: {completed}/{total} ({percent:.1f}%), ETA {eta_seconds}s")
            for endpoint_name, secid, payload, latency_ms, status_code, error_text in future.result():
                if payload is None:
                    _save_endpoint_health(endpoint_name, secid, "error", "network", status_code, latency_ms, None, error_text)
                    _update_circuit_breaker(endpoint_name, is_success=False)
                    logger.warning("Details %s for %s failed: %s", endpoint_name, secid, error_text)
                    continue

                _update_circuit_breaker(endpoint_name, is_success=True)
                network_fetched_at = datetime.now().isoformat(timespec="seconds")
                if _watermark_exists(endpoint_name, secid, network_fetched_at):
                    logger.info("Skip details update for %s:%s by watermark %s", endpoint_name, secid, network_fetched_at)
                    continue

                persisted_fetched_at = _save_endpoint_payload(
                    endpoint_name,
                    secid,
                    payload,
                    fetched_at=network_fetched_at,
                )
                blocks = _extract_blocks(payload)
                _save_endpoint_health(endpoint_name, secid, "ok", "network", status_code, latency_ms, list(blocks.keys()), None)
                _save_watermark(endpoint_name, secid, persisted_fetched_at)
                logger.info("Details %s for %s loaded from network", endpoint_name, secid)
                for block_name, block_df in blocks.items():
                    if not block_df.empty:
                        endpoint_frames[endpoint_name].setdefault(block_name, []).append(_normalize_block_frame(secid, block_name, block_df))

    merged_frames: dict[str, dict[str, pd.DataFrame]] = {}
    for endpoint_name, blocks in endpoint_frames.items():
        merged_frames[endpoint_name] = {}
        for block_name, frames in blocks.items():
            valid_frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]
            if valid_frames:
                merged_frames[endpoint_name][block_name] = pd.concat(valid_frames, ignore_index=True, sort=False)

    enrichment_df = _build_enrichment_frame(merged_frames)
    excel_sheets = _write_details_excel(merged_frames, enrichment_df)
    parquet_files = _update_details_parquet(merged_frames)

    finish_df = _merge_base_with_enrichment(dataframe, enrichment_df)
    batch_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    batch_path = _export_finish_incremental(finish_df, export_date, batch_id)
    _persist_finish_to_sqlite(finish_df, batch_id=batch_id, export_date=export_date, source=source)
    refresh_endpoint_health_mv()
    return len(secids), excel_sheets, parquet_files, len(finish_df), batch_path


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


def _resolve_details_worker_processes(cli_value: int | None, logger: logging.Logger) -> int:
    if cli_value is not None:
        value = cli_value
        source = "cli"
    else:
        raw_env = os.getenv("DETAILS_WORKER_PROCESSES", str(DEFAULT_DETAILS_WORKER_PROCESSES)).strip()
        try:
            value = int(raw_env)
        except ValueError:
            logger.warning(
                "Invalid DETAILS_WORKER_PROCESSES env value '%s'. Fallback to %s",
                raw_env,
                DEFAULT_DETAILS_WORKER_PROCESSES,
            )
            value = DEFAULT_DETAILS_WORKER_PROCESSES
        source = "env/default"

    if value < 1:
        logger.warning("DETAILS_WORKER_PROCESSES must be >= 1, got %s. Fallback to 1", value)
        value = 1

    logger.info("Details worker processes resolved to %s (%s)", value, source)
    return value


def _build_daily_dq_report(logger: logging.Logger, report_day: date) -> Path:
    report_path = LOGS_DIR / f"dq_daily_report_{report_day.isoformat()}.txt"
    day_start = datetime.combine(report_day, datetime.min.time())
    day_end = day_start + timedelta(days=1)

    with sqlite3.connect(CACHE_DB_PATH) as connection:
        daily_rows = connection.execute(
            """
            SELECT run_at, row_count, notes
            FROM dq_run_history
            WHERE run_at >= ? AND run_at < ?
            ORDER BY run_at ASC
            """,
            (day_start.isoformat(timespec="seconds"), day_end.isoformat(timespec="seconds")),
        ).fetchall()
        trend_rows = connection.execute(
            """
            SELECT substr(run_at, 1, 10) AS run_day, AVG(row_count) AS avg_row_count, MAX(row_count) AS max_row_count
            FROM dq_run_history
            WHERE run_at >= ?
            GROUP BY run_day
            ORDER BY run_day DESC
            LIMIT 7
            """,
            ((day_start - timedelta(days=30)).isoformat(timespec="seconds"),),
        ).fetchall()

    warning_counts: dict[str, int] = {}
    for _, _, notes in daily_rows:
        if not notes:
            continue
        for note in [part.strip() for part in str(notes).split("|") if part.strip()]:
            warning_counts[note] = warning_counts.get(note, 0) + 1

    top_warnings = sorted(warning_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    trend_rows_desc = trend_rows
    trend_rows_asc = list(reversed(trend_rows_desc))

    lines = [
        f"DQ daily report for {report_day.isoformat()}",
        f"Runs today: {len(daily_rows)}",
        "Top warnings:",
    ]
    if top_warnings:
        lines.extend([f"- {warning} (count={count})" for warning, count in top_warnings])
    else:
        lines.append("- none")

    lines.append("Row count trend (last <=7 days):")
    if trend_rows_asc:
        for run_day, avg_row_count, max_row_count in trend_rows_asc:
            lines.append(f"- {run_day}: avg={avg_row_count:.1f}, max={int(max_row_count)}")
    else:
        lines.append("- no history")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Daily DQ report saved to %s", report_path)
    return report_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MOEX bonds ETL pipeline")
    parser.add_argument(
        "--details-worker-processes",
        type=int,
        default=None,
        help="Worker process count for details fetching. Overrides DETAILS_WORKER_PROCESSES env.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logger = setup_logging()
    init_db()
    session = build_retry_session()
    details_worker_processes = _resolve_details_worker_processes(args.details_worker_processes, logger)

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

        run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        dq_notes = _run_data_quality_checks(dataframe, run_id=run_id, source=data_source, logger=logger)

        details_started_at = time.perf_counter()
        secids_count, details_sheets, parquet_files, finish_rows, finish_batch_path = fetch_and_save_bond_details(
            dataframe,
            logger,
            session,
            export_date=today,
            source=data_source,
            details_worker_processes=details_worker_processes,
        )
        details_elapsed = time.perf_counter() - details_started_at

        dq_daily_report_path = _build_daily_dq_report(logger, report_day=date.today())

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
        logger.info("Saved enriched finish dataset with %s rows to %s", finish_rows, FINISH_EXCEL_PATH)
        logger.info("Saved incremental finish batch to %s", finish_batch_path)
        logger.info("DQ warnings count: %s", len(dq_notes))
        logger.info("DQ daily report path: %s", dq_daily_report_path)
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
