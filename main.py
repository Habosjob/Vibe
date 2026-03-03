from __future__ import annotations

import html
import json
import logging
import math
import re
import signal
import sqlite3
import threading
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any

import pandas as pd
import requests
import urllib3
from openpyxl.formatting.rule import FormulaRule
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from playwright.sync_api import sync_playwright
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
EXPERT_RA_BASE_URL = "https://raexpert.ru"
ACRA_BASE_URL = "https://www.acra-ratings.ru"
ACRA_PROXY_BASE_URL = "https://r.jina.ai/http://www.acra-ratings.ru"
NKR_BASE_URL = "https://ratings.ru"
NRA_BASE_URL = "https://www.ra-national.ru"
OUTPUT_DIR = Path(__file__).resolve().parent
LOGS_DIR = OUTPUT_DIR / "logs"
CACHE_DIR = OUTPUT_DIR / "cache"
EXPORT_DIR = OUTPUT_DIR / "output"
DB_DIR = OUTPUT_DIR / "DB"
LOG_FILE = LOGS_DIR / "main.log"
SHARES_FILE = EXPORT_DIR / "moex_shares.xlsx"
BONDS_FILE = EXPORT_DIR / "moex_bonds.xlsx"
EMITTERS_FILE = EXPORT_DIR / "moex_emitters.xlsx"
GREEN_BONDS_FILE = EXPORT_DIR / "GreenBonds.xlsx"
SQLITE_FILE = DB_DIR / "vibe_data.sqlite"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 64
PROXY_SOURCE_TIMEOUT = 6
PROXY_VALIDATION_TIMEOUT = 2.0
PROXY_VALIDATION_WORKERS = 512
PROXY_VALIDATION_TARGET = 200
PROXY_REQUEST_TIMEOUT = 2.5
PROXY_MAX_ATTEMPTS = 2
PROXY_VALIDATION_TIME_BUDGET = 15
ACRA_PAGE_WORKERS = 24
ACRA_ISSUER_WORKERS = 64
ACRA_SEARCH_WORKERS = 120
NKR_PLAYWRIGHT_TIMEOUT_MS = 60_000
ACRA_PLAYWRIGHT_TIMEOUT_MS = 45_000
PROXYLIST_FILE = OUTPUT_DIR / "proxylist.csv"
USE_CACHE = True
CACHE_FILE = CACHE_DIR / "emitter_cache.json"
SHARES_CACHE_FILE = CACHE_DIR / "shares_snapshot.json"
BONDS_CACHE_FILE = CACHE_DIR / "bonds_snapshot.json"
EMITTERS_CACHE_FILE = CACHE_DIR / "emitters_snapshot.json"
GREEN_BONDS_CACHE_FILE = CACHE_DIR / "green_bonds_snapshot.json"
EXPERT_RA_CACHE_FILE = CACHE_DIR / "expert_ra_ratings.json"
ACRA_CACHE_FILE = CACHE_DIR / "acra_ratings.json"
NKR_CACHE_FILE = CACHE_DIR / "nkr_ratings.json"
NRA_CACHE_FILE = CACHE_DIR / "nra_ratings.json"
HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ZEBRA_FILL = PatternFill(fill_type="solid", fgColor="E8F2FF")
THIN_BORDER = Border(
    left=Side(style="thin", color="000000"),
    right=Side(style="thin", color="000000"),
    top=Side(style="thin", color="000000"),
    bottom=Side(style="thin", color="000000"),
)
CENTERED_WRAP_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ALLOWED_SCORE_VALUES = {"Red", "Yellow", "Green"}
PROXY_SOURCES = [
    "https://proxifly.dev/tools/proxy-list",
    "https://hide-my-name.com/proxy-list/",
    "https://free.geonix.com/ru/",
    "https://best-proxies.ru/proxylist/free/",
    "https://proxyverity.com/free-proxy-list/",
    "https://geonode.com/free-proxy-list",
    "https://spys.one/en/",
    "https://htmlweb.ru/analiz/proxy_list.php",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt",
]




def normalize_inn(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, float):
        if value.is_integer():
            text = str(int(value))
        else:
            text = format(value, "f")
    else:
        text = str(value).strip()

    if text.endswith('.0') and text.replace('.', '', 1).isdigit():
        text = text[:-2]

    digits = ''.join(ch for ch in text if ch.isdigit())
    if len(digits) in {10, 12}:
        return digits

    if digits.endswith('0') and len(digits[:-1]) in {10, 12}:
        return digits[:-1]

    return digits if len(digits) in {10, 12} else None

def progress(total: int, desc: str, unit: str):
    return tqdm(total=total, desc=desc, unit=unit, position=0, leave=False, dynamic_ncols=True)


def ensure_project_dirs() -> None:
    for directory in [LOGS_DIR, CACHE_DIR, EXPORT_DIR, DB_DIR, OUTPUT_DIR / "docs"]:
        directory.mkdir(parents=True, exist_ok=True)


def open_sqlite_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _serialize_sql_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (str, bytes)):
        return value
    if hasattr(value, "tolist") and not isinstance(value, (pd.Series, pd.DataFrame)):
        try:
            return json.dumps(value.tolist(), ensure_ascii=False)
        except Exception:
            pass
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return None
    except Exception:
        pass
    return value


def upsert_records_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    records: list[dict[str, Any]],
    key_columns: list[str],
    logger: logging.Logger,
) -> None:
    if not records:
        return
    frame = pd.DataFrame(records)
    upsert_dataframe_sqlite(conn, table_name, frame, key_columns, logger)


def upsert_dataframe_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    frame: pd.DataFrame,
    key_columns: list[str],
    logger: logging.Logger,
) -> None:
    if frame.empty:
        logger.info("SQLite upsert skipped table=%s (empty frame)", table_name)
        return

    columns = list(frame.columns)
    quoted_columns = ", ".join(f'"{column}" TEXT' for column in columns)
    pk = ", ".join(f'"{column}"' for column in key_columns)
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({quoted_columns}, "updated_at" TEXT, PRIMARY KEY ({pk}))')

    insert_columns = columns + ["updated_at"]
    insert_column_clause = ", ".join([f'"{col}"' for col in insert_columns])
    placeholders = ", ".join(["?" for _ in insert_columns])
    conflict_clause = ", ".join([f'"{col}"' for col in key_columns])
    update_clause = ", ".join([f'"{col}"=excluded."{col}"' for col in columns if col not in key_columns] + ['"updated_at"=excluded."updated_at"'])
    insert_sql = (
        f'INSERT INTO "{table_name}" ({insert_column_clause}) '
        f'VALUES ({placeholders}) '
        f'ON CONFLICT({conflict_clause}) DO UPDATE SET {update_clause}'
    )

    timestamp = datetime.now().isoformat(timespec="seconds")
    records = []
    for row in frame.to_dict(orient="records"):
        values = [_serialize_sql_value(row.get(column)) for column in columns]
        values.append(timestamp)
        records.append(values)

    conn.executemany(insert_sql, records)
    conn.commit()
    logger.info("SQLite upsert done table=%s rows=%s", table_name, len(records))


def load_json_dict(path: Path, logger: logging.Logger) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception as error:
        logger.exception("JSON cache load failed path=%s: %s", path, error)
        return {}


def save_json_dict(path: Path, payload: dict[str, Any], logger: logging.Logger) -> None:
    try:
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
    except Exception as error:
        logger.exception("JSON cache save failed path=%s: %s", path, error)


def load_daily_ratings_cache(path: Path, logger: logging.Logger) -> tuple[dict[str, str], bool]:
    if not USE_CACHE:
        logger.info("Daily rating cache disabled path=%s", path)
        return {}, False
    payload = load_json_dict(path, logger)
    today = date.today().isoformat()

    cached_on = payload.get("cached_on") if isinstance(payload, dict) else None
    ratings_payload = payload.get("ratings") if isinstance(payload, dict) and isinstance(payload.get("ratings"), dict) else None

    if ratings_payload is None and isinstance(payload, dict):
        ratings_payload = payload

    if not isinstance(ratings_payload, dict):
        return {}, False

    ratings = {str(key): str(value) for key, value in ratings_payload.items() if value is not None}
    return ratings, cached_on == today


def save_daily_ratings_cache(path: Path, ratings: dict[str, str], logger: logging.Logger) -> None:
    if not USE_CACHE:
        logger.info("Daily rating cache save skipped path=%s", path)
        return
    payload = {
        "cached_on": date.today().isoformat(),
        "ratings": ratings,
    }
    save_json_dict(path, payload, logger)


def save_dataframe_snapshot(path: Path, frame: pd.DataFrame, logger: logging.Logger) -> None:
    if not USE_CACHE:
        logger.info("Dataframe snapshot save skipped path=%s", path)
        return
    payload = {"columns": frame.columns.tolist(), "records": frame.where(pd.notna(frame), None).to_dict(orient="records")}
    save_json_dict(path, payload, logger)


def load_dataframe_snapshot(path: Path, logger: logging.Logger) -> pd.DataFrame:
    if not USE_CACHE:
        logger.info("Dataframe snapshot disabled path=%s", path)
        return pd.DataFrame()
    payload = load_json_dict(path, logger)
    columns = payload.get("columns")
    records = payload.get("records")
    if not isinstance(columns, list) or not isinstance(records, list):
        return pd.DataFrame()
    return pd.DataFrame(records, columns=columns)


class ProxyRotatingSession(requests.Session):
    def __init__(self, logger: logging.Logger, proxies: list[str], prefer_proxies: bool = False) -> None:
        super().__init__()
        self.logger = logger
        self.proxy_pool = proxies.copy()
        self.prefer_proxies = prefer_proxies
        self._proxy_index = 0
        self._proxy_lock = threading.Lock()
        self._proxy_failures: dict[str, int] = {}
        self.trust_env = False
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def _next_proxy(self) -> str | None:
        with self._proxy_lock:
            if not self.proxy_pool:
                return None
            pool_size = len(self.proxy_pool)
            for _ in range(pool_size):
                proxy = self.proxy_pool[self._proxy_index % pool_size]
                self._proxy_index += 1
                if self._proxy_failures.get(proxy, 0) < 2:
                    return proxy
            return self.proxy_pool[self._proxy_index % pool_size]

    def _mark_failed(self, proxy: str | None) -> None:
        if not proxy:
            return
        with self._proxy_lock:
            self._proxy_failures[proxy] = self._proxy_failures.get(proxy, 0) + 1
            if self._proxy_failures[proxy] >= 3 and proxy in self.proxy_pool and len(self.proxy_pool) > 10:
                self.proxy_pool.remove(proxy)

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        if not self.prefer_proxies or not self.proxy_pool:
            return super().request(method, url, **kwargs)

        max_attempts = min(PROXY_MAX_ATTEMPTS, max(1, len(self.proxy_pool))) if self.proxy_pool else 0
        last_error: Exception | None = None
        base_timeout = kwargs.get("timeout", REQUEST_TIMEOUT)

        for _ in range(max_attempts):
            request_kwargs = dict(kwargs)
            proxy = self._next_proxy()
            if not proxy:
                break
            request_kwargs["proxies"] = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            request_kwargs["timeout"] = min(base_timeout, PROXY_REQUEST_TIMEOUT)
            try:
                return super().request(method, url, **request_kwargs)
            except requests.RequestException as error:
                last_error = error
                self._mark_failed(proxy)
                self.logger.warning("Proxy request failed proxy=%s url=%s error=%s", proxy, url, error)

        fallback_kwargs = dict(kwargs)
        fallback_kwargs.pop("proxies", None)
        try:
            return super().request(method, url, **fallback_kwargs)
        except requests.RequestException as error:
            last_error = error

        if last_error:
            raise last_error
        raise requests.RequestException(f"Request failed without attempts url={url}")


def _extract_proxies_from_text(text: str) -> set[str]:
    proxy_regex = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})\b")
    result: set[str] = set()
    for ip, port in proxy_regex.findall(text):
        octets = ip.split(".")
        if len(octets) != 4 or any(int(octet) > 255 for octet in octets):
            continue
        port_num = int(port)
        if 1 <= port_num <= 65535:
            result.add(f"{ip}:{port_num}")
    return result


def fetch_proxy_candidates(logger: logging.Logger) -> list[str]:
    session = requests.Session()
    session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
    collected: set[str] = set()

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(PROXY_SOURCES))) as executor:
        futures = {executor.submit(session.get, url, timeout=PROXY_SOURCE_TIMEOUT): url for url in PROXY_SOURCES}
        with progress(total=len(futures), desc="Сбор прокси", unit="источник") as pbar:
            for future in as_completed(futures):
                url = futures[future]
                try:
                    response = future.result()
                    response.raise_for_status()
                    found = _extract_proxies_from_text(response.text)
                    collected.update(found)
                    logger.info("Proxy source parsed: %s proxies=%s", url, len(found))
                except Exception as error:
                    logger.warning("Proxy source failed: %s error=%s", url, error)
                pbar.update(1)

    proxies = sorted(collected)
    logger.info("Proxy candidates collected: %s", len(proxies))
    return proxies


def validate_proxies(proxies: list[str], logger: logging.Logger) -> list[str]:
    if not proxies:
        return []

    def check_proxy(proxy: str) -> tuple[str, float] | None:
        started = perf_counter()
        try:
            response = requests.get(
                "http://httpbin.org/ip",
                timeout=PROXY_VALIDATION_TIMEOUT,
                proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            )
            response.raise_for_status()
            return proxy, perf_counter() - started
        except Exception:
            return None

    valid_latency: dict[str, float] = {}
    started_at = perf_counter()
    total = len(proxies)
    max_pending = max(PROXY_VALIDATION_WORKERS * 2, 1)
    submitted = 0
    completed = 0
    with ThreadPoolExecutor(max_workers=PROXY_VALIDATION_WORKERS) as executor:
        in_flight: dict[Any, str] = {}

        def submit_next() -> bool:
            nonlocal submitted
            if submitted >= total:
                return False
            proxy = proxies[submitted]
            submitted += 1
            in_flight[executor.submit(check_proxy, proxy)] = proxy
            return True

        while len(in_flight) < min(max_pending, total) and submit_next():
            pass

        with progress(total=total, desc="Проверка прокси", unit="прокси") as pbar:
            while in_flight:
                timed_out = (perf_counter() - started_at) >= PROXY_VALIDATION_TIME_BUDGET
                if timed_out or len(valid_latency) >= PROXY_VALIDATION_TARGET:
                    completed += len(in_flight)
                    for pending_future in list(in_flight):
                        pending_future.cancel()
                    in_flight.clear()
                    pbar.n = completed
                    pbar.refresh()
                    break

                done_future = next(as_completed(in_flight))
                in_flight.pop(done_future, None)
                resolved = done_future.result()
                if resolved:
                    proxy, latency = resolved
                    previous = valid_latency.get(proxy)
                    if previous is None or latency < previous:
                        valid_latency[proxy] = latency
                completed += 1
                pbar.update(1)

                while len(in_flight) < max_pending and submit_next():
                    pass

    ordered = sorted(valid_latency.items(), key=lambda item: item[1])
    fastest = [proxy for proxy, _ in ordered[:PROXY_VALIDATION_TARGET]]
    logger.info("Proxy validated: total=%s valid=%s selected=%s", len(proxies), len(valid_latency), len(fastest))
    logger.info("Fastest proxies sample: %s", ordered[:10])
    return fastest


def save_valid_proxies_csv(proxies: list[str], logger: logging.Logger) -> None:
    frame = pd.DataFrame([{"proxy": proxy, "priority": idx + 1} for idx, proxy in enumerate(proxies)])
    frame.to_csv(PROXYLIST_FILE, index=False, encoding="utf-8-sig")
    logger.info("Proxy list saved to %s rows=%s", PROXYLIST_FILE, len(frame))


def load_cache(logger: logging.Logger) -> dict[str, dict[str, Any]]:
    if not USE_CACHE:
        logger.info("Cache disabled by USE_CACHE flag")
        return {"secid_to_emitter": {}, "emitters": {}}

    if not CACHE_FILE.exists():
        return {"secid_to_emitter": {}, "emitters": {}}

    try:
        with CACHE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            return {
                "secid_to_emitter": data.get("secid_to_emitter", {}),
                "emitters": data.get("emitters", {}),
            }
    except Exception as error:
        logger.exception("Cache load failed: %s", error)

    return {"secid_to_emitter": {}, "emitters": {}}


def save_cache(cache: dict[str, dict[str, Any]], logger: logging.Logger) -> None:
    if not USE_CACHE:
        logger.info("Cache save skipped by USE_CACHE flag")
        return

    try:
        with CACHE_FILE.open("w", encoding="utf-8") as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)
    except Exception as error:
        logger.exception("Cache save failed: %s", error)


def setup_logging() -> logging.Logger:
    ensure_project_dirs()
    logger = logging.getLogger("moex_export")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


class MoexClient:
    def __init__(self, logger: logging.Logger, proxies: list[str]) -> None:
        self.logger = logger
        self.session = ProxyRotatingSession(logger, proxies)
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self.logger.info("GET %s params=%s status=%s", url, params, response.status_code)
        return response.json()

    def fetch_market_securities(self, market: str, columns: list[str]) -> pd.DataFrame:
        with progress(total=1, desc=f"MOEX {market}", unit="запрос") as pbar:
            data = self._get(
                f"/engines/stock/markets/{market}/securities.json",
                params={"iss.meta": "off", "iss.only": "securities", "securities.columns": ",".join(columns)},
            )
            pbar.update(1)

        return pd.DataFrame(data.get("securities", {}).get("data", []), columns=data.get("securities", {}).get("columns", []))

    def fetch_emitter_id_by_secid(self, secid: str) -> int | None:
        data = self._get(
            f"/securities/{secid}.json",
            params={"iss.meta": "off", "iss.only": "description"},
        )
        rows = data.get("description", {}).get("data", [])
        mapping = {row[0]: row[2] for row in rows if len(row) >= 3}
        emitter_id = mapping.get("EMITTER_ID") or mapping.get("EMITENT_ID")
        try:
            return int(emitter_id) if emitter_id is not None else None
        except (TypeError, ValueError):
            return None

    def fetch_emitter_info(self, emitter_id: int) -> dict[str, Any]:
        data = self._get(
            f"/emitters/{emitter_id}.json",
            params={"iss.meta": "off", "iss.only": "emitter", "emitter.columns": "EMITTER_ID,SHORT_TITLE,INN"},
        )
        row = data.get("emitter", {}).get("data", [])
        if not row:
            return {"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None}
        return {"EMITTER_ID": int(row[0][0]), "EMITTER_NAME": row[0][1], "INN": row[0][2]}

    def fetch_security_description(self, secid: str) -> dict[str, Any]:
        data = self._get(f"/securities/{secid}.json", params={"iss.meta": "off", "iss.only": "description"})
        rows = data.get("description", {}).get("data", [])
        description: dict[str, Any] = {}
        for row in rows:
            if len(row) >= 3:
                description[str(row[0]).upper()] = row[2]
        return description

    def fetch_security_market_snapshot(self, secid: str) -> dict[str, Any]:
        data = self._get(
            "/engines/stock/markets/bonds/securities/{secid}.json".format(secid=secid),
            params={"iss.meta": "off", "iss.only": "securities,marketdata"},
        )
        result: dict[str, Any] = {}
        for block_name in ["securities", "marketdata"]:
            block = data.get(block_name, {})
            cols = block.get("columns", [])
            rows = block.get("data", [])
            if rows:
                for idx, col in enumerate(cols):
                    result[col.upper()] = rows[0][idx] if idx < len(rows[0]) else None
        return result

    def fetch_bondization(self, secid: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        data = self._get(
            f"/statistics/engines/stock/markets/bonds/bondization/{secid}.json",
            params={"iss.meta": "off", "iss.only": "coupons,amortizations"},
        )
        coupons = pd.DataFrame(data.get("coupons", {}).get("data", []), columns=data.get("coupons", {}).get("columns", []))
        amortizations = pd.DataFrame(
            data.get("amortizations", {}).get("data", []),
            columns=data.get("amortizations", {}).get("columns", []),
        )
        return coupons, amortizations


class ExpertRaClient:
    def __init__(self, logger: logging.Logger, proxies: list[str]) -> None:
        self.logger = logger
        self.session = ProxyRotatingSession(logger, proxies)
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})

    def _normalize_inn(self, value: Any) -> str | None:
        return normalize_inn(value)

    def _clean_text(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    def _format_date(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d.%m.%Y")
        if isinstance(value, date):
            return value.strftime("%d.%m.%Y")
        text = str(value).strip()
        if not text:
            return ""
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.strftime("%d.%m.%Y")
        return text

    def _fetch_export_paths(self) -> list[str]:
        response = self.session.get(f"{EXPERT_RA_BASE_URL}/ratings/", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        found_paths = set(re.findall(r'data-path="/([^/"]+)/"', response.text))
        paths = sorted(path for path in found_paths if path)
        self.logger.info("Expert RA export paths resolved: %s", len(paths))
        return paths

    def _download_ratings_workbook(self, paths: list[str]) -> bytes:
        labels = [f"Категория {path}" for path in paths]
        payload = {"all": {"labels": labels, "paths": paths}}
        virtual_date = date.today().strftime("%d.%m.%Y")
        response = self.session.post(
            f"{EXPERT_RA_BASE_URL}/ratings/ratings-xlsx-export",
            params={"isSinglePage": 1, "virtual_date": virtual_date},
            json=payload,
            timeout=REQUEST_TIMEOUT * 3,
        )
        response.raise_for_status()
        self.logger.info(
            "Expert RA export downloaded: status=%s size=%s",
            response.status_code,
            len(response.content),
        )
        return response.content

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(inn) for inn in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        paths = self._fetch_export_paths()
        if not paths:
            self.logger.warning("Expert RA export paths not found")
            return {}

        workbook_bytes = self._download_ratings_workbook(paths)
        workbook = pd.read_excel(BytesIO(workbook_bytes), header=5)
        workbook.columns = [str(col).strip() for col in workbook.columns]

        required_columns = {"ИНН", "Рейтинг", "Прогноз", "Дата присвоения/актуализации/изменения рейтинга"}
        missing_columns = required_columns - set(workbook.columns)
        if missing_columns:
            self.logger.warning("Expert RA missing columns in export: %s", sorted(missing_columns))
            return {}

        ratings_by_inn: dict[str, dict[str, Any]] = {}

        with progress(total=len(workbook), desc="Парсинг Эксперт РА", unit="строка") as pbar:
            for _, row in workbook.iterrows():
                inn = self._normalize_inn(row.get("ИНН"))
                if not inn or inn not in normalized_inns:
                    pbar.update(1)
                    continue

                row_date = pd.to_datetime(row.get("Дата присвоения/актуализации/изменения рейтинга"), errors="coerce", dayfirst=True)
                row_date_for_sort = row_date if pd.notna(row_date) else pd.Timestamp.min
                current_best = ratings_by_inn.get(inn)

                if current_best is None or row_date_for_sort > current_best["_sort_date"]:
                    rating = self._clean_text(row.get("Рейтинг"))
                    if not rating or not rating.lower().startswith("ru"):
                        pbar.update(1)
                        continue

                    forecast = self._clean_text(row.get("Прогноз"))
                    date_text = self._format_date(row.get("Дата присвоения/актуализации/изменения рейтинга"))
                    rating_parts = [part for part in [rating, forecast, date_text] if part]
                    ratings_by_inn[inn] = {
                        "_sort_date": row_date_for_sort,
                        "value": "\n".join(rating_parts),
                    }

                pbar.update(1)

        result = {inn: payload["value"] for inn, payload in ratings_by_inn.items()}
        self.logger.info("Expert RA ratings matched by INN: %s", len(result))
        return result


class AcraClient:
    def __init__(self, logger: logging.Logger, proxies: list[str]) -> None:
        self.logger = logger
        self.session = ProxyRotatingSession(logger, proxies)
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
        self.direct_session = requests.Session()
        self.direct_session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
        self.direct_session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))
        self.direct_session.mount("http://", requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))
        self._request_mode = "auto"
        self._request_mode_lock = threading.Lock()

    def _normalize_inn(self, value: Any) -> str | None:
        return normalize_inn(value)

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    def _extract_total_issuers(self, text: str) -> int | None:
        match = re.search(r"Найдено:\s*(\d+)", text)
        return int(match.group(1)) if match else None

    def _extract_search_rows(self, text: str) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        row_blocks = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", text, flags=re.IGNORECASE)
        for block in row_blocks:
            issuer_match = re.search(r'href="(/ratings/issuers/[^"#?]+/)"[^>]*>([\s\S]*?)</a>', block, flags=re.IGNORECASE)
            if not issuer_match:
                continue
            issuer_path = issuer_match.group(1)
            issuer_name = self._clean_text(re.sub(r"<[^>]+>", " ", issuer_match.group(2)))

            rating_match = re.search(r"([A-Z]{1,4}[+-]?\(RU\))", block)
            rating = rating_match.group(1) if rating_match else ""

            forecast_match = re.search(r"(Позитивный|Стабильный|Негативный|Развивающийся|Под наблюдением)", block, flags=re.IGNORECASE)
            forecast = self._clean_text(forecast_match.group(1)) if forecast_match else ""

            date_match = re.search(r"(\d{1,2}\s+[а-я]+\s+\d{4}|\d{1,2}\.\d{1,2}\.\d{2,4})", block, flags=re.IGNORECASE)
            date_text = self._parse_ru_date(date_match.group(1)) if date_match else ""

            rows.append(
                {
                    "issuer_path": issuer_path,
                    "issuer_name": issuer_name,
                    "rating": rating,
                    "forecast": forecast,
                    "date": date_text,
                }
            )
        return rows

    def _extract_search_rows_markdown(self, text: str) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        pattern = re.compile(
            r"\[([^\]]+)\]\(https?://www\.acra-ratings\.ru(/ratings/issuers/\d+/)\)\s+"
            r"([A-Z]{1,4}(?:[+-]|-)?\(RU\))?\s*"
            r"(Позитивный|Стабильный|Негативный|Развивающийся|Под наблюдением)?[\s\S]{0,120}?"
            r"\[(\d{1,2}\s+[а-я]+\s+\d{4}|\d{1,2}\.\d{1,2}\.\d{2,4})\]",
            flags=re.IGNORECASE,
        )
        for match in pattern.finditer(text):
            issuer_name, issuer_path, rating, forecast, raw_date = match.groups()
            rows.append(
                {
                    "issuer_path": issuer_path,
                    "issuer_name": self._clean_text(issuer_name),
                    "rating": rating or "",
                    "forecast": self._clean_text(forecast) if forecast else "",
                    "date": self._parse_ru_date(raw_date) if raw_date else "",
                }
            )
        return rows


    def _parse_ru_date(self, raw_value: str) -> str:
        month_map = {
            "янв": "01",
            "фев": "02",
            "мар": "03",
            "апр": "04",
            "мая": "05",
            "май": "05",
            "июн": "06",
            "июл": "07",
            "авг": "08",
            "сен": "09",
            "окт": "10",
            "ноя": "11",
            "дек": "12",
        }
        normalized = self._clean_text(raw_value.lower())
        match = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", normalized)
        if not match:
            return self._clean_text(raw_value)

        day, month_text, year = match.groups()
        month = month_map.get(month_text[:3])
        if not month:
            return self._clean_text(raw_value)
        return f"{int(day):02d}.{month}.{year}"

    def _get_page_text(self, path: str, params: dict[str, Any] | None = None) -> str:
        request_params = params or {}
        url = f"{ACRA_BASE_URL}{path}"
        errors: list[str] = []

        def call_direct(verify: bool = True) -> str:
            response = self.direct_session.get(url, params=request_params, timeout=REQUEST_TIMEOUT, verify=verify)
            response.raise_for_status()
            self.logger.info("GET ACRA direct verify=%s %s params=%s status=%s", verify, url, request_params, response.status_code)
            return response.text

        def call_proxy() -> str:
            proxy_response = self.direct_session.get(
                f"{ACRA_PROXY_BASE_URL}{path}",
                params=request_params,
                timeout=REQUEST_TIMEOUT,
            )
            proxy_response.raise_for_status()
            self.logger.info("GET ACRA proxy %s params=%s status=%s", path, request_params, proxy_response.status_code)
            return proxy_response.text

        with self._request_mode_lock:
            mode = self._request_mode

        if mode == "proxy":
            return call_proxy()
        if mode == "direct":
            return call_direct(verify=True)
        if mode == "direct_insecure":
            return call_direct(verify=False)

        try:
            text = call_direct(verify=True)
            with self._request_mode_lock:
                self._request_mode = "direct"
            return text
        except requests.RequestException as error:
            errors.append(f"direct-verify-on: {error}")
            self.logger.warning("ACRA direct request failed with certificate verification: %s", error)

        try:
            text = call_direct(verify=False)
            with self._request_mode_lock:
                self._request_mode = "direct_insecure"
            return text
        except requests.RequestException as error:
            errors.append(f"direct-verify-off: {error}")
            self.logger.warning("ACRA direct request without certificate verification failed: %s", error)

        try:
            text = call_proxy()
            with self._request_mode_lock:
                self._request_mode = "proxy"
            return text
        except requests.RequestException as error:
            errors.append(f"proxy: {error}")
            self.logger.warning("ACRA proxy request failed: %s", error)

        raise requests.RequestException("; ".join(errors))

    def _extract_issuer_links(self, text: str) -> list[str]:
        matches = re.findall(r"/ratings/issuers/(\d+)/", text)
        unique_ids = sorted(set(matches), key=lambda value: int(value))
        return [f"/ratings/issuers/{issuer_id}/" for issuer_id in unique_ids]

    def _parse_issuer_card(self, text: str) -> tuple[str | None, str | None]:
        raw_text = text
        if "<html" in text.lower() or "<body" in text.lower():
            cleaned = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
            cleaned = re.sub(r"<style[\s\S]*?</style>", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"</?(?:br|p|div|li|tr|td|th|h1|h2|h3|h4|h5|h6)\b[^>]*>", "\n", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"<[^>]+>", " ", cleaned)
            raw_text = html.unescape(cleaned)

        lines = [self._clean_text(line) for line in raw_text.splitlines() if self._clean_text(line)]
        if not lines:
            return None, None

        inn: str | None = None
        for index, line in enumerate(lines):
            upper_line = line.upper()
            if upper_line == "ИНН" and index + 1 < len(lines):
                candidate = self._normalize_inn(lines[index + 1])
                if candidate:
                    inn = candidate
                    break
            if "ИНН" in upper_line:
                inn_match = re.search(r"ИНН\D{0,30}(\d{10,12})", line, flags=re.IGNORECASE)
                if inn_match:
                    inn = inn_match.group(1)
                    break

        if not inn:
            full_text = "\n".join(lines)
            inn_match = re.search(r"ИНН\D{0,30}(\d{10,12})", full_text, flags=re.IGNORECASE)
            if inn_match:
                inn = inn_match.group(1)

        if not inn:
            return None, None

        current_start: int | None = None
        for index, line in enumerate(lines):
            if "текущий рейтинг" in line.lower():
                current_start = index
                break

        if current_start is None:
            current_block = lines
        else:
            end_index = len(lines)
            for index in range(current_start + 1, len(lines)):
                lowered = lines[index].lower()
                if lowered == "история рейтингов":
                    end_index = index
                    break
            current_block = lines[current_start:end_index]
        current_text = "\n".join(current_block)

        rating_match = re.search(r"([A-Z]{1,4}[+-]?\(RU\))", current_text)
        rating = rating_match.group(1) if rating_match else None

        forecast: str | None = None
        forecast_match = re.search(
            r"прогноз\s+([А-Яа-яA-Za-z ,()\-]+)",
            current_text,
            flags=re.IGNORECASE,
        )
        if forecast_match:
            forecast = self._clean_text(forecast_match.group(1))
        else:
            for line in current_block:
                lower_line = line.lower()
                if "под наблюдением" in lower_line or lower_line in {"позитивный", "стабильный", "негативный", "развивающийся"}:
                    forecast = line
                    break

        date_value: str | None = None
        date_match = re.search(r"\b(\d{1,2}\s+[а-я]+\s+\d{4})\b", current_text.lower())
        if date_match:
            date_value = self._parse_ru_date(date_match.group(1))

        if not rating:
            rating_match = re.search(r"\b([A-Z]{1,4}(?:[+-]|-)?\(RU\))\b", "\n".join(lines))
            rating = rating_match.group(1) if rating_match else None

        if not rating:
            return inn, None

        rating_parts = [part for part in [rating, forecast, date_value] if part]
        return inn, "\n".join(rating_parts) if rating_parts else None

    def _fetch_latest_ratings_by_inn_playwright(self, inns: list[str]) -> dict[str, str]:
        ratings_by_inn: dict[str, str] = {}
        issuer_cache: dict[str, tuple[str | None, str | None]] = {}

        with sync_playwright() as playwright:
            request = playwright.request.new_context(ignore_https_errors=True)

            with progress(total=len(inns), desc="Парсинг АКРА (Playwright)", unit="ИНН") as pbar:
                for inn in inns:
                    try:
                        response = request.get(
                            f"{ACRA_BASE_URL}/ratings/issuers/",
                            params={"text": inn},
                            timeout=ACRA_PLAYWRIGHT_TIMEOUT_MS,
                        )
                        search_text = response.text()
                        rows = self._extract_search_rows(search_text)
                        for row in rows:
                            parts = [part for part in [row.get("rating", ""), row.get("forecast", ""), row.get("date", "")] if part]
                            if parts:
                                ratings_by_inn[inn] = "\n".join(parts)
                                break

                        if inn in ratings_by_inn:
                            pbar.update(1)
                            continue

                        issuer_links = self._extract_issuer_links(search_text)
                        for issuer_path in issuer_links[:10]:
                            if issuer_path not in issuer_cache:
                                issuer_response = request.get(
                                    f"{ACRA_BASE_URL}{issuer_path}",
                                    timeout=ACRA_PLAYWRIGHT_TIMEOUT_MS,
                                )
                                issuer_cache[issuer_path] = self._parse_issuer_card(issuer_response.text())

                            parsed_inn, parsed_value = issuer_cache[issuer_path]
                            if parsed_inn == inn and parsed_value:
                                ratings_by_inn[inn] = parsed_value
                                break
                    except Exception as error:
                        self.logger.warning("ACRA Playwright query failed inn=%s: %s", inn, error)
                    pbar.update(1)

            request.dispose()

        return ratings_by_inn

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(value) for value in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        ratings_by_inn: dict[str, str] = {}
        issuer_cache: dict[str, tuple[str | None, str | None]] = {}
        issuer_cache_lock = threading.Lock()

        def fetch_issuer_cached(issuer_path: str) -> tuple[str | None, str | None]:
            with issuer_cache_lock:
                if issuer_path in issuer_cache:
                    return issuer_cache[issuer_path]

            issuer_text = self._get_page_text(issuer_path)
            parsed = self._parse_issuer_card(issuer_text)
            with issuer_cache_lock:
                issuer_cache[issuer_path] = parsed
            return parsed

        def fetch_for_inn(inn: str) -> tuple[str, str | None]:
            search_text = self._get_page_text("/ratings/issuers/", params={"text": inn})
            rows = self._extract_search_rows(search_text)
            if not rows and "URL Source:" in search_text:
                rows = self._extract_search_rows_markdown(search_text)
            for row in rows:
                parts = [part for part in [row.get("rating", ""), row.get("forecast", ""), row.get("date", "")] if part]
                if parts:
                    return inn, "\n".join(parts)

            issuer_links = self._extract_issuer_links(search_text)
            for issuer_path in issuer_links[:10]:
                parsed_inn, parsed_value = fetch_issuer_cached(issuer_path)
                if parsed_inn == inn and parsed_value:
                    return inn, parsed_value
            return inn, None

        ordered_inns = sorted(normalized_inns)
        with progress(total=len(ordered_inns), desc="Парсинг АКРА", unit="ИНН") as pbar:
            with ThreadPoolExecutor(max_workers=min(ACRA_SEARCH_WORKERS, max(len(ordered_inns), 1))) as executor:
                futures = {executor.submit(fetch_for_inn, inn): inn for inn in ordered_inns}
                for future in as_completed(futures):
                    inn = futures[future]
                    try:
                        resolved_inn, rating_value = future.result()
                        if rating_value:
                            ratings_by_inn[resolved_inn] = rating_value
                    except requests.RequestException as error:
                        self.logger.warning("ACRA INN query failed inn=%s: %s", inn, error)
                    except Exception as error:
                        self.logger.exception("ACRA parse failed inn=%s: %s", inn, error)
                    pbar.update(1)

        coverage_ratio = (len(ratings_by_inn) / len(normalized_inns)) if normalized_inns else 0
        if coverage_ratio < 0.1 and normalized_inns:
            self.logger.warning(
                "ACRA coverage is too low (%s/%s). Trying Playwright fallback.",
                len(ratings_by_inn),
                len(normalized_inns),
            )
            unresolved = [inn for inn in ordered_inns if inn not in ratings_by_inn]
            try:
                playwright_ratings = self._fetch_latest_ratings_by_inn_playwright(unresolved)
                ratings_by_inn.update(playwright_ratings)
            except Exception as error:
                self.logger.warning("ACRA Playwright fallback failed: %s", error)

        self.logger.info("ACRA request mode=%s target_inn=%s", self._request_mode, len(normalized_inns))
        self.logger.info("ACRA ratings matched by INN: %s", len(ratings_by_inn))
        return ratings_by_inn


class NkrClient:
    def __init__(self, logger: logging.Logger, proxies: list[str]) -> None:
        self.logger = logger
        self.session = ProxyRotatingSession(logger, proxies)
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})

    def _normalize_inn(self, value: Any) -> str | None:
        return normalize_inn(value)

    def _normalize_date(self, value: Any) -> str:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.strftime("%d.%m.%Y")
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    def _clean_text(self, value: str) -> str:
        text = html.unescape(value)
        return re.sub(r"\s+", " ", text).strip()

    def _parse_issuers_rows(self, text: str) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        row_pattern = re.compile(r"<tr[^>]*>([\s\S]*?)</tr>", flags=re.IGNORECASE)
        for block in row_pattern.findall(text):
            href_match = re.search(r'href="(/ratings/issuers/[^"#?]+/)"', block, flags=re.IGNORECASE)
            if not href_match:
                continue

            cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", block, flags=re.IGNORECASE)
            if len(cells) < 6:
                continue

            rating = self._clean_text(re.sub(r"<[^>]+>", " ", cells[1]))
            if not rating:
                continue
            forecast = self._clean_text(re.sub(r"<[^>]+>", " ", cells[2]))
            date_value = self._clean_text(re.sub(r"<[^>]+>", " ", cells[5]))
            rows.append({"issuer_path": href_match.group(1), "rating": rating, "forecast": forecast, "date": date_value})

        self.logger.info("NKR issuer rows parsed: %s", len(rows))
        return rows

    def _fetch_issuer_inn(self, issuer_path: str) -> str | None:
        response = self.session.get(f"{NKR_BASE_URL}{issuer_path}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        text = response.text

        match = re.search(
            r"<span[^>]*>\s*ИНН\s*</span>\s*<span[^>]*>\s*(\d{10,12})\s*</span>",
            text,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1)

        fallback = re.search(r"ИНН\D{0,30}(\d{10,12})", re.sub(r"<[^>]+>", " ", text), flags=re.IGNORECASE)
        return fallback.group(1) if fallback else None

    def _download_excel_report(self) -> Path:
        final_path = CACHE_DIR / "nkr_issuers_latest.xlsx"
        final_path.parent.mkdir(parents=True, exist_ok=True)

        with TemporaryDirectory(prefix="nkr_export_") as temp_dir:
            temp_path = Path(temp_dir)
            try:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    page.goto(f"{NKR_BASE_URL}/ratings/issuers/", wait_until="networkidle", timeout=NKR_PLAYWRIGHT_TIMEOUT_MS)

                    export_locator = page.get_by_role("button", name="Выгрузить в Excel")
                    if export_locator.count() == 0:
                        export_locator = page.get_by_role("link", name="Выгрузить в Excel")
                    if export_locator.count() == 0:
                        export_locator = page.locator("a[href*='issuers.php'], button:has-text('Выгрузить в Excel')").first
                    export_locator.wait_for(state="visible", timeout=NKR_PLAYWRIGHT_TIMEOUT_MS)

                    with page.expect_download(timeout=NKR_PLAYWRIGHT_TIMEOUT_MS) as download_info:
                        export_locator.click(force=True)
                    download = download_info.value
                    suggested = download.suggested_filename or "nkr_issuers.xlsx"
                    downloaded_path = temp_path / suggested
                    download.save_as(str(downloaded_path))

                    context.close()
                    browser.close()

                if downloaded_path.exists() and downloaded_path.stat().st_size > 0:
                    downloaded_path.replace(final_path)
                    self.logger.info("NKR report downloaded via Playwright")
                    return final_path
                raise ValueError("NKR Playwright downloaded empty file")
            except Exception as error:
                self.logger.warning("NKR Playwright export failed, fallback to direct endpoint: %s", error)

        direct_url = f"{NKR_BASE_URL}/issuers.php"
        response = self.session.get(direct_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if not response.content:
            raise ValueError("NKR direct export returned empty content")
        final_path.write_bytes(response.content)
        self.logger.info("NKR report downloaded directly (fallback): %s", direct_url)
        return final_path

    def _build_ratings_from_excel(self, excel_path: Path, target_inns: set[str]) -> dict[str, str]:
        frame = pd.read_excel(excel_path)
        if frame.empty:
            return {}

        frame.columns = [str(column).strip() for column in frame.columns]
        required_columns = {"Date", "TIN", "Rating", "Outlook"}
        missing = required_columns.difference(frame.columns)
        if missing:
            raise ValueError(f"NKR export missing columns: {sorted(missing)}")

        frame = frame[["Date", "TIN", "Rating", "Outlook"]].copy()
        frame["INN"] = frame["TIN"].map(self._normalize_inn)
        frame = frame[frame["INN"].isin(target_inns)]
        if frame.empty:
            return {}

        frame["DATE_TS"] = pd.to_datetime(frame["Date"], errors="coerce", dayfirst=True)
        frame["SORT_DATE"] = frame["DATE_TS"].fillna(pd.Timestamp.min)
        frame = frame.sort_values(by=["INN", "SORT_DATE"], ascending=[True, False])
        latest_rows = frame.drop_duplicates(subset=["INN"], keep="first")

        ratings_by_inn: dict[str, str] = {}
        for row in latest_rows.to_dict(orient="records"):
            inn = row.get("INN")
            if not inn:
                continue
            rating = str(row.get("Rating") or "").strip()
            outlook = str(row.get("Outlook") or "").strip()
            normalized_date = self._normalize_date(row.get("Date"))
            parts = [part for part in [rating, outlook, normalized_date] if part]
            if parts:
                ratings_by_inn[inn] = "\n".join(parts)

        return ratings_by_inn

    def _fetch_latest_ratings_by_inn_fallback(self, normalized_inns: set[str]) -> dict[str, str]:
        response = self.session.get(f"{NKR_BASE_URL}/ratings/issuers/", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        rows = self._parse_issuers_rows(response.text)
        if not rows:
            return {}

        ratings_by_inn: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self._fetch_issuer_inn, row["issuer_path"]): row for row in rows}
            with progress(total=len(futures), desc="Парсинг НКР (fallback)", unit="эмитент") as pbar:
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        inn = self._normalize_inn(future.result())
                    except requests.RequestException as error:
                        self.logger.warning("NKR issuer page skipped path=%s: %s", row["issuer_path"], error)
                        pbar.update(1)
                        continue
                    except Exception as error:
                        self.logger.exception("NKR issuer parse failed path=%s: %s", row["issuer_path"], error)
                        pbar.update(1)
                        continue

                    if inn and inn in normalized_inns:
                        rating_parts = [part for part in [row["rating"], row["forecast"], row["date"] if row["date"] else ""] if part]
                        ratings_by_inn[inn] = "\n".join(rating_parts)
                    pbar.update(1)

        return ratings_by_inn

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(value) for value in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        try:
            with progress(total=3, desc="Парсинг НКР", unit="этап") as pbar:
                excel_path = self._download_excel_report()
                pbar.update(1)
                ratings_by_inn = self._build_ratings_from_excel(excel_path, normalized_inns)
                pbar.update(1)
                self.logger.info("NKR excel parsed target_inn=%s matched=%s file=%s", len(normalized_inns), len(ratings_by_inn), excel_path)
                pbar.update(1)
        except Exception as error:
            self.logger.warning("NKR Playwright export failed, using fallback parser: %s", error)
            ratings_by_inn = self._fetch_latest_ratings_by_inn_fallback(normalized_inns)

        self.logger.info("NKR ratings matched by INN: %s", len(ratings_by_inn))
        return ratings_by_inn


class NraClient:
    def __init__(self, logger: logging.Logger, proxies: list[str]) -> None:
        self.logger = logger
        self.session = ProxyRotatingSession(logger, proxies)
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})

    def _normalize_inn(self, value: Any) -> str | None:
        return normalize_inn(value)

    def _clean_text(self, value: str) -> str:
        text = html.unescape(value)
        text = re.sub(r"<[^>]+>", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _normalize_date(self, value: str) -> str:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.strftime("%d.%m.%Y")
        return value

    def _extract_table_rows(self, page_text: str) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        table_blocks = re.findall(r"<table[^>]*class=\"[^\"]*wpdtSimpleTable[^\"]*\"[^>]*>([\s\S]*?)</table>", page_text, flags=re.IGNORECASE)

        for table_block in table_blocks:
            header_map: dict[str, int] = {}
            tr_blocks = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", table_block, flags=re.IGNORECASE)
            for tr_block in tr_blocks:
                cells_raw = re.findall(r"<td[^>]*>([\s\S]*?)</td>", tr_block, flags=re.IGNORECASE)
                if not cells_raw:
                    continue
                cells = [self._clean_text(cell) for cell in cells_raw]
                upper_cells = [cell.upper() for cell in cells]
                if "ИНН" in upper_cells and "ПРИСВОЕН РЕЙТИНГ" in upper_cells:
                    header_map = {name: idx for idx, name in enumerate(upper_cells)}
                    continue

                if not header_map:
                    continue

                inn_idx = header_map.get("ИНН")
                rating_idx = header_map.get("ПРИСВОЕН РЕЙТИНГ")
                forecast_idx = header_map.get("ПРОГНОЗ ПО РЕЙТИНГУ")
                date_idx = header_map.get("ДАТА ПУБЛИКАЦИИ")
                status_idx = header_map.get("СТАТУС РЕЙТИНГА")

                if inn_idx is None or rating_idx is None:
                    continue
                if max(filter(lambda value: value is not None, [inn_idx, rating_idx, forecast_idx, date_idx, status_idx])) >= len(cells):
                    continue

                rows.append(
                    {
                        "inn": cells[inn_idx],
                        "rating": cells[rating_idx],
                        "forecast": cells[forecast_idx] if forecast_idx is not None else "",
                        "date": self._normalize_date(cells[date_idx]) if date_idx is not None and cells[date_idx] else "",
                        "status": cells[status_idx] if status_idx is not None else "",
                    }
                )

        self.logger.info("NRA rows parsed: %s", len(rows))
        return rows

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(value) for value in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        response = self.session.get(f"{NRA_BASE_URL}/list-of-credit-ratings/", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        rows = self._extract_table_rows(response.text)

        ratings_by_inn: dict[str, dict[str, Any]] = {}
        with progress(total=len(rows), desc="Парсинг НРА", unit="строка") as pbar:
            for row in rows:
                inn = self._normalize_inn(row["inn"])
                rating = row["rating"].strip()
                if not inn or inn not in normalized_inns or not rating:
                    pbar.update(1)
                    continue

                parsed_date = pd.to_datetime(row["date"], errors="coerce", dayfirst=True)
                sort_date = parsed_date if pd.notna(parsed_date) else pd.Timestamp.min
                status = row["status"].lower()
                is_active = 1 if "действ" in status else 0
                current = ratings_by_inn.get(inn)
                current_sort = (current["is_active"], current["sort_date"]) if current else (-1, pd.Timestamp.min)

                if (is_active, sort_date) >= current_sort:
                    rating_parts = [part for part in [rating, row["forecast"], row["date"]] if part]
                    ratings_by_inn[inn] = {
                        "is_active": is_active,
                        "sort_date": sort_date,
                        "value": "\n".join(rating_parts),
                    }

                pbar.update(1)

        result = {inn: payload["value"] for inn, payload in ratings_by_inn.items()}
        self.logger.info("NRA ratings matched by INN: %s", len(result))
        return result


def enrich_emitters(
    client: MoexClient,
    shares: pd.DataFrame,
    bonds: pd.DataFrame,
    logger: logging.Logger,
    cache: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "EMITTER_ID" not in shares.columns:
        shares["EMITTER_ID"] = pd.NA
    if "EMITTER_ID" not in bonds.columns:
        bonds["EMITTER_ID"] = pd.NA

    shares_emitter_ids = pd.to_numeric(shares["EMITTER_ID"], errors="coerce")
    bonds_emitter_ids = pd.to_numeric(bonds["EMITTER_ID"], errors="coerce")
    shares["EMITTER_ID"] = shares_emitter_ids
    bonds["EMITTER_ID"] = bonds_emitter_ids

    existing_pairs = pd.concat([shares[["SECID", "EMITTER_ID"]], bonds[["SECID", "EMITTER_ID"]]], ignore_index=True)
    cached_pairs = pd.DataFrame(
        [{"SECID": secid, "EMITTER_ID": emitter_id} for secid, emitter_id in cache.get("secid_to_emitter", {}).items()]
    )
    existing_pairs = pd.concat([existing_pairs, cached_pairs], ignore_index=True)
    existing_pairs["EMITTER_ID"] = pd.to_numeric(existing_pairs["EMITTER_ID"], errors="coerce")
    existing_pairs = existing_pairs.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["SECID"], keep="first")
    existing_secids = set(existing_pairs["SECID"].tolist())

    secids = sorted((set(shares["SECID"].tolist()) | set(bonds["SECID"].tolist())) - existing_secids)
    logger.info("Emitter enrichment start for secids=%s", len(secids))

    secid_rows: list[dict[str, Any]] = existing_pairs.to_dict("records")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_id_by_secid, secid): secid for secid in secids}
        with progress(total=len(futures), desc="Определение EMITTER_ID", unit="бумага") as pbar:
            for future in as_completed(futures):
                secid = futures[future]
                try:
                    emitter_id = future.result()
                except requests.RequestException as error:
                    logger.exception("Emitter id fetch failed secid=%s: %s", secid, error)
                    emitter_id = None
                except Exception as error:
                    logger.exception("Unexpected emitter id error secid=%s: %s", secid, error)
                    emitter_id = None
                secid_rows.append({"SECID": secid, "EMITTER_ID": emitter_id})
                if emitter_id is not None:
                    cache.setdefault("secid_to_emitter", {})[secid] = int(emitter_id)
                pbar.update(1)

    secid_map = pd.DataFrame(secid_rows).drop_duplicates(subset=["SECID"], keep="first")
    secid_map["EMITTER_ID"] = pd.to_numeric(secid_map["EMITTER_ID"], errors="coerce")
    emitter_ids = sorted({int(x) for x in secid_map["EMITTER_ID"].dropna().tolist()})
    logger.info("Resolved emitter ids=%s", len(emitter_ids))

    cached_emitters = cache.get("emitters", {})
    emitter_rows: list[dict[str, Any]] = []
    missing_emitter_ids = []
    for emitter_id in emitter_ids:
        cached = cached_emitters.get(str(emitter_id))
        if cached:
            emitter_rows.append(cached)
        else:
            missing_emitter_ids.append(emitter_id)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_info, emitter_id): emitter_id for emitter_id in missing_emitter_ids}
        with progress(total=len(futures), desc="Дозагрузка эмитентов", unit="эмитент") as pbar:
            for future in as_completed(futures):
                emitter_id = futures[future]
                try:
                    emitter_info = future.result()
                    emitter_rows.append(emitter_info)
                    cache.setdefault("emitters", {})[str(emitter_id)] = emitter_info
                except requests.RequestException as error:
                    logger.exception("Emitter info failed id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                except Exception as error:
                    logger.exception("Unexpected emitter info error id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                pbar.update(1)

    emitters_df = pd.DataFrame(emitter_rows).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    secid_map = secid_map.rename(columns={"EMITTER_ID": "EMITTER_ID_RESOLVED"})

    shares = shares.merge(secid_map, on="SECID", how="left")
    shares_existing = pd.to_numeric(shares["EMITTER_ID"], errors="coerce")
    shares_resolved = pd.to_numeric(shares["EMITTER_ID_RESOLVED"], errors="coerce")
    shares["EMITTER_ID"] = shares_existing.where(shares_existing.notna(), shares_resolved)
    shares = shares.drop(columns=["EMITTER_ID_RESOLVED"])

    bonds = bonds.merge(secid_map, on="SECID", how="left")
    bonds_existing = pd.to_numeric(bonds["EMITTER_ID"], errors="coerce")
    bonds_resolved = pd.to_numeric(bonds["EMITTER_ID_RESOLVED"], errors="coerce")
    bonds["EMITTER_ID"] = bonds_existing.where(bonds_existing.notna(), bonds_resolved)
    bonds = bonds.drop(columns=["EMITTER_ID_RESOLVED"])

    shares = shares.merge(emitters_df, on="EMITTER_ID", how="left")
    bonds = bonds.merge(emitters_df, on="EMITTER_ID", how="left")

    logger.info(
        "Emitter fill ratio: shares(name=%s inn=%s), bonds(name=%s inn=%s)",
        shares["EMITTER_NAME"].notna().mean(),
        shares["INN"].notna().mean(),
        bonds["EMITTER_NAME"].notna().mean(),
        bonds["INN"].notna().mean(),
    )
    return shares, bonds


def build_emitters_table(shares: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    shares_grouped = (
        shares.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_SHARES")
    )
    bonds_grouped = (
        bonds.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_BONDS")
    )

    emitters = shares_grouped.merge(bonds_grouped, on="EMITTER_ID", how="outer")
    details = pd.concat([shares[["EMITTER_ID", "EMITTER_NAME", "INN"]], bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]]], ignore_index=True)
    details = details.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    emitters = emitters.merge(details, on="EMITTER_ID", how="left")
    return emitters[["EMITTER_NAME", "INN", "TRADED_SHARES", "TRADED_BONDS", "EMITTER_ID"]].sort_values(
        by=["EMITTER_NAME", "EMITTER_ID"], na_position="last"
    )


def _normalize_score_value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_date_score_value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d")
    text = str(value).strip()
    return text or None


def _load_manual_scores(logger: logging.Logger) -> pd.DataFrame:
    if not EMITTERS_FILE.exists():
        return pd.DataFrame(columns=["EMITTER_ID", "ScoreList", "DateScore"])

    try:
        existing = pd.read_excel(EMITTERS_FILE, sheet_name="Data")
    except Exception as error:
        logger.exception("Failed to load manual scores from %s: %s", EMITTERS_FILE, error)
        return pd.DataFrame(columns=["EMITTER_ID", "ScoreList", "DateScore"])

    if "EMITTER_ID" not in existing.columns:
        logger.warning("Manual scores source without EMITTER_ID column: %s", EMITTERS_FILE)
        return pd.DataFrame(columns=["EMITTER_ID", "ScoreList", "DateScore"])

    for column in ["ScoreList", "DateScore"]:
        if column not in existing.columns:
            existing[column] = pd.NA

    result = existing[["EMITTER_ID", "ScoreList", "DateScore"]].copy()
    result["EMITTER_ID"] = pd.to_numeric(result["EMITTER_ID"], errors="coerce")
    result = result.dropna(subset=["EMITTER_ID"])
    result["EMITTER_ID"] = result["EMITTER_ID"].astype("int64")
    result = result.drop_duplicates(subset=["EMITTER_ID"], keep="first")
    result["ScoreList"] = result["ScoreList"].map(_normalize_score_value)
    result["DateScore"] = result["DateScore"].map(_normalize_date_score_value)
    return result


def apply_manual_score_columns(emitters: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    result = emitters.copy()
    manual_scores = _load_manual_scores(logger)

    if "EMITTER_ID" not in result.columns:
        result["EMITTER_ID"] = pd.NA

    result["EMITTER_ID"] = pd.to_numeric(result["EMITTER_ID"], errors="coerce")
    result = result.dropna(subset=["EMITTER_ID"])
    result["EMITTER_ID"] = result["EMITTER_ID"].astype("int64")

    result = result.merge(manual_scores, on="EMITTER_ID", how="left")
    result["ScoreList"] = result["ScoreList"].map(_normalize_score_value)
    result["DateScore"] = result["DateScore"].map(_normalize_date_score_value)

    invalid_mask = result["ScoreList"].notna() & ~result["ScoreList"].isin(ALLOWED_SCORE_VALUES)
    if invalid_mask.any():
        invalid_values = sorted(set(result.loc[invalid_mask, "ScoreList"].astype(str).tolist()))
        raise ValueError(
            "Недопустимые значения ScoreList: "
            f"{', '.join(invalid_values)}. Допустимо только: {', '.join(sorted(ALLOWED_SCORE_VALUES))}"
        )

    today = date.today().isoformat()
    add_date_mask = result["ScoreList"].notna() & result["DateScore"].isna()
    result.loc[add_date_mask, "DateScore"] = today

    result["ScoreList"] = result["ScoreList"].where(result["ScoreList"].notna(), pd.NA)
    result["DateScore"] = result["DateScore"].where(result["DateScore"].notna(), pd.NA)
    return result


def apply_expert_ra_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = normalize_inn(inn)
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["Рейтинг Эксперт РА"] = result["INN"].map(rating_for_row)
    return result


def apply_acra_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = normalize_inn(inn)
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["Рейтинг Акра"] = result["INN"].map(rating_for_row)
    return result


def apply_nkr_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = normalize_inn(inn)
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["НКР Рейтинг"] = result["INN"].map(rating_for_row)
    return result


def apply_nra_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = normalize_inn(inn)
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["НРА рейтинг"] = result["INN"].map(rating_for_row)
    return result


def load_ratings_from_emitters_excel(column_name: str, logger: logging.Logger) -> dict[str, str]:
    if not EMITTERS_FILE.exists():
        return {}
    try:
        frame = pd.read_excel(EMITTERS_FILE, sheet_name="Data")
    except Exception as error:
        logger.warning("Failed to load historical ratings from emitters Excel: %s", error)
        return {}

    if "INN" not in frame.columns or column_name not in frame.columns:
        return {}

    ratings: dict[str, str] = {}
    for row in frame[["INN", column_name]].to_dict(orient="records"):
        inn = normalize_inn(row.get("INN"))
        value = row.get(column_name)
        if inn and value is not None and not pd.isna(value):
            text_value = str(value).strip()
            if text_value:
                ratings[inn] = text_value
    return ratings


def collect_green_bonds(
    client: MoexClient,
    emitters: pd.DataFrame,
    bonds: pd.DataFrame,
    logger: logging.Logger,
    sqlite_conn: sqlite3.Connection | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if emitters.empty or bonds.empty or "ScoreList" not in emitters.columns:
        return pd.DataFrame(), pd.DataFrame()

    green_emitters = emitters[emitters["ScoreList"].fillna("").str.lower() == "green"].copy()
    if green_emitters.empty:
        return pd.DataFrame(), pd.DataFrame()

    green_emitter_ids = set(pd.to_numeric(green_emitters["EMITTER_ID"], errors="coerce").dropna().astype("int64").tolist())
    scoped_bonds = bonds[pd.to_numeric(bonds["EMITTER_ID"], errors="coerce").fillna(-1).astype("int64").isin(green_emitter_ids)].copy()

    records: list[dict[str, Any]] = []
    payment_records: list[dict[str, Any]] = []

    scoped_records = scoped_bonds.to_dict(orient="records")

    def fetch_bond_payload(row: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        secid = row.get("SECID")
        description = client.fetch_security_description(str(secid))
        market = client.fetch_security_market_snapshot(str(secid))
        coupons, amortizations = client.fetch_bondization(str(secid))

        amort_startdate = None
        if not amortizations.empty and "amortdate" in amortizations.columns:
            amortizations["amortdate"] = pd.to_datetime(amortizations["amortdate"], errors="coerce")
            value_col = "valueprc" if "valueprc" in amortizations.columns else ("value" if "value" in amortizations.columns else None)
            if value_col:
                non_zero = amortizations[pd.to_numeric(amortizations[value_col], errors="coerce").fillna(0) > 0]
                if not non_zero.empty:
                    amort_startdate = non_zero["amortdate"].min()

        coupon_records: list[dict[str, Any]] = []
        if not coupons.empty:
            coupons = coupons.copy()
            coupons["SECID"] = secid
            coupon_records = coupons.where(pd.notna(coupons), None).to_dict(orient="records")

        record = {
            "SECID": secid,
            "ISIN": row.get("ISIN"),
            "SHORTNAME": row.get("SHORTNAME"),
            "EMITTER_ID": row.get("EMITTER_ID"),
            "EMITTER_NAME": row.get("EMITTER_NAME"),
            "INN": row.get("INN"),
            "MATDATE": row.get("MATDATE"),
            "offerdate": description.get("OFFERDATE") or description.get("BUYBACKDATE"),
            "maturity_date": description.get("MATDATE") or description.get("REPAYDATE"),
            "is_qualified_investors": description.get("ISQUALIFIEDINVESTORS") or description.get("QUALIFIEDINVESTOR"),
            "coupon_percent": description.get("COUPONPERCENT") or market.get("COUPONPERCENT"),
            "coupon_value": description.get("COUPONVALUE") or market.get("COUPONVALUE"),
            "yield": market.get("YIELD") or market.get("YIELDATWAP"),
            "accrued_coupon_income": market.get("ACCRUEDINT") or market.get("ACCRUEDINTVALUE"),
            "last_price": market.get("LAST") or market.get("LASTVALUE"),
            "close_price": market.get("CLOSE") or market.get("LCLOSE") or market.get("LEGALCLOSEPRICE"),
            "market_price": market.get("MARKETPRICE"),
            "amort_startdate": amort_startdate.date().isoformat() if pd.notna(amort_startdate) else None,
            "coupons_json": coupon_records,
            "amortizations_json": amortizations.where(pd.notna(amortizations), None).to_dict(orient="records"),
            "description_json": description,
            "market_json": market,
        }
        return record, coupon_records

    with progress(total=len(scoped_records), desc="Green bonds", unit="бумага") as pbar:
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(len(scoped_records), 1))) as executor:
            futures = {executor.submit(fetch_bond_payload, row): row.get("SECID") for row in scoped_records}
            for future in as_completed(futures):
                secid = futures[future]
                try:
                    record, coupon_records = future.result()
                    records.append(record)
                    payment_records.extend(coupon_records)

                    if sqlite_conn is not None:
                        upsert_records_sqlite(sqlite_conn, "green_bonds", [record], ["SECID"], logger)
                        if coupon_records:
                            coupons_frame = pd.DataFrame(coupon_records)
                            if "coupondate" not in coupons_frame.columns:
                                coupons_frame["coupondate"] = pd.NA
                            upsert_dataframe_sqlite(sqlite_conn, "green_bond_payments", coupons_frame, ["SECID", "coupondate"], logger)
                except Exception as error:
                    logger.exception("Green bond details failed secid=%s: %s", secid, error)
                pbar.update(1)

    return pd.DataFrame(records), pd.DataFrame(payment_records)


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple, set, dict)):
        return False
    if hasattr(value, "shape") and not isinstance(value, (str, bytes, pd.Series, pd.DataFrame)):
        return False
    try:
        missing = pd.isna(value)
    except Exception:
        return False
    return bool(missing) if isinstance(missing, (bool, int)) else False


def _fit_column_widths(worksheet: Any, df: pd.DataFrame) -> None:
    sample_limit = 2000
    for col_idx, column_name in enumerate(df.columns, start=1):
        values = df[column_name]
        if values.empty:
            max_len = len(str(column_name))
        else:
            sampled_values = values.head(sample_limit)
            series_len = sampled_values.map(lambda value: 0 if _is_missing_scalar(value) else len(str(value)))
            max_len = max(len(str(column_name)), int(series_len.max()))

        adjusted_width = min(max_len + 2, 80)
        worksheet.column_dimensions[get_column_letter(col_idx)].width = max(10, adjusted_width)


def _extract_coupon_summary(coupons: Any) -> dict[str, Any]:
    if not isinstance(coupons, list) or not coupons:
        return {
            "coupons_count": 0,
            "next_coupon_date": None,
            "next_coupon_value": None,
            "next_coupon_period_days": None,
            "last_coupon_date": None,
        }

    rows = pd.DataFrame(coupons)
    if rows.empty:
        return {
            "coupons_count": 0,
            "next_coupon_date": None,
            "next_coupon_value": None,
            "next_coupon_period_days": None,
            "last_coupon_date": None,
        }

    rows["coupondate_ts"] = pd.to_datetime(rows.get("coupondate"), errors="coerce")
    now = pd.Timestamp.now().normalize()
    future_rows = rows[rows["coupondate_ts"] >= now].sort_values("coupondate_ts")
    latest_rows = rows.sort_values("coupondate_ts")
    next_row = future_rows.iloc[0] if not future_rows.empty else None
    last_row = latest_rows.iloc[-1] if not latest_rows.empty else None
    return {
        "coupons_count": int(len(rows)),
        "next_coupon_date": next_row["coupondate_ts"].date().isoformat() if next_row is not None and pd.notna(next_row["coupondate_ts"]) else None,
        "next_coupon_value": next_row.get("value") if next_row is not None else None,
        "next_coupon_period_days": next_row.get("couponperiod") if next_row is not None else None,
        "last_coupon_date": last_row["coupondate_ts"].date().isoformat() if last_row is not None and pd.notna(last_row["coupondate_ts"]) else None,
    }


def _extract_amortization_summary(amortizations: Any) -> dict[str, Any]:
    if not isinstance(amortizations, list) or not amortizations:
        return {"amortizations_count": 0, "next_amort_date": None, "next_amort_value": None}

    rows = pd.DataFrame(amortizations)
    if rows.empty:
        return {"amortizations_count": 0, "next_amort_date": None, "next_amort_value": None}

    rows["amortdate_ts"] = pd.to_datetime(rows.get("amortdate"), errors="coerce")
    if "data_source" in rows.columns:
        data_source = rows["data_source"].astype(str).str.lower()
        rows = rows[~data_source.isin(["maturity", "issue"])]

    if "valueprc" in rows.columns:
        rows = rows[pd.to_numeric(rows["valueprc"], errors="coerce").fillna(0) < 100]

    if rows.empty:
        return {"amortizations_count": 0, "next_amort_date": None, "next_amort_value": None}

    now = pd.Timestamp.now().normalize()
    future_rows = rows[rows["amortdate_ts"] >= now].sort_values("amortdate_ts")
    next_row = future_rows.iloc[0] if not future_rows.empty else rows.sort_values("amortdate_ts").iloc[-1]
    return {
        "amortizations_count": int(len(rows)),
        "next_amort_date": next_row["amortdate_ts"].date().isoformat() if pd.notna(next_row["amortdate_ts"]) else None,
        "next_amort_value": next_row.get("value") if next_row is not None else None,
    }


def prepare_green_bonds_for_export(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    result = frame.copy()
    coupon_summary = result["coupons_json"].map(_extract_coupon_summary) if "coupons_json" in result.columns else pd.Series([{} for _ in range(len(result))])
    amort_summary = result["amortizations_json"].map(_extract_amortization_summary) if "amortizations_json" in result.columns else pd.Series([{} for _ in range(len(result))])

    result = pd.concat([result, pd.json_normalize(coupon_summary), pd.json_normalize(amort_summary)], axis=1)

    if "description_json" in result.columns:
        result["bond_type"] = result["description_json"].map(lambda payload: payload.get("BOND_TYPE") if isinstance(payload, dict) else None)
        result["bond_subtype"] = result["description_json"].map(lambda payload: payload.get("BOND_SUBTYPE") if isinstance(payload, dict) else None)
    if "market_json" in result.columns:
        result["market_board"] = result["market_json"].map(lambda payload: payload.get("BOARDID") if isinstance(payload, dict) else None)
        result["bid"] = result["market_json"].map(lambda payload: payload.get("BID") if isinstance(payload, dict) else None)
        result["offer"] = result["market_json"].map(lambda payload: payload.get("OFFER") if isinstance(payload, dict) else None)

    for column in ["coupons_json", "amortizations_json", "description_json", "market_json"]:
        if column in result.columns:
            result = result.drop(columns=column)

    return result


def save_to_excel(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        worksheet = writer.sheets["Data"]

        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for cell in worksheet[1]:
            cell.alignment = CENTERED_WRAP_ALIGNMENT
            cell.border = THIN_BORDER
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT

        if worksheet.max_row >= 2:
            max_col_letter = get_column_letter(worksheet.max_column)
            zebra_range = f"A2:{max_col_letter}{worksheet.max_row}"
            zebra_rule = FormulaRule(formula=["MOD(ROW(),2)=0"], fill=ZEBRA_FILL)
            worksheet.conditional_formatting.add(zebra_range, zebra_rule)

        if path == EMITTERS_FILE and "ScoreList" in df.columns and worksheet.max_row >= 2:
            score_col_idx = df.columns.get_loc("ScoreList") + 1
            score_col_letter = get_column_letter(score_col_idx)
            validation = DataValidation(
                type="list",
                formula1='"Green,Yellow,Red"',
                allow_blank=True,
                showErrorMessage=True,
                errorTitle="Недопустимое значение",
                error=f"Допустимо только: {', '.join(sorted(ALLOWED_SCORE_VALUES))}",
            )
            worksheet.add_data_validation(validation)
            validation.add(f"{score_col_letter}2:{score_col_letter}{worksheet.max_row}")

        _fit_column_widths(worksheet, df)

    logger.info("Saved %s rows=%s", path, len(df))


def run() -> None:
    logger = setup_logging()
    logger.info("Script started")

    interrupted = {"value": False}
    stage_times: dict[str, float] = {}
    skipped_sources: list[str] = []
    restored_sources: list[str] = []
    script_started_at = perf_counter()

    def handle_sigint(signum: int, frame: Any) -> None:
        _ = (signum, frame)
        interrupted["value"] = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)

    print("=====\nЭтап 0: Сбор и проверка прокси")
    stage_started_at = perf_counter()
    proxy_candidates = fetch_proxy_candidates(logger)
    valid_proxies = validate_proxies(proxy_candidates, logger)
    save_valid_proxies_csv(valid_proxies, logger)
    stage_times["Этап 0: Сбор и проверка прокси"] = perf_counter() - stage_started_at

    client = MoexClient(logger, valid_proxies)
    expert_ra_client = ExpertRaClient(logger, valid_proxies)
    acra_client = AcraClient(logger, valid_proxies)
    nkr_client = NkrClient(logger, valid_proxies)
    nra_client = NraClient(logger, valid_proxies)
    cache = load_cache(logger)

    shares = pd.DataFrame()
    bonds = pd.DataFrame()
    emitters = pd.DataFrame()
    green_bonds = pd.DataFrame()
    green_payments = pd.DataFrame()
    green_bonds_future = None
    green_executor: ThreadPoolExecutor | None = None
    sqlite_conn = open_sqlite_connection()

    try:
        def fetch_shares_online() -> pd.DataFrame:
            result = client.fetch_market_securities("shares", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "LISTLEVEL", "STATUS", "EMITTER_ID"])
            result = result[(result["BOARDID"] == "TQBR") & (result["STATUS"].fillna("") != "N")].copy()
            save_dataframe_snapshot(SHARES_CACHE_FILE, result, logger)
            return result

        def fetch_bonds_online() -> pd.DataFrame:
            result = client.fetch_market_securities("bonds", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL", "STATUS", "EMITTER_ID"])
            result = result[result["BOARDID"].isin(["TQCB", "TQOB", "TQOD", "TQIR", "TQOE"])].copy()
            result = result[result["STATUS"].fillna("") != "N"].copy()
            result["MATDATE"] = pd.to_datetime(result["MATDATE"], errors="coerce").dt.date
            result = result[(result["MATDATE"].isna()) | (result["MATDATE"] >= date.today())].copy()
            save_dataframe_snapshot(BONDS_CACHE_FILE, result, logger)
            return result

        bonds_future = None
        moex_prefetch_executor = ThreadPoolExecutor(max_workers=1)
        bonds_future = moex_prefetch_executor.submit(fetch_bonds_online)

        print("=====\nЭтап 1: Сбор акций")
        stage_started_at = perf_counter()
        try:
            shares = fetch_shares_online()
        except requests.RequestException as error:
            logger.warning("Shares stage failed, trying cache: %s", error)
            skipped_sources.append("MOEX (акции)")
            shares = load_dataframe_snapshot(SHARES_CACHE_FILE, logger)
            if not shares.empty:
                restored_sources.append("MOEX (акции)")
        stage_times["Этап 1: Сбор акций"] = perf_counter() - stage_started_at

        print("Этап 2: Сбор облигаций")
        stage_started_at = perf_counter()
        try:
            bonds = bonds_future.result() if bonds_future is not None else fetch_bonds_online()
        except requests.RequestException as error:
            logger.warning("Bonds stage failed, trying cache: %s", error)
            skipped_sources.append("MOEX (облигации)")
            bonds = load_dataframe_snapshot(BONDS_CACHE_FILE, logger)
            if not bonds.empty:
                restored_sources.append("MOEX (облигации)")
        stage_times["Этап 2: Сбор облигаций"] = perf_counter() - stage_started_at
        moex_prefetch_executor.shutdown(wait=False)

        print("Этап 3: Обогащение эмитентов")
        stage_started_at = perf_counter()
        if not shares.empty and not bonds.empty:
            shares, bonds = enrich_emitters(client, shares, bonds, logger, cache)
            save_dataframe_snapshot(SHARES_CACHE_FILE, shares, logger)
            save_dataframe_snapshot(BONDS_CACHE_FILE, bonds, logger)
        else:
            skipped_sources.append("MOEX (обогащение эмитентов)")
        stage_times["Этап 3: Обогащение эмитентов"] = perf_counter() - stage_started_at

        print("Этап 4: Получение рейтингов Эксперт РА")
        stage_started_at = perf_counter()
        emitters = build_emitters_table(shares, bonds) if not shares.empty or not bonds.empty else load_dataframe_snapshot(EMITTERS_CACHE_FILE, logger)
        emitters = apply_manual_score_columns(emitters, logger)
        inns = {normalize_inn(value) for value in emitters["INN"].tolist()} if not emitters.empty and "INN" in emitters.columns else set()
        inns = {inn for inn in inns if inn}
        logger.info("Emitters scope: rows=%s unique_inn=%s", len(emitters), len(inns))

        green_executor = ThreadPoolExecutor(max_workers=1)
        green_bonds_future = green_executor.submit(collect_green_bonds, client, emitters, bonds, logger, None)

        expert_ra_cached, expert_ra_cached_today = load_daily_ratings_cache(EXPERT_RA_CACHE_FILE, logger)
        acra_cached, acra_cached_today = load_daily_ratings_cache(ACRA_CACHE_FILE, logger)
        nkr_cached, nkr_cached_today = load_daily_ratings_cache(NKR_CACHE_FILE, logger)
        nra_cached, nra_cached_today = load_daily_ratings_cache(NRA_CACHE_FILE, logger)

        expert_ra_ratings: dict[str, str] = {}
        acra_history_prefetch = load_ratings_from_emitters_excel("Рейтинг Акра", logger)
        rating_executor = ThreadPoolExecutor(max_workers=3)
        acra_prefetched_exclude = set(acra_cached.keys()) | set(acra_history_prefetch.keys())
        acra_future = rating_executor.submit(acra_client.fetch_latest_ratings_by_inn, inns - acra_prefetched_exclude) if not acra_cached_today else None
        nkr_future = rating_executor.submit(nkr_client.fetch_latest_ratings_by_inn, inns) if not nkr_cached_today else None
        nra_future = rating_executor.submit(nra_client.fetch_latest_ratings_by_inn, inns) if not nra_cached_today else None
        if expert_ra_cached_today:
            expert_ra_ratings = expert_ra_cached
            restored_sources.append("Эксперт РА (дневной кэш)")
        else:
            try:
                expert_ra_ratings = expert_ra_client.fetch_latest_ratings_by_inn(inns)
                save_daily_ratings_cache(EXPERT_RA_CACHE_FILE, expert_ra_ratings, logger)
            except requests.RequestException as error:
                logger.warning("Expert RA stage failed, trying cache: %s", error)
                skipped_sources.append("Эксперт РА")
                expert_ra_ratings = expert_ra_cached
                if expert_ra_ratings:
                    restored_sources.append("Эксперт РА")

        if not emitters.empty:
            emitters = apply_expert_ra_ratings(emitters, expert_ra_ratings)
        logger.info("Expert RA coverage: %s/%s", len(expert_ra_ratings), len(inns))
        stage_times["Этап 4: Получение рейтингов Эксперт РА"] = perf_counter() - stage_started_at

        print("Этап 5: Получение рейтингов АКРА")
        stage_started_at = perf_counter()
        acra_ratings: dict[str, str] = {}
        acra_history = acra_history_prefetch
        acra_cached_scope = {inn: rating for inn, rating in acra_cached.items() if inn in inns}
        acra_history_scope = {inn: rating for inn, rating in acra_history.items() if inn in inns and inn not in acra_cached_scope}
        acra_cached_scope = {**acra_history_scope, **acra_cached_scope}
        pending_acra_inns = inns - set(acra_cached_scope.keys())

        if acra_cached_today or not pending_acra_inns:
            acra_ratings = acra_cached_scope
            restored_sources.append("АКРА (кэш)")
        else:
            try:
                fetched_acra = acra_future.result() if acra_future is not None else acra_client.fetch_latest_ratings_by_inn(pending_acra_inns)
                acra_ratings = {**acra_cached_scope, **fetched_acra}
                save_daily_ratings_cache(ACRA_CACHE_FILE, acra_ratings, logger)
            except requests.RequestException as error:
                logger.warning("ACRA stage failed, trying cache: %s", error)
                skipped_sources.append("АКРА")
                acra_ratings = acra_cached_scope
                if acra_ratings:
                    restored_sources.append("АКРА")
        if not emitters.empty:
            emitters = apply_acra_ratings(emitters, acra_ratings)
            save_dataframe_snapshot(EMITTERS_CACHE_FILE, emitters, logger)
        logger.info("ACRA coverage: %s/%s", len(acra_ratings), len(inns))
        stage_times["Этап 5: Получение рейтингов АКРА"] = perf_counter() - stage_started_at

        print("Этап 6: Получение рейтингов НКР")
        stage_started_at = perf_counter()
        nkr_ratings: dict[str, str] = {}
        if nkr_cached_today:
            nkr_ratings = nkr_cached
            restored_sources.append("НКР (дневной кэш)")
        else:
            try:
                nkr_ratings = nkr_future.result() if nkr_future is not None else nkr_client.fetch_latest_ratings_by_inn(inns)
                save_daily_ratings_cache(NKR_CACHE_FILE, nkr_ratings, logger)
            except requests.RequestException as error:
                logger.warning("NKR stage failed, trying cache: %s", error)
                skipped_sources.append("НКР")
                nkr_ratings = nkr_cached
                if nkr_ratings:
                    restored_sources.append("НКР")
        rating_executor.shutdown(wait=False)

        if not emitters.empty:
            emitters = apply_nkr_ratings(emitters, nkr_ratings)
            save_dataframe_snapshot(EMITTERS_CACHE_FILE, emitters, logger)
        logger.info("NKR coverage: %s/%s", len(nkr_ratings), len(inns))
        stage_times["Этап 6: Получение рейтингов НКР"] = perf_counter() - stage_started_at

        print("Этап 7: Получение рейтингов НРА")
        stage_started_at = perf_counter()
        nra_ratings: dict[str, str] = {}
        if nra_cached_today:
            nra_ratings = nra_cached
            restored_sources.append("НРА (дневной кэш)")
        else:
            try:
                nra_ratings = nra_future.result() if nra_future is not None else nra_client.fetch_latest_ratings_by_inn(inns)
                save_daily_ratings_cache(NRA_CACHE_FILE, nra_ratings, logger)
            except requests.RequestException as error:
                logger.warning("NRA stage failed, trying cache: %s", error)
                skipped_sources.append("НРА")
                nra_ratings = nra_cached
                if nra_ratings:
                    restored_sources.append("НРА")

        if not emitters.empty:
            emitters = apply_nra_ratings(emitters, nra_ratings)
            save_dataframe_snapshot(EMITTERS_CACHE_FILE, emitters, logger)
        logger.info("NRA coverage: %s/%s", len(nra_ratings), len(inns))
        stage_times["Этап 7: Получение рейтингов НРА"] = perf_counter() - stage_started_at

        print("Этап 8: Обновление SQLite (инкремент)")
        stage_started_at = perf_counter()
        sqlite_targets = [
            ("moex_shares", shares, ["SECID"]),
            ("moex_bonds", bonds, ["SECID"]),
            ("moex_emitters", emitters, ["EMITTER_ID"]),
        ]

        def write_sqlite_target(item: tuple[str, pd.DataFrame, list[str]]) -> None:
            table_name, frame, key_cols = item
            conn = open_sqlite_connection()
            try:
                upsert_dataframe_sqlite(conn, table_name, frame, key_cols, logger)
            finally:
                conn.close()

        with progress(total=len(sqlite_targets), desc="SQLite upsert", unit="таблица") as pbar:
            with ThreadPoolExecutor(max_workers=len(sqlite_targets)) as executor:
                futures = [executor.submit(write_sqlite_target, item) for item in sqlite_targets]
                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)
        stage_times["Этап 8: Обновление SQLite (инкремент)"] = perf_counter() - stage_started_at

        print("Этап 9: Green bonds (ScoreList=Green)")
        stage_started_at = perf_counter()
        if green_bonds_future is not None:
            green_bonds, green_payments = green_bonds_future.result()
        else:
            green_bonds, green_payments = collect_green_bonds(client, emitters, bonds, logger, sqlite_conn=None)

        if not green_bonds.empty:
            upsert_dataframe_sqlite(sqlite_conn, "green_bonds", green_bonds, ["SECID"], logger)
        if not green_payments.empty:
            payments_frame = green_payments.copy()
            if "coupondate" not in payments_frame.columns:
                payments_frame["coupondate"] = pd.NA
            upsert_dataframe_sqlite(sqlite_conn, "green_bond_payments", payments_frame, ["SECID", "coupondate"], logger)

        stage_times["Этап 9: Green bonds (ScoreList=Green)"] = perf_counter() - stage_started_at

        print("Этап 10: Формирование Excel")
        stage_started_at = perf_counter()
        green_bonds_export = prepare_green_bonds_for_export(green_bonds)
        excel_exports = [(emitters, EMITTERS_FILE), (green_bonds_export, GREEN_BONDS_FILE)]

        def save_excel_item(item: tuple[pd.DataFrame, Path]) -> None:
            frame, output_path = item
            save_to_excel(frame, output_path, logger)

        with progress(total=len(excel_exports), desc="Экспорт Excel", unit="файл") as pbar:
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(excel_exports))) as executor:
                futures = [executor.submit(save_excel_item, item) for item in excel_exports]
                for future in as_completed(futures):
                    future.result()
                    pbar.update(1)
        stage_times["Этап 10: Формирование Excel"] = perf_counter() - stage_started_at

        print("=====\nГотово")
        logger.info("Script completed successfully")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by Ctrl+C")
        print("\nОстановлено пользователем (Ctrl+C)")
    except Exception as error:
        logger.exception("Script failed: %s", error)
        raise
    finally:
        if green_executor is not None:
            green_executor.shutdown(wait=False)
        save_cache(cache, logger)
        sqlite_conn.close()
        logger.info("Script finished. interrupted=%s", interrupted["value"])

        total_time = perf_counter() - script_started_at
        print("=====\nSummary")
        for stage_name, duration in stage_times.items():
            print(f"{stage_name}: {duration:.2f} сек")

        print("Пропущенные источники:")
        if skipped_sources:
            for source in sorted(set(skipped_sources)):
                print(f"- {source}")
        else:
            print("- Нет")

        print("Источники восстановленные из кэша:")
        if restored_sources:
            for source in sorted(set(restored_sources)):
                print(f"- {source}")
        else:
            print("- Нет")

        print(f"Всего: {total_time:.2f} сек")
        print("=====")


if __name__ == "__main__":
    run()
