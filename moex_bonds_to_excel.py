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
SECID_BATCH_SIZE = 50
EMITTER_BATCH_SIZE = 80

ROOT_DIR = Path(__file__).resolve().parent
LOGS_DIR = ROOT_DIR / "logs"
RAW_DIR = ROOT_DIR / "raw"
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"

LOG_FILE = LOGS_DIR / "moex_bonds.log"
RAW_FILE = RAW_DIR / "moex_bonds_raw.json"
CACHE_FILE = CACHE_DIR / "moex_bonds_cache.json"
ISSUER_CACHE_FILE = CACHE_DIR / "issuer_directory_cache.json"
ISSUER_CHECKPOINT_FILE = CACHE_DIR / "issuer_enrichment_checkpoint.json"
DAILY_CACHE_FILE = CACHE_DIR / "daily_security_metrics_cache.json"
OUTPUT_FILE = OUTPUT_DIR / "moex_bonds.xlsx"

# Колонки, которые пользователь попросил убрать из итогового файла
REMOVED_COLUMNS = {"SECNAME", "LISTLEVEL", "STATUS"}
# SECID нужен для технической работы, но в Excel должен быть скрыт
HIDDEN_COLUMN_NAME = "SECID"
ISSUER_COLUMN_NAME = "ISSUER_NAME"
ISSUER_INN_COLUMN_NAME = "ISSUER_INN"
FIRST_COLUMN_NAME = "ISIN"
QUALIFIED_INVESTOR_COLUMN_NAME = "QUALIFIED_INVESTOR"
MATURITY_DATE_COLUMN_NAME = "MATDATE"
AMORTIZATION_FLAG_COLUMN_NAME = "HAS_AMORTIZATION"
AMORTIZATION_START_DATE_COLUMN_NAME = "AMORTIZATION_START_DATE"
ACCRUED_INT_COLUMN_NAME = "ACCRUEDINT"
SECURITY_DAILY_CACHE_TTL_HOURS = 24
MIN_MATURITY_YEARS = 1


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


def load_issuer_directory_cache() -> tuple[dict[str, int | None], dict[int, str], dict[int, str], dict[str, str]]:
    """Читает пожизненный кэш соответствий SECID -> EMITTER_ID/QUALIFIED_INVESTOR, EMITTER_ID -> имя и ИНН."""
    if not ISSUER_CACHE_FILE.exists():
        return {}, {}, {}, {}

    try:
        payload = json.loads(ISSUER_CACHE_FILE.read_text(encoding="utf-8"))
        secid_to_emitter_id: dict[str, int | None] = {}
        emitter_id_to_name: dict[int, str] = {}
        emitter_id_to_inn: dict[int, str] = {}
        secid_to_qualified_sign: dict[str, str] = {}

        for secid, emitter_id in payload.get("secid_to_emitter_id", {}).items():
            if emitter_id is None:
                secid_to_emitter_id[str(secid)] = None
            else:
                try:
                    secid_to_emitter_id[str(secid)] = int(emitter_id)
                except (TypeError, ValueError):
                    secid_to_emitter_id[str(secid)] = None

        for emitter_id, emitter_name in payload.get("emitter_id_to_name", {}).items():
            if not emitter_name:
                continue
            try:
                emitter_id_to_name[int(emitter_id)] = str(emitter_name)
            except (TypeError, ValueError):
                continue

        for emitter_id, emitter_inn in payload.get("emitter_id_to_inn", {}).items():
            if not emitter_inn:
                continue
            try:
                emitter_id_to_inn[int(emitter_id)] = str(emitter_inn)
            except (TypeError, ValueError):
                continue

        for secid, qualified_sign in payload.get("secid_to_qualified_sign", {}).items():
            if str(qualified_sign) in {"✔", "✖"}:
                secid_to_qualified_sign[str(secid)] = str(qualified_sign)

        logging.info(
            "Загружен пожизненный справочник эмитентов: SECID=%s, EMITTER_ID=%s.",
            len(secid_to_emitter_id),
            len(emitter_id_to_name),
        )
        return secid_to_emitter_id, emitter_id_to_name, emitter_id_to_inn, secid_to_qualified_sign
    except Exception as exc:
        logging.warning("Не удалось прочитать пожизненный кэш эмитентов: %s", exc)
        return {}, {}, {}, {}


def save_issuer_directory_cache(
    secid_to_emitter_id: dict[str, int | None],
    emitter_id_to_name: dict[int, str],
    emitter_id_to_inn: dict[int, str],
    secid_to_qualified_sign: dict[str, str],
) -> None:
    """Сохраняет пожизненный справочник эмитентов."""
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "secid_to_emitter_id": secid_to_emitter_id,
        "secid_to_qualified_sign": secid_to_qualified_sign,
        "emitter_id_to_name": {str(k): v for k, v in emitter_id_to_name.items()},
        "emitter_id_to_inn": {str(k): v for k, v in emitter_id_to_inn.items()},
    }
    ISSUER_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_issuer_checkpoint() -> tuple[dict[str, int | None], dict[int, str], dict[int, str], dict[str, str]]:
    """Возвращает checkpoint по этапу обогащения эмитентов, если он есть."""
    if not ISSUER_CHECKPOINT_FILE.exists():
        return {}, {}, {}, {}

    try:
        payload = json.loads(ISSUER_CHECKPOINT_FILE.read_text(encoding="utf-8"))
        secid_to_emitter_id: dict[str, int | None] = {}
        emitter_id_to_name: dict[int, str] = {}
        emitter_id_to_inn: dict[int, str] = {}
        secid_to_qualified_sign: dict[str, str] = {}

        for secid, emitter_id in payload.get("secid_to_emitter_id", {}).items():
            if emitter_id is None:
                secid_to_emitter_id[str(secid)] = None
            else:
                try:
                    secid_to_emitter_id[str(secid)] = int(emitter_id)
                except (TypeError, ValueError):
                    secid_to_emitter_id[str(secid)] = None

        for emitter_id, emitter_name in payload.get("emitter_id_to_name", {}).items():
            if not emitter_name:
                continue
            try:
                emitter_id_to_name[int(emitter_id)] = str(emitter_name)
            except (TypeError, ValueError):
                continue

        for emitter_id, emitter_inn in payload.get("emitter_id_to_inn", {}).items():
            if not emitter_inn:
                continue
            try:
                emitter_id_to_inn[int(emitter_id)] = str(emitter_inn)
            except (TypeError, ValueError):
                continue

        for secid, qualified_sign in payload.get("secid_to_qualified_sign", {}).items():
            if str(qualified_sign) in {"✔", "✖"}:
                secid_to_qualified_sign[str(secid)] = str(qualified_sign)

        logging.info(
            "Найден checkpoint обогащения: SECID=%s, EMITTER_ID=%s.",
            len(secid_to_emitter_id),
            len(emitter_id_to_name),
        )
        return secid_to_emitter_id, emitter_id_to_name, emitter_id_to_inn, secid_to_qualified_sign
    except Exception as exc:
        logging.warning("Не удалось прочитать checkpoint эмитентов: %s", exc)
        return {}, {}, {}, {}


def save_issuer_checkpoint(
    secid_to_emitter_id: dict[str, int | None],
    emitter_id_to_name: dict[int, str],
    emitter_id_to_inn: dict[int, str],
    secid_to_qualified_sign: dict[str, str],
) -> None:
    """Сохраняет checkpoint обогащения эмитентов после каждого пакета."""
    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "secid_to_emitter_id": secid_to_emitter_id,
        "secid_to_qualified_sign": secid_to_qualified_sign,
        "emitter_id_to_name": {str(k): v for k, v in emitter_id_to_name.items()},
        "emitter_id_to_inn": {str(k): v for k, v in emitter_id_to_inn.items()},
    }
    ISSUER_CHECKPOINT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_issuer_checkpoint() -> None:
    """Удаляет checkpoint после успешного завершения этапа обогащения."""
    if ISSUER_CHECKPOINT_FILE.exists():
        ISSUER_CHECKPOINT_FILE.unlink()


def load_daily_security_cache() -> dict[str, dict[str, Any]]:
    """Читает суточный кэш по полям, которые нужно обновлять раз в день."""
    if not DAILY_CACHE_FILE.exists():
        return {}

    try:
        payload = json.loads(DAILY_CACHE_FILE.read_text(encoding="utf-8"))
        return payload.get("secid_to_metrics", {})
    except Exception as exc:
        logging.warning("Не удалось прочитать суточный кэш бумаг: %s", exc)
        return {}


def save_daily_security_cache(secid_to_metrics: dict[str, dict[str, Any]]) -> None:
    """Сохраняет суточный кэш по полям бумаг (амортизация и НКД)."""
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "secid_to_metrics": secid_to_metrics,
    }
    DAILY_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_date_safe(value: Any) -> datetime | None:
    """Преобразует дату формата YYYY-MM-DD в datetime или возвращает None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def filter_rows_by_maturity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Оставляет только бумаги с погашением не раньше, чем через 1 год."""
    today = datetime.now().date()
    min_allowed_date = today + timedelta(days=365 * MIN_MATURITY_YEARS)
    filtered_rows: list[dict[str, Any]] = []
    skipped_count = 0

    for row in rows:
        maturity_date = parse_date_safe(row.get(MATURITY_DATE_COLUMN_NAME))
        if maturity_date is None:
            skipped_count += 1
            continue
        if maturity_date.date() < min_allowed_date:
            skipped_count += 1
            continue
        filtered_rows.append(row)

    logging.info(
        "Фильтр по сроку до погашения: исключено %s бумаг, оставлено %s.",
        skipped_count,
        len(filtered_rows),
    )
    return filtered_rows


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    """Разбивает список на пакеты фиксированного размера."""
    return [items[idx : idx + size] for idx in range(0, len(items), size)]



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
        "securities.columns": "SECID,SHORTNAME,ISIN,MATDATE,FACEVALUE,FACEUNIT,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,PRIMARYBOARDID,PREVLEGALCLOSEPRICE,PREVPRICE,ACCRUEDINT",
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
    logging.info("Этап 1/7: Запрос данных с MOEX ISS...")

    with build_session() as session:
        page = fetch_page(session, 0)
        unique = {(row.get("SECID"), row.get("ISIN")): row for row in page.rows}
        rows = list(unique.values())
        logging.info("Получено записей: %s", len(rows))
        return rows


def fetch_emitter_info_for_security(session: requests.Session, secid: str) -> tuple[int | None, str]:
    """Возвращает ID эмитента и признак квалифицированного инвестора по SECID."""
    params = {
        "iss.meta": "off",
        "iss.only": "description",
        "description.columns": "name,value",
    }
    response = session.get(f"https://iss.moex.com/iss/securities/{secid}.json", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    rows = data.get("description", {}).get("data", [])
    emitter_id: int | None = None
    qualified_investor_sign = "✖"

    for name, value in rows:
        if name == "EMITTER_ID" and value is not None:
            try:
                emitter_id = int(value)
            except (TypeError, ValueError):
                emitter_id = None
        if name == "ISQUALIFIEDINVESTORS":
            qualified_investor_sign = "✔" if str(value) == "1" else "✖"

    return emitter_id, qualified_investor_sign


def fetch_daily_security_metrics(session: requests.Session, secid: str, maturity_date: str | None) -> tuple[str, str | None]:
    """Получает суточные метрики бумаги: наличие амортизации и дату её начала.

    Важно: финальное погашение (строка с data_source=maturity на дату MATDATE) не считаем амортизацией.
    """
    params = {"iss.meta": "off"}
    response = session.get(f"https://iss.moex.com/iss/securities/{secid}/bondization.json", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    amort_data = data.get("amortizations", {}).get("data", [])
    amort_columns = data.get("amortizations", {}).get("columns", [])

    if not amort_data or not amort_columns:
        return "✖", None

    amort_records = [dict(zip(amort_columns, row)) for row in amort_data]

    real_amortization_dates: list[str] = []
    for record in amort_records:
        amort_date = str(record.get("amortdate") or "")
        if not amort_date:
            continue

        data_source = str(record.get("data_source") or "").lower()
        if data_source and data_source != "maturity":
            real_amortization_dates.append(amort_date)
            continue

        if maturity_date and amort_date < str(maturity_date):
            real_amortization_dates.append(amort_date)

    if not real_amortization_dates:
        return "✖", None

    return "✔", min(real_amortization_dates)


def fetch_emitter_details(session: requests.Session, emitter_id: int) -> tuple[str | None, str | None]:
    """Возвращает наименование и ИНН эмитента по его ID."""
    params = {"iss.meta": "off"}
    response = session.get(f"https://iss.moex.com/iss/emitters/{emitter_id}.json", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    emitter_rows = data.get("emitter", {}).get("data", [])
    if not emitter_rows:
        return None, None

    columns = data.get("emitter", {}).get("columns", [])
    record = dict(zip(columns, emitter_rows[0]))
    emitter_name = record.get("SHORT_TITLE") or record.get("TITLE")
    emitter_inn = record.get("INN")
    return emitter_name, str(emitter_inn) if emitter_inn else None


def validate_rows(rows: list[dict[str, Any]]) -> None:
    """Проверяет качество данных перед выгрузкой и пишет понятный отчёт в лог."""
    logging.info("Этап 3/7: Проверка качества данных...")

    empty_isin_count = sum(1 for row in rows if not row.get("ISIN"))
    duplicate_keys = len(rows) - len({(row.get("SECID"), row.get("ISIN")) for row in rows})
    invalid_coupon_count = sum(1 for row in rows if isinstance(row.get("COUPONPERCENT"), (int, float)) and row.get("COUPONPERCENT") < 0)

    logging.info(
        "Проверка качества завершена: пустых ISIN=%s, дубликатов ключа (SECID+ISIN)=%s, отрицательных COUPONPERCENT=%s.",
        empty_isin_count,
        duplicate_keys,
        invalid_coupon_count,
    )


def enrich_with_issuer_names(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Добавляет в каждую строку имя/ИНН эмитента и признак квалифицированного инвестора."""
    logging.info("Этап 2/7: Обогащение данных наименованиями эмитентов...")
    secids = sorted({str(row.get("SECID")) for row in rows if row.get("SECID")})

    if not secids:
        for row in rows:
            row[ISSUER_COLUMN_NAME] = ""
            row[ISSUER_INN_COLUMN_NAME] = ""
            row[QUALIFIED_INVESTOR_COLUMN_NAME] = "✖"
        return rows

    cache_secid_to_emitter_id, cache_emitter_id_to_name, cache_emitter_id_to_inn, cache_secid_to_qualified_sign = load_issuer_directory_cache()
    checkpoint_secid_to_emitter_id, checkpoint_emitter_id_to_name, checkpoint_emitter_id_to_inn, checkpoint_secid_to_qualified_sign = load_issuer_checkpoint()

    secid_to_emitter_id: dict[str, int | None] = {**cache_secid_to_emitter_id, **checkpoint_secid_to_emitter_id}
    emitter_cache: dict[int, str] = {**cache_emitter_id_to_name, **checkpoint_emitter_id_to_name}
    emitter_inn_cache: dict[int, str] = {**cache_emitter_id_to_inn, **checkpoint_emitter_id_to_inn}
    secid_to_qualified_sign: dict[str, str] = {**cache_secid_to_qualified_sign, **checkpoint_secid_to_qualified_sign}


    missing_secids = [secid for secid in secids if secid not in secid_to_emitter_id or secid not in secid_to_qualified_sign]
    secid_batches = chunked(missing_secids, SECID_BATCH_SIZE)

    def resolve_emitter_batch(batch: list[str]) -> tuple[dict[str, int | None], dict[str, str]]:
        resolved: dict[str, int | None] = {}
        resolved_qualified: dict[str, str] = {}
        with build_session() as local_session:
            for secid in batch:
                try:
                    emitter_id, qualified_sign = fetch_emitter_info_for_security(local_session, secid)
                    resolved[secid] = emitter_id
                    resolved_qualified[secid] = qualified_sign
                except Exception as exc:
                    logging.warning("Не удалось получить EMITTER_ID для %s: %s", secid, exc)
                    resolved[secid] = None
                    resolved_qualified[secid] = "✖"
        return resolved, resolved_qualified

    if secid_batches:
        logging.info("Пакетный режим: нужно обработать %s пакетов SECID.", len(secid_batches))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(resolve_emitter_batch, batch): idx for idx, batch in enumerate(secid_batches, start=1)}
        for processed, future in enumerate(as_completed(futures), start=1):
            secid_chunk, qualified_chunk = future.result()
            secid_to_emitter_id.update(secid_chunk)
            secid_to_qualified_sign.update(qualified_chunk)
            save_issuer_checkpoint(secid_to_emitter_id, emitter_cache, emitter_inn_cache, secid_to_qualified_sign)
            if processed % 5 == 0 or processed == len(secid_batches):
                logging.info("SECID пакеты: %s/%s.", processed, len(secid_batches))

    unique_emitter_ids = sorted({emitter_id for emitter_id in secid_to_emitter_id.values() if emitter_id is not None})
    missing_emitter_ids = [
        emitter_id
        for emitter_id in unique_emitter_ids
        if emitter_id not in emitter_cache or emitter_id not in emitter_inn_cache
    ]
    emitter_batches = chunked(missing_emitter_ids, EMITTER_BATCH_SIZE)

    def resolve_emitter_names_batch(batch: list[int]) -> tuple[dict[int, str], dict[int, str]]:
        resolved_names: dict[int, str] = {}
        resolved_inn: dict[int, str] = {}
        with build_session() as local_session:
            for emitter_id in batch:
                try:
                    emitter_name, emitter_inn = fetch_emitter_details(local_session, emitter_id)
                    if emitter_name:
                        resolved_names[emitter_id] = emitter_name
                    if emitter_inn:
                        resolved_inn[emitter_id] = emitter_inn
                except Exception as exc:
                    logging.warning("Не удалось получить наименование эмитента %s: %s", emitter_id, exc)
        return resolved_names, resolved_inn

    if emitter_batches:
        logging.info("Пакетный режим: нужно обработать %s пакетов эмитентов.", len(emitter_batches))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(resolve_emitter_names_batch, batch): idx for idx, batch in enumerate(emitter_batches, start=1)}
        for processed, future in enumerate(as_completed(futures), start=1):
            names_chunk, inn_chunk = future.result()
            emitter_cache.update(names_chunk)
            emitter_inn_cache.update(inn_chunk)
            save_issuer_checkpoint(secid_to_emitter_id, emitter_cache, emitter_inn_cache, secid_to_qualified_sign)
            if processed % 5 == 0 or processed == len(emitter_batches):
                logging.info("Пакеты эмитентов: %s/%s.", processed, len(emitter_batches))

    for row in rows:
        secid = str(row.get("SECID", ""))
        emitter_id = secid_to_emitter_id.get(secid)
        row[ISSUER_COLUMN_NAME] = emitter_cache.get(emitter_id) or ""
        row[ISSUER_INN_COLUMN_NAME] = emitter_inn_cache.get(emitter_id) or ""
        row[QUALIFIED_INVESTOR_COLUMN_NAME] = secid_to_qualified_sign.get(secid, "✖")

    save_issuer_directory_cache(secid_to_emitter_id, emitter_cache, emitter_inn_cache, secid_to_qualified_sign)
    clear_issuer_checkpoint()

    return rows


def enrich_with_daily_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Добавляет НКД, признак амортизации и дату начала амортизации с суточным кэшем."""
    logging.info("Этап 4/7: Обогащение суточными метриками (амортизация и НКД)...")
    secids = sorted({str(row.get("SECID")) for row in rows if row.get("SECID")})
    if not secids:
        return rows

    row_by_secid = {str(row.get("SECID")): row for row in rows if row.get("SECID")}

    cache = load_daily_security_cache()
    now = datetime.now()

    def needs_refresh(secid: str) -> bool:
        entry = cache.get(secid)
        if not entry:
            return True
        updated_at_raw = entry.get("updated_at")
        if not updated_at_raw:
            return True
        try:
            updated_at = datetime.fromisoformat(str(updated_at_raw))
        except ValueError:
            return True
        return now - updated_at > timedelta(hours=SECURITY_DAILY_CACHE_TTL_HOURS)

    missing_secids = [secid for secid in secids if needs_refresh(secid)]
    batches = chunked(missing_secids, SECID_BATCH_SIZE)

    def resolve_daily_batch(batch: list[str]) -> dict[str, dict[str, Any]]:
        resolved: dict[str, dict[str, Any]] = {}
        with build_session() as local_session:
            for secid in batch:
                try:
                    has_amortization, amort_start_date = fetch_daily_security_metrics(local_session, secid, str(row_by_secid.get(secid, {}).get(MATURITY_DATE_COLUMN_NAME) or ""))
                    resolved[secid] = {
                        AMORTIZATION_FLAG_COLUMN_NAME: has_amortization,
                        AMORTIZATION_START_DATE_COLUMN_NAME: amort_start_date or "",
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                except Exception as exc:
                    logging.warning("Не удалось получить суточные метрики для %s: %s", secid, exc)
        return resolved

    if batches:
        logging.info("Суточные метрики: нужно обработать %s пакетов SECID.", len(batches))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(resolve_daily_batch, batch) for batch in batches]
        for processed, future in enumerate(as_completed(futures), start=1):
            cache.update(future.result())
            if processed % 5 == 0 or processed == len(batches):
                logging.info("Суточные пакеты: %s/%s.", processed, len(batches))

    save_daily_security_cache(cache)

    for row in rows:
        secid = str(row.get("SECID", ""))
        entry = cache.get(secid, {})
        row[AMORTIZATION_FLAG_COLUMN_NAME] = entry.get(AMORTIZATION_FLAG_COLUMN_NAME, "✖")
        row[AMORTIZATION_START_DATE_COLUMN_NAME] = entry.get(AMORTIZATION_START_DATE_COLUMN_NAME, "")
        row[ACCRUED_INT_COLUMN_NAME] = row.get(ACCRUED_INT_COLUMN_NAME)

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
    logging.info("Этап 5/7: Сырые данные сохранены: %s", RAW_FILE)


def apply_excel_formatting(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    """Применяет форматирование итогового Excel и скрывает техническую колонку SECID."""
    ws = writer.sheets["MOEX_BONDS"]
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    ws.sheet_view.showGridLines = False
    ws.sheet_properties.outlinePr.summaryRight = True

    header_fill = PatternFill(fill_type="solid", start_color="1F4E78", end_color="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    even_fill = PatternFill(fill_type="solid", start_color="F5F9FF", end_color="F5F9FF")
    odd_fill = PatternFill(fill_type="solid", start_color="FFFFFF", end_color="FFFFFF")

    ws.row_dimensions[1].height = 22

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    numeric_columns = {"FACEVALUE", "COUPONVALUE", "COUPONPERIOD", "COUPONPERCENT", "PREVLEGALCLOSEPRICE", "PREVPRICE", ACCRUED_INT_COLUMN_NAME}

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

        if col_name == ISSUER_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 70

        if col_name == ISSUER_INN_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 16

        if col_name == QUALIFIED_INVESTOR_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 15

        if col_name == AMORTIZATION_FLAG_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 17

        if col_name == AMORTIZATION_START_DATE_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 24

        if col_name == MATURITY_DATE_COLUMN_NAME:
            ws.column_dimensions[col_letter].width = 14

        if col_name == HIDDEN_COLUMN_NAME:
            ws.column_dimensions[col_letter].hidden = True

    grouped_columns = [ISSUER_COLUMN_NAME, ISSUER_INN_COLUMN_NAME, "SHORTNAME"]
    grouped_indexes = [idx for idx, col_name in enumerate(df.columns, start=1) if col_name in grouped_columns]
    if grouped_indexes:
        for grouped_idx in grouped_indexes:
            ws.column_dimensions[get_column_letter(grouped_idx)].outlineLevel = 1
        ws.column_dimensions[get_column_letter(max(grouped_indexes))].collapsed = True


def add_info_sheet(writer: pd.ExcelWriter) -> None:
    """Добавляет лист INFO с пояснением полей простым языком."""
    info_rows = [
        {"Поле": "SECID", "Описание": "Технический код бумаги на бирже. В основном листе скрыт, но сохранён для аналитики и связок."},
        {"Поле": "ISIN", "Описание": "Международный идентификатор ценной бумаги."},
        {"Поле": "SHORTNAME", "Описание": "Краткое название облигации."},
        {"Поле": "ISSUER_NAME", "Описание": "Наименование эмитента облигации (компании или организации, которая выпустила бумагу)."},
        {"Поле": "ISSUER_INN", "Описание": "ИНН эмитента для быстрой сверки компании в ваших внутренних системах и документах."},
        {"Поле": "QUALIFIED_INVESTOR", "Описание": "Показывает, предназначена ли облигация только для квалифицированных инвесторов: ✔ — да, ✖ — нет."},
        {"Поле": "HAS_AMORTIZATION", "Описание": "Есть ли у бумаги амортизация номинала: ✔ — да, ✖ — нет."},
        {"Поле": "AMORTIZATION_START_DATE", "Описание": "Дата начала амортизации (если есть)."},
        {"Поле": "MATDATE", "Описание": "Дата погашения облигации (когда эмитент должен вернуть номинал)."},
        {"Поле": "FACEVALUE", "Описание": "Номинал облигации."},
        {"Поле": "FACEUNIT", "Описание": "Валюта номинала (например, RUB)."},
        {"Поле": "COUPONVALUE", "Описание": "Размер купонной выплаты."},
        {"Поле": "ACCRUEDINT", "Описание": "Накопленный купонный доход (НКД) на текущую дату."},
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
    logging.info("Этап 6/7: Подготовка итогового Excel...")
    df = pd.DataFrame(rows)

    for col in REMOVED_COLUMNS:
        if col in df.columns:
            df = df.drop(columns=[col])

    sort_columns = [col for col in ["SECID", "ISIN"] if col in df.columns]
    if sort_columns:
        df = df.sort_values(by=sort_columns).reset_index(drop=True)

    if FIRST_COLUMN_NAME in df.columns:
        ordered_columns = [FIRST_COLUMN_NAME]
        for preferred_col in [ISSUER_COLUMN_NAME, ISSUER_INN_COLUMN_NAME, "SHORTNAME", QUALIFIED_INVESTOR_COLUMN_NAME, AMORTIZATION_FLAG_COLUMN_NAME, AMORTIZATION_START_DATE_COLUMN_NAME, MATURITY_DATE_COLUMN_NAME]:
            if preferred_col in df.columns:
                ordered_columns.append(preferred_col)

        ordered_columns.extend(
            [col for col in df.columns if col not in set(ordered_columns)]
        )
        df = df[ordered_columns]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MOEX_BONDS")
        apply_excel_formatting(writer, df)
        add_info_sheet(writer)

    logging.info("Этап 7/7: Excel файл сохранён: %s", OUTPUT_FILE)


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
                "Этап 2/7: Инкрементальное обновление завершено. Новых: %s, обновлённых: %s, всего: %s.",
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

    rows = filter_rows_by_maturity(rows)
    validate_rows(rows)
    rows = enrich_with_issuer_names(rows)
    rows = enrich_with_daily_metrics(rows)
    save_cache(rows)
    save_raw(rows)
    save_excel(rows)

    elapsed = time.perf_counter() - started_at
    logging.info("Готово. Всего облигаций: %s", len(rows))
    logging.info("Время выполнения: %.2f сек.", elapsed)


if __name__ == "__main__":
    main()
