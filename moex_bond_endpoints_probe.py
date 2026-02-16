#!/usr/bin/env python3
"""Probe curated endpoint'ов ISS MOEX для облигаций и сохранение инвентаризации в один Excel."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from moex_bonds_endpoints import BASE_ISS_PARAMS, EndpointSpec, curated_bond_endpoint_specs

BASE_URL = "https://iss.moex.com"
LOGGER = logging.getLogger("moex_bond_endpoints_probe")
THREAD_LOCAL = threading.local()
DEBUG_MODE = True


@dataclass
class TableInfo:
    name: str
    rows: int
    columns: list[str]
    sample_rows: list[dict[str, Any]]


@dataclass
class ProbeResult:
    secid: str
    endpoint: str
    status: str
    tables: list[TableInfo]
    elapsed_ms: int
    from_cache: bool


def setup_logging(log_file: Path, level: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Собирает inventory по curated endpoint'ам ISS MOEX для облигаций.")
    parser.add_argument("--input", type=Path, default=Path("moex_bonds.xlsx"), help="Excel-файл со списком облигаций (SECID).")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/moex_endpoint_probe"), help="Каталог кэша HTTP JSON.")
    parser.add_argument("--cache-ttl", type=int, default=1800, help="TTL кэша в секундах.")
    parser.add_argument("--static-secid", type=str, default="SU26238RMFS4", help="Статичный SECID для режима DEBUG.")
    parser.add_argument("--seed", type=int, default=42, help="Seed для выбора random SECID в DEBUG.")
    parser.add_argument("--log-file", type=Path, default=Path("logs/moex_bond_endpoints_probe.log"), help="Путь к log-файлу.")
    parser.add_argument("--log-level", type=str, default="INFO", help="Уровень логирования: DEBUG/INFO/WARNING/ERROR.")
    parser.add_argument("--workers", type=int, default=8, help="Количество потоков для загрузки endpoint'ов.")
    return parser.parse_args()


def configure_session_retries(session: requests.Session) -> None:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def build_configured_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "moex-bonds-endpoints-probe/2.0"})
    configure_session_retries(session)
    return session


def get_thread_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = build_configured_session()
        THREAD_LOCAL.session = session
    return session


def load_secids_from_excel(path: Path) -> list[str]:
    LOGGER.info("Читаю SECID из %s", path)
    df = pd.read_excel(path)
    if "SECID" not in df.columns:
        raise ValueError(f"В файле {path} отсутствует колонка SECID")
    secids = [str(value).strip() for value in df["SECID"].dropna().tolist() if str(value).strip()]
    unique_secids = sorted(set(secids))
    LOGGER.info("Найдено уникальных SECID: %s", len(unique_secids))
    return unique_secids


def pick_secids(all_secids: list[str], static_secid: str, seed: int) -> list[str]:
    if not DEBUG_MODE:
        return all_secids

    random.seed(seed)
    random_candidates = [secid for secid in all_secids if secid != static_secid]
    random_secid = random.choice(random_candidates) if random_candidates else static_secid
    selected = [static_secid, random_secid] if random_secid != static_secid else [static_secid]
    LOGGER.info("DEBUG_MODE=True, выбраны SECID: %s", selected)
    return selected


def build_cache_key(url: str, params: dict[str, Any]) -> str:
    fingerprint = json.dumps({"url": url, "params": params}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()


def request_json_or_status(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    cache_dir: Path,
    cache_ttl: int,
) -> tuple[dict[str, Any] | None, str, bool]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = build_cache_key(url, params)
    cache_file = cache_dir / f"{cache_key}.json"

    if cache_file.exists() and time.time() - cache_file.stat().st_mtime <= cache_ttl:
        with cache_file.open("r", encoding="utf-8") as handle:
            return json.load(handle), "OK", True

    try:
        response = session.get(url, params=params, timeout=45)
    except requests.RequestException as exc:
        LOGGER.warning("Ошибка запроса %s: %s", url, exc)
        return None, "ERROR", False

    if response.status_code != 200:
        return None, "ERROR", False

    try:
        payload = response.json()
    except (JSONDecodeError, ValueError):
        body_start = response.text[:200].strip().lower()
        if "<html" in body_start or "<!doctype html" in body_start:
            return None, "BLOCKED_HTML", False
        return None, "ERROR", False

    with cache_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
    return payload, "OK", False


def parse_payload_tables(payload: dict[str, Any]) -> list[TableInfo]:
    tables: list[TableInfo] = []
    for table_name, block in payload.items():
        if not isinstance(block, dict):
            continue
        columns = block.get("columns")
        rows = block.get("data")
        if not isinstance(columns, list) or not isinstance(rows, list):
            continue
        frame = pd.DataFrame(rows, columns=columns)
        sample = frame.head(5).to_dict(orient="records")
        tables.append(TableInfo(name=table_name, rows=len(frame), columns=[str(c) for c in columns], sample_rows=sample))
    return tables


def sheet_name_for_endpoint(endpoint: str, used: set[str]) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"sample_{endpoint}")[:31]
    name = base
    index = 1
    while name in used:
        suffix = f"_{index}"
        name = f"{base[:31-len(suffix)]}{suffix}"
        index += 1
    used.add(name)
    return name


def probe_one(spec: EndpointSpec, secid: str, cache_dir: Path, cache_ttl: int) -> ProbeResult:
    session = get_thread_session()
    path = spec.path_template.format(secid=secid)
    url = f"{BASE_URL}{path}"
    params = dict(BASE_ISS_PARAMS)
    params.update(spec.params)

    started = time.perf_counter()
    payload, base_status, from_cache = request_json_or_status(
        session=session,
        url=url,
        params=params,
        cache_dir=cache_dir,
        cache_ttl=cache_ttl,
    )
    elapsed_ms = int((time.perf_counter() - started) * 1000)

    if payload is None:
        return ProbeResult(secid=secid, endpoint=spec.name, status=base_status, tables=[], elapsed_ms=elapsed_ms, from_cache=from_cache)

    tables = parse_payload_tables(payload)
    total_rows = sum(table.rows for table in tables)
    status = "OK" if total_rows > 0 else "NO_DATA"
    return ProbeResult(secid=secid, endpoint=spec.name, status=status, tables=tables, elapsed_ms=elapsed_ms, from_cache=from_cache)


def save_probe_workbook(results: list[ProbeResult]) -> Path:
    date_folder = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path("data/raw/moex/bonds_probe") / date_folder
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "bonds_probe.xlsx"

    catalog_rows: list[dict[str, Any]] = []
    table_rows: list[dict[str, Any]] = []
    sample_frames: dict[str, pd.DataFrame] = {}

    for result in results:
        table_names = [table.name for table in result.tables]
        total_rows = sum(table.rows for table in result.tables)
        catalog_rows.append(
            {
                "secid": result.secid,
                "endpoint": result.endpoint,
                "status": result.status,
                "tables": ", ".join(table_names),
                "total_rows": total_rows,
                "elapsed_ms": result.elapsed_ms,
                "from_cache": result.from_cache,
            }
        )

        samples_accum: list[dict[str, Any]] = []
        for table in result.tables:
            table_rows.append(
                {
                    "secid": result.secid,
                    "endpoint": result.endpoint,
                    "table_name": table.name,
                    "rows": table.rows,
                    "columns": ", ".join(table.columns),
                }
            )
            for row in table.sample_rows:
                enriched = {"secid": result.secid, "table_name": table.name, **row}
                samples_accum.append(enriched)

        if samples_accum:
            sample_frames[result.endpoint] = pd.DataFrame(samples_accum).head(5)

    catalog_df = pd.DataFrame(catalog_rows)
    tables_df = pd.DataFrame(table_rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        catalog_df.to_excel(writer, sheet_name="catalog", index=False)
        tables_df.to_excel(writer, sheet_name="tables", index=False)

        used_sheet_names = {"catalog", "tables"}
        for endpoint, frame in sample_frames.items():
            frame.to_excel(writer, sheet_name=sheet_name_for_endpoint(endpoint, used_sheet_names), index=False)

    LOGGER.info("Сохранён файл инвентаризации: %s", output_path)
    return output_path


def main() -> None:
    started = time.perf_counter()
    args = parse_args()
    setup_logging(args.log_file, args.log_level)

    all_secids = load_secids_from_excel(args.input)
    if not all_secids:
        raise ValueError("Список SECID пуст")

    selected_secids = pick_secids(all_secids, args.static_secid, args.seed)
    endpoint_specs = curated_bond_endpoint_specs()
    LOGGER.info("Curated endpoint'ов: %s", len(endpoint_specs))

    jobs: list[tuple[EndpointSpec, str]] = [(spec, secid) for secid in selected_secids for spec in endpoint_specs]
    results: list[ProbeResult] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = [
            executor.submit(probe_one, spec, secid, args.cache_dir, args.cache_ttl)
            for spec, secid in jobs
        ]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: (item.secid, item.endpoint))
    output_path = save_probe_workbook(results)

    status_count: dict[str, int] = {}
    for result in results:
        status_count[result.status] = status_count.get(result.status, 0) + 1

    LOGGER.info("Статусы: %s", status_count)
    LOGGER.info("Готово за %.2f сек. Файл: %s", time.perf_counter() - started, output_path)


if __name__ == "__main__":
    main()
