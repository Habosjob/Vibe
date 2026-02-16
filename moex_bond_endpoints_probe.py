#!/usr/bin/env python3
"""Пробует все доступные endpoint'ы ISS MOEX для облигаций по выбранным SECID и сохраняет ответы в Excel."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

REFERENCE_URL = "https://iss.moex.com/iss/reference"
BASE_URL = "https://iss.moex.com"

LOGGER = logging.getLogger("moex_bond_endpoints_probe")


def setup_logging(log_file: Path, level: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_secids_from_excel(path: Path) -> list[str]:
    LOGGER.info("Читаю SECID из %s", path)
    df = pd.read_excel(path)
    if "SECID" not in df.columns:
        raise ValueError(f"В файле {path} отсутствует колонка SECID")

    secids = [str(value).strip() for value in df["SECID"].dropna().tolist() if str(value).strip()]
    unique_secids = sorted(set(secids))
    LOGGER.info("Найдено уникальных SECID: %s", len(unique_secids))
    return unique_secids


def parse_security_endpoint_templates(reference_html: str) -> list[str]:
    endpoints = re.findall(r'<dt><a href="\.\/\d+">([^<]+)</a></dt>', reference_html)
    templates: list[str] = []
    for endpoint in endpoints:
        if "[security]" not in endpoint:
            continue
        if endpoint.startswith("/iss/cci/"):
            continue
        templates.append(endpoint)

    deduped = sorted(set(templates))
    LOGGER.info("Найдено шаблонов endpoint с [security]: %s", len(deduped))
    return deduped


def normalize_sheet_name(value: str, used_names: set[str]) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", value).strip()
    if not cleaned:
        cleaned = "sheet"

    base = cleaned[:31]
    candidate = base
    idx = 1
    while candidate in used_names:
        suffix = f"_{idx}"
        candidate = f"{base[:31-len(suffix)]}{suffix}"
        idx += 1
    used_names.add(candidate)
    return candidate


def build_context_for_secid(session: requests.Session, secid: str) -> dict[str, list[str]]:
    url = f"{BASE_URL}/iss/securities/{secid}.json"
    response = session.get(url, params={"iss.meta": "off"}, timeout=30)
    response.raise_for_status()
    payload = response.json()

    boards_df = pd.DataFrame(
        payload.get("boards", {}).get("data", []),
        columns=payload.get("boards", {}).get("columns", []),
    )
    if boards_df.empty:
        return {"board": [], "boardgroup": [], "session": ["total"]}

    if {"engine", "market"}.issubset(boards_df.columns):
        boards_df = boards_df[(boards_df["engine"] == "stock") & (boards_df["market"] == "bonds")].copy()

    boards = sorted({str(board).strip() for board in boards_df.get("boardid", pd.Series(dtype=str)).dropna().tolist() if str(board).strip()})
    boardgroups = sorted({str(boardgroup).strip() for boardgroup in boards_df.get("board_group_id", pd.Series(dtype=str)).dropna().tolist() if str(boardgroup).strip()})
    return {
        "board": boards,
        "boardgroup": boardgroups,
        "session": ["total"],
    }


def instantiate_endpoints(template: str, secid: str, context: dict[str, list[str]]) -> list[str]:
    values: list[dict[str, str]] = [
        {
            "[engine]": "stock",
            "[market]": "bonds",
            "[security]": secid,
        }
    ]

    if "[board]" in template:
        values = [
            {**base, "[board]": board}
            for base in values
            for board in context.get("board", [])
        ]
    if "[boardgroup]" in template:
        values = [
            {**base, "[boardgroup]": boardgroup}
            for base in values
            for boardgroup in context.get("boardgroup", [])
        ]
    if "[session]" in template:
        values = [
            {**base, "[session]": session}
            for base in values
            for session in context.get("session", ["total"])
        ]

    if not values:
        return []

    urls: list[str] = []
    for mapping in values:
        endpoint = template
        for key, replacement in mapping.items():
            endpoint = endpoint.replace(key, replacement)
        urls.append(f"{BASE_URL}{endpoint}.json")
    return sorted(set(urls))


def build_cache_key(url: str, params: dict[str, Any]) -> str:
    fingerprint = json.dumps({"url": url, "params": params}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()


def fetch_json_with_cache(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    cache_dir: Path,
    cache_ttl: int,
) -> tuple[dict[str, Any] | None, str]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = build_cache_key(url, params)
    cache_file = cache_dir / f"{cache_key}.json"

    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age <= cache_ttl:
            with cache_file.open("r", encoding="utf-8") as handle:
                return json.load(handle), "cache"

    try:
        response = session.get(url, params=params, timeout=45)
        if response.status_code != 200:
            LOGGER.debug("Пропуск endpoint %s: HTTP %s", response.url, response.status_code)
            return None, f"http_{response.status_code}"

        payload = response.json()
        with cache_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return payload, "api"
    except requests.RequestException as exc:
        LOGGER.warning("Ошибка запроса %s: %s", url, exc)
        return None, "error"


def payload_to_frames(payload: dict[str, Any], secid: str, request_url: str) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for block_name, block in payload.items():
        if not isinstance(block, dict):
            continue
        data = block.get("data")
        columns = block.get("columns")
        if not isinstance(data, list) or not isinstance(columns, list):
            continue
        frame = pd.DataFrame(data, columns=columns)
        if "REQUEST_URL" not in frame.columns:
            frame.insert(0, "REQUEST_URL", request_url)
        if "SECID" not in frame.columns:
            frame.insert(0, "SECID", secid)
        frames[block_name] = frame
    return frames


def save_endpoint_workbook(endpoint_slug: str, frames: dict[str, pd.DataFrame], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{endpoint_slug}.xlsx"
    used_sheet_names: set[str] = set()
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_raw_name, frame in frames.items():
            sheet_name = normalize_sheet_name(sheet_raw_name, used_sheet_names)
            frame.to_excel(writer, index=False, sheet_name=sheet_name)
    return output_path


def endpoint_slug_from_url(url: str) -> str:
    slug = url.replace(BASE_URL, "").strip("/")
    slug = slug.replace(".json", "")
    slug = re.sub(r"[^a-zA-Z0-9_\-/]+", "_", slug)
    slug = slug.replace("/", "__")
    return slug[:180]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Собирает данные по endpoint'ам ISS MOEX для облигаций.")
    parser.add_argument("--input", type=Path, default=Path("moex_bonds.xlsx"), help="Excel-файл со списком облигаций (SECID).")
    parser.add_argument("--output-dir", type=Path, default=Path("endpoint_excels"), help="Каталог для Excel-файлов по endpoint'ам.")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/moex_endpoint_probe"), help="Каталог кэша HTTP JSON.")
    parser.add_argument("--cache-ttl", type=int, default=1800, help="TTL кэша в секундах.")
    parser.add_argument("--static-secid", type=str, default="SU26238RMFS4", help="Статичный SECID для проверки кэша.")
    parser.add_argument("--seed", type=int, default=42, help="Seed для выбора random SECID.")
    parser.add_argument("--log-file", type=Path, default=Path("logs/moex_bond_endpoints_probe.log"), help="Путь к log-файлу.")
    parser.add_argument("--log-level", type=str, default="INFO", help="Уровень логирования: DEBUG/INFO/WARNING/ERROR.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file, args.log_level)

    secids = load_secids_from_excel(args.input)
    if not secids:
        raise ValueError("Список SECID пуст")

    random.seed(args.seed)
    random_candidates = [secid for secid in secids if secid != args.static_secid]
    random_secid = random.choice(random_candidates) if random_candidates else args.static_secid
    selected_secids = [args.static_secid, random_secid] if random_secid != args.static_secid else [args.static_secid]

    LOGGER.info("Выбранные SECID на период отладки: %s", selected_secids)

    with requests.Session() as session:
        session.headers.update({"User-Agent": "moex-bonds-endpoints-probe/1.0"})

        LOGGER.info("Получаю список endpoint'ов из %s", REFERENCE_URL)
        reference_html = session.get(REFERENCE_URL, timeout=30).text
        templates = parse_security_endpoint_templates(reference_html)

        endpoint_frames: dict[str, dict[str, pd.DataFrame]] = {}
        stats = {"api": 0, "cache": 0, "errors": 0}

        for secid in selected_secids:
            LOGGER.info("Обрабатываю SECID=%s", secid)
            context = build_context_for_secid(session=session, secid=secid)
            LOGGER.debug("Контекст SECID=%s: %s", secid, context)

            secid_urls: set[str] = set()
            for template in templates:
                for url in instantiate_endpoints(template=template, secid=secid, context=context):
                    secid_urls.add(url)

            LOGGER.info("SECID=%s: сформировано endpoint URL: %s", secid, len(secid_urls))

            for url in sorted(secid_urls):
                params = {"iss.meta": "off", "limit": 100}
                payload, source = fetch_json_with_cache(
                    session=session,
                    url=url,
                    params=params,
                    cache_dir=args.cache_dir,
                    cache_ttl=args.cache_ttl,
                )

                if payload is None:
                    stats["errors"] += 1
                    continue

                stats[source] += 1
                endpoint_slug = endpoint_slug_from_url(url)
                frames = payload_to_frames(payload=payload, secid=secid, request_url=url)
                if not frames:
                    continue

                endpoint_frames.setdefault(endpoint_slug, {})
                for block_name, frame in frames.items():
                    sheet_key = f"{secid}_{block_name}"
                    endpoint_frames[endpoint_slug][sheet_key] = frame

        LOGGER.info(
            "Статистика запросов: api=%s cache=%s errors=%s",
            stats["api"],
            stats["cache"],
            stats["errors"],
        )

        LOGGER.info("Сохраняю Excel по каждому endpoint в %s", args.output_dir)
        for endpoint_slug, sheets in endpoint_frames.items():
            output_path = save_endpoint_workbook(endpoint_slug=endpoint_slug, frames=sheets, output_dir=args.output_dir)
            LOGGER.info("Сохранён endpoint Excel: %s (листов=%s)", output_path, len(sheets))

    LOGGER.info("Готово. Всего endpoint Excel: %s", len(endpoint_frames))


if __name__ == "__main__":
    main()
