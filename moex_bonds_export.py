"""Скрипт выгружает торгуемые облигации MOEX и сохраняет Excel-отчёты."""

from __future__ import annotations

import json
import logging
import shutil
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from openpyxl.styles import Font, PatternFill

import moex_bonds_config as cfg


@dataclass
class MoexBlocks:
    descriptions: list[dict[str, Any]]
    coupons: list[dict[str, Any]]
    amortizations: list[dict[str, Any]]
    offers: list[dict[str, Any]]


RUSSIAN_COLUMN_NAMES = {
    "SECID": "Код бумаги",
    "SHORTNAME": "Краткое наименование",
    "LATNAME": "Латинское наименование",
    "NAME": "Полное наименование",
    "ISIN": "ISIN",
    "REGNUMBER": "Регистрационный номер",
    "LISTLEVEL": "Уровень листинга",
    "FACEUNIT": "Валюта номинала",
    "PREVPRICE": "Цена предыдущей сделки",
    "LOTSIZE": "Лот",
    "FACEVALUE": "Номинал",
    "MATDATE": "Дата погашения",
    "COUPONFREQUENCY": "Частота купона",
    "COUPONPERCENT": "Ставка купона, %",
    "COUPONVALUE": "Размер купона",
    "BUYBACKPRICE": "Цена оферты",
    "BUYBACKDATE": "Дата оферты",
    "EMITENT_ID": "Код эмитента",
    "EMITENT_TITLE": "Эмитент",
    "EMITENT_INN": "ИНН эмитента",
    "EMITENT_OKPO": "ОКПО эмитента",
    "TYPE": "Тип инструмента",
    "GROUP": "Группа инструментов",
    "PRIMARY_BOARDID": "Основной режим торгов",
    "MARKETPRICE_BOARDID": "Режим цены рынка",
    "STATUS": "Статус",
    "IS_TRADED": "Торгуется",
    "NUMTRADES": "Количество сделок",
    "VOLTODAY": "Объем за день",
    "VALTODAY": "Оборот за день",
}


def setup_environment() -> None:
    """Готовит папки проекта и очищает временные данные перед запуском."""
    cfg.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    cfg.RAW_DIR.mkdir(parents=True, exist_ok=True)
    cfg.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cfg.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cfg.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cfg.RAW_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    if cfg.LOG_FILE.exists():
        cfg.LOG_FILE.unlink()

    for item in cfg.RAW_DIR.iterdir():
        if item.name == ".gitkeep":
            continue
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(cfg.LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def request_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Делает HTTP-запрос к ISS MOEX c повторными попытками при временных сбоях."""
    last_exception: Exception | None = None
    for attempt in range(1, cfg.RETRY_COUNT + 1):
        try:
            response = requests.get(url, params=params, timeout=cfg.REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_exception = exc
            logging.warning("Попытка %s/%s для %s завершилась ошибкой: %s", attempt, cfg.RETRY_COUNT, url, exc)
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"Не удалось получить данные: {url}") from last_exception


def fetch_all_pages(url: str, block_name: str, extra_params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Собирает все страницы ISS-блока через параметр start."""
    start = 0
    all_rows: list[list[Any]] = []
    columns: list[str] | None = None
    previous_rows: list[list[Any]] | None = None
    page_counter = 0
    while True:
        page_counter += 1
        params = {"iss.meta": "off", "start": start}
        if extra_params:
            params.update(extra_params)

        payload = request_json(url, params=params)
        block = payload.get(block_name)
        if not block:
            raise RuntimeError(f"В ответе нет блока '{block_name}' для URL {url}")

        if columns is None:
            columns = block["columns"]

        rows = block["data"]
        if not rows:
            break

        if previous_rows is not None and rows == previous_rows:
            logging.warning(
                "Пагинация для %s перестала сдвигаться (start=%s). Останавливаю цикл, чтобы избежать дублей.",
                url,
                start,
            )
            break

        all_rows.extend(rows)
        if page_counter == 1 or page_counter % 10 == 0:
            logging.info(
                "Пагинация %s: страница=%s, start=%s, строк в странице=%s, накоплено=%s",
                block_name,
                page_counter,
                start,
                len(rows),
                len(all_rows),
            )

        if len(rows) < 100:
            break

        previous_rows = rows
        start += len(rows)

    return pd.DataFrame(all_rows, columns=columns or [])


def is_cache_valid(cache_file: Path, ttl_hours: int) -> bool:
    if not cache_file.exists():
        return False
    max_age = timedelta(hours=ttl_hours)
    age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
    return age <= max_age


def fetch_reference_row(secid: str) -> dict[str, Any] | None:
    ref_cache_dir = cfg.CACHE_DIR / "reference_rows"
    ref_cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = ref_cache_dir / f"{secid}.json"

    if is_cache_valid(cache_file, cfg.CACHE_TTL_HOURS["reference_rows"]):
        return json.loads(cache_file.read_text(encoding="utf-8"))

    payload = request_json(
        f"{cfg.MOEX_BASE_URL}/securities.json",
        params={"iss.meta": "off", "engine": "stock", "market": "bonds", "q": secid},
    )
    block = payload.get("securities", {"columns": [], "data": []})
    columns = block.get("columns", [])
    rows = block.get("data", [])

    secid_index = columns.index("secid") if "secid" in columns else None
    target_row = None
    if secid_index is not None:
        for row in rows:
            if str(row[secid_index]) == secid:
                target_row = dict(zip(columns, row))
                break

    if target_row is not None:
        cache_file.write_text(json.dumps(target_row, ensure_ascii=False), encoding="utf-8")

    return target_row


def collect_reference_data_for_secids(secids: list[str]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    total = len(secids)
    logging.info("Загружаю справочные данные по %s облигациям через точечные запросы", total)

    with ThreadPoolExecutor(max_workers=cfg.MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_reference_row, secid): secid for secid in secids}
        for index, future in enumerate(as_completed(future_map), start=1):
            secid = future_map[future]
            try:
                row = future.result()
                if row:
                    records.append(row)
            except Exception as exc:  # noqa: BLE001
                logging.error("Ошибка загрузки справочника для %s: %s", secid, exc)

            if index % 200 == 0 or index == total:
                logging.info("Справочник: обработано %s/%s", index, total)

    return pd.DataFrame(records)


def to_records(payload_block: dict[str, Any]) -> list[dict[str, Any]]:
    columns = payload_block.get("columns", [])
    return [dict(zip(columns, row)) for row in payload_block.get("data", [])]


def fetch_security_details(secid: str) -> MoexBlocks:
    cache_file = cfg.CACHE_DIR / f"{secid}.json"

    if is_cache_valid(cache_file, cfg.CACHE_TTL_HOURS["security_details"]):
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        details_payload = cached["details"]
        bondization_payload = cached["bondization"]
    else:
        details_payload = request_json(f"{cfg.MOEX_BASE_URL}/securities/{secid}.json", params={"iss.meta": "off"})
        bondization_payload = request_json(f"{cfg.MOEX_BASE_URL}/securities/{secid}/bondization.json", params={"iss.meta": "off"})
        cache_file.write_text(
            json.dumps({"details": details_payload, "bondization": bondization_payload}, ensure_ascii=False),
            encoding="utf-8",
        )

    description_rows = to_records(details_payload.get("description", {"columns": [], "data": []}))
    descriptions = [
        {
            "secid": secid,
            "field": row.get("name"),
            "title": row.get("title"),
            "value": row.get("value"),
        }
        for row in description_rows
    ]

    coupons = to_records(bondization_payload.get("coupons", {"columns": [], "data": []}))
    amortizations = to_records(bondization_payload.get("amortizations", {"columns": [], "data": []}))
    offers = to_records(bondization_payload.get("offers", {"columns": [], "data": []}))

    for row in coupons:
        row["secid"] = secid
    for row in amortizations:
        row["secid"] = secid
    for row in offers:
        row["secid"] = secid

    return MoexBlocks(
        descriptions=descriptions,
        coupons=coupons,
        amortizations=amortizations,
        offers=offers,
    )


def chunk_list(items: list[str], chunks: int) -> list[list[str]]:
    if not items:
        return []
    chunk_size = max(1, len(items) // chunks + (1 if len(items) % chunks else 0))
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def load_checkpoint_state() -> dict[str, Any]:
    if not is_cache_valid(cfg.CHECKPOINT_STATE_FILE, cfg.CACHE_TTL_HOURS["checkpoint"]):
        return {"completed_secids": []}
    return json.loads(cfg.CHECKPOINT_STATE_FILE.read_text(encoding="utf-8"))


def _load_checkpoint_rows(path: Path) -> list[dict[str, Any]]:
    if not is_cache_valid(path, cfg.CACHE_TTL_HOURS["checkpoint"]):
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def save_checkpoint(blocks: MoexBlocks, completed_secids: list[str]) -> None:
    cfg.CHECKPOINT_STATE_FILE.write_text(
        json.dumps({"completed_secids": completed_secids, "updated_at": datetime.now().isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    cfg.CHECKPOINT_DESCRIPTIONS_FILE.write_text(json.dumps(blocks.descriptions, ensure_ascii=False), encoding="utf-8")
    cfg.CHECKPOINT_COUPONS_FILE.write_text(json.dumps(blocks.coupons, ensure_ascii=False), encoding="utf-8")
    cfg.CHECKPOINT_AMORTIZATIONS_FILE.write_text(json.dumps(blocks.amortizations, ensure_ascii=False), encoding="utf-8")
    cfg.CHECKPOINT_OFFERS_FILE.write_text(json.dumps(blocks.offers, ensure_ascii=False), encoding="utf-8")


def load_checkpoint_blocks() -> MoexBlocks:
    return MoexBlocks(
        descriptions=_load_checkpoint_rows(cfg.CHECKPOINT_DESCRIPTIONS_FILE),
        coupons=_load_checkpoint_rows(cfg.CHECKPOINT_COUPONS_FILE),
        amortizations=_load_checkpoint_rows(cfg.CHECKPOINT_AMORTIZATIONS_FILE),
        offers=_load_checkpoint_rows(cfg.CHECKPOINT_OFFERS_FILE),
    )


def clear_checkpoint() -> None:
    for file_path in [
        cfg.CHECKPOINT_STATE_FILE,
        cfg.CHECKPOINT_DESCRIPTIONS_FILE,
        cfg.CHECKPOINT_COUPONS_FILE,
        cfg.CHECKPOINT_AMORTIZATIONS_FILE,
        cfg.CHECKPOINT_OFFERS_FILE,
    ]:
        file_path.unlink(missing_ok=True)


def collect_extended_data(secids: list[str]) -> MoexBlocks:
    state = load_checkpoint_state()
    completed_secids = set(state.get("completed_secids", []))
    blocks = load_checkpoint_blocks()

    if completed_secids:
        logging.info("Найден checkpoint: пропускаю уже обработанные облигации (%s шт.)", len(completed_secids))

    secids_to_process = [secid for secid in secids if secid not in completed_secids]
    grouped = chunk_list(secids_to_process, cfg.CHUNK_COUNT)

    logging.info(
        "Старт загрузки расширенных данных: всего=%s, осталось после checkpoint=%s, чанков=%s",
        len(secids),
        len(secids_to_process),
        len(grouped),
    )

    failed_secids: set[str] = set()
    for chunk_index, secid_chunk in enumerate(grouped, start=1):
        print(f"Обрабатываю чанк {chunk_index}/{len(grouped)} ({len(secid_chunk)} облигаций)...")
        with ThreadPoolExecutor(max_workers=cfg.MAX_WORKERS) as executor:
            future_map = {executor.submit(fetch_security_details, secid): secid for secid in secid_chunk}
            for index, future in enumerate(as_completed(future_map), start=1):
                secid = future_map[future]
                try:
                    item = future.result()
                    blocks.descriptions.extend(item.descriptions)
                    blocks.coupons.extend(item.coupons)
                    blocks.amortizations.extend(item.amortizations)
                    blocks.offers.extend(item.offers)
                    completed_secids.add(secid)
                except Exception as exc:  # noqa: BLE001
                    failed_secids.add(secid)
                    logging.error("Ошибка при загрузке %s: %s", secid, exc)

                if index % 50 == 0 or index == len(secid_chunk):
                    logging.info(
                        "Чанк %s/%s: обработано %s/%s",
                        chunk_index,
                        len(grouped),
                        index,
                        len(secid_chunk),
                    )

        save_checkpoint(blocks, sorted(completed_secids))
        logging.info("Checkpoint сохранен после чанка %s/%s", chunk_index, len(grouped))

    if failed_secids:
        print(f"Дополнительная попытка для проблемных бумаг: {len(failed_secids)} шт.")
        logging.warning("Запускаю повторную загрузку проблемных SECID: %s шт.", len(failed_secids))
        with ThreadPoolExecutor(max_workers=cfg.MAX_WORKERS) as executor:
            future_map = {executor.submit(fetch_security_details, secid): secid for secid in sorted(failed_secids)}
            for future in as_completed(future_map):
                secid = future_map[future]
                try:
                    item = future.result()
                    blocks.descriptions.extend(item.descriptions)
                    blocks.coupons.extend(item.coupons)
                    blocks.amortizations.extend(item.amortizations)
                    blocks.offers.extend(item.offers)
                    completed_secids.add(secid)
                except Exception as exc:  # noqa: BLE001
                    logging.error("Повторная попытка также завершилась ошибкой для %s: %s", secid, exc)

        save_checkpoint(blocks, sorted(completed_secids))

    return blocks


def save_raw(df: pd.DataFrame, file_name: str) -> None:
    (cfg.RAW_DIR / file_name).write_text(df.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")


def save_parquet(df: pd.DataFrame, file_path: Path) -> None:
    """Сохраняет таблицу в Parquet для быстрого машинного чтения."""
    df.to_parquet(file_path, index=False)


def build_emitents_sheet(traded_bonds: pd.DataFrame) -> pd.DataFrame:
    emitent_cols_upper = ["EMITENT_ID", "EMITENT_TITLE", "EMITENT_INN", "EMITENT_OKPO"]
    existing_cols = [col for col in emitent_cols_upper if col in traded_bonds.columns]
    if not existing_cols:
        return pd.DataFrame(columns=emitent_cols_upper + ["bonds_count", "secids"])

    emitent_id_column = "EMITENT_ID" if "EMITENT_ID" in existing_cols else existing_cols[0]
    emitents = traded_bonds[existing_cols + ["SECID"]].dropna(subset=[emitent_id_column], how="all").copy()

    return emitents.groupby(existing_cols, dropna=False, as_index=False).agg(
        bonds_count=("SECID", "count"),
        secids=("SECID", lambda x: ", ".join(sorted(set(map(str, x))))),
    )


def merge_emitents_incremental(new_emitents: pd.DataFrame) -> pd.DataFrame:
    """Добавляет только новых эмитентов к существующему справочнику."""
    if "EMITENT_ID" not in new_emitents.columns:
        return new_emitents

    if not cfg.EMITENTS_OUTPUT_FILE.exists():
        return new_emitents.sort_values("EMITENT_TITLE", na_position="last").reset_index(drop=True)

    existing_emitents = pd.read_parquet(cfg.EMITENTS_OUTPUT_FILE)
    if "EMITENT_ID" not in existing_emitents.columns:
        return new_emitents.sort_values("EMITENT_TITLE", na_position="last").reset_index(drop=True)

    existing_ids = set(existing_emitents["EMITENT_ID"].dropna().astype(str))
    candidates = new_emitents.copy()
    candidates["EMITENT_ID"] = candidates["EMITENT_ID"].astype(str)
    new_only = candidates[~candidates["EMITENT_ID"].isin(existing_ids)]

    if new_only.empty:
        return existing_emitents.sort_values("EMITENT_TITLE", na_position="last").reset_index(drop=True)

    merged_emitents = pd.concat([existing_emitents, new_only], ignore_index=True)
    return merged_emitents.sort_values("EMITENT_TITLE", na_position="last").reset_index(drop=True)


def build_descriptions_wide_sheet(descriptions_df: pd.DataFrame) -> pd.DataFrame:
    """Превращает длинный формат описаний в широкий (1 строка = 1 облигация)."""
    if descriptions_df.empty:
        return pd.DataFrame(columns=["secid"])

    descriptions_df = descriptions_df.copy()
    descriptions_df["key"] = descriptions_df["title"].fillna(descriptions_df["field"]).fillna("unknown")
    descriptions_df = descriptions_df.drop_duplicates(subset=["secid", "key"], keep="last")

    wide = descriptions_df.pivot(index="secid", columns="key", values="value").reset_index()
    wide.columns.name = None
    return wide


def build_merged_bonds_sheet(traded_bonds_df: pd.DataFrame, descriptions_wide_df: pd.DataFrame) -> pd.DataFrame:
    """Объединяет торговые данные и описание облигаций в один лист без дублей."""
    merged = traded_bonds_df.copy()
    merged["secid"] = merged["SECID"]

    merged = merged.merge(descriptions_wide_df, how="left", on="secid")

    duplicate_pairs = {
        "SECID": "secid",
        "SHORTNAME": "Краткое наименование",
        "LATNAME": "Латинское наименование",
        "NAME": "Полное наименование",
        "ISIN": "ISIN",
        "REGNUMBER": "Регистрационный номер",
        "LISTLEVEL": "Уровень листинга",
        "FACEUNIT": "Валюта номинала",
        "PREVPRICE": "Цена предыдущей сделки",
        "LOTSIZE": "Лот",
        "FACEVALUE": "Номинал",
        "MATDATE": "Дата погашения",
        "COUPONFREQUENCY": "Частота купона",
        "COUPONPERCENT": "Ставка купона, %",
        "COUPONVALUE": "Размер купона",
        "BUYBACKPRICE": "Цена оферты",
        "BUYBACKDATE": "Дата оферты",
    }
    drop_columns = [desc_col for src_col, desc_col in duplicate_pairs.items() if src_col in merged.columns and desc_col in merged.columns]
    if drop_columns:
        merged = merged.drop(columns=drop_columns)

    renamed_columns = {col: RUSSIAN_COLUMN_NAMES[col] for col in merged.columns if col in RUSSIAN_COLUMN_NAMES}
    merged = merged.rename(columns=renamed_columns)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged


def build_data_quality_sheet(merged_bonds_df: pd.DataFrame) -> pd.DataFrame:
    """Формирует простой отчет по заполненности ключевых полей."""
    important_columns = [
        "Код бумаги",
        "ISIN",
        "Код эмитента",
        "Эмитент",
        "Торгуется",
        "Статус",
    ]
    rows: list[dict[str, Any]] = []
    total = len(merged_bonds_df)
    for column in important_columns:
        if column in merged_bonds_df.columns:
            column_data = merged_bonds_df[column]
            if isinstance(column_data, pd.DataFrame):
                empty_count = int(column_data.isna().all(axis=1).sum())
            else:
                empty_count = int(column_data.isna().sum())
        else:
            empty_count = total
        fill_rate = 0.0 if total == 0 else round((1 - empty_count / total) * 100, 2)
        rows.append({
            "Поле": column,
            "Всего строк": total,
            "Пустых значений": empty_count,
            "Заполнено, %": fill_rate,
        })
    return pd.DataFrame(rows)


def archive_raw_data() -> None:
    """Архивирует raw JSON после успешного запуска и удаляет старые архивы."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = cfg.RAW_ARCHIVE_DIR / f"raw_{timestamp}.zip"
    raw_files = sorted(cfg.RAW_DIR.glob("*.json"))
    if not raw_files:
        return

    with zipfile.ZipFile(archive_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in raw_files:
            zf.write(file_path, arcname=file_path.name)

    old_archives = sorted(cfg.RAW_ARCHIVE_DIR.glob("raw_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    for stale_archive in old_archives[cfg.RAW_ARCHIVE_KEEP_LAST :]:
        stale_archive.unlink(missing_ok=True)


def beautify_sheet(worksheet: Any) -> None:
    """Улучшает читаемость листа: стиль заголовка, автофильтр, freeze pane и ширина."""
    if worksheet.max_row < 1 or worksheet.max_column < 1:
        return

    header_fill = PatternFill("solid", fgColor="1F4E78")
    for cell in worksheet[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for column_cells in worksheet.columns:
        first_cell = column_cells[0]
        col_letter = first_cell.column_letter
        values = [str(c.value) if c.value is not None else "" for c in column_cells[:200]]
        max_len = max((len(v) for v in values), default=10)
        worksheet.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 50)


def write_excel(file_path: Path, sheet_name: str, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        beautify_sheet(writer.book[sheet_name])


def write_core_excel(merged_bonds_df: pd.DataFrame, quality_df: pd.DataFrame) -> None:
    with pd.ExcelWriter(cfg.CORE_OUTPUT_FILE, engine="openpyxl") as writer:
        merged_bonds_df.to_excel(writer, sheet_name="bonds_traded", index=False)
        quality_df.to_excel(writer, sheet_name="data_quality", index=False)
        beautify_sheet(writer.book["bonds_traded"])
        beautify_sheet(writer.book["data_quality"])


def main() -> None:
    start_time = time.perf_counter()
    setup_environment()
    setup_logging()

    print("[1/7] Загружаю список облигаций по рынку MOEX...")
    bonds_market_df = fetch_all_pages(
        f"{cfg.MOEX_BASE_URL}/engines/stock/markets/bonds/securities.json",
        block_name="securities",
    )

    print("[2/7] Загружаю справочник эмитентов и торговых атрибутов...")
    source_secids = bonds_market_df["SECID"].dropna().astype(str).unique().tolist()
    if cfg.MAX_BONDS_TO_PROCESS:
        source_secids = source_secids[: cfg.MAX_BONDS_TO_PROCESS]
        bonds_market_df = bonds_market_df[bonds_market_df["SECID"].isin(source_secids)].copy()
        logging.warning("Для отладки ограничен список облигаций до %s штук", cfg.MAX_BONDS_TO_PROCESS)

    all_securities_df = collect_reference_data_for_secids(source_secids)
    all_securities_df = all_securities_df.rename(columns=str.upper)

    required_reference_cols = [
        "SECID",
        "IS_TRADED",
        "EMITENT_ID",
        "EMITENT_TITLE",
        "EMITENT_INN",
        "EMITENT_OKPO",
        "TYPE",
        "GROUP",
        "PRIMARY_BOARDID",
        "MARKETPRICE_BOARDID",
    ]
    for column in required_reference_cols:
        if column not in all_securities_df.columns:
            all_securities_df[column] = None

    merged_df = bonds_market_df.merge(
        all_securities_df[required_reference_cols],
        how="left",
        on="SECID",
    )

    traded_bonds_df = merged_df[(merged_df["IS_TRADED"] == 1) | (merged_df["STATUS"] == "A")].copy()
    traded_bonds_df = traded_bonds_df.sort_values(["EMITENT_TITLE", "SECID"], na_position="last")

    secids = traded_bonds_df["SECID"].dropna().astype(str).unique().tolist()
    print(f"Найдено торгуемых облигаций: {len(secids)}")

    print("[3/7] Загружаю расширенные данные (10 чанков + checkpoint)...")
    blocks = collect_extended_data(secids)

    descriptions_df = pd.DataFrame(blocks.descriptions)
    descriptions_wide_df = build_descriptions_wide_sheet(descriptions_df)
    coupons_df = pd.DataFrame(blocks.coupons)
    amortizations_df = pd.DataFrame(blocks.amortizations)
    offers_df = pd.DataFrame(blocks.offers)
    emitents_df = build_emitents_sheet(traded_bonds_df)
    emitents_df = merge_emitents_incremental(emitents_df)
    merged_bonds_df = build_merged_bonds_sheet(traded_bonds_df, descriptions_wide_df)
    quality_df = build_data_quality_sheet(merged_bonds_df)

    print("[4/7] Сохраняю сырые данные в raw/...")
    save_raw(traded_bonds_df, "traded_bonds.json")
    save_raw(descriptions_df, "bond_descriptions_long.json")
    save_raw(descriptions_wide_df, "bond_descriptions_wide.json")
    save_raw(coupons_df, "bond_coupons.json")
    save_raw(amortizations_df, "bond_amortizations.json")
    save_raw(offers_df, "bond_offers.json")
    save_raw(emitents_df, "emitents.json")
    save_raw(merged_bonds_df, "bonds_traded_merged.json")

    print("[5/7] Формирую основной Excel (облегченный)...")
    write_core_excel(merged_bonds_df, quality_df)

    print("[6/7] Сохраняю справочники в быстрый формат Parquet...")
    save_parquet(emitents_df, cfg.EMITENTS_OUTPUT_FILE)
    save_parquet(coupons_df, cfg.COUPONS_OUTPUT_FILE)
    save_parquet(amortizations_df, cfg.AMORTIZATIONS_OUTPUT_FILE)
    save_parquet(offers_df, cfg.OFFERS_OUTPUT_FILE)

    archive_raw_data()
    clear_checkpoint()
    elapsed = time.perf_counter() - start_time

    print("[7/7] Готово.")
    print(f"Основной Excel сохранен: {cfg.CORE_OUTPUT_FILE}")
    print(f"Время выполнения: {elapsed:.2f} сек.")

    logging.info("Основной Excel сохранен: %s", cfg.CORE_OUTPUT_FILE)
    logging.info(
        "Доп. файлы (Parquet): %s, %s, %s, %s",
        cfg.EMITENTS_OUTPUT_FILE,
        cfg.COUPONS_OUTPUT_FILE,
        cfg.AMORTIZATIONS_OUTPUT_FILE,
        cfg.OFFERS_OUTPUT_FILE,
    )
    logging.info("Время выполнения: %.2f сек.", elapsed)


if __name__ == "__main__":
    main()
