from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import Alignment, Font, PatternFill
from tqdm import tqdm

import config
from app.bootstrap import load_state, save_state
from app.database import Database

LOGGER = logging.getLogger(__name__)


@dataclass
class RunSummary:
    fetched_count: int
    selected_count: int
    saved_count: int
    errors_count: int
    from_cache_count: int
    duration_total: float
    duration_load: float
    duration_calc: float
    duration_save: float
    output_path: Path


@dataclass
class BondRow:
    secid: str
    isin: str
    short_name: str
    emitter_name: str
    emitter_inn: str
    credit_rating: str
    rating_date: str
    rating_description: str
    current_price: float | None
    previous_price: float | None
    price_change_percent: float | None
    volume_today: float
    volume_20d: float
    maturity_date: str
    offer_date: str
    amortization_start: str
    qualified_investor: str
    default_flag: str
    technical_default_flag: str
    bond_type: str
    sec_sub_type: str
    coupon_period: int | None
    accrued_int: float | None
    coupon_percent: float | None


class MoexClient:
    async def close(self) -> None:
        return None

    @staticmethod
    def _load_json_sync(url: str) -> dict[str, Any]:
        import urllib.request

        with urllib.request.urlopen(
            url,
            timeout=config.REQUEST_CONNECT_TIMEOUT_SEC + config.REQUEST_READ_TIMEOUT_SEC,
        ) as response:
            return json.load(response)

    async def get_json(self, url: str) -> dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(1, config.REQUEST_RETRIES + 1):
            try:
                return await asyncio.to_thread(self._load_json_sync, url)
            except Exception as exc:
                last_exc = exc
                if attempt == config.REQUEST_RETRIES:
                    break
                backoff = config.REQUEST_BACKOFF_SEC * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
        raise RuntimeError(f"Не удалось получить данные: {url}. Ошибка: {last_exc}")


async def _fetch_all_traded_bonds(client: MoexClient) -> list[dict[str, Any]]:
    progress = tqdm(total=1, desc="Список облигаций MOEX", unit="запрос", dynamic_ncols=True)
    url = (
        "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
        "?iss.meta=off&is_traded=1"
        "&iss.only=securities,marketdata"
        "&securities.columns=SECID,ISIN,SHORTNAME,SECNAME,MATDATE,OFFERDATE,COUPONPERIOD,ACCRUEDINT,"
        "COUPONPERCENT,BONDTYPE,BONDSUBTYPE,PREVPRICE"
        "&marketdata.columns=SECID,LAST,LCURRENTPRICE,LCLOSEPRICE,PREVPRICE,VOLTODAY,VALTODAY"
    )
    payload = await client.get_json(url)
    progress.update(1)
    progress.close()

    sec_cols = payload["securities"]["columns"]
    md_cols = payload["marketdata"]["columns"]
    sec_data = [dict(zip(sec_cols, row)) for row in payload["securities"]["data"]]
    md_data = {row[0]: dict(zip(md_cols, row)) for row in payload["marketdata"]["data"]}
    records: list[dict[str, Any]] = []
    for row in sec_data:
        row["marketdata"] = md_data.get(row["SECID"], {})
        records.append(row)

    uniq: dict[str, dict[str, Any]] = {}
    for row in records:
        secid = str(row.get("SECID") or "")
        if not secid:
            continue
        current = _pick_price(row)
        prev = row.get("PREVPRICE")
        score = (current is not None, prev is not None, float(row.get("marketdata", {}).get("VALTODAY") or 0.0))
        if secid not in uniq:
            uniq[secid] = row | {"_score": score}
        elif score > uniq[secid]["_score"]:
            uniq[secid] = row | {"_score": score}
    return [{k: v for k, v in row.items() if k != "_score"} for row in uniq.values()]


async def _fetch_volume_20d(client: MoexClient) -> dict[str, float]:
    start_dt = (date.today() - timedelta(days=20)).isoformat()
    end_dt = date.today().isoformat()
    aggregated: dict[str, float] = {}
    start = 0
    page_size = 100
    progress = tqdm(desc="История объема 20д", unit="стр", dynamic_ncols=True)
    while True:
        url = (
            "https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json"
            f"?iss.meta=off&iss.only=history&from={start_dt}&till={end_dt}"
            "&history.columns=SECID,VOLUME"
            f"&start={start}"
        )
        payload = await client.get_json(url)
        cols = payload["history"]["columns"]
        rows = payload["history"]["data"]
        if not rows:
            break
        for row in rows:
            data = dict(zip(cols, row))
            secid = str(data.get("SECID") or "")
            if secid:
                aggregated[secid] = aggregated.get(secid, 0.0) + float(data.get("VOLUME") or 0.0)
        progress.update(len(rows))
        start += len(rows)
        if len(rows) < page_size:
            break
    progress.close()
    return aggregated


def _pick_price(row: dict[str, Any]) -> float | None:
    md = row.get("marketdata", {})
    for key in ("LAST", "LCURRENTPRICE", "LCLOSEPRICE", "PREVPRICE"):
        value = md.get(key)
        if value is not None:
            return float(value)
    prev = row.get("PREVPRICE")
    return float(prev) if prev is not None else None


async def _fetch_bond_description(client: MoexClient, secid: str, semaphore: asyncio.Semaphore) -> tuple[str, dict[str, Any]]:
    async with semaphore:
        url = f"https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
        payload = await client.get_json(url)
        cols = payload["description"]["columns"]
        values = {row[0]: dict(zip(cols, row)).get("value") for row in payload["description"]["data"]}
        return secid, values


async def _fetch_emitters(client: MoexClient, emitter_ids: set[int], semaphore: asyncio.Semaphore) -> dict[int, dict[str, str]]:
    result: dict[int, dict[str, str]] = {}

    async def fetch_one(emitter_id: int) -> tuple[int, dict[str, str]]:
        async with semaphore:
            payload = await client.get_json(f"https://iss.moex.com/iss/emitters/{emitter_id}.json?iss.meta=off")
            cols = payload["emitter"]["columns"]
            data_rows = payload["emitter"]["data"]
            if not data_rows:
                return emitter_id, {"name": "", "inn": ""}
            data = dict(zip(cols, data_rows[0]))
            return emitter_id, {"name": str(data.get("TITLE") or ""), "inn": str(data.get("INN") or "")}

    tasks = [asyncio.create_task(fetch_one(emitter_id)) for emitter_id in sorted(emitter_ids)]
    if tasks:
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Загрузка эмитентов", unit="эмит", dynamic_ncols=True):
            try:
                emitter_id, emitter_data = await task
                result[emitter_id] = emitter_data
            except Exception as exc:
                LOGGER.warning("Не удалось загрузить эмитента: %s", exc)
    return result


async def _fetch_amortizations(client: MoexClient, secids: list[str], semaphore: asyncio.Semaphore) -> dict[str, str]:
    result: dict[str, str] = {}

    async def fetch_one(secid: str) -> tuple[str, str]:
        async with semaphore:
            url = f"https://iss.moex.com/iss/securities/{secid}/bondization.json?iss.meta=off&iss.only=amortizations"
            payload = await client.get_json(url)
            rows = payload.get("amortizations", {}).get("data", [])
            cols = payload.get("amortizations", {}).get("columns", [])
            if not rows or not cols:
                return secid, ""

            earliest: date | None = None
            for row in rows:
                data = dict(zip(cols, row))
                amort_date_raw = data.get("amortdate")
                face_value = data.get("facevalue")
                initial_face_value = data.get("initialfacevalue")
                value_prc = data.get("valueprc")
                if not amort_date_raw:
                    continue
                has_amort = False
                if face_value is not None and initial_face_value is not None:
                    try:
                        has_amort = float(face_value) < float(initial_face_value)
                    except Exception:
                        has_amort = False
                if not has_amort and value_prc is not None:
                    try:
                        has_amort = float(value_prc) < 100.0
                    except Exception:
                        has_amort = False
                if not has_amort:
                    continue
                parsed = datetime.fromisoformat(str(amort_date_raw)).date()
                if earliest is None or parsed < earliest:
                    earliest = parsed
            return secid, earliest.strftime("%d.%m.%Y") if earliest else ""

    tasks = [asyncio.create_task(fetch_one(secid)) for secid in secids]
    if tasks:
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Амортизация", unit="обл", dynamic_ncols=True):
            try:
                secid, amort = await task
                result[secid] = amort
            except Exception as exc:
                LOGGER.warning("Не удалось загрузить амортизацию: %s", exc)
    return result


def _to_yes_no(value: Any) -> str:
    return "Да" if str(value) in {"1", "true", "True"} else "Нет"


def _format_date(value: Any) -> str:
    if not value:
        return ""
    text = str(value)
    try:
        return datetime.fromisoformat(text).strftime("%d.%m.%Y")
    except Exception:
        return text


def _save_excel(rows: list[BondRow], output_path: Path) -> int:
    wb = Workbook()
    ws = wb.active
    ws.title = config.EXCEL_SHEET_NAME

    headers = [
        "Secid",
        "ISIN",
        "Короткое название",
        "Наименование Эмитента",
        "ИНН эмитента",
        "Кредитный рейтинг эмитента",
        "Дата рейтинга эмитента",
        "Raiting_discription",
        "Актуальная Цена сейчас",
        "Предыдущая цена выгрузки",
        "Динамика цены, %",
        "Объем сделок по бумаге",
        "Объем сделок за 20 дней",
        "Дата погашения",
        "Дата оферты",
        "Дата начала амортизации",
        "Квалифицированный инвестор",
        "Дефолт",
        "Технический дефолт",
        "BOND_TYPE",
        "SECSUBTYPE",
        "Купонный период",
        "НКД",
        "% купона",
    ]

    ws.append(headers)
    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    for cell in ws[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row in rows:
        ws.append(
            [
                row.secid,
                row.isin,
                row.short_name,
                row.emitter_name,
                row.emitter_inn,
                row.credit_rating,
                row.rating_date,
                row.rating_description,
                row.current_price,
                row.previous_price,
                row.price_change_percent,
                row.volume_today,
                row.volume_20d,
                row.maturity_date,
                row.offer_date,
                row.amortization_start,
                row.qualified_investor,
                row.default_flag,
                row.technical_default_flag,
                row.bond_type,
                row.sec_sub_type,
                row.coupon_period,
                row.accrued_int,
                row.coupon_percent,
            ]
        )

    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"

    for column in ws.columns:
        max_len = 0
        col_letter = column[0].column_letter
        for cell in column:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(value))
            if col_letter == "H":
                cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.column_dimensions[col_letter].width = min(max_len + 2, 60)

    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    ws.conditional_formatting.add(
        f"K2:K{ws.max_row}",
        CellIsRule(operator="greaterThan", formula=["0"], fill=green_fill),
    )
    ws.conditional_formatting.add(
        f"K2:K{ws.max_row}",
        CellIsRule(operator="lessThan", formula=["0"], fill=red_fill),
    )

    wb.save(output_path)
    return len(rows)


async def run_pipeline(db: Database) -> RunSummary:
    total_start = time.perf_counter()
    state = load_state()
    load_start = time.perf_counter()
    errors_count = 0
    from_cache_count = 0

    client = MoexClient()
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_TASKS)
    bonds: list[dict[str, Any]] = []
    descriptions: dict[str, dict[str, Any]] = {}
    emitters: dict[int, dict[str, str]] = {}
    amortizations: dict[str, str] = {}
    volume_20d: dict[str, float] = {}

    try:
        LOGGER.info("Этап 1/4: загрузка списка облигаций")
        bonds = await _fetch_all_traded_bonds(client)
        LOGGER.info("Получено строк после дедупликации: %s", len(bonds))

        LOGGER.info("Этап 2/4: загрузка истории объемов")
        volume_20d = await _fetch_volume_20d(client)

        processed_ids: list[str] = list(state.get("processed_ids", []))
        secids = [str(bond.get("SECID")) for bond in bonds if bond.get("SECID")]
        desc_tasks = [asyncio.create_task(_fetch_bond_description(client, secid, semaphore)) for secid in secids]
        progress = tqdm(total=len(desc_tasks), desc="Описание облигаций", unit="обл", dynamic_ncols=True)
        for task in asyncio.as_completed(desc_tasks):
            try:
                secid, desc = await task
                descriptions[secid] = desc
                processed_ids.append(secid)
                save_state({"processed_ids": processed_ids, "last_stage": "description"})
            except Exception as exc:
                errors_count += 1
                LOGGER.warning("Ошибка загрузки описания: %s", exc)
            progress.update(1)
        progress.close()

        LOGGER.info("Этап 3/4: загрузка карточек эмитентов")
        emitter_ids: set[int] = set()
        for desc in descriptions.values():
            emitter_id = desc.get("EMITTER_ID")
            if emitter_id is None:
                continue
            try:
                emitter_ids.add(int(emitter_id))
            except Exception:
                continue
        emitters = await _fetch_emitters(client, emitter_ids, semaphore)

        LOGGER.info("Этап 4/4: загрузка амортизаций")
        amortizations = await _fetch_amortizations(client, secids, semaphore)

    finally:
        await client.close()

    duration_load = time.perf_counter() - load_start

    calc_start = time.perf_counter()
    previous_prices = db.fetch_previous_prices()
    prepared_rows: list[BondRow] = []

    progress_calc = tqdm(total=len(bonds), desc="Подготовка строк", unit="обл", dynamic_ncols=True)
    for bond in bonds:
        secid = str(bond.get("SECID") or "")
        if not secid:
            progress_calc.update(1)
            continue
        desc = descriptions.get(secid, {})
        emitter_id_raw = desc.get("EMITTER_ID")
        emitter_info = {"name": "", "inn": ""}
        if emitter_id_raw is not None:
            try:
                emitter_info = emitters.get(int(emitter_id_raw), emitter_info)
            except Exception:
                pass

        current_price = _pick_price(bond)
        previous_price = previous_prices.get(secid)
        if previous_price is not None and current_price not in (None, 0):
            price_change = ((current_price - previous_price) / previous_price) * 100
        else:
            price_change = None

        prepared_rows.append(
            BondRow(
                secid=secid,
                isin=str(bond.get("ISIN") or ""),
                short_name=str(bond.get("SHORTNAME") or ""),
                emitter_name=emitter_info["name"],
                emitter_inn=emitter_info["inn"],
                credit_rating="",
                rating_date="",
                rating_description="",
                current_price=current_price,
                previous_price=previous_price,
                price_change_percent=round(price_change, 4) if price_change is not None else None,
                volume_today=float(bond.get("marketdata", {}).get("VOLTODAY") or 0.0),
                volume_20d=round(volume_20d.get(secid, 0.0), 2),
                maturity_date=_format_date(bond.get("MATDATE")),
                offer_date=_format_date(bond.get("OFFERDATE")),
                amortization_start=amortizations.get(secid, ""),
                qualified_investor=_to_yes_no(desc.get("ISQUALIFIEDINVESTORS")),
                default_flag=_to_yes_no(desc.get("HASDEFAULT")),
                technical_default_flag=_to_yes_no(desc.get("HASTECHNICALDEFAULT")),
                bond_type=str(desc.get("BOND_TYPE") or bond.get("BONDTYPE") or ""),
                sec_sub_type=str(desc.get("BOND_SUBTYPE") or bond.get("BONDSUBTYPE") or ""),
                coupon_period=int(bond.get("COUPONPERIOD")) if bond.get("COUPONPERIOD") is not None else None,
                accrued_int=float(bond.get("ACCRUEDINT")) if bond.get("ACCRUEDINT") is not None else None,
                coupon_percent=float(bond.get("COUPONPERCENT")) if bond.get("COUPONPERCENT") is not None else None,
            )
        )
        progress_calc.update(1)
    progress_calc.close()

    prepared_rows.sort(key=lambda x: x.secid)

    snapshot_rows = [(row.secid, row.current_price, datetime.now(timezone.utc).isoformat()) for row in prepared_rows]
    db.upsert_snapshot(snapshot_rows)
    save_state({"processed_ids": [x.secid for x in prepared_rows], "last_stage": "calc"})
    duration_calc = time.perf_counter() - calc_start

    save_start = time.perf_counter()
    output_path = config.get_output_file_path()
    saved_count = _save_excel(prepared_rows, output_path)
    save_state({"processed_ids": [x.secid for x in prepared_rows], "last_stage": "done"})
    duration_save = time.perf_counter() - save_start

    return RunSummary(
        fetched_count=len(bonds),
        selected_count=len(prepared_rows),
        saved_count=saved_count,
        errors_count=errors_count,
        from_cache_count=from_cache_count,
        duration_total=time.perf_counter() - total_start,
        duration_load=duration_load,
        duration_calc=duration_calc,
        duration_save=duration_save,
        output_path=output_path,
    )
