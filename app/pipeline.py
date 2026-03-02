from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
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
    rating_date: date | None
    rating_description: str
    current_price: float | None
    previous_price: float | None
    price_change_percent: float | None
    volume_today: float
    volume_20d: float
    maturity_date: date | None
    offer_date: date | None
    amortization_start: date | None
    qualified_investor: str
    default_flag: str
    technical_default_flag: str
    bond_type: str
    sec_sub_type: str
    coupon_period: int | None
    accrued_int: float | None
    coupon_percent: float | None


class MoexClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> MoexClient:
        timeout = aiohttp.ClientTimeout(
            total=config.REQUEST_CONNECT_TIMEOUT_SEC + config.REQUEST_READ_TIMEOUT_SEC,
            connect=config.REQUEST_CONNECT_TIMEOUT_SEC,
            sock_read=config.REQUEST_READ_TIMEOUT_SEC,
        )
        connector = aiohttp.TCPConnector(limit=max(16, config.MAX_CONCURRENT_TASKS * 2), ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_json(self, url: str) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("HTTP-сессия не инициализирована")
        last_exc: Exception | None = None
        for attempt in range(1, config.REQUEST_RETRIES + 1):
            try:
                async with self._session.get(url) as response:
                    response.raise_for_status()
                    return await response.json(content_type=None)
            except Exception as exc:
                last_exc = exc
                if attempt == config.REQUEST_RETRIES:
                    break
                await asyncio.sleep(config.REQUEST_BACKOFF_SEC * (2 ** (attempt - 1)))
        raise RuntimeError(f"Не удалось получить данные: {url}. Ошибка: {last_exc}")


def _as_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        try:
            return datetime.strptime(text, "%d.%m.%Y").date()
        except Exception:
            return None


def _to_yes_no(value: Any) -> str:
    normalized = str(value).strip().lower()
    return "Да" if normalized in {"1", "true"} else "Нет"


def _pick_price(row: dict[str, Any]) -> float | None:
    md = row.get("marketdata", {})
    for key in ("LAST", "LCURRENTPRICE", "LCLOSEPRICE", "PREVPRICE"):
        value = md.get(key)
        if value is not None:
            return float(value)
    prev = row.get("PREVPRICE")
    return float(prev) if prev is not None else None


def _extract_rating(desc: dict[str, Any]) -> tuple[str, date | None, str]:
    rating_keys = (
        "__RATING_VALUE",
        "CREDIT_RATING",
        "EMITTER_CREDIT_RATING",
        "RATING",
        "CREDITRATING",
    )
    rating_date_keys = (
        "__RATING_DATE",
        "CREDIT_RATING_DATE",
        "RATING_DATE",
        "EMITTER_RATING_DATE",
    )
    rating_desc_keys = (
        "__RATING_DESCRIPTION",
        "RATING_DESCRIPTION",
        "RATING_DISCRIPTION",
        "RATING_DESC",
        "EMITTER_RATING_DESCRIPTION",
    )

    credit_rating = ""
    for key in rating_keys:
        if desc.get(key):
            credit_rating = str(desc[key])
            break
    if not credit_rating:
        for key, value in desc.items():
            upper = str(key).upper()
            if "RATING" in upper and value not in (None, "") and "DATE" not in upper and "DESCR" not in upper:
                credit_rating = str(value)
                break

    rating_date: date | None = None
    for key in rating_date_keys:
        parsed = _as_date(desc.get(key))
        if parsed is not None:
            rating_date = parsed
            break

    rating_description = ""
    for key in rating_desc_keys:
        if desc.get(key):
            rating_description = str(desc[key])
            break
    if not rating_description:
        for key, value in desc.items():
            upper = str(key).upper()
            if "RATING" in upper and ("DESCR" in upper or "COMMENT" in upper) and value not in (None, ""):
                rating_description = str(value)
                break

    return credit_rating, rating_date, rating_description


async def _fetch_all_traded_bonds(client: MoexClient) -> list[dict[str, Any]]:
    url = (
        "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
        "?iss.meta=off&is_traded=1"
        "&iss.only=securities,marketdata"
        "&securities.columns=SECID,ISIN,SHORTNAME,SECNAME,MATDATE,OFFERDATE,COUPONPERIOD,ACCRUEDINT,"
        "COUPONPERCENT,BONDTYPE,BONDSUBTYPE,PREVPRICE"
        "&marketdata.columns=SECID,LAST,LCURRENTPRICE,LCLOSEPRICE,PREVPRICE,VOLTODAY,VALTODAY"
    )
    payload = await client.get_json(url)
    sec_cols = payload["securities"]["columns"]
    md_cols = payload["marketdata"]["columns"]
    sec_data = [dict(zip(sec_cols, row)) for row in payload["securities"]["data"]]
    md_data = {row[0]: dict(zip(md_cols, row)) for row in payload["marketdata"]["data"]}

    uniq: dict[str, dict[str, Any]] = {}
    for row in sec_data:
        secid = str(row.get("SECID") or "")
        if not secid:
            continue
        row["marketdata"] = md_data.get(secid, {})
        current = _pick_price(row)
        prev = row.get("PREVPRICE")
        score = (current is not None, prev is not None, float(row.get("marketdata", {}).get("VALTODAY") or 0.0))
        if secid not in uniq or score > uniq[secid]["_score"]:
            uniq[secid] = row | {"_score": score}

    return [{k: v for k, v in row.items() if k != "_score"} for row in uniq.values()]


async def _fetch_volume_20d(client: MoexClient) -> dict[str, float]:
    start_dt = (date.today() - timedelta(days=20)).isoformat()
    end_dt = date.today().isoformat()
    aggregated: dict[str, float] = {}
    start = 0
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
        if len(rows) < 100:
            break
    progress.close()
    return aggregated


async def _fetch_descriptions_with_cache(
    client: MoexClient,
    db: Database,
    secids: list[str],
    semaphore: asyncio.Semaphore,
) -> tuple[dict[str, dict[str, Any]], int, int]:
    min_ts = int(time.time()) - config.CACHE_TTL_SEC
    cached_raw, missing = db.get_cached_descriptions(secids, min_ts)
    result: dict[str, dict[str, Any]] = {}
    for secid, payload in cached_raw.items():
        parsed = json.loads(payload)
        result[secid] = parsed
        has_rating_markers = any(key in parsed for key in ("__RATING_VALUE", "__RATING_DATE", "__RATING_DESCRIPTION"))
        if not has_rating_markers:
            missing.append(secid)
            result.pop(secid, None)

    async def fetch_one(secid: str) -> tuple[str, dict[str, Any]]:
        async with semaphore:
            url = f"https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
            payload = await client.get_json(url)
            cols = payload["description"]["columns"]
            values: dict[str, Any] = {}
            rating_value = ""
            rating_date = ""
            rating_description = ""
            for row in payload["description"]["data"]:
                item = dict(zip(cols, row))
                key = str(item.get("name") or "")
                title = str(item.get("title") or "").lower()
                value = item.get("value")
                if key:
                    values[key] = value
                if value in (None, ""):
                    continue
                value_text = str(value).strip()
                if not value_text:
                    continue
                if ("рейтинг" in title or "rating" in title) and "дата" not in title and "date" not in title:
                    if "опис" in title or "коммент" in title or "прогноз" in title or "outlook" in title:
                        if not rating_description:
                            rating_description = value_text
                    elif not rating_value:
                        rating_value = value_text
                if ("рейтинг" in title or "rating" in title) and ("дата" in title or "date" in title):
                    if not rating_date:
                        rating_date = value_text
            if rating_value:
                values["__RATING_VALUE"] = rating_value
            if rating_date:
                values["__RATING_DATE"] = rating_date
            if rating_description:
                values["__RATING_DESCRIPTION"] = rating_description
            return secid, values

    missing = list(dict.fromkeys(missing))
    cache_hits = len(result)

    upserts: list[tuple[str, str, int]] = []
    errors = 0
    tasks = [asyncio.create_task(fetch_one(secid)) for secid in missing]
    progress = tqdm(total=len(tasks), desc="Описание облигаций", unit="обл", dynamic_ncols=True)
    for task in asyncio.as_completed(tasks):
        try:
            secid, desc = await task
            result[secid] = desc
            upserts.append((secid, json.dumps(desc, ensure_ascii=False), int(time.time())))
        except Exception as exc:
            errors += 1
            LOGGER.warning("Ошибка загрузки описания: %s", exc)
        progress.update(1)
    progress.close()
    db.upsert_descriptions(upserts)
    return result, cache_hits, errors


async def _fetch_emitters_with_cache(
    client: MoexClient,
    db: Database,
    emitter_ids: set[int],
    semaphore: asyncio.Semaphore,
) -> tuple[dict[int, dict[str, str]], int, int]:
    emitter_list = sorted(emitter_ids)
    min_ts = int(time.time()) - config.CACHE_TTL_SEC
    cached, missing = db.get_cached_emitters(emitter_list, min_ts)
    result = dict(cached)

    async def fetch_one(emitter_id: int) -> tuple[int, dict[str, str]]:
        async with semaphore:
            payload = await client.get_json(f"https://iss.moex.com/iss/emitters/{emitter_id}.json?iss.meta=off")
            cols = payload["emitter"]["columns"]
            data_rows = payload["emitter"]["data"]
            if not data_rows:
                return emitter_id, {"name": "", "inn": ""}
            data = dict(zip(cols, data_rows[0]))
            return emitter_id, {"name": str(data.get("TITLE") or ""), "inn": str(data.get("INN") or "")}

    tasks = [asyncio.create_task(fetch_one(emitter_id)) for emitter_id in missing]
    upserts: list[tuple[int, str, str, int]] = []
    errors = 0
    progress = tqdm(total=len(tasks), desc="Загрузка эмитентов", unit="эмит", dynamic_ncols=True)
    for task in asyncio.as_completed(tasks):
        try:
            emitter_id, emitter_data = await task
            result[emitter_id] = emitter_data
            upserts.append((emitter_id, emitter_data["name"], emitter_data["inn"], int(time.time())))
        except Exception as exc:
            errors += 1
            LOGGER.warning("Не удалось загрузить эмитента: %s", exc)
        progress.update(1)
    progress.close()
    db.upsert_emitters(upserts)
    return result, len(cached), errors


async def _fetch_amortizations_with_cache(
    client: MoexClient,
    db: Database,
    secids: list[str],
    semaphore: asyncio.Semaphore,
) -> tuple[dict[str, date | None], int, int]:
    min_ts = int(time.time()) - config.CACHE_TTL_SEC
    cached_raw, missing = db.get_cached_amortizations(secids, min_ts)
    result: dict[str, date | None] = {secid: _as_date(value) for secid, value in cached_raw.items()}

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
                parsed = _as_date(amort_date_raw)
                if parsed is None:
                    continue
                face_value = data.get("facevalue")
                initial_face_value = data.get("initialfacevalue")
                value_prc = data.get("valueprc")
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
                if has_amort and (earliest is None or parsed < earliest):
                    earliest = parsed
            return secid, earliest.isoformat() if earliest else ""

    tasks = [asyncio.create_task(fetch_one(secid)) for secid in missing]
    upserts: list[tuple[str, str, int]] = []
    errors = 0
    progress = tqdm(total=len(tasks), desc="Амортизация", unit="обл", dynamic_ncols=True)
    for task in asyncio.as_completed(tasks):
        try:
            secid, amort = await task
            result[secid] = _as_date(amort)
            upserts.append((secid, amort, int(time.time())))
        except Exception as exc:
            errors += 1
            LOGGER.warning("Не удалось загрузить амортизацию: %s", exc)
        progress.update(1)
    progress.close()
    db.upsert_amortizations(upserts)
    return result, cache_hits, errors


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

    for row_idx in range(2, ws.max_row + 1):
        for col in (7, 14, 15, 16):
            cell = ws.cell(row=row_idx, column=col)
            if isinstance(cell.value, date):
                cell.number_format = "yyyy-mm-dd"

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
    ws.conditional_formatting.add(f"K2:K{ws.max_row}", CellIsRule(operator="greaterThan", formula=["0"], fill=green_fill))
    ws.conditional_formatting.add(f"K2:K{ws.max_row}", CellIsRule(operator="lessThan", formula=["0"], fill=red_fill))

    wb.save(output_path)
    return len(rows)


async def run_pipeline(db: Database) -> RunSummary:
    total_start = time.perf_counter()
    state = load_state()
    load_start = time.perf_counter()
    errors_count = 0
    from_cache_count = 0

    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_TASKS)
    bonds: list[dict[str, Any]] = []
    descriptions: dict[str, dict[str, Any]] = {}
    emitters: dict[int, dict[str, str]] = {}
    amortizations: dict[str, date | None] = {}

    async with MoexClient() as client:
        LOGGER.info("Этап 1/4: загрузка списка облигаций")
        bonds = await _fetch_all_traded_bonds(client)
        LOGGER.info("Получено строк после дедупликации: %s", len(bonds))

        LOGGER.info("Этап 2/4: загрузка истории объемов")
        volume_20d = await _fetch_volume_20d(client)

        secids = [str(bond.get("SECID")) for bond in bonds if bond.get("SECID")]
        descriptions, cache_desc_count, desc_errors = await _fetch_descriptions_with_cache(client, db, secids, semaphore)
        from_cache_count += cache_desc_count
        errors_count += desc_errors
        save_state({"processed_ids": list(descriptions.keys()), "last_stage": "description"})

        LOGGER.info("Этап 3/4: загрузка карточек эмитентов")
        emitter_ids: set[int] = set()
        for desc in descriptions.values():
            emitter_id = desc.get("EMITTER_ID")
            if emitter_id is not None:
                try:
                    emitter_ids.add(int(emitter_id))
                except Exception:
                    continue
        emitters, cache_emit_count, emit_errors = await _fetch_emitters_with_cache(client, db, emitter_ids, semaphore)
        from_cache_count += cache_emit_count
        errors_count += emit_errors

        LOGGER.info("Этап 4/4: загрузка амортизаций")
        amortizations, cache_amort_count, amort_errors = await _fetch_amortizations_with_cache(client, db, secids, semaphore)
        from_cache_count += cache_amort_count
        errors_count += amort_errors

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
        if previous_price not in (None, 0) and current_price is not None:
            price_change = ((current_price - previous_price) / previous_price) * 100
        else:
            price_change = None

        credit_rating, rating_date, rating_description = _extract_rating(desc)

        prepared_rows.append(
            BondRow(
                secid=secid,
                isin=str(bond.get("ISIN") or ""),
                short_name=str(bond.get("SHORTNAME") or ""),
                emitter_name=emitter_info["name"],
                emitter_inn=emitter_info["inn"],
                credit_rating=credit_rating,
                rating_date=rating_date,
                rating_description=rating_description,
                current_price=current_price,
                previous_price=previous_price,
                price_change_percent=round(price_change, 4) if price_change is not None else None,
                volume_today=float(bond.get("marketdata", {}).get("VOLTODAY") or 0.0),
                volume_20d=round(volume_20d.get(secid, 0.0), 2),
                maturity_date=_as_date(bond.get("MATDATE")),
                offer_date=_as_date(bond.get("OFFERDATE")),
                amortization_start=amortizations.get(secid),
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
    db.upsert_snapshot([(row.secid, row.current_price, datetime.now(timezone.utc).isoformat()) for row in prepared_rows])
    save_state({"processed_ids": [x.secid for x in prepared_rows], "last_stage": "calc", "prev_stage": state.get("last_stage", "init")})
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
