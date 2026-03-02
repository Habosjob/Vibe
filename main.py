import asyncio
import hashlib
import json
import logging
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from tqdm import tqdm

import config


@dataclass
class Summary:
    received_total: int = 0
    active_total: int = 0
    matured_excluded: int = 0
    no_trades_10d_total: int = 0
    excel_exported_rows: int = 0
    errors_total: int = 0
    cache_hits: int = 0
    excel_warn_locked: bool = False


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}
        if not self.data:
            self.data = {"processed_secids": [], "stages": {}, "updated_at": None}

    def save(self) -> None:
        self.data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def mark_stage(self, name: str, value: Any) -> None:
        self.data.setdefault("stages", {})[name] = value
        self.save()

    def mark_processed(self, secid: str) -> None:
        processed = set(self.data.get("processed_secids", []))
        processed.add(secid)
        self.data["processed_secids"] = sorted(processed)


class ProgressReporter:
    def __init__(self, total: int, logger: logging.Logger) -> None:
        self.total = total
        self.logger = logger
        self.current = 0
        self.started_at = time.perf_counter()
        self.last_report_at = self.started_at
        self._use_tqdm = bool(config.SHOW_PROGRESS_BAR and total > 0 and sys.stdout.isatty())
        self._bar: tqdm | None = None
        if self._use_tqdm:
            self._bar = tqdm(
                total=total,
                desc="Обработка облигаций",
                unit="bond",
                dynamic_ncols=True,
                leave=True,
                file=sys.stdout,
            )
        elif total > 0:
            print(f"Обработка облигаций: 0/{total} (0%)")

    def update(self, amount: int = 1) -> None:
        self.current += amount
        if self._bar:
            self._bar.update(amount)
            return
        if self.total <= 0:
            return
        now = time.perf_counter()
        should_print = self.current >= self.total or (now - self.last_report_at) >= config.PROGRESS_FALLBACK_INTERVAL_SEC
        if not should_print:
            return
        elapsed = max(now - self.started_at, 0.001)
        per_item = elapsed / max(self.current, 1)
        eta = max(self.total - self.current, 0) * per_item
        percent = (self.current / self.total) * 100
        print(f"Обработка облигаций: {self.current}/{self.total} ({percent:.1f}%), ETA {eta:.1f} сек")
        self.last_report_at = now

    def close(self) -> None:
        if self._bar:
            self._bar.close()
        elif self.total > 0 and self.current < self.total:
            print(f"Обработка облигаций: {self.total}/{self.total} (100.0%), ETA 0.0 сек")


class DB:
    def __init__(self, path: Path) -> None:
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._create_schema()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS issuers (
                emitter_id TEXT PRIMARY KEY,
                issuer_name TEXT,
                inn TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS issuer_ratings (
                emitter_id TEXT,
                agency TEXT,
                rating TEXT,
                rating_date TEXT,
                status TEXT,
                raw_json TEXT,
                PRIMARY KEY (emitter_id, agency, rating, rating_date)
            );
            CREATE TABLE IF NOT EXISTS coupons (
                secid TEXT,
                coupon_date TEXT,
                coupon_value REAL,
                coupon_percent REAL,
                PRIMARY KEY (secid, coupon_date)
            );
            CREATE TABLE IF NOT EXISTS amortizations (
                secid TEXT,
                amort_date TEXT,
                amort_value REAL,
                PRIMARY KEY (secid, amort_date)
            );
            CREATE TABLE IF NOT EXISTS liquidity_10d (
                secid TEXT PRIMARY KEY,
                from_date TEXT,
                till_date TEXT,
                trades_total REAL,
                volume_total REAL,
                computed_at TEXT,
                method_fields TEXT
            );
            CREATE TABLE IF NOT EXISTS securities (
                secid TEXT PRIMARY KEY,
                isin TEXT,
                shortname TEXT,
                issuer_name TEXT,
                issuer_inn TEXT,
                issuer_credit_rating TEXT,
                issuer_rating_date TEXT,
                rating_description TEXT,
                price_current REAL,
                price_prev_export REAL,
                price_change_pct REAL,
                matdate TEXT,
                offerdate TEXT,
                amort_start_date TEXT,
                amort_has_started INTEGER,
                amort_starts_within_1y INTEGER,
                qualified_only TEXT,
                default_flag TEXT,
                technical_default_flag TEXT,
                bond_type TEXT,
                secsubtype TEXT,
                coupon_count INTEGER,
                coupon_period_days REAL,
                nkd REAL,
                coupon_percent REAL,
                trades_10d_total REAL,
                volume_10d_total REAL,
                no_trades_10d_flag INTEGER,
                emitter_id TEXT,
                source_hash TEXT,
                inactive_flag INTEGER DEFAULT 0,
                updated_at TEXT,
                last_exported_at TEXT
            );
            """
        )
        self.conn.commit()

    def get_prev_price(self, secid: str) -> float | None:
        row = self.conn.execute("SELECT price_current FROM securities WHERE secid=?", (secid,)).fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def get_source_hash(self, secid: str) -> str | None:
        row = self.conn.execute("SELECT source_hash FROM securities WHERE secid=?", (secid,)).fetchone()
        return str(row[0]) if row and row[0] else None

    def upsert_issuer(self, emitter_id: str, issuer_name: str | None, inn: str | None) -> None:
        self.conn.execute(
            """
            INSERT INTO issuers (emitter_id, issuer_name, inn, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(emitter_id) DO UPDATE SET
                issuer_name=excluded.issuer_name,
                inn=excluded.inn,
                updated_at=excluded.updated_at
            """,
            (emitter_id, issuer_name, inn, datetime.now().isoformat(timespec="seconds")),
        )

    def replace_ratings(self, emitter_id: str, rows: list[dict[str, Any]]) -> None:
        self.conn.execute("DELETE FROM issuer_ratings WHERE emitter_id=?", (emitter_id,))
        for r in rows:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO issuer_ratings
                (emitter_id, agency, rating, rating_date, status, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    emitter_id,
                    r.get("agency"),
                    r.get("rating"),
                    r.get("rating_date"),
                    r.get("status"),
                    json.dumps(r, ensure_ascii=False),
                ),
            )

    def replace_coupons(self, secid: str, rows: list[dict[str, Any]]) -> None:
        self.conn.execute("DELETE FROM coupons WHERE secid=?", (secid,))
        for r in rows:
            self.conn.execute(
                "INSERT OR REPLACE INTO coupons (secid, coupon_date, coupon_value, coupon_percent) VALUES (?, ?, ?, ?)",
                (secid, r.get("coupon_date"), r.get("coupon_value"), r.get("coupon_percent")),
            )

    def replace_amortizations(self, secid: str, rows: list[dict[str, Any]]) -> None:
        self.conn.execute("DELETE FROM amortizations WHERE secid=?", (secid,))
        for r in rows:
            self.conn.execute(
                "INSERT OR REPLACE INTO amortizations (secid, amort_date, amort_value) VALUES (?, ?, ?)",
                (secid, r.get("amort_date"), r.get("amort_value")),
            )

    def upsert_liquidity(self, secid: str, payload: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO liquidity_10d
            (secid, from_date, till_date, trades_total, volume_total, computed_at, method_fields)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(secid) DO UPDATE SET
                from_date=excluded.from_date,
                till_date=excluded.till_date,
                trades_total=excluded.trades_total,
                volume_total=excluded.volume_total,
                computed_at=excluded.computed_at,
                method_fields=excluded.method_fields
            """,
            (
                secid,
                payload.get("from_date"),
                payload.get("till_date"),
                payload.get("trades_10d_total"),
                payload.get("volume_10d_total"),
                datetime.now().isoformat(timespec="seconds"),
                payload.get("method_fields"),
            ),
        )

    def upsert_security(self, row: dict[str, Any]) -> None:
        keys = list(row.keys())
        columns = ",".join(keys)
        placeholders = ",".join(["?"] * len(keys))
        updates = ",".join([f"{k}=excluded.{k}" for k in keys if k != "secid"])
        self.conn.execute(
            f"INSERT INTO securities ({columns}) VALUES ({placeholders}) ON CONFLICT(secid) DO UPDATE SET {updates}",
            tuple(row[k] for k in keys),
        )

    def mark_inactive(self, secid: str) -> None:
        self.conn.execute(
            "UPDATE securities SET inactive_flag=1, updated_at=? WHERE secid=?",
            (datetime.now().isoformat(timespec="seconds"), secid),
        )

    def get_all_active_for_export(self) -> pd.DataFrame:
        query = "SELECT * FROM securities WHERE inactive_flag=0 ORDER BY secid"
        return pd.read_sql_query(query, self.conn)

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("moex_bonds")
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(message)s")

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        config.LOG_FILE_PATH,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(stream)
    logger.addHandler(file_handler)
    return logger


def validate_config() -> None:
    if config.MAX_CONCURRENCY < 1:
        raise ValueError("MAX_CONCURRENCY должен быть >= 1")
    if config.RETRY_COUNT < 0:
        raise ValueError("RETRY_COUNT должен быть >= 0")
    if config.LOOKBACK_TRADING_DAYS < 1:
        raise ValueError("LOOKBACK_TRADING_DAYS должен быть >= 1")
    if config.LIQUIDITY_LOOKBACK_CALENDAR_DAYS < config.LOOKBACK_TRADING_DAYS:
        raise ValueError("LIQUIDITY_LOOKBACK_CALENDAR_DAYS должен быть >= LOOKBACK_TRADING_DAYS")
    if config.PROGRESS_FALLBACK_INTERVAL_SEC <= 0:
        raise ValueError("PROGRESS_FALLBACK_INTERVAL_SEC должен быть > 0")


class MoexClient:
    def __init__(self, logger: logging.Logger, summary: Summary) -> None:
        timeout = httpx.Timeout(connect=config.HTTP_TIMEOUTS[0], read=config.HTTP_TIMEOUTS[1], write=config.HTTP_TIMEOUTS[1], pool=config.HTTP_TIMEOUTS[0])
        self.client = httpx.AsyncClient(base_url=config.MOEX_BASE_URL, timeout=timeout)
        self.sem = asyncio.Semaphore(config.MAX_CONCURRENCY)
        self.logger = logger
        self.summary = summary

    async def close(self) -> None:
        await self.client.aclose()

    async def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        for attempt in range(config.RETRY_COUNT + 1):
            try:
                async with self.sem:
                    resp = await self.client.get(url, params=params)
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError("5xx", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                if attempt >= config.RETRY_COUNT:
                    self.summary.errors_total += 1
                    self.logger.error("Request failed: %s params=%s err=%s", url, params, exc)
                    return None
                await asyncio.sleep(config.RETRY_BACKOFF * (2 ** attempt))
        return None

    @staticmethod
    def parse_block(data: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
        if not data or block_name not in data:
            return []
        block = data.get(block_name, {})
        columns = block.get("columns", [])
        rows = block.get("data", [])
        return [dict(zip(columns, row)) for row in rows]

    async def fetch_traded_bonds(self) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        start = 0
        while True:
            params = {
                "iss.meta": "off",
                "start": start,
                "securities.columns": "SECID,ISIN,SHORTNAME,MATDATE,OFFERDATE,IS_QUALIFIED_INVESTORS,BOND_TYPE,SECSUBTYPE,FACEUNIT",
            }
            payload = await self.get_json("/iss/engines/stock/markets/bonds/securities.json", params=params)
            if not payload:
                break
            rows = self.parse_block(payload, "securities")
            if not rows:
                break
            filtered = [r for r in rows if str(r.get("IS_TRADED", 1)) in {"1", "True", "true"} or r.get("IS_TRADED") is None]
            all_rows.extend(filtered)
            if len(rows) < 100:
                break
            start += 100
        return all_rows


RANK_MAP = {
    "AAA(RU)": 100,
    "AA+(RU)": 95,
    "AA(RU)": 90,
    "AA-(RU)": 85,
    "A+(RU)": 80,
    "A(RU)": 75,
    "A-(RU)": 70,
    "BBB+(RU)": 65,
    "BBB(RU)": 60,
    "BBB-(RU)": 55,
    "BB+(RU)": 50,
    "BB(RU)": 45,
    "BB-(RU)": 40,
    "B+(RU)": 35,
    "B(RU)": 30,
    "B-(RU)": 25,
    "CCC": 15,
    "CC": 10,
    "C": 5,
    "D": 1,
}


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value)
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    return None


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def rating_sort_key(item: dict[str, Any]) -> tuple[int, date]:
    rating = str(item.get("rating") or "").replace(" ", "").upper()
    rank = RANK_MAP.get(rating, 0)
    d = parse_date(item.get("rating_date")) or date(1900, 1, 1)
    return (rank, d)


def build_rating_fields(ratings: list[dict[str, Any]]) -> tuple[str | None, str | None, str]:
    lines: list[str] = []
    valid = []
    for r in ratings:
        agency = str(r.get("agency") or r.get("agency_name") or "")
        rating = str(r.get("rating") or r.get("value") or "")
        status = str(r.get("status") or "")
        d = parse_date(r.get("rating_date") or r.get("date"))
        d_str = d.strftime("%d.%m.%Y") if d else ""
        line = f"{agency} {rating} ({d_str})".strip()
        if status:
            line = f"{line} [{status}]"
        lines.append(line)
        if "withdraw" not in status.lower() and "отозв" not in status.lower() and rating:
            valid.append({"agency": agency, "rating": rating, "rating_date": d.isoformat() if d else None})
    if not valid:
        return None, None, "\n".join(lines)
    best = sorted(valid, key=rating_sort_key, reverse=True)[0]
    best_date = parse_date(best.get("rating_date"))
    return best.get("rating"), (best_date.isoformat() if best_date else None), "\n".join(lines)


async def process_one_bond(
    base: dict[str, Any],
    client: MoexClient,
    db: DB,
    logger: logging.Logger,
    today: date,
    missing_counter: dict[str, int],
) -> dict[str, Any] | None:
    secid = str(base.get("SECID") or "").strip()
    if not secid:
        return None

    matdate = parse_date(base.get("MATDATE"))
    if config.SKIP_INACTIVE_MATURED and matdate and matdate < today:
        db.mark_inactive(secid)
        return {"excluded_matured": True, "secid": secid}

    desc = await client.get_json(f"/iss/securities/{secid}.json", params={"iss.only": "description"})
    desc_rows = client.parse_block(desc or {}, "description")
    desc_map = {str(r.get("name") or "").upper(): r.get("value") for r in desc_rows}
    emitter_id = str(desc_map.get("EMITTER_ID") or "")

    issuer_name = None
    issuer_inn = None
    ratings_raw: list[dict[str, Any]] = []
    if emitter_id:
        company = await client.get_json(f"/iss/cci/info/companies/{emitter_id}.json")
        company_rows = client.parse_block(company or {}, "companies") or client.parse_block(company or {}, "company")
        if company_rows:
            issuer_name = company_rows[0].get("issuer_name") or company_rows[0].get("name")
            issuer_inn = company_rows[0].get("issuer_inn") or company_rows[0].get("inn")
        ratings_payload = await client.get_json(f"/iss/cci/rating/companies/{emitter_id}.json")
        ratings_raw = client.parse_block(ratings_payload or {}, "ratings") or client.parse_block(ratings_payload or {}, "company_ratings")
        cleaned_ratings = []
        for r in ratings_raw:
            cleaned_ratings.append(
                {
                    "agency": r.get("agency") or r.get("agency_name"),
                    "rating": r.get("rating") or r.get("value"),
                    "rating_date": (parse_date(r.get("rating_date") or r.get("date")) or date(1900, 1, 1)).isoformat(),
                    "status": r.get("status") or r.get("rating_status"),
                }
            )
        db.upsert_issuer(emitter_id, issuer_name, issuer_inn)
        db.replace_ratings(emitter_id, cleaned_ratings)

    issuer_credit_rating, issuer_rating_date, rating_description = build_rating_fields(ratings_raw)

    sec_data = await client.get_json(
        f"/iss/engines/stock/markets/bonds/securities/{secid}.json",
        params={"iss.meta": "off"},
    )
    market_rows = client.parse_block(sec_data or {}, "marketdata")
    sec_rows = client.parse_block(sec_data or {}, "securities")
    market = market_rows[0] if market_rows else {}
    sec_info = sec_rows[0] if sec_rows else {}

    last_price = to_float(market.get("LAST"))
    close_price = to_float(market.get("CLOSEPRICE") or market.get("LCLOSEPRICE"))
    price_current = last_price if last_price is not None else close_price
    if price_current is None:
        missing_counter["price_current"] = missing_counter.get("price_current", 0) + 1

    nkd = to_float(market.get("ACCRUEDINT") or market.get("NKD") or sec_info.get("ACCRUEDINT"))
    coupon_percent = to_float(sec_info.get("COUPONPERCENT"))

    bondization = await client.get_json(f"/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json", params={"iss.meta": "off"})
    coupons_rows_raw = client.parse_block(bondization or {}, "coupons")
    amort_rows_raw = client.parse_block(bondization or {}, "amortizations")

    coupons_rows = []
    for r in coupons_rows_raw:
        coupons_rows.append(
            {
                "coupon_date": (parse_date(r.get("coupondate") or r.get("date")) or date(1900, 1, 1)).isoformat(),
                "coupon_value": to_float(r.get("value") or r.get("couponvalue")),
                "coupon_percent": to_float(r.get("valueprc") or r.get("couponpercent")),
            }
        )
    amort_rows = []
    for r in amort_rows_raw:
        amort_rows.append(
            {
                "amort_date": (parse_date(r.get("amortdate") or r.get("date")) or date(1900, 1, 1)).isoformat(),
                "amort_value": to_float(r.get("value") or r.get("amortvalue")),
            }
        )

    db.replace_coupons(secid, coupons_rows)
    db.replace_amortizations(secid, amort_rows)

    valid_amort_dates = [parse_date(x.get("amort_date")) for x in amort_rows if to_float(x.get("amort_value") or 0) and parse_date(x.get("amort_date"))]
    amort_start_date = min(valid_amort_dates) if valid_amort_dates else None
    amort_has_started = bool(amort_start_date and amort_start_date <= today)
    amort_starts_within_1y = bool(amort_start_date and today <= amort_start_date <= today + timedelta(days=365))

    coupon_dates = sorted([parse_date(x.get("coupon_date")) for x in coupons_rows if parse_date(x.get("coupon_date"))])
    coupon_count = len(coupon_dates)
    coupon_period_days = None
    if len(coupon_dates) >= 2:
        diffs = [(coupon_dates[i + 1] - coupon_dates[i]).days for i in range(len(coupon_dates) - 1)]
        coupon_period_days = sum(diffs) / len(diffs) if diffs else None

    from_date = today - timedelta(days=config.LIQUIDITY_LOOKBACK_CALENDAR_DAYS)
    hist = await client.get_json(
        f"/iss/history/engines/stock/markets/bonds/securities/{secid}.json",
        params={"iss.meta": "off", "from": from_date.isoformat(), "till": today.isoformat()},
    )
    hist_rows = client.parse_block(hist or {}, "history")

    by_date: dict[str, dict[str, Any]] = {}
    for r in hist_rows:
        d = parse_date(r.get("TRADEDATE"))
        if not d:
            continue
        key = d.isoformat()
        by_date[key] = r

    last_trade_days = sorted(by_date.keys(), reverse=True)[: config.LOOKBACK_TRADING_DAYS]
    selected = [by_date[d] for d in last_trade_days]

    has_numtrades = any("NUMTRADES" in r for r in selected)
    trades_total = None
    if has_numtrades:
        trades_total = float(sum((to_float(r.get("NUMTRADES")) or 0.0) for r in selected))

    use_value = any(r.get("VALUE") not in (None, "") for r in selected)
    volume_total = None
    if use_value:
        volume_total = float(sum((to_float(r.get("VALUE")) or 0.0) for r in selected))
    elif any(r.get("VOLUME") not in (None, "") for r in selected):
        volume_total = float(sum((to_float(r.get("VOLUME")) or 0.0) for r in selected))

    if has_numtrades:
        no_trades_10d_flag = bool((trades_total or 0.0) == 0.0)
    else:
        no_trades_10d_flag = bool((volume_total or 0.0) == 0.0)

    db.upsert_liquidity(
        secid,
        {
            "from_date": from_date.isoformat(),
            "till_date": today.isoformat(),
            "trades_10d_total": trades_total,
            "volume_10d_total": volume_total,
            "method_fields": "NUMTRADES->VALUE->VOLUME",
        },
    )

    prev_price = db.get_prev_price(secid)
    price_change_pct = None
    if prev_price not in (None, 0) and price_current is not None:
        price_change_pct = (price_current - prev_price) / prev_price

    row = {
        "secid": secid,
        "isin": base.get("ISIN"),
        "shortname": base.get("SHORTNAME"),
        "issuer_name": issuer_name,
        "issuer_inn": issuer_inn,
        "issuer_credit_rating": issuer_credit_rating,
        "issuer_rating_date": issuer_rating_date,
        "rating_description": rating_description,
        "price_current": price_current,
        "price_prev_export": prev_price,
        "price_change_pct": price_change_pct,
        "matdate": matdate.isoformat() if matdate else None,
        "offerdate": (parse_date(base.get("OFFERDATE")) or parse_date(sec_info.get("OFFERDATE"))).isoformat() if (parse_date(base.get("OFFERDATE")) or parse_date(sec_info.get("OFFERDATE"))) else None,
        "amort_start_date": amort_start_date.isoformat() if amort_start_date else None,
        "amort_has_started": 1 if amort_has_started else 0,
        "amort_starts_within_1y": 1 if amort_starts_within_1y else 0,
        "qualified_only": base.get("IS_QUALIFIED_INVESTORS") if base.get("IS_QUALIFIED_INVESTORS") is not None else sec_info.get("IS_QUALIFIED_INVESTORS"),
        "default_flag": sec_info.get("DEFOLT") or sec_info.get("DEFAULT"),
        "technical_default_flag": sec_info.get("TECHDEFAULT") or sec_info.get("TECHNICAL_DEFAULT"),
        "bond_type": base.get("BOND_TYPE") or sec_info.get("BOND_TYPE"),
        "secsubtype": base.get("SECSUBTYPE") or sec_info.get("SECSUBTYPE"),
        "coupon_count": coupon_count,
        "coupon_period_days": coupon_period_days,
        "nkd": nkd,
        "coupon_percent": coupon_percent,
        "trades_10d_total": trades_total,
        "volume_10d_total": volume_total,
        "no_trades_10d_flag": 1 if no_trades_10d_flag else 0,
        "emitter_id": emitter_id,
        "inactive_flag": 0,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "last_exported_at": datetime.now().isoformat(timespec="seconds"),
    }
    source_hash = hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    row["source_hash"] = source_hash

    prev_hash = db.get_source_hash(secid)
    if prev_hash and prev_hash == source_hash:
        return {"unchanged": True, "row": row}

    db.upsert_security(row)
    return {"unchanged": False, "row": row}


def export_excel(df: pd.DataFrame, summary: Summary, logger: logging.Logger) -> Path | None:
    if not config.EXPORT_EXCEL:
        return None
    target = config.OUTPUT_DIR / config.EXCEL_FILE_NAME
    fallback = config.OUTPUT_DIR / config.EXCEL_LOCKED_FILE_NAME
    output_path = target
    try:
        with pd.ExcelWriter(target, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="MoexBonds", index=False)
            ws = writer.book["MoexBonds"]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for cell in ws[1]:
                cell.font = Font(bold=True)
            for col_idx, col in enumerate(df.columns, start=1):
                max_len = max([len(str(col))] + [len(str(v)) for v in df[col].head(500).fillna("").tolist()])
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 45)

            date_cols = ["matdate", "offerdate", "amort_start_date", "issuer_rating_date"]
            pct_cols = ["price_change_pct"]
            for c in date_cols:
                if c in df.columns:
                    idx = df.columns.get_loc(c) + 1
                    for r in range(2, ws.max_row + 1):
                        ws.cell(row=r, column=idx).number_format = "DD.MM.YYYY"
            for c in pct_cols:
                if c in df.columns:
                    idx = df.columns.get_loc(c) + 1
                    for r in range(2, ws.max_row + 1):
                        ws.cell(row=r, column=idx).number_format = "0.00%"

            if "price_change_pct" in df.columns:
                idx = df.columns.get_loc("price_change_pct") + 1
                for r in range(2, ws.max_row + 1):
                    val = ws.cell(row=r, column=idx).value
                    if isinstance(val, (float, int)):
                        if val > 0:
                            ws.cell(row=r, column=idx).fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                        elif val < 0:
                            ws.cell(row=r, column=idx).fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

            if "no_trades_10d_flag" in df.columns:
                idx = df.columns.get_loc("no_trades_10d_flag") + 1
                for r in range(2, ws.max_row + 1):
                    val = ws.cell(row=r, column=idx).value
                    if str(val).lower() in {"1", "true"}:
                        ws.cell(row=r, column=idx).fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    except PermissionError:
        summary.excel_warn_locked = True
        output_path = fallback
        logger.warning("Не удалось перезаписать %s, файл занят. Сохраняю в %s", target, fallback)
        with pd.ExcelWriter(fallback, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="MoexBonds", index=False)
    return output_path


async def async_main() -> int:
    for d in config.REQUIRED_DIRS:
        d.mkdir(parents=True, exist_ok=True)

    validate_config()
    logger = setup_logging()
    summary = Summary()
    state = StateStore(config.STATE_PATH)
    today = date.today()

    t0 = time.perf_counter()
    stage_times: dict[str, float] = {}

    try:
        db = DB(config.DB_PATH)
    except Exception as exc:
        print(f"Критическая ошибка БД: {exc}")
        return 1

    client = MoexClient(logger, summary)
    missing_counter: dict[str, int] = {}
    try:
        t_stage = time.perf_counter()
        bonds = await client.fetch_traded_bonds()
        stage_times["Загрузка списка бумаг"] = time.perf_counter() - t_stage
        state.mark_stage("bonds_list", {"count": len(bonds), "at": datetime.now().isoformat(timespec="seconds")})
        summary.received_total = len(bonds)

        t_stage = time.perf_counter()
        rows_for_excel: list[dict[str, Any]] = []
        batch_counter = 0
        progress = ProgressReporter(total=len(bonds), logger=logger)
        for item in bonds:
            secid = str(item.get("SECID") or "")
            try:
                result = await process_one_bond(item, client, db, logger, today, missing_counter)
                if result and result.get("excluded_matured"):
                    summary.matured_excluded += 1
                elif result and result.get("row"):
                    row = result["row"]
                    rows_for_excel.append(row)
                    summary.active_total += 1
                    if row.get("no_trades_10d_flag") == 1:
                        summary.no_trades_10d_total += 1
                    if result.get("unchanged"):
                        summary.cache_hits += 1
                    state.mark_processed(secid)
                batch_counter += 1
                if batch_counter >= config.DB_COMMIT_BATCH_SIZE:
                    db.commit()
                    state.save()
                    batch_counter = 0
            except Exception as exc:
                summary.errors_total += 1
                logger.exception("Ошибка обработки secid=%s: %s", secid, exc)
            progress.update(1)
        progress.close()
        db.commit()
        state.save()
        stage_times["Загрузка/обработка деталей"] = time.perf_counter() - t_stage

        t_stage = time.perf_counter()
        exported_path = None
        if config.EXPORT_EXCEL:
            df = pd.DataFrame(rows_for_excel)
            exported_path = export_excel(df, summary, logger)
            summary.excel_exported_rows = len(df)
        stage_times["Сохранение витрины"] = time.perf_counter() - t_stage

        total_time = time.perf_counter() - t0
        if missing_counter:
            logger.warning("missing_count: %s", missing_counter)

        print("\nSummary")
        print(f"Получено облигаций: {summary.received_total}")
        print(f"Актуальных: {summary.active_total}")
        print(f"Исключено погашенных: {summary.matured_excluded}")
        print(f"Нет сделок за 10 дней: {summary.no_trades_10d_total}")
        print(f"Сохранено в Excel: {summary.excel_exported_rows}")
        print(f"Ошибок: {summary.errors_total}")
        print(f"Взято инкрементально/без изменений: {summary.cache_hits}")
        if exported_path:
            print(f"Excel: {exported_path}")
            if summary.excel_warn_locked:
                print("WARN: основной Excel был занят, сохранён MoexBonds.locked.xlsx. Закройте MoexBonds.xlsx и перезапустите.")
        print(f"Лог: {config.LOG_FILE_PATH}")
        print(f"Общее время: {total_time:.1f} сек")
        for name, sec in stage_times.items():
            print(f"  - {name}: {sec:.1f} сек")

    finally:
        db.close()
        await client.close()

    return 0


def main() -> None:
    try:
        code = asyncio.run(async_main())
        raise SystemExit(code)
    except ValueError as exc:
        print(f"Ошибка конфигурации: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
