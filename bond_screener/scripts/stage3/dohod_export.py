from __future__ import annotations

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from core.db import execute_with_retry, executemany_with_retry, get_connection, utc_now_iso
from core.excel_debug import export_dataframe_styled, should_export
from core.logging import get_script_logger
from core.settings import AppSettings
from net.cache import HttpCache
from net.http_client import HttpClient

ISIN_RU_RE = re.compile(r"^RU[A-Z0-9]{10}$")


@dataclass(frozen=True)
class DohodStats:
    total_candidates: int
    skipped_no_isin: int
    invalid_isin: int
    skipped_fresh: int
    done: int
    failed: int
    duration_s: float


class DohodExporter:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.cfg = settings.stage3.dohod
        self.logger = get_script_logger(settings.paths.logs_dir / "stage3_dohod_export.log", "stage3.dohod_export")

    def run(self) -> DohodStats:
        if not self.settings.stage3.enabled or not self.cfg.enabled:
            self.logger.info("Dohod export отключен в config")
            return DohodStats(0, 0, 0, 0, 0, 0, 0.0)

        started = time.perf_counter()
        self._ensure_tables()
        candidates = self._load_candidate_isins()
        self._init_checkpoint_rows(candidates)

        now_utc = datetime.now(timezone.utc)
        eligible: list[str] = []
        skipped_no_isin = 0
        invalid_isin = 0
        skipped_fresh = 0

        for isin in candidates:
            if not isin:
                skipped_no_isin += 1
                self.logger.info("skip candidate without isin")
                continue
            if not ISIN_RU_RE.match(isin):
                invalid_isin += 1
                self.logger.warning("skip invalid isin=%s", isin)
                continue
            if self._is_fresh_done(isin, now_utc):
                skipped_fresh += 1
                continue
            eligible.append(isin)

        self.logger.info(
            "Старт DOHOD export: total=%s, eligible=%s, skipped_no_isin=%s, invalid_isin=%s, skipped_fresh=%s",
            len(candidates),
            len(eligible),
            skipped_no_isin,
            invalid_isin,
            skipped_fresh,
        )

        done, failed = asyncio.run(self._process_all(eligible)) if eligible else (0, 0)
        self._export_debug_if_needed()
        duration = time.perf_counter() - started
        return DohodStats(len(candidates), skipped_no_isin, invalid_isin, skipped_fresh, done, failed, duration)

    def _ensure_tables(self) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS dohod_bond_profile (
                    isin TEXT PRIMARY KEY,
                    bond_name TEXT,
                    status TEXT,
                    currency TEXT,
                    issue_date TEXT,
                    maturity_date TEXT,
                    ytm_percent REAL,
                    price_last REAL,
                    nkd REAL,
                    current_nominal REAL,
                    coupon_freq_per_year INTEGER,
                    coupon_type TEXT,
                    coupon_formula_text TEXT,
                    next_payment_date TEXT,
                    internal_rating TEXT,
                    liquidity_score REAL,
                    warning_text TEXT,
                    fetched_at TEXT,
                    source_hash TEXT
                );
                CREATE TABLE IF NOT EXISTS dohod_export_items (
                    isin TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    fetched_at TEXT
                );
                """
            )

    def _load_candidate_isins(self) -> list[str]:
        with get_connection(self.settings.paths.db_file) as conn:
            rows = conn.execute("SELECT DISTINCT TRIM(COALESCE(isin, '')) AS isin FROM candidate_bonds ORDER BY isin").fetchall()
        return [str(r["isin"]).upper().strip() for r in rows]

    def _init_checkpoint_rows(self, isins: list[str]) -> None:
        now_iso = utc_now_iso()
        payload = [(isin, now_iso) for isin in isins if isin]
        if not payload:
            return
        with get_connection(self.settings.paths.db_file) as conn:
            executemany_with_retry(
                conn,
                """
                INSERT OR IGNORE INTO dohod_export_items (isin, status, last_error, fetched_at)
                VALUES (?, 'pending', NULL, ?)
                """,
                payload,
            )

    def _is_fresh_done(self, isin: str, now_utc: datetime) -> bool:
        with get_connection(self.settings.paths.db_file) as conn:
            row = conn.execute("SELECT status, fetched_at FROM dohod_export_items WHERE isin = ?", (isin,)).fetchone()
        if not row or row["status"] != "done" or not row["fetched_at"]:
            return False
        try:
            fetched_at = datetime.fromisoformat(row["fetched_at"])
        except ValueError:
            return False
        return now_utc - fetched_at < timedelta(hours=max(0, self.cfg.ttl_hours))

    async def _process_all(self, isins: list[str]) -> tuple[int, int]:
        semaphore = asyncio.Semaphore(max(1, self.cfg.concurrency))
        client = HttpClient(self.settings, HttpCache(self.settings.paths.cache_http_dir))
        try:
            done = 0
            failed = 0
            with tqdm(
                total=len(isins),
                desc="Stage3/DOHOD export",
                unit="isin",
                dynamic_ncols=True,
                position=max(0, self.cfg.progressbar_position),
                leave=True,
                mininterval=0.2,
            ) as pbar:
                results = await asyncio.gather(
                    *[self._process_one(client, semaphore, isin) for isin in isins],
                    return_exceptions=True,
                )
                for result in results:
                    if result is True:
                        done += 1
                    else:
                        failed += 1
                    pbar.update(1)
            return done, failed
        finally:
            await client.aclose()

    async def _process_one(self, client: HttpClient, semaphore: asyncio.Semaphore, isin: str) -> bool:
        async with semaphore:
            max_attempts = 2
            for attempt in range(1, max_attempts + 1):
                try:
                    html = await self._fetch_html(client, isin)
                    profile = self._parse_profile(isin, html)
                    self._save_profile(profile)
                    self._set_checkpoint(isin, "done", None)
                    await asyncio.sleep(max(0.0, self.cfg.min_delay_s))
                    return True
                except httpx.HTTPStatusError as exc:
                    code = exc.response.status_code
                    if code == 404:
                        self.logger.warning("ISIN not found on DOHOD: %s", isin)
                    else:
                        self.logger.warning("HTTP error for %s: %s", isin, code)
                    self._set_checkpoint(isin, "failed", f"http_{code}")
                    await asyncio.sleep(max(0.0, self.cfg.min_delay_s))
                    return False
                except Exception as exc:  # noqa: BLE001
                    if attempt >= max_attempts:
                        self.logger.exception("Ошибка обработки isin=%s", isin)
                        self._set_checkpoint(isin, "failed", str(exc))
                        await asyncio.sleep(max(0.0, self.cfg.min_delay_s))
                        return False
                    self.logger.warning("Повтор isin=%s attempt=%s/%s после ошибки: %s", isin, attempt, max_attempts, exc)
                    await asyncio.sleep(min(1.0, 0.1 * (2 ** (attempt - 1))))
            return False

    async def _fetch_html(self, client: HttpClient, isin: str) -> str:
        url = self.cfg.base_url.format(isin=isin)
        headers = {"User-Agent": self.cfg.user_agent}
        cache_key = client.cache.make_key(url, None, headers)
        cached = client.cache.get(cache_key)
        if cached and not cached.is_expired():
            return cached.payload_file.read_text(encoding="utf-8", errors="replace")

        retries = 3
        for attempt in range(1, retries + 1):
            try:
                response = await client._client.get(url=url, headers=headers, timeout=self.cfg.page_timeout_s)  # noqa: SLF001
                if response.status_code in (403, 404):
                    response.raise_for_status()
                if 500 <= response.status_code < 600:
                    raise httpx.HTTPStatusError("server error", request=response.request, response=response)
                response.raise_for_status()
                payload = response.content
                client.cache.set(
                    cache_key,
                    payload,
                    ttl_s=max(1, int(self.cfg.ttl_hours * 3600)),
                    content_type=response.headers.get("content-type", "text/html"),
                )
                return payload.decode("utf-8", errors="replace")
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) and exc.response else None
                if code in (403, 404) or attempt >= retries:
                    raise
                await asyncio.sleep(min(1.0, 0.1 * (2 ** (attempt - 1))))
        raise RuntimeError(f"Не удалось загрузить DOHOD страницу: {isin}")

    def _parse_profile(self, isin: str, html: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        text_map = self._extract_label_values(soup)
        warnings: list[str] = []

        def pick(*keys: str) -> str | None:
            for key in keys:
                value = text_map.get(self._normalize_label(key))
                if value:
                    return value
            warnings.append(f"missing:{keys[0]}")
            return None

        bond_name = pick("Название", "Наименование") or self._extract_title(soup)
        status = pick("Статус")
        currency = pick("Валюта")
        issue_date = self._normalize_date(pick("Дата размещения", "Дата выпуска"))
        maturity_date = self._normalize_date(pick("Дата погашения"))
        ytm_percent = self._to_float(pick("Доходность", "YTM"))
        price_last = self._to_float(pick("Цена", "Последняя цена"))
        nkd = self._to_float(pick("НКД"))
        current_nominal = self._to_float(pick("Текущий номинал", "Номинал"))
        coupon_freq_per_year = self._to_int(pick("Частота купона", "Купонов в год"))
        coupon_type = pick("Тип купона")
        coupon_formula_text = pick("Формула купона", "Формула")
        next_payment_date = self._normalize_date(pick("Ближайшая выплата", "Дата следующего купона"))
        internal_rating = pick("Внутренний рейтинг", "Рейтинг")
        liquidity_score = self._to_float(pick("Ликвидность", "Оценка ликвидности"))

        fetched_at = utc_now_iso()
        source_hash = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()
        warning_text = "; ".join(dict.fromkeys(warnings)) if warnings else None

        return {
            "isin": isin,
            "bond_name": bond_name,
            "status": status,
            "currency": currency,
            "issue_date": issue_date,
            "maturity_date": maturity_date,
            "ytm_percent": ytm_percent,
            "price_last": price_last,
            "nkd": nkd,
            "current_nominal": current_nominal,
            "coupon_freq_per_year": coupon_freq_per_year,
            "coupon_type": coupon_type,
            "coupon_formula_text": coupon_formula_text,
            "next_payment_date": next_payment_date,
            "internal_rating": internal_rating,
            "liquidity_score": liquidity_score,
            "warning_text": warning_text,
            "fetched_at": fetched_at,
            "source_hash": source_hash,
        }

    def _save_profile(self, profile: dict[str, Any]) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(
                conn,
                """
                INSERT INTO dohod_bond_profile (
                    isin, bond_name, status, currency, issue_date, maturity_date, ytm_percent,
                    price_last, nkd, current_nominal, coupon_freq_per_year, coupon_type,
                    coupon_formula_text, next_payment_date, internal_rating, liquidity_score,
                    warning_text, fetched_at, source_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(isin) DO UPDATE SET
                    bond_name=excluded.bond_name,
                    status=excluded.status,
                    currency=excluded.currency,
                    issue_date=excluded.issue_date,
                    maturity_date=excluded.maturity_date,
                    ytm_percent=excluded.ytm_percent,
                    price_last=excluded.price_last,
                    nkd=excluded.nkd,
                    current_nominal=excluded.current_nominal,
                    coupon_freq_per_year=excluded.coupon_freq_per_year,
                    coupon_type=excluded.coupon_type,
                    coupon_formula_text=excluded.coupon_formula_text,
                    next_payment_date=excluded.next_payment_date,
                    internal_rating=excluded.internal_rating,
                    liquidity_score=excluded.liquidity_score,
                    warning_text=excluded.warning_text,
                    fetched_at=excluded.fetched_at,
                    source_hash=excluded.source_hash
                """,
                (
                    profile["isin"],
                    profile["bond_name"],
                    profile["status"],
                    profile["currency"],
                    profile["issue_date"],
                    profile["maturity_date"],
                    profile["ytm_percent"],
                    profile["price_last"],
                    profile["nkd"],
                    profile["current_nominal"],
                    profile["coupon_freq_per_year"],
                    profile["coupon_type"],
                    profile["coupon_formula_text"],
                    profile["next_payment_date"],
                    profile["internal_rating"],
                    profile["liquidity_score"],
                    profile["warning_text"],
                    profile["fetched_at"],
                    profile["source_hash"],
                ),
            )

    def _set_checkpoint(self, isin: str, status: str, last_error: str | None) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(
                conn,
                """
                INSERT INTO dohod_export_items (isin, status, last_error, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(isin) DO UPDATE SET
                    status=excluded.status,
                    last_error=excluded.last_error,
                    fetched_at=excluded.fetched_at
                """,
                (isin, status, (last_error or "")[:2000] or None, utc_now_iso()),
            )

    def _extract_label_values(self, soup: BeautifulSoup) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for row in soup.select("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = self._normalize_label(cells[0].get_text(" ", strip=True))
            val = cells[1].get_text(" ", strip=True)
            if key and val and key not in mapping:
                mapping[key] = val

        for block in soup.select("dt"):
            dd = block.find_next_sibling("dd")
            if not dd:
                continue
            key = self._normalize_label(block.get_text(" ", strip=True))
            val = dd.get_text(" ", strip=True)
            if key and val and key not in mapping:
                mapping[key] = val

        for node in soup.select("div, span"):
            raw = node.get_text(" ", strip=True)
            if ":" not in raw or len(raw) > 120:
                continue
            key_raw, value_raw = raw.split(":", 1)
            key = self._normalize_label(key_raw)
            val = value_raw.strip()
            if key and val and key not in mapping:
                mapping[key] = val
        return mapping

    @staticmethod
    def _normalize_label(value: str) -> str:
        value = value.lower().replace("ё", "е")
        return re.sub(r"\s+", " ", value).strip(" :")

    @staticmethod
    def _extract_title(soup: BeautifulSoup) -> str | None:
        title = soup.find("h1")
        if title:
            return title.get_text(" ", strip=True)
        if soup.title:
            return soup.title.get_text(" ", strip=True)
        return None

    @staticmethod
    def _normalize_date(value: str | None) -> str | None:
        if not value:
            return None
        cleaned = value.strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(cleaned, fmt).strftime("%d.%m.%Y")
            except ValueError:
                continue
        match = re.search(r"(\d{2}\.\d{2}\.\d{4})", cleaned)
        return match.group(1) if match else None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if not value:
            return None
        cleaned = value.replace("%", "")
        cleaned = re.sub(r"[^\d,\-. ]", "", cleaned)
        cleaned = cleaned.replace(" ", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        if not value:
            return None
        match = re.search(r"-?\d+", value)
        return int(match.group(0)) if match else None

    def _export_debug_if_needed(self) -> None:
        if not should_export(self.settings, "stage3"):
            return
        with get_connection(self.settings.paths.db_file) as conn:
            rows = [dict(r) for r in conn.execute("SELECT * FROM dohod_bond_profile ORDER BY isin").fetchall()]
        out = export_dataframe_styled(
            self.settings,
            filename="stage3_debug_dohod_bond_profile.xlsx",
            df=pd.DataFrame(rows),
            export_name="stage3",
            date_columns=["issue_date", "maturity_date", "next_payment_date", "fetched_at"],
        )
        if out:
            self.logger.info("Excel debug выгрузка создана: %s", out)
