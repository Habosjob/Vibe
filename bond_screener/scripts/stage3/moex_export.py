from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pandas as pd
from tqdm import tqdm

from core.db import execute_with_retry, executemany_with_retry, get_connection, utc_now_iso
from core.excel_debug import export_dataframe_styled, should_export
from core.logging import get_script_logger
from core.settings import AppSettings
from net.cache import HttpCache
from net.http_client import HttpClient, moex_get


@dataclass(frozen=True)
class Stage3Stats:
    total_candidates: int
    processed: int
    skipped_fresh: int
    failed: int
    duration_s: float


class MoexExporter:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.cfg = settings.stage3
        self.logger = get_script_logger(settings.paths.logs_dir / "stage3_moex_export.log", "stage3.moex_export")

    def run(self) -> Stage3Stats:
        if not self.cfg.enabled or not self.cfg.moex.enabled:
            self.logger.info("Stage3 отключен в config: stage3.enabled=false")
            return Stage3Stats(total_candidates=0, processed=0, skipped_fresh=0, failed=0, duration_s=0.0)

        started = time.perf_counter()
        self._ensure_tables()
        candidates = self._load_candidates()
        self._init_checkpoint_rows(candidates)

        now_utc = datetime.now(timezone.utc)
        to_process = [item for item in candidates if not self._is_fresh_done(item["secid"], now_utc)]
        skipped_fresh = len(candidates) - len(to_process)
        self.logger.info(
            "Старт Stage3: candidates=%s, to_process=%s, skipped_fresh=%s, ttl_hours=%s",
            len(candidates),
            len(to_process),
            skipped_fresh,
            self.cfg.moex.ttl_hours,
        )

        failed = asyncio.run(self._process_all(to_process)) if to_process else 0
        self._export_debug_if_needed()
        duration_s = time.perf_counter() - started
        return Stage3Stats(len(candidates), len(to_process), skipped_fresh, failed, duration_s)

    def _ensure_tables(self) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS moex_security_info (
                    secid TEXT PRIMARY KEY,
                    isin TEXT,
                    shortname TEXT,
                    name TEXT,
                    issuer_key TEXT,
                    matdate TEXT,
                    facevalue REAL,
                    faceunit TEXT,
                    currencyid TEXT,
                    typenm TEXT,
                    sectype TEXT,
                    primary_boardid TEXT,
                    fetched_at TEXT NOT NULL,
                    source_hash TEXT
                );
                CREATE TABLE IF NOT EXISTS moex_marketdata (
                    secid TEXT NOT NULL,
                    boardid TEXT,
                    tradedate TEXT,
                    last REAL,
                    close REAL,
                    bid REAL,
                    offer REAL,
                    waprice REAL,
                    ytm REAL,
                    yield REAL,
                    duration REAL,
                    accruedint REAL,
                    cleanprice REAL,
                    fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS moex_coupons (
                    secid TEXT NOT NULL,
                    coupondate TEXT,
                    recorddate TEXT,
                    startdate TEXT,
                    enddate TEXT,
                    value REAL,
                    value_rub REAL,
                    rate REAL,
                    currencyid TEXT,
                    fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS moex_amortizations (
                    secid TEXT NOT NULL,
                    amortdate TEXT,
                    value REAL,
                    value_rub REAL,
                    currencyid TEXT,
                    fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS moex_offers (
                    secid TEXT NOT NULL,
                    offerdate TEXT,
                    offertype TEXT,
                    offerdatestart TEXT,
                    offerdateend TEXT,
                    price REAL,
                    value REAL,
                    currencyid TEXT,
                    fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS moex_export_items (
                    secid TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    fetched_at TEXT,
                    info_ok INTEGER NOT NULL DEFAULT 0,
                    market_ok INTEGER NOT NULL DEFAULT 0,
                    bondization_ok INTEGER NOT NULL DEFAULT 0,
                    offers_ok INTEGER NOT NULL DEFAULT 0
                );
                """
            )

    def _load_candidates(self) -> list[dict[str, str | None]]:
        with get_connection(self.settings.paths.db_file) as conn:
            rows = conn.execute(
                "SELECT secid, isin, issuer_key FROM candidate_bonds WHERE COALESCE(secid, '') <> '' ORDER BY secid"
            ).fetchall()
        return [dict(row) for row in rows]

    def _init_checkpoint_rows(self, candidates: list[dict[str, str | None]]) -> None:
        now_iso = utc_now_iso()
        payload = [(row["secid"], now_iso) for row in candidates]
        if not payload:
            return
        with get_connection(self.settings.paths.db_file) as conn:
            executemany_with_retry(
                conn,
                """
                INSERT OR IGNORE INTO moex_export_items
                (secid, status, last_error, fetched_at, info_ok, market_ok, bondization_ok, offers_ok)
                VALUES (?, 'pending', NULL, ?, 0, 0, 0, 0)
                """,
                payload,
            )

    def _is_fresh_done(self, secid: str, now_utc: datetime) -> bool:
        with get_connection(self.settings.paths.db_file) as conn:
            row = conn.execute("SELECT status, fetched_at FROM moex_export_items WHERE secid = ?", (secid,)).fetchone()
        if not row or row["status"] != "done" or not row["fetched_at"]:
            return False
        try:
            fetched_at = datetime.fromisoformat(row["fetched_at"])
        except ValueError:
            return False
        return now_utc - fetched_at < timedelta(hours=max(0, self.cfg.moex.ttl_hours))

    async def _process_all(self, items: list[dict[str, str | None]]) -> int:
        sem = asyncio.Semaphore(max(1, self.cfg.moex.concurrency))
        client = HttpClient(self.settings, HttpCache(self.settings.paths.cache_http_dir))
        failed = 0
        try:
            with tqdm(
                total=len(items),
                desc="Stage3/MOEX export",
                unit="sec",
                dynamic_ncols=True,
                position=max(0, self.cfg.moex.progressbar_position),
                leave=True,
                mininterval=0.2,
            ) as pbar:
                for start in range(0, len(items), max(1, self.cfg.batch_size)):
                    batch = items[start : start + max(1, self.cfg.batch_size)]
                    results = await asyncio.gather(*[self._process_one(client, sem, r) for r in batch], return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception) or r is False:
                            failed += 1
                        pbar.update(1)
        finally:
            await client.aclose()
        return failed

    async def _process_one(self, client: HttpClient, sem: asyncio.Semaphore, row: dict[str, str | None]) -> bool:
        secid = str(row["secid"])
        async with sem:
            try:
                fetched_at = utc_now_iso()
                sec_rows, market_rows = await self._fetch_security_and_market(client, secid)
                info_ok = self._save_security_info(secid, row.get("isin"), row.get("issuer_key"), sec_rows, fetched_at)
                market_ok = self._save_marketdata(secid, market_rows, fetched_at)

                payload = await self._fetch_bondization(client, secid) if self.cfg.moex.bondization.enabled else {}
                coupons = self._extract_table_rows(payload, "coupons")
                amortizations = self._extract_table_rows(payload, "amortizations")
                offers = self._extract_table_rows(payload, "offers")
                bondization_ok = bool(coupons or amortizations)
                offers_ok = bool(offers) if self.cfg.moex.bondization.include_offers else False

                self._save_coupons(secid, coupons, fetched_at)
                self._save_amortizations(secid, amortizations, fetched_at)
                self._save_offers(secid, offers, fetched_at)

                last_error = None
                if not bondization_ok:
                    last_error = "bondization_unavailable"
                    self.logger.warning("bondization unavailable for secid=%s", secid)

                self._update_checkpoint(
                    secid,
                    "done",
                    fetched_at,
                    1 if info_ok else 0,
                    1 if market_ok else 0,
                    1 if bondization_ok else 0,
                    1 if offers_ok else 0,
                    last_error,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("Ошибка экспорта secid=%s", secid)
                self._update_checkpoint(secid, "failed", utc_now_iso(), 0, 0, 0, 0, str(exc))
                return False

    async def _fetch_security_and_market(self, client: HttpClient, secid: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        endpoint = f"/iss/engines/{self.cfg.moex.engine}/markets/{self.cfg.moex.market}/securities/{secid}.json"
        sec_payload = await moex_get(
            client,
            endpoint,
            params={
                "iss.meta": "off",
                "iss.only": "securities",
                "securities.columns": "SECID,ISIN,SHORTNAME,NAME,MATDATE,FACEVALUE,FACEUNIT,CURRENCYID,TYPENAME,SECTYPE,PRIMARY_BOARDID",
            },
            ttl_s=self.cfg.moex.ttl_hours * 3600,
        )
        sec_rows = self._extract_table_rows(sec_payload, "securities")

        market_rows = await self._fetch_table_with_cursor(
            client=client,
            endpoint=endpoint,
            table_name="marketdata",
            params={
                "iss.meta": "off",
                "iss.only": "marketdata",
                "marketdata.columns": "SECID,BOARDID,TRADEDATE,LAST,CLOSE,BID,OFFER,WAPRICE,YTM,YIELD,DURATION,ACCRUEDINT,CLEANPRICE",
            },
            page_size=max(1, self.cfg.moex.page_size),
        )
        if self.cfg.moex.boards:
            allowed = {b.strip().upper() for b in self.cfg.moex.boards if b and b.strip()}
            market_rows = [r for r in market_rows if str(r.get("BOARDID") or "").upper() in allowed]
        return sec_rows, market_rows

    async def _fetch_bondization(self, client: HttpClient, secid: str) -> dict[str, Any]:
        iss_only = "amortizations,coupons"
        if self.cfg.moex.bondization.include_offers:
            iss_only += ",offers"
        params: dict[str, Any] = {"iss.meta": "off", "iss.json": "extended", "iss.only": iss_only}
        if self.cfg.moex.bondization.from_date:
            params["from"] = self.cfg.moex.bondization.from_date
        if self.cfg.moex.bondization.till:
            params["till"] = self.cfg.moex.bondization.till
        endpoint = f"/iss/statistics/engines/{self.cfg.moex.engine}/markets/{self.cfg.moex.market}/bondization/{secid}.json"
        try:
            return await moex_get(client, endpoint, params=params, ttl_s=self.cfg.moex.ttl_hours * 3600)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                self.logger.warning("bondization 404 secid=%s", secid)
                return {}
            raise

    async def _fetch_table_with_cursor(
        self,
        client: HttpClient,
        endpoint: str,
        table_name: str,
        params: dict[str, Any],
        page_size: int,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        local = dict(params)
        only_value = str(local.get("iss.only", "")).strip()
        cursor_token = f"{table_name}.cursor"
        if cursor_token not in only_value.split(","):
            local["iss.only"] = ",".join([v for v in [only_value, cursor_token] if v])
        local["start"] = 0
        local[f"{table_name}.start"] = 0
        local[f"{table_name}.limit"] = page_size
        seen_signatures: set[tuple[int, str]] = set()

        while True:
            payload = await moex_get(client, endpoint, params=local, ttl_s=self.cfg.moex.ttl_hours * 3600)
            rows = self._extract_table_rows(payload, table_name)
            out.extend(rows)

            cursor = self._extract_table_cursor(payload, table_name)
            if not cursor:
                break

            index = int(cursor.get("INDEX", local[f"{table_name}.start"]))
            pagesize = int(cursor.get("PAGESIZE", page_size))
            total = int(cursor.get("TOTAL", index + len(rows)))

            first = ""
            if rows:
                first = str(rows[0].get("SECID") or rows[0].get("secid") or json.dumps(rows[0], ensure_ascii=False))
            signature = (index, first)
            if signature in seen_signatures:
                self.logger.warning("Anti-loop stop: table=%s endpoint=%s start=%s", table_name, endpoint, index)
                break
            seen_signatures.add(signature)

            if (index + pagesize) >= total:
                break

            next_start = index + pagesize
            local["start"] = next_start
            local[f"{table_name}.start"] = next_start

        return out

    @staticmethod
    def _extract_table_rows(payload: Any, table_name: str) -> list[dict[str, Any]]:
        # Формат iss.json=extended:
        # [ {"charsetinfo": ...}, {"coupons": [...], "amortizations": [...], ...} ]
        if isinstance(payload, list):
            for node in payload:
                if not isinstance(node, dict) or table_name not in node:
                    continue
                block = node[table_name]
                if isinstance(block, list):
                    if not block:
                        return []
                    if isinstance(block[0], dict) and "columns" not in block[0]:
                        return [dict(row) for row in block if isinstance(row, dict)]
                    if len(block) >= 2 and isinstance(block[0], dict):
                        cols = block[0].get("columns", [])
                        return [dict(zip(cols, row, strict=False)) for row in block[1]]
                if isinstance(block, dict):
                    cols = block.get("columns", [])
                    return [dict(zip(cols, row, strict=False)) for row in block.get("data", [])]
            return []

        if not isinstance(payload, dict):
            return []

        # Стандартный формат ISS: {'table': {'columns': [...], 'data': [...]}}
        block = payload.get(table_name)
        if isinstance(block, dict):
            cols = block.get("columns", [])
            return [dict(zip(cols, row, strict=False)) for row in block.get("data", [])]
        # Альтернативный формат ISS: {'table': [{'field': value, ...}, ...]}
        if isinstance(block, list) and block and isinstance(block[0], dict) and "columns" not in block[0]:
            return [dict(row) for row in block]
        if isinstance(block, list) and len(block) >= 2 and isinstance(block[0], dict):
            cols = block[0].get("columns", [])
            return [dict(zip(cols, row, strict=False)) for row in block[1]]
        return []

    @staticmethod
    def _extract_table_cursor(payload: Any, table_name: str) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        block = payload.get(f"{table_name}.cursor")
        if isinstance(block, dict):
            data = block.get("data", [])
            if data:
                return dict(zip(block.get("columns", []), data[0], strict=False))
        if isinstance(block, list) and len(block) >= 2 and isinstance(block[0], dict) and block[1]:
            return dict(zip(block[0].get("columns", []), block[1][0], strict=False))
        return None

    def _save_security_info(
        self,
        secid: str,
        isin: str | None,
        issuer_key: str | None,
        rows: list[dict[str, Any]],
        fetched_at: str,
    ) -> bool:
        if not rows:
            return False
        row = rows[0]
        source_hash = hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(
                conn,
                """
                INSERT INTO moex_security_info (
                    secid, isin, shortname, name, issuer_key, matdate, facevalue, faceunit,
                    currencyid, typenm, sectype, primary_boardid, fetched_at, source_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    isin=excluded.isin,
                    shortname=excluded.shortname,
                    name=excluded.name,
                    issuer_key=excluded.issuer_key,
                    matdate=excluded.matdate,
                    facevalue=excluded.facevalue,
                    faceunit=excluded.faceunit,
                    currencyid=excluded.currencyid,
                    typenm=excluded.typenm,
                    sectype=excluded.sectype,
                    primary_boardid=excluded.primary_boardid,
                    fetched_at=excluded.fetched_at,
                    source_hash=excluded.source_hash
                """,
                (
                    secid,
                    row.get("ISIN") or isin,
                    row.get("SHORTNAME"),
                    row.get("NAME"),
                    issuer_key,
                    row.get("MATDATE"),
                    row.get("FACEVALUE"),
                    row.get("FACEUNIT"),
                    row.get("CURRENCYID"),
                    row.get("TYPENAME"),
                    row.get("SECTYPE"),
                    row.get("PRIMARY_BOARDID"),
                    fetched_at,
                    source_hash,
                ),
            )
        return True

    def _save_marketdata(self, secid: str, rows: list[dict[str, Any]], fetched_at: str) -> bool:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(conn, "DELETE FROM moex_marketdata WHERE secid = ?", (secid,))
            executemany_with_retry(conn,
                """
                INSERT INTO moex_marketdata (
                    secid, boardid, tradedate, last, close, bid, offer, waprice,
                    ytm, yield, duration, accruedint, cleanprice, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        secid,
                        row.get("BOARDID"),
                        row.get("TRADEDATE"),
                        row.get("LAST"),
                        row.get("CLOSE"),
                        row.get("BID"),
                        row.get("OFFER"),
                        row.get("WAPRICE"),
                        row.get("YTM"),
                        row.get("YIELD"),
                        row.get("DURATION"),
                        row.get("ACCRUEDINT"),
                        row.get("CLEANPRICE"),
                        fetched_at,
                    )
                    for row in rows
                ],
            )
        return bool(rows)

    def _save_coupons(self, secid: str, rows: list[dict[str, Any]], fetched_at: str) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(conn, "DELETE FROM moex_coupons WHERE secid = ?", (secid,))
            executemany_with_retry(conn,
                "INSERT INTO moex_coupons VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        secid,
                        row.get("COUPONDATE") or row.get("coupondate"),
                        row.get("RECORDDATE") or row.get("recorddate"),
                        row.get("STARTDATE") or row.get("startdate"),
                        row.get("ENDDATE") or row.get("enddate"),
                        row.get("VALUE") or row.get("value"),
                        row.get("VALUE_RUB") or row.get("value_rub"),
                        row.get("RATE") or row.get("rate"),
                        row.get("CURRENCYID") or row.get("currencyid"),
                        fetched_at,
                    )
                    for row in rows
                ],
            )

    def _save_amortizations(self, secid: str, rows: list[dict[str, Any]], fetched_at: str) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(conn, "DELETE FROM moex_amortizations WHERE secid = ?", (secid,))
            executemany_with_retry(conn,
                "INSERT INTO moex_amortizations VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        secid,
                        row.get("AMORTDATE") or row.get("amortdate"),
                        row.get("VALUE") or row.get("value"),
                        row.get("VALUE_RUB") or row.get("value_rub"),
                        row.get("CURRENCYID") or row.get("currencyid"),
                        fetched_at,
                    )
                    for row in rows
                ],
            )

    def _save_offers(self, secid: str, rows: list[dict[str, Any]], fetched_at: str) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(conn, "DELETE FROM moex_offers WHERE secid = ?", (secid,))
            executemany_with_retry(conn,
                "INSERT INTO moex_offers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        secid,
                        row.get("OFFERDATE") or row.get("offerdate"),
                        row.get("OFFERTYPE") or row.get("offertype"),
                        row.get("OFFERDATESTART") or row.get("offerdatestart"),
                        row.get("OFFERDATEEND") or row.get("offerdateend"),
                        row.get("PRICE") or row.get("price"),
                        row.get("VALUE") or row.get("value"),
                        row.get("CURRENCYID") or row.get("currencyid"),
                        fetched_at,
                    )
                    for row in rows
                ],
            )

    def _update_checkpoint(
        self,
        secid: str,
        status: str,
        fetched_at: str,
        info_ok: int,
        market_ok: int,
        bondization_ok: int,
        offers_ok: int,
        last_error: str | None,
    ) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            execute_with_retry(
                conn,
                """
                INSERT INTO moex_export_items
                (secid, status, last_error, fetched_at, info_ok, market_ok, bondization_ok, offers_ok)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(secid) DO UPDATE SET
                    status=excluded.status,
                    last_error=excluded.last_error,
                    fetched_at=excluded.fetched_at,
                    info_ok=excluded.info_ok,
                    market_ok=excluded.market_ok,
                    bondization_ok=excluded.bondization_ok,
                    offers_ok=excluded.offers_ok
                """,
                (secid, status, (last_error or "")[:2000] or None, fetched_at, info_ok, market_ok, bondization_ok, offers_ok),
            )

    def _export_debug_if_needed(self) -> None:
        if not should_export(self.settings, "stage3"):
            return
        exports = [
            ("stage3_debug_moex_security_info.xlsx", "moex_security_info", ["matdate", "fetched_at"]),
            ("stage3_debug_moex_marketdata.xlsx", "moex_marketdata", ["tradedate", "fetched_at"]),
            (
                "stage3_debug_moex_coupons.xlsx",
                "moex_coupons",
                ["coupondate", "recorddate", "startdate", "enddate", "fetched_at"],
            ),
            ("stage3_debug_moex_amortizations.xlsx", "moex_amortizations", ["amortdate", "fetched_at"]),
        ]
        if self.cfg.moex.bondization.include_offers:
            exports.append(
                ("stage3_debug_moex_offers.xlsx", "moex_offers", ["offerdate", "offerdatestart", "offerdateend", "fetched_at"])
            )

        with get_connection(self.settings.paths.db_file) as conn:
            for filename, table, date_cols in exports:
                rows = [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY secid").fetchall()]
                out = export_dataframe_styled(
                    self.settings,
                    filename=filename,
                    df=pd.DataFrame(rows),
                    export_name="stage3",
                    date_columns=date_cols,
                )
                if out:
                    self.logger.info("Excel debug выгрузка создана: %s", out)
