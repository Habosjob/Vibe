"""Клиент MOEX ISS для получения списка облигаций."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from .config import AppConfig
from .raw_store import RawStore

ProgressCallback = Callable[[dict[str, Any]], None]
CheckpointSaver = Callable[[dict[str, Any]], None]

AMORTIZATION_CHECKPOINT_VERSION = 3


class MoexClient:
    def __init__(self, config: AppConfig, logger: logging.Logger, raw_store: RawStore | None = None) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.raw_store = raw_store
        self._request_lock = threading.Lock()
        self._next_request_ts = 0.0
        self._thread_local = threading.local()

    def fetch_all_bonds(
        self,
        checkpoint_data: dict[str, Any] | None = None,
        checkpoint_saver: CheckpointSaver | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        bonds = list(checkpoint_data.get("bonds", [])) if checkpoint_data else []
        errors = 0
        start = int(checkpoint_data.get("next_start", 0)) if checkpoint_data else 0
        seen_secids = {str(secid) for secid in checkpoint_data.get("seen_secids", [])} if checkpoint_data else set()
        completed = True

        if progress_callback:
            progress_callback(
                {
                    "stage": "fetch_bonds",
                    "fetched": len(bonds),
                    "start": start,
                    "message": "Возобновление загрузки списка облигаций",
                }
            )

        while True:
            self.logger.info("Запрос страницы MOEX: start=%s", start)
            page_data, page_errors, request_failed = self._fetch_page(start)
            errors += page_errors

            if request_failed:
                completed = False
                break

            if not page_data:
                break

            new_items: list[dict[str, Any]] = []
            for item in page_data:
                secid = item.get("SECID")
                if not secid or secid not in seen_secids:
                    if secid:
                        seen_secids.add(secid)
                    new_items.append(item)

            bonds.extend(new_items)

            if progress_callback:
                progress_callback(
                    {
                        "stage": "fetch_bonds",
                        "fetched": len(bonds),
                        "new_items": len(new_items),
                        "start": start,
                    }
                )

            if start > 0 and not new_items:
                self.logger.warning(
                    "Пагинация MOEX вернула дубликаты для start=%s. Останавливаемся, чтобы избежать бесконечного цикла.",
                    start,
                )
                break

            if start == 0 and len(page_data) > self.config.page_size:
                self.logger.info(
                    "MOEX вернула %s строк за один запрос (больше page_size=%s). Считаем, что получен полный список.",
                    len(page_data),
                    self.config.page_size,
                )
                break

            if len(page_data) < self.config.page_size:
                break

            next_start = start + self.config.page_size
            if checkpoint_saver:
                checkpoint_saver(
                    {
                        "bonds": bonds,
                        "next_start": next_start,
                        "seen_secids": sorted(seen_secids),
                        "completed": False,
                    }
                )
            start = next_start

        if checkpoint_saver:
            checkpoint_saver(
                {
                    "bonds": bonds,
                    "next_start": start,
                    "seen_secids": sorted(seen_secids),
                    "completed": completed,
                }
            )

        return bonds, errors, completed

    def enrich_amortization_start_dates(
        self,
        bonds: list[dict[str, Any]],
        checkpoint_data: dict[str, Any] | None = None,
        checkpoint_saver: CheckpointSaver | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> int:
        """Обогащает список бумаг полем Amortization_start_date.

        Значение заполняется самой ранней датой амортизации по данным MOEX,
        либо пустой строкой, если амортизации нет.
        """

        errors = 0
        processed: dict[str, str] = dict(checkpoint_data.get("processed", {})) if checkpoint_data else {}
        total = len(bonds)
        secid_to_indices: dict[str, list[int]] = {}

        for idx, bond in enumerate(bonds):
            secid = str(bond.get("SECID") or "").strip()
            if secid:
                secid_to_indices.setdefault(secid, []).append(idx)

        progress_processed = 0
        pending: list[tuple[str, str]] = []

        for secid, indices in secid_to_indices.items():
            sample_bond = bonds[indices[0]]
            matdate = str(sample_bond.get("MATDATE") or "")
            if not secid:
                continue

            if secid in processed:
                value = str(processed.get(secid) or "")
                for idx in indices:
                    bonds[idx]["Amortization_start_date"] = value
                progress_processed += len(indices)
                if progress_callback:
                    progress_callback(
                        {
                            "stage": "amortization",
                            "processed": progress_processed,
                            "total": total,
                            "secid": secid,
                            "resumed": True,
                        }
                    )
                continue

            pending.append((secid, matdate))

        for bond in bonds:
            secid = str(bond.get("SECID") or "").strip()
            if not secid:
                bond["Amortization_start_date"] = ""
                progress_processed += 1

        workers = max(1, self.config.amortization_workers)
        if pending:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self._fetch_amortization_start_date, secid, matdate): secid
                    for secid, matdate in pending
                }
                for future in as_completed(futures):
                    secid = futures[future]
                    try:
                        date_value, request_errors = future.result()
                    except Exception as error:  # noqa: BLE001
                        self.logger.exception("Необработанная ошибка в задаче амортизации secid=%s: %s", secid, error)
                        date_value, request_errors = "", 1

                    errors += request_errors
                    if request_errors == 0:
                        processed[secid] = date_value
                    for idx in secid_to_indices.get(secid, []):
                        bonds[idx]["Amortization_start_date"] = date_value
                    progress_processed += len(secid_to_indices.get(secid, []))

                    if checkpoint_saver:
                        checkpoint_saver(
                            {
                                "version": AMORTIZATION_CHECKPOINT_VERSION,
                                "processed": processed,
                                "completed": False,
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )

                    if progress_callback:
                        progress_callback(
                            {
                                "stage": "amortization",
                                "processed": progress_processed,
                                "total": total,
                                "secid": secid,
                                "resumed": False,
                            }
                        )

        if checkpoint_saver:
            checkpoint_saver(
                {
                    "version": AMORTIZATION_CHECKPOINT_VERSION,
                    "processed": processed,
                    "completed": True,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        return errors

    def _fetch_page(self, start: int) -> tuple[list[dict[str, Any]], int, bool]:
        params = {
            "iss.meta": "off",
            "iss.only": "securities",
            "start": start,
            "limit": self.config.page_size,
        }

        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    self.config.base_url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                    delay_seconds=self.config.request_delay_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_json(f"bonds_page_{start}.json", response.text)

                columns = payload["securities"]["columns"]
                rows = payload["securities"]["data"]
                items = [dict(zip(columns, row, strict=False)) for row in rows]
                return items, 0, False
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса start=%s попытка=%s: %s", start, attempt, error)
                if attempt == self.config.retries:
                    return [], 1, True
                time.sleep(self.config.request_delay_seconds * attempt)

        return [], 1, True

    def fetch_security_description(self, secid: str) -> tuple[dict[str, str], int]:
        """Возвращает словарь NAME->VALUE из блока description для инструмента."""
        url = f"https://iss.moex.com/iss/securities/{secid}.json"
        params = {"iss.meta": "off", "iss.only": "description"}

        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                    delay_seconds=self.config.request_delay_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                description = payload.get("description") or {}
                columns = description.get("columns") or []
                rows = description.get("data") or []
                if not columns or not rows:
                    return {}, 0

                name_idx = columns.index("name") if "name" in columns else None
                value_idx = columns.index("value") if "value" in columns else None
                if name_idx is None or value_idx is None:
                    return {}, 0

                parsed: dict[str, str] = {}
                for row in rows:
                    if len(row) <= max(name_idx, value_idx):
                        continue
                    raw_name = row[name_idx]
                    raw_value = row[value_idx]
                    if raw_name is None:
                        continue
                    key = str(raw_name).strip().upper()
                    if not key:
                        continue
                    parsed[key] = "" if raw_value is None else str(raw_value).strip()

                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_json(f"security_description_{secid}.json", response.text)

                return parsed, 0
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса description secid=%s попытка=%s: %s", secid, attempt, error)
                if attempt == self.config.retries:
                    return {}, 1
                time.sleep(self.config.request_delay_seconds * attempt)

        return {}, 1

    def fetch_market_securities(self, market: str) -> tuple[list[dict[str, Any]], int]:
        """Загружает инструменты по рынку MOEX (например, bonds/shares)."""
        url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/securities.json"
        start = 0
        errors = 0
        items: list[dict[str, Any]] = []

        while True:
            params = {
                "iss.meta": "off",
                "iss.only": "securities",
                "start": start,
                "limit": self.config.page_size,
            }
            page, page_errors, failed = self._fetch_generic_securities_page(url=url, params=params)
            errors += page_errors
            if failed or not page:
                break

            items.extend(page)

            if start == 0 and len(page) > self.config.page_size:
                break
            if len(page) < self.config.page_size:
                break

            start += self.config.page_size

        return items, errors

    def _fetch_generic_securities_page(self, url: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], int, bool]:
        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                    delay_seconds=self.config.request_delay_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                columns = payload.get("securities", {}).get("columns") or []
                rows = payload.get("securities", {}).get("data") or []
                parsed = [dict(zip(columns, row, strict=False)) for row in rows]
                return parsed, 0, False
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса %s попытка=%s: %s", url, attempt, error)
                if attempt == self.config.retries:
                    return [], 1, True
                time.sleep(self.config.request_delay_seconds * attempt)

        return [], 1, True

    def _fetch_amortization_start_date(self, secid: str, matdate: str = "") -> tuple[str, int]:
        url = f"https://iss.moex.com/iss/securities/{secid}/bondization.json"
        params = {"iss.meta": "off", "iss.only": "amortizations"}

        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                    delay_seconds=self.config.amortization_request_delay_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_json(f"amortization_{secid}.json", response.text)

                earliest = self._extract_earliest_amortization_date(payload, matdate=matdate)
                return earliest or "", 0
            except requests.RequestException as error:
                self.logger.warning(
                    "Ошибка запроса амортизации secid=%s попытка=%s: %s",
                    secid,
                    attempt,
                    error,
                )
                if attempt == self.config.retries:
                    return "", 1
                time.sleep(self.config.amortization_request_delay_seconds * attempt)
            except Exception as error:  # noqa: BLE001
                self.logger.warning(
                    "Ошибка обработки ответа амортизации secid=%s попытка=%s: %s",
                    secid,
                    attempt,
                    error,
                )
                if attempt == self.config.retries:
                    return "", 1
                time.sleep(self.config.amortization_request_delay_seconds * attempt)

        return "", 1

    @staticmethod
    def _extract_earliest_amortization_date(payload: dict[str, Any], matdate: str = "") -> str | None:
        amortizations = payload.get("amortizations") or {}
        columns = amortizations.get("columns") or []
        rows = amortizations.get("data") or []
        if not columns or not rows:
            return None

        col_map = {name.upper(): idx for idx, name in enumerate(columns)}
        date_idx = col_map.get("AMORTDATE")
        value_prc_idx = col_map.get("VALUEPRC")
        if date_idx is None:
            return None

        parsed_items: list[tuple[datetime, float | None]] = []
        partial_dates: list[datetime] = []
        for row in rows:
            if len(row) <= date_idx:
                continue
            raw_date = row[date_idx]
            if not isinstance(raw_date, str) or raw_date == "0000-00-00":
                continue
            try:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d")
            except ValueError:
                continue

            value_prc: float | None = None
            if value_prc_idx is not None and len(row) > value_prc_idx:
                raw_value_prc = row[value_prc_idx]
                try:
                    value_prc = float(raw_value_prc)
                except (TypeError, ValueError):
                    value_prc = None
            parsed_items.append((parsed_date, value_prc))
            if value_prc is None or value_prc < 99.999:
                partial_dates.append(parsed_date)

        if partial_dates:
            return min(partial_dates).strftime("%Y-%m-%d")

        if not parsed_items:
            return None

        if len(parsed_items) == 1:
            only_date, only_value_prc = parsed_items[0]
            only_date_str = only_date.strftime("%Y-%m-%d")
            is_full_redemption = only_value_prc is not None and only_value_prc >= 99.999
            if is_full_redemption or (matdate and only_date_str == matdate):
                return None
            return only_date_str

        if value_prc_idx is not None:
            # Если VALUEPRC есть и частичных выплат нет, это погашения, а не амортизация.
            return None

        return min(date for date, _ in parsed_items).strftime("%Y-%m-%d")

    def _get_with_rate_limit(
        self,
        url: str,
        params: dict[str, Any],
        timeout: int,
        delay_seconds: float,
    ) -> requests.Response:
        delay = max(0.0, float(delay_seconds))
        sleep_for = 0.0
        if delay > 0:
            with self._request_lock:
                now = time.monotonic()
                sleep_for = max(0.0, self._next_request_ts - now)
                reserve_from = max(now, self._next_request_ts)
                self._next_request_ts = reserve_from + delay
        if sleep_for > 0:
            time.sleep(sleep_for)

        session = self._get_thread_session()
        return session.get(url, params=params, timeout=timeout)

    def _get_thread_session(self) -> requests.Session:
        if threading.current_thread() is threading.main_thread():
            return self.session

        thread_session = getattr(self._thread_local, "session", None)
        if thread_session is None:
            thread_session = requests.Session()
            self._thread_local.session = thread_session
        return thread_session
