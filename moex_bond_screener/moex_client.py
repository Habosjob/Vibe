"""Клиент MOEX ISS для получения списка облигаций."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import requests

from .config import AppConfig
from .raw_store import RawStore


class MoexClient:
    def __init__(self, config: AppConfig, logger: logging.Logger, raw_store: RawStore | None = None) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.raw_store = raw_store

    def fetch_all_bonds(self) -> tuple[list[dict[str, Any]], int]:
        bonds: list[dict[str, Any]] = []
        errors = 0
        start = 0
        seen_secids: set[str] = set()

        while True:
            self.logger.info("Запрос страницы MOEX: start=%s", start)
            page_data, page_errors = self._fetch_page(start)
            errors += page_errors

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
            start += self.config.page_size
            time.sleep(self.config.request_delay_seconds)

        return bonds, errors

    def enrich_amortization_start_dates(self, bonds: list[dict[str, Any]]) -> int:
        """Обогащает список бумаг полем Amortization_start_date.

        Значение заполняется самой ранней датой амортизации по данным MOEX,
        либо пустой строкой, если амортизации нет.
        """

        errors = 0
        for bond in bonds:
            secid = str(bond.get("SECID") or "").strip()
            if not secid:
                bond["Amortization_start_date"] = ""
                continue

            date_value, request_errors = self._fetch_amortization_start_date(secid)
            errors += request_errors
            bond["Amortization_start_date"] = date_value

            if self.config.request_delay_seconds > 0:
                time.sleep(self.config.request_delay_seconds)

        return errors

    def _fetch_page(self, start: int) -> tuple[list[dict[str, Any]], int]:
        params = {
            "iss.meta": "off",
            "iss.only": "securities",
            "start": start,
            "limit": self.config.page_size,
        }

        for attempt in range(1, self.config.retries + 1):
            try:
                response = self.session.get(
                    self.config.base_url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_json(f"bonds_page_{start}.json", response.text)

                columns = payload["securities"]["columns"]
                rows = payload["securities"]["data"]
                items = [dict(zip(columns, row, strict=False)) for row in rows]
                return items, 0
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса start=%s попытка=%s: %s", start, attempt, error)
                if attempt == self.config.retries:
                    return [], 1
                time.sleep(self.config.request_delay_seconds * attempt)

        return [], 1

    def _fetch_amortization_start_date(self, secid: str) -> tuple[str, int]:
        url = f"https://iss.moex.com/iss/securities/{secid}/bondization.json"
        params = {"iss.meta": "off", "iss.only": "amortizations"}

        for attempt in range(1, self.config.retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_json(f"amortization_{secid}.json", response.text)

                earliest = self._extract_earliest_amortization_date(payload)
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
                time.sleep(self.config.request_delay_seconds * attempt)

        return "", 1

    @staticmethod
    def _extract_earliest_amortization_date(payload: dict[str, Any]) -> str | None:
        amortizations = payload.get("amortizations") or {}
        columns = amortizations.get("columns") or []
        rows = amortizations.get("data") or []
        if not columns or not rows:
            return None

        col_map = {name.upper(): idx for idx, name in enumerate(columns)}
        date_idx = col_map.get("AMORTDATE")
        if date_idx is None:
            return None

        parsed_dates: list[datetime] = []
        for row in rows:
            if len(row) <= date_idx:
                continue
            raw_date = row[date_idx]
            if not isinstance(raw_date, str) or raw_date == "0000-00-00":
                continue
            try:
                parsed_dates.append(datetime.strptime(raw_date, "%Y-%m-%d"))
            except ValueError:
                continue

        if not parsed_dates:
            return None

        return min(parsed_dates).strftime("%Y-%m-%d")
