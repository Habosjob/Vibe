"""Клиент MOEX ISS для получения списка облигаций."""

from __future__ import annotations

import logging
import time
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

    def _fetch_page(self, start: int) -> tuple[list[dict[str, Any]], int]:
        params = {
            "iss.meta": "off",
            "start": start,
            "limit": self.config.page_size,
            "securities.columns": "SECID,SHORTNAME,ISIN,FACEUNIT,LISTLEVEL,PREVLEGALCLOSEPRICE",
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
