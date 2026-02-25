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

        while True:
            self.logger.info("Запрос страницы MOEX: start=%s", start)
            page_data, page_errors = self._fetch_page(start)
            errors += page_errors

            if not page_data:
                break

            bonds.extend(page_data)
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
