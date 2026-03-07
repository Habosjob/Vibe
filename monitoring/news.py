from __future__ import annotations

import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from . import config
from .helpers import md5_short, request_with_retries, sanitize_str


class NewsCacheManager:
    def __init__(self, cache_file: Path):
        self.cache_file = cache_file
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.rows = self._load()
        self.known_hashes = {row["hash"] for row in self.rows if row.get("hash")}

    def _load(self) -> list[dict[str, str]]:
        if not self.cache_file.exists():
            return []
        with self.cache_file.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def is_new(self, hash_value: str) -> bool:
        return hash_value not in self.known_hashes

    def add(self, row: dict[str, str]) -> None:
        if row["hash"] in self.known_hashes:
            return
        self.rows.append(row)
        self.known_hashes.add(row["hash"])

    def save(self) -> None:
        fieldnames = ["hash", "company_name", "company_inn", "date", "title", "source", "url", "added_date"]
        with self.cache_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.rows:
                writer.writerow(row)


class SmartlabNewsCollector:
    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": config.BROWSER_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru,en;q=0.9",
                "Connection": "keep-alive",
            }
        )

    def _normalize_date(self, text: str) -> datetime:
        now = datetime.now()
        t = sanitize_str(text)
        try:
            if "/" in t and len(t) <= 5:
                d, m = t.split("/")
                dt = datetime(year=now.year, month=int(m), day=int(d))
                if dt.date() > now.date():
                    dt = dt.replace(year=now.year - 1)
                return dt
            if ":" in t and len(t) <= 5:
                hh, mm = t.split(":")
                return now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
            for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(t, fmt)
                except ValueError:
                    continue
        except Exception:  # noqa: BLE001
            pass
        return now

    def _parse_news_lines(self, html: str) -> list[dict[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        rows = []
        for block in soup.select("div.news__line")[:50]:
            date_node = block.select_one("div.news__date")
            link_node = block.select_one("div.news__link a")
            if not link_node:
                continue
            title = sanitize_str(link_node.get_text(" ", strip=True))
            href = sanitize_str(link_node.get("href"))
            if href.startswith("/"):
                href = f"https://smartlab.news{href}"
            dt = self._normalize_date(date_node.get_text(" ", strip=True) if date_node else "")
            if dt < datetime.now() - timedelta(days=config.NEWS_DAYS_BACK):
                continue
            rows.append({"title": title, "url": href, "news_date": dt.date().isoformat()})
        return rows

    def _relevant_for_company(self, title: str, company_name: str) -> bool:
        stop = {"пао", "ао", "ооо", "зао", "публичное", "акционерное", "общество"}
        words = [w for w in sanitize_str(company_name).lower().replace('"', "").split() if len(w) > 2 and w not in stop]
        low_title = sanitize_str(title).lower()
        return any(w in low_title for w in words) if words else True

    def _tag_name(self, company_name: str) -> str:
        text = sanitize_str(company_name).lower().replace('"', "")
        for token in ["пао", "ао", "ооо", "зао", "публичное акционерное общество", "акционерное общество"]:
            text = text.replace(token, "")
        return quote(text.strip())

    def collect_for_item(self, item: dict[str, str]) -> list[dict[str, str]]:
        ticker = item.get("instrument_code", "")
        company_name = item.get("company_name", "")
        result: list[dict[str, str]] = []

        if ticker:
            url = f"https://smartlab.news/company/{ticker}"
            try:
                response = request_with_retries(self.session, "GET", url, self.logger)
                result = self._parse_news_lines(response.text)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Smartlab ticker strategy failed %s: %s", ticker, exc)
            time.sleep(config.NEWS_REQUEST_PAUSE_SECONDS)

        if not result:
            tag = self._tag_name(company_name)
            url = f"https://smartlab.news/tag/{tag}"
            try:
                response = request_with_retries(self.session, "GET", url, self.logger)
                parsed = self._parse_news_lines(response.text)
                result = [row for row in parsed if self._relevant_for_company(row["title"], company_name)]
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Smartlab fallback failed %s: %s", company_name, exc)
            time.sleep(config.NEWS_REQUEST_PAUSE_SECONDS)
        return result


def build_news_hash(url: str, title: str, date_value: str) -> str:
    return md5_short(f"{url}_{sanitize_str(title)[:50]}_{date_value}", 16)
