from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from . import config
from .helpers import (
    is_cache_fresh,
    json_dump,
    json_load,
    md5_short,
    parse_date,
    request_with_retries,
    sanitize_str,
    to_iso_date_str,
)


class EDisclosureClient:
    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": config.BROWSER_USER_AGENT,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "ru,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Origin": "https://www.e-disclosure.ru",
                "Referer": "https://www.e-disclosure.ru/poisk-po-kompaniyam",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            }
        )
        self._init_session()

    def _cache_file(self, company_id: str, data_type: str) -> Path:
        key = md5_short(f"{company_id}_{data_type}", 10)
        return config.CACHE_DIR / "edisclosure" / f"{key}.json"

    def _init_session(self) -> None:
        request_with_retries(self.session, "GET", "https://www.e-disclosure.ru/", self.logger)
        request_with_retries(
            self.session,
            "GET",
            "https://www.e-disclosure.ru/poisk-po-kompaniyam",
            self.logger,
        )

    def search_company_by_inn(self, inn: str) -> list[dict[str, str]]:
        payload = {
            "textfield": inn,
            "radReg": "FederalDistricts",
            "districtsCheckboxGroup": "-1",
            "regionsCheckboxGroup": "-1",
            "branchesCheckboxGroup": "-1",
            "lastPageSize": "10",
            "lastPageNumber": "1",
            "query": inn,
            "mode": "companies",
        }
        response = request_with_retries(
            self.session,
            "POST",
            "https://www.e-disclosure.ru/api/search/companies",
            self.logger,
            data=payload,
        )
        data = response.json() if response.text else {}
        rows = data.get("foundCompaniesList") or []
        result: list[dict[str, str]] = []
        for item in rows:
            company_id = sanitize_str(item.get("id"))
            if not company_id:
                continue
            result.append(
                {
                    "id": company_id,
                    "name": sanitize_str(item.get("name")),
                    "district": sanitize_str(item.get("district")),
                    "region": sanitize_str(item.get("region")),
                    "branch": sanitize_str(item.get("branch")),
                    "lastActivity": sanitize_str(item.get("lastActivity")),
                    "docCount": sanitize_str(item.get("docCount")),
                    "url": f"https://www.e-disclosure.ru/portal/company.aspx?id={company_id}",
                }
            )
        return result

    def get_company_card(self, company_id: str) -> dict[str, str]:
        cache_path = self._cache_file(company_id, "card")
        if is_cache_fresh(cache_path, config.EDISCLOSURE_CARD_TTL_HOURS):
            cached = json_load(cache_path)
            if cached:
                return cached
        url = f"https://www.e-disclosure.ru/portal/company.aspx?id={company_id}"
        response = request_with_retries(self.session, "GET", url, self.logger)
        soup = BeautifulSoup(response.text, "lxml")
        text = soup.get_text(" ", strip=True)

        def re_find(pattern: str) -> str:
            import re

            match = re.search(pattern, text, flags=re.IGNORECASE)
            return sanitize_str(match.group(1)) if match else ""

        card = {
            "inn": re_find(r"ИНН\s*:?\s*(\d{10,12})"),
            "ogrn": re_find(r"ОГРН\s*:?\s*(\d{13,15})"),
            "registration_date": re_find(r"Дата\s+регистрац(?:ии|ии:)\s*:?\s*(\d{2}[./]\d{2}[./]\d{4})"),
            "address": "",
            "url": url,
        }
        for tr in soup.select("tr"):
            row_text = tr.get_text(" ", strip=True).lower()
            if "адрес" in row_text and len(row_text) > 8:
                card["address"] = sanitize_str(tr.get_text(" ", strip=True).replace("Адрес", ""))
                break
        json_dump(cache_path, card)
        return card

    def choose_best_candidate(self, inn: str, candidates: list[dict[str, str]], company_name: str = "") -> dict[str, str] | None:
        if not candidates:
            return None
        # 1) точный ИНН через карточку
        for candidate in candidates:
            card = self.get_company_card(candidate["id"])
            if sanitize_str(card.get("inn")) == sanitize_str(inn):
                return candidate
        # 2) релевантность по названию + активности
        low_name = sanitize_str(company_name).lower()
        ranked = sorted(
            candidates,
            key=lambda c: (
                1 if low_name and low_name in c.get("name", "").lower() else 0,
                sanitize_str(c.get("docCount", "0")).isdigit(),
                sanitize_str(c.get("lastActivity", "")),
            ),
            reverse=True,
        )
        return ranked[0]

    def get_company_events(self, company_id: str, days_back: int = 365) -> list[dict[str, str]]:
        cache_path = self._cache_file(company_id, "events")
        if is_cache_fresh(cache_path, config.EDISCLOSURE_EVENTS_TTL_HOURS):
            cached = json_load(cache_path)
            if cached and isinstance(cached.get("items"), list):
                return cached["items"]

        now = datetime.now()
        min_date = now - timedelta(days=days_back)
        years = {now.year, min_date.year}
        events: list[dict[str, str]] = []
        for year in sorted(years):
            url = f"https://www.e-disclosure.ru/api/events/page?companyId={company_id}&year={year}"
            try:
                response = request_with_retries(self.session, "GET", url, self.logger)
                rows = response.json() if response.text else []
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Events load failed company=%s year=%s: %s", company_id, year, exc)
                continue
            for item in rows if isinstance(rows, list) else []:
                event_date = to_iso_date_str(item.get("eventDate"))
                pub_date = to_iso_date_str(item.get("pubDate"))
                ref_date = parse_date(event_date) or parse_date(pub_date)
                if ref_date and ref_date < min_date:
                    continue
                guid = sanitize_str(item.get("pseudoGUID"))
                if not guid:
                    continue
                events.append(
                    {
                        "pseudoGUID": guid,
                        "eventName": sanitize_str(item.get("eventName")),
                        "eventDate": event_date,
                        "pubDate": pub_date,
                        "isCorrectedByAnotherEvent": str(item.get("isCorrectedByAnotherEvent", "")),
                        "url": f"https://www.e-disclosure.ru/portal/event.aspx?EventId={guid}",
                    }
                )
        events.sort(key=lambda x: x.get("pubDate") or x.get("eventDate") or "", reverse=True)
        json_dump(cache_path, {"items": events})
        return events

    def get_financial_reports(self, company_id: str) -> list[dict[str, str]]:
        cache_path = self._cache_file(company_id, "reports")
        if is_cache_fresh(cache_path, config.EDISCLOSURE_REPORTS_TTL_HOURS):
            cached = json_load(cache_path)
            if cached and isinstance(cached.get("items"), list):
                return cached["items"]

        report_types = {
            2: "Годовая",
            3: "Финансовая",
            4: "Консолидированная",
            5: "Отчет эмитента",
        }
        keywords = ("отчет", "бухгалтер", "финанс", "баланс", "прибыль", "убыток", "аудитор", "годовой", "промежуточный")
        result: list[dict[str, str]] = []
        for type_id, report_type in report_types.items():
            page_url = f"https://www.e-disclosure.ru/portal/files.aspx?id={company_id}&type={type_id}"
            try:
                response = request_with_retries(self.session, "GET", page_url, self.logger)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Files load failed company=%s type=%s: %s", company_id, type_id, exc)
                continue
            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", class_="zebra")
            if not table:
                continue
            for tr in table.select("tr"):
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue
                doc_type = sanitize_str(tds[0].get_text(" ", strip=True))
                period = sanitize_str(tds[1].get_text(" ", strip=True)) if len(tds) > 1 else ""
                foundation_date = to_iso_date_str(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else ""
                placement_date = to_iso_date_str(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else ""
                link_cell = tds[4] if len(tds) > 4 else tds[-1]
                anchor = link_cell.find("a", href=True)
                file_url = ""
                if anchor:
                    href = sanitize_str(anchor.get("href"))
                    if href.startswith("/"):
                        href = f"https://www.e-disclosure.ru{href}"
                    if "FileLoad.ashx" in href:
                        file_url = href
                low_doc = doc_type.lower()
                relevant = any(k in low_doc for k in keywords) or bool(period)
                if not relevant:
                    continue
                event_hash = md5_short(
                    f"{company_id}_{type_id}_{doc_type}_{period}_{placement_date}",
                    16,
                )
                result.append(
                    {
                        "hash": event_hash,
                        "company_id": company_id,
                        "type_id": str(type_id),
                        "report_type": report_type,
                        "doc_type": doc_type,
                        "period": period,
                        "foundation_date": foundation_date,
                        "placement_date": placement_date,
                        "file_url": file_url,
                        "page_url": page_url,
                    }
                )
        dedup = {row["hash"]: row for row in result}
        rows = list(dedup.values())
        rows.sort(key=lambda x: x.get("placement_date") or x.get("foundation_date") or "", reverse=True)
        json_dump(cache_path, {"items": rows})
        return rows
