from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from tqdm import tqdm

# Важно для запуска на Windows как: python monitoring/main.py
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import config  # noqa: E402


# -----------------------------
# Базовые утилиты и лог
# -----------------------------
def ensure_dirs() -> None:
    for path in [
        config.CACHE_DIR,
        config.RAW_DIR,
        config.DB_DIR,
        config.LOGS_DIR,
        config.BASE_SNAPSHOTS_DIR,
        config.CACHE_DIR / "edisclosure",
        config.CACHE_DIR / "news",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dirs()
    logger = logging.getLogger("monitoring")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(config.LOG_FILE, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def sanitize_str(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def md5_short(value: str, size: int = 16) -> str:
    return hashlib.md5(value.encode("utf-8", errors="ignore")).hexdigest()[:size]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_iso() -> str:
    return date.today().isoformat()


def parse_date(value: Any) -> datetime | None:
    text = sanitize_str(value)
    if not text:
        return None
    for fmt in [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%y",
    ]:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def to_iso_date_str(value: Any) -> str:
    dt = parse_date(value)
    return dt.date().isoformat() if dt else ""


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    logger: logging.Logger,
    timeout: float | None = None,
    **kwargs: Any,
) -> requests.Response:
    timeout = timeout or config.REQUEST_TIMEOUT_SECONDS
    last_error: Exception | None = None
    for attempt in range(config.HTTP_RETRIES + 1):
        try:
            response = session.request(method=method, url=url, timeout=timeout, **kwargs)
            if response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}: {url}")
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= config.HTTP_RETRIES:
                break
            sleep_for = config.BACKOFF_BASE_SECONDS * (attempt + 1)
            logger.warning("Retry %s for %s %s due to %s", attempt + 1, method, url, exc)
            time.sleep(sleep_for)
    raise RuntimeError(f"Request failed: {method} {url}: {last_error}")


def timed(func):
    started = time.perf_counter()
    result = func()
    return result, time.perf_counter() - started


# -----------------------------
# SQLite
# -----------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS company_map (
    inn TEXT PRIMARY KEY,
    company_id TEXT,
    company_name TEXT,
    company_url TEXT,
    last_checked_at TEXT
);
CREATE TABLE IF NOT EXISTS report_events (
    event_hash TEXT PRIMARY KEY,
    inn TEXT,
    company_name TEXT,
    scoring_date TEXT,
    event_date TEXT,
    event_type TEXT,
    event_url TEXT,
    source TEXT,
    payload_json TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS emitents_snapshot (
    inn TEXT PRIMARY KEY,
    company_name TEXT,
    scoring TEXT,
    scoring_date TEXT,
    nra_rate TEXT,
    acra_rate TEXT,
    nkr_rate TEXT,
    raex_rate TEXT,
    snapshot_at TEXT
);
CREATE TABLE IF NOT EXISTS news_events (
    event_hash TEXT PRIMARY KEY,
    instrument_type TEXT,
    instrument_code TEXT,
    inn TEXT,
    company_name TEXT,
    news_date TEXT,
    title TEXT,
    url TEXT,
    source TEXT,
    first_seen_at TEXT
);
CREATE TABLE IF NOT EXISTS portfolio_items (
    instrument_type TEXT,
    instrument_code TEXT,
    inn TEXT,
    company_name TEXT,
    source_file TEXT,
    loaded_at TEXT,
    PRIMARY KEY (instrument_type, instrument_code)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


# -----------------------------
# E-disclosure
# -----------------------------
class EDisclosureClient:
    def __init__(self, logger: logging.Logger):
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
        request_with_retries(self.session, "GET", "https://www.e-disclosure.ru/", logger)
        request_with_retries(self.session, "GET", "https://www.e-disclosure.ru/poisk-po-kompaniyam", logger)

    def _cache_file(self, company_id: str, data_type: str) -> Path:
        return config.CACHE_DIR / "edisclosure" / f"{md5_short(f'{company_id}_{data_type}', 10)}.json"

    def _load_cache(self, company_id: str, data_type: str, ttl_hours: int) -> dict[str, Any] | None:
        path = self._cache_file(company_id, data_type)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > ttl_hours * 3600:
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def _save_cache(self, company_id: str, data_type: str, payload: dict[str, Any]) -> None:
        path = self._cache_file(company_id, data_type)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

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
        items = data.get("foundCompaniesList") or []
        out = []
        for row in items:
            company_id = sanitize_str(row.get("id"))
            if not company_id:
                continue
            out.append(
                {
                    "id": company_id,
                    "name": sanitize_str(row.get("name")),
                    "district": sanitize_str(row.get("district")),
                    "region": sanitize_str(row.get("region")),
                    "branch": sanitize_str(row.get("branch")),
                    "lastActivity": sanitize_str(row.get("lastActivity")),
                    "docCount": sanitize_str(row.get("docCount")),
                    "url": f"https://www.e-disclosure.ru/portal/company.aspx?id={company_id}",
                }
            )
        return out

    def get_company_card(self, company_id: str) -> dict[str, str]:
        cached = self._load_cache(company_id, "card", config.EDISCLOSURE_CARD_TTL_HOURS)
        if cached:
            return cached
        url = f"https://www.e-disclosure.ru/portal/company.aspx?id={company_id}"
        html = request_with_retries(self.session, "GET", url, self.logger).text
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)

        def find_re(pattern: str) -> str:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            return sanitize_str(match.group(1)) if match else ""

        card = {
            "inn": find_re(r"ИНН\s*:?\s*(\d{10,12})"),
            "ogrn": find_re(r"ОГРН\s*:?\s*(\d{13,15})"),
            "registration_date": find_re(r"Дата\s+регистрац(?:ии|ии:)\s*:?\s*(\d{2}[./]\d{2}[./]\d{4})"),
            "address": "",
            "url": url,
        }
        for tr in soup.select("tr"):
            row_text = tr.get_text(" ", strip=True).lower()
            if "адрес" in row_text:
                card["address"] = sanitize_str(tr.get_text(" ", strip=True).replace("Адрес", ""))
                break
        self._save_cache(company_id, "card", card)
        return card

    def choose_best_candidate(self, inn: str, candidates: list[dict[str, str]], company_name: str) -> dict[str, str] | None:
        if not candidates:
            return None
        for candidate in candidates:
            card = self.get_company_card(candidate["id"])
            if sanitize_str(card.get("inn")) == sanitize_str(inn):
                return candidate
        low_name = sanitize_str(company_name).lower()
        ranked = sorted(
            candidates,
            key=lambda x: (
                1 if low_name and low_name in x.get("name", "").lower() else 0,
                int(sanitize_str(x.get("docCount", "0")) or "0"),
                sanitize_str(x.get("lastActivity", "")),
            ),
            reverse=True,
        )
        return ranked[0]

    def get_financial_reports(self, company_id: str) -> list[dict[str, str]]:
        cached = self._load_cache(company_id, "reports", config.EDISCLOSURE_REPORTS_TTL_HOURS)
        if cached and isinstance(cached.get("items"), list):
            return cached["items"]

        report_types = {2: "Годовая", 3: "Финансовая", 4: "Консолидированная", 5: "Отчет эмитента"}
        keywords = ("отчет", "бухгалтер", "финанс", "баланс", "прибыль", "убыток", "аудитор", "годовой", "промежуточный")
        rows: list[dict[str, str]] = []

        for type_id, type_name in report_types.items():
            page_url = f"https://www.e-disclosure.ru/portal/files.aspx?id={company_id}&type={type_id}"
            try:
                html = request_with_retries(self.session, "GET", page_url, self.logger).text
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("files load failed company=%s type=%s: %s", company_id, type_id, exc)
                continue
            soup = BeautifulSoup(html, "lxml")
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
                if not (any(k in doc_type.lower() for k in keywords) or period):
                    continue
                anchor = (tds[4] if len(tds) > 4 else tds[-1]).find("a", href=True)
                file_url = ""
                if anchor:
                    href = sanitize_str(anchor.get("href"))
                    if href.startswith("/"):
                        href = f"https://www.e-disclosure.ru{href}"
                    if "FileLoad.ashx" in href:
                        file_url = href
                rows.append(
                    {
                        "hash": md5_short(f"{company_id}_{type_id}_{doc_type}_{period}_{placement_date}", 16),
                        "company_id": company_id,
                        "type_id": str(type_id),
                        "report_type": type_name,
                        "doc_type": doc_type,
                        "period": period,
                        "foundation_date": foundation_date,
                        "placement_date": placement_date,
                        "file_url": file_url,
                        "page_url": page_url,
                    }
                )
        dedup = list({x["hash"]: x for x in rows}.values())
        dedup.sort(key=lambda x: x.get("placement_date") or x.get("foundation_date") or "", reverse=True)
        self._save_cache(company_id, "reports", {"items": dedup})
        return dedup


# -----------------------------
# Emitents snapshot + rating changes
# -----------------------------
OUTLOOK_MARKERS = {
    "позитив",
    "положитель",
    "stable",
    "стабиль",
    "negative",
    "негатив",
    "developing",
    "развива",
    "positive",
    "watch",
    "revision",
}


@dataclass
class EmitentRow:
    inn: str
    company_name: str
    scoring: str
    scoring_date: str
    nra_rate: str
    acra_rate: str
    nkr_rate: str
    raex_rate: str


def load_emitents_rows(path: Path) -> list[EmitentRow]:
    if not path.exists():
        return []
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    headers = [sanitize_str(c.value).replace(" ", "").lower() for c in ws[1]]
    idx = {h: i for i, h in enumerate(headers)}

    def get(values: list[Any], key: str) -> str:
        pos = idx.get(key)
        if pos is None or pos >= len(values):
            return ""
        value = values[pos]
        if hasattr(value, "isoformat"):
            try:
                return value.date().isoformat()
            except Exception:  # noqa: BLE001
                return value.isoformat()
        return sanitize_str(value)

    result: list[EmitentRow] = []
    for raw in ws.iter_rows(min_row=2, values_only=True):
        values = list(raw)
        inn = get(values, "inn")
        if not inn:
            continue
        result.append(
            EmitentRow(
                inn=inn,
                company_name=get(values, "emitentname"),
                scoring=get(values, "scoring"),
                scoring_date=get(values, "datescoring"),
                nra_rate=get(values, "nra_rate"),
                acra_rate=get(values, "acra_rate"),
                nkr_rate=get(values, "nkr_rate"),
                raex_rate=get(values, "raex_rate"),
            )
        )
    return result


def save_emitents_snapshot_excel(rows: list[EmitentRow]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "emitents_snapshot"
    ws.append(["INN", "EMITENTNAME", "Scoring", "DateScoring", "NRA_Rate", "Acra_Rate", "NKR_Rate", "RAEX_Rate", "SnapshotAt"])
    for row in rows:
        ws.append([row.inn, row.company_name, row.scoring, row.scoring_date, row.nra_rate, row.acra_rate, row.nkr_rate, row.raex_rate, today_iso()])
    wb.save(config.EMITENTS_SNAPSHOT_XLSX)


def split_rating_and_outlook(text: str) -> tuple[str, str]:
    low = sanitize_str(text).lower()
    if not low:
        return "", ""
    outlook = [m for m in OUTLOOK_MARKERS if m in low]
    rate_part = low
    for m in outlook:
        rate_part = rate_part.replace(m, "")
    return " ".join(rate_part.split()), "|".join(sorted(outlook))


def classify_rating_change(old: str, new: str) -> str | None:
    old_clean, new_clean = sanitize_str(old), sanitize_str(new)
    if old_clean == new_clean:
        return None
    if old_clean and not new_clean:
        return "Рейтинг отозван / снят"
    old_rate, old_outlook = split_rating_and_outlook(old_clean)
    new_rate, new_outlook = split_rating_and_outlook(new_clean)
    if old_rate == new_rate and old_outlook != new_outlook:
        return "Изменен прогноз"
    return "Изменен рейтинг"


# -----------------------------
# Portfolio loader
# -----------------------------
def find_portfolio_file() -> Path | None:
    if config.PORTFOLIO_SOURCE_FILE:
        explicit = Path(config.PORTFOLIO_SOURCE_FILE)
        if explicit.exists():
            return explicit
    candidates: list[Path] = []
    for pattern in config.PORTFOLIO_GLOBS:
        candidates.extend(Path.cwd().glob(pattern))
    filtered = []
    for p in candidates:
        rp = p.resolve()
        if str(config.BASE_DIR.resolve()).lower() in str(rp).lower():
            continue
        if rp.name in {config.PORTFOLIO_XLSX.name, config.REPORTS_XLSX.name}:
            continue
        filtered.append(rp)
    if not filtered:
        return None
    return sorted(filtered, key=lambda x: x.stat().st_mtime, reverse=True)[0]


def load_portfolio_items(path: Path | None, logger: logging.Logger) -> list[dict[str, str]]:
    if not path or not path.exists():
        logger.info("Portfolio source not found")
        return []
    try:
        wb = load_workbook(path, data_only=True)
    except Exception as exc:  # noqa: BLE001
        logger.error("Cannot open portfolio file %s: %s", path, exc)
        return []

    def norm(name: str) -> str:
        return sanitize_str(name).replace(" ", "").lower()

    def parse_sheet(ws, instrument_type: str) -> list[dict[str, str]]:
        items = []
        headers = [norm(c.value or "") for c in ws[1]]
        idx = {h: i for i, h in enumerate(headers)}

        def get(values, *keys: str) -> str:
            for key in keys:
                pos = idx.get(norm(key))
                if pos is not None and pos < len(values):
                    value = sanitize_str(values[pos])
                    if value:
                        return value
            return ""

        for values in ws.iter_rows(min_row=2, values_only=True):
            values = list(values)
            inn = get(values, "ИНН")
            company_name = get(values, "Наименование эмитента")
            ticker = get(values, "Тикер")
            isin = get(values, "ISIN")
            issuer_ticker = get(values, "Тикер эмитента")
            if instrument_type == "Stock":
                code = ticker
            else:
                code = isin or issuer_ticker or ticker
                if not code:
                    code = md5_short(f"{inn}_{company_name}", 12)
            if not code and not inn and not company_name:
                continue
            items.append(
                {
                    "instrument_type": instrument_type,
                    "instrument_code": code,
                    "inn": inn,
                    "company_name": company_name,
                }
            )
        return items

    result: list[dict[str, str]] = []
    for sheet_name, instrument_type in [("Акции", "Stock"), ("Облигации", "Bond")]:
        if sheet_name not in wb.sheetnames:
            logger.warning("Sheet %s missing in portfolio", sheet_name)
            continue
        try:
            result.extend(parse_sheet(wb[sheet_name], instrument_type))
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to process sheet %s: %s", sheet_name, exc)
            continue
    return list({(x["instrument_type"], x["instrument_code"]): x for x in result if x.get("instrument_code")}.values())


# -----------------------------
# News
# -----------------------------
class NewsCacheManager:
    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.rows = self._load()
        self.known_hashes = {row["hash"] for row in self.rows if row.get("hash")}

    def _load(self) -> list[dict[str, str]]:
        if not self.cache_path.exists():
            return []
        with self.cache_path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def is_new(self, hash_value: str) -> bool:
        return hash_value not in self.known_hashes

    def add(self, row: dict[str, str]) -> None:
        if row["hash"] in self.known_hashes:
            return
        self.rows.append(row)
        self.known_hashes.add(row["hash"])

    def save(self) -> None:
        with self.cache_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["hash", "company_name", "company_inn", "date", "title", "source", "url", "added_date"],
            )
            writer.writeheader()
            for row in self.rows:
                writer.writerow(row)


class SmartlabNewsCollector:
    def __init__(self, logger: logging.Logger):
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
                    pass
        except Exception:  # noqa: BLE001
            pass
        return now

    def _parse_news_lines(self, html: str) -> list[dict[str, str]]:
        rows = []
        soup = BeautifulSoup(html, "lxml")
        for block in soup.select("div.news__line")[:50]:
            date_node = block.select_one("div.news__date")
            link_node = block.select_one("div.news__link > a")
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

    def _tag_name(self, company_name: str) -> str:
        text = sanitize_str(company_name).lower().replace('"', "")
        for token in ["пао", "ао", "ооо", "зао", "публичное акционерное общество", "акционерное общество"]:
            text = text.replace(token, "")
        return quote(text.strip())

    def _relevant_title(self, title: str, company_name: str) -> bool:
        stop_words = {"пао", "ао", "ооо", "зао", "публичное", "акционерное", "общество"}
        words = [w for w in sanitize_str(company_name).lower().replace('"', "").split() if len(w) > 2 and w not in stop_words]
        low_title = sanitize_str(title).lower()
        return any(w in low_title for w in words) if words else True

    def collect(self, item: dict[str, str]) -> list[dict[str, str]]:
        ticker = item.get("instrument_code", "")
        company_name = item.get("company_name", "")
        result: list[dict[str, str]] = []

        if ticker:
            try:
                html = request_with_retries(self.session, "GET", f"https://smartlab.news/company/{ticker}", self.logger).text
                result = self._parse_news_lines(html)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Smartlab ticker strategy failed %s: %s", ticker, exc)
            time.sleep(config.NEWS_REQUEST_PAUSE_SECONDS)

        if not result:
            tag = self._tag_name(company_name)
            try:
                html = request_with_retries(self.session, "GET", f"https://smartlab.news/tag/{tag}", self.logger).text
                result = [x for x in self._parse_news_lines(html) if self._relevant_title(x["title"], company_name)]
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Smartlab fallback failed %s: %s", company_name, exc)
            time.sleep(config.NEWS_REQUEST_PAUSE_SECONDS)

        return result


# -----------------------------
# Excel exporters
# -----------------------------
def apply_ws_style(ws, url_headers: set[str]) -> None:
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    headers = [c.value for c in ws[1]]
    fill = PatternFill(start_color=config.NEW_ITEM_FILL_COLOR, end_color=config.NEW_ITEM_FILL_COLOR, fill_type="solid")

    for row in ws.iter_rows(min_row=2):
        row_map = {headers[i]: row[i] for i in range(len(headers))}
        for h in url_headers:
            c = row_map.get(h)
            if c and c.value:
                c.hyperlink = str(c.value)
                c.style = "Hyperlink"
        if row_map.get("_is_new") and row_map["_is_new"].value:
            for c in row:
                c.fill = fill

    for i, header in enumerate(headers, start=1):
        if header == "_is_new":
            ws.column_dimensions[get_column_letter(i)].hidden = True

    for col in ws.columns:
        letter = get_column_letter(col[0].column)
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[letter].width = min(max_len + 2, config.MAX_EXCEL_COL_WIDTH)


def export_reports(events: list[dict[str, str]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Reports"
    ws.append(["ИНН", "Наименование", "Дата скоринга", "Дата события", "Событие", "Ссылка", "_is_new"])
    for row in sorted(events, key=lambda x: x.get("event_date", ""), reverse=True):
        ws.append([
            row.get("inn", ""),
            row.get("company_name", ""),
            row.get("scoring_date", ""),
            row.get("event_date", ""),
            row.get("event_type", ""),
            row.get("event_url", ""),
            "1" if row.get("is_new") else "",
        ])
    apply_ws_style(ws, {"Ссылка"})
    wb.save(config.REPORTS_XLSX)


def export_portfolio(
    portfolio_items: list[dict[str, str]],
    latest_event_by_inn: dict[str, dict[str, str]],
    latest_news_by_key: dict[tuple[str, str], dict[str, str]],
    news_rows: list[dict[str, str]],
) -> None:
    wb = Workbook()

    ws_all = wb.active
    ws_all.title = "Portfolio_All"
    ws_all.append([
        "Тип", "ISIN / Тикер", "ИНН", "Наименование", "Дата скоринга", "Последнее событие",
        "Дата последнего события", "Ссылка на последнее событие", "Последняя новость", "Дата последней новости",
        "Ссылка на последнюю новость", "_is_new",
    ])
    for item in portfolio_items:
        evt = latest_event_by_inn.get(item.get("inn", ""), {})
        news = latest_news_by_key.get((item.get("instrument_type", ""), item.get("instrument_code", "")), {})
        ws_all.append([
            item.get("instrument_type", ""),
            item.get("instrument_code", ""),
            item.get("inn", ""),
            item.get("company_name", ""),
            evt.get("scoring_date", ""),
            evt.get("event_type", ""),
            evt.get("event_date", ""),
            evt.get("event_url", ""),
            news.get("title", ""),
            news.get("news_date", ""),
            news.get("url", ""),
            "1" if news.get("is_new") else "",
        ])
    apply_ws_style(ws_all, {"Ссылка на последнее событие", "Ссылка на последнюю новость"})

    ws_unique = wb.create_sheet("Portfolio_UniqueEmitents")
    ws_unique.append([
        "ИНН", "Наименование", "Кол-во инструментов в портфеле", "Инструменты", "Дата скоринга",
        "Последнее событие", "Дата последнего события", "Ссылка на последнее событие", "Последняя новость",
        "Дата последней новости", "Ссылка на последнюю новость", "_is_new",
    ])
    grouped: dict[str, list[dict[str, str]]] = {}
    for item in portfolio_items:
        grouped.setdefault(item.get("inn", ""), []).append(item)
    for inn, items in grouped.items():
        first = items[0] if items else {}
        evt = latest_event_by_inn.get(inn, {})
        news_candidates = [latest_news_by_key.get((x.get("instrument_type", ""), x.get("instrument_code", "")), {}) for x in items]
        news_candidates.sort(key=lambda x: x.get("news_date", ""), reverse=True)
        news = news_candidates[0] if news_candidates else {}
        ws_unique.append([
            inn,
            first.get("company_name", ""),
            len(items),
            ", ".join(x.get("instrument_code", "") for x in items),
            evt.get("scoring_date", ""),
            evt.get("event_type", ""),
            evt.get("event_date", ""),
            evt.get("event_url", ""),
            news.get("title", ""),
            news.get("news_date", ""),
            news.get("url", ""),
            "1" if news.get("is_new") else "",
        ])
    apply_ws_style(ws_unique, {"Ссылка на последнее событие", "Ссылка на последнюю новость"})

    ws_news = wb.create_sheet("News")
    ws_news.append(["Тип", "ISIN / Тикер", "ИНН", "Наименование", "Дата новости", "Заголовок", "Ссылка", "Источник", "Новое", "_is_new"])
    for row in sorted(news_rows, key=lambda x: x.get("news_date", ""), reverse=True):
        ws_news.append([
            row.get("instrument_type", ""),
            row.get("instrument_code", ""),
            row.get("inn", ""),
            row.get("company_name", ""),
            row.get("news_date", ""),
            row.get("title", ""),
            row.get("url", ""),
            row.get("source", "Smartlab"),
            "✓ НОВОЕ" if row.get("is_new") else "",
            "1" if row.get("is_new") else "",
        ])
    apply_ws_style(ws_news, {"Ссылка"})

    wb.save(config.PORTFOLIO_XLSX)


# -----------------------------
# Pipeline
# -----------------------------
def run_monitoring() -> None:
    logger = setup_logger()
    conn = db_connect()
    stage_times: dict[str, float] = {}
    all_new_event_hashes: set[str] = set()

    print("=====\nЭтап 1: Загрузка эмитентов")
    emitents, elapsed = timed(lambda: load_emitents_rows(config.EMITENTS_SOURCE_FILE))
    stage_times["Этап 1: Загрузка эмитентов"] = elapsed

    print("Этап 2: Сбор отчетности")

    def stage_reports() -> None:
        client = EDisclosureClient(logger)
        for row in tqdm(emitents, desc="Сбор отчетности", position=0):
            inn = sanitize_str(row.inn)
            if not inn:
                continue
            try:
                mapping = conn.execute("SELECT * FROM company_map WHERE inn = ?", (inn,)).fetchone()
                company = None
                if mapping:
                    checked = parse_date(mapping["last_checked_at"])
                    if checked and checked >= datetime.now() - timedelta(days=config.COMPANY_MAP_TTL_DAYS):
                        company = {"id": mapping["company_id"], "name": mapping["company_name"], "url": mapping["company_url"]}
                if not company:
                    cands = client.search_company_by_inn(inn)
                    company = client.choose_best_candidate(inn, cands, row.company_name)
                    if company:
                        conn.execute(
                            """
                            INSERT INTO company_map (inn, company_id, company_name, company_url, last_checked_at)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(inn) DO UPDATE SET company_id=excluded.company_id, company_name=excluded.company_name,
                            company_url=excluded.company_url, last_checked_at=excluded.last_checked_at
                            """,
                            (inn, company.get("id", ""), company.get("name", ""), company.get("url", ""), now_iso()),
                        )
                        conn.commit()
                if not company or not company.get("id"):
                    logger.info("No company_id for INN=%s", inn)
                    continue

                reports = client.get_financial_reports(company["id"])
                latest_report_date = ""
                for rep in reports:
                    event_date = rep.get("placement_date") or rep.get("foundation_date")
                    if event_date and (not latest_report_date or event_date > latest_report_date):
                        latest_report_date = event_date
                    event = {
                        "event_hash": rep["hash"],
                        "inn": inn,
                        "company_name": row.company_name or company.get("name", ""),
                        "scoring_date": row.scoring_date,
                        "event_date": event_date,
                        "event_type": "Опубликована новая отчетность",
                        "event_url": rep.get("file_url") or rep.get("page_url", ""),
                        "source": "e-disclosure",
                        "payload": rep,
                    }
                    exists = conn.execute("SELECT event_hash FROM report_events WHERE event_hash = ?", (event["event_hash"],)).fetchone()
                    if exists:
                        conn.execute("UPDATE report_events SET last_seen_at = ? WHERE event_hash = ?", (now_iso(), event["event_hash"]))
                    else:
                        conn.execute(
                            """
                            INSERT INTO report_events (event_hash, inn, company_name, scoring_date, event_date, event_type, event_url,
                            source, payload_json, first_seen_at, last_seen_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                event["event_hash"],
                                event["inn"],
                                event["company_name"],
                                event["scoring_date"],
                                event["event_date"],
                                event["event_type"],
                                event["event_url"],
                                event["source"],
                                json.dumps(event["payload"], ensure_ascii=False),
                                now_iso(),
                                now_iso(),
                            ),
                        )
                        all_new_event_hashes.add(event["event_hash"])
                    conn.commit()

                if latest_report_date:
                    stale_dt = parse_date(latest_report_date)
                    if stale_dt and stale_dt < datetime.now() - timedelta(days=config.REPORT_STALE_DAYS):
                        stale_hash = md5_short(f"stale_{inn}_{latest_report_date}", 16)
                        exists = conn.execute("SELECT event_hash FROM report_events WHERE event_hash = ?", (stale_hash,)).fetchone()
                        if exists:
                            conn.execute("UPDATE report_events SET last_seen_at = ? WHERE event_hash = ?", (now_iso(), stale_hash))
                        else:
                            conn.execute(
                                """
                                INSERT INTO report_events (event_hash, inn, company_name, scoring_date, event_date, event_type, event_url,
                                source, payload_json, first_seen_at, last_seen_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    stale_hash,
                                    inn,
                                    row.company_name,
                                    row.scoring_date,
                                    today_iso(),
                                    "Нет новой отчетности дольше порога",
                                    company.get("url", ""),
                                    "stale-alert",
                                    json.dumps({"latest_report_date": latest_report_date}, ensure_ascii=False),
                                    now_iso(),
                                    now_iso(),
                                ),
                            )
                            all_new_event_hashes.add(stale_hash)
                        conn.commit()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed reports INN=%s: %s", inn, exc)

    _, elapsed = timed(stage_reports)
    stage_times["Этап 2: Сбор отчетности"] = elapsed

    print("Этап 3: События по рейтингам")

    def stage_ratings() -> None:
        prev_rows = conn.execute("SELECT * FROM emitents_snapshot").fetchall()
        prev = {r["inn"]: dict(r) for r in prev_rows if r["inn"]}

        for row in tqdm(emitents, desc="Рейтинговые изменения", position=0):
            old = prev.get(row.inn)
            if not old:
                continue
            for field, source in [("nra_rate", "NRA"), ("acra_rate", "ACRA"), ("nkr_rate", "NKR"), ("raex_rate", "RAEX")]:
                event_type = classify_rating_change(old.get(field, ""), getattr(row, field))
                if not event_type:
                    continue
                event_date = row.scoring_date or today_iso()
                event_hash = md5_short(f"rate_{row.inn}_{field}_{old.get(field,'')}_{getattr(row, field)}_{event_date}", 16)
                exists = conn.execute("SELECT event_hash FROM report_events WHERE event_hash = ?", (event_hash,)).fetchone()
                if exists:
                    conn.execute("UPDATE report_events SET last_seen_at = ? WHERE event_hash = ?", (now_iso(), event_hash))
                else:
                    conn.execute(
                        """
                        INSERT INTO report_events (event_hash, inn, company_name, scoring_date, event_date, event_type, event_url,
                        source, payload_json, first_seen_at, last_seen_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_hash,
                            row.inn,
                            row.company_name,
                            row.scoring_date,
                            to_iso_date_str(event_date) or today_iso(),
                            event_type,
                            "",
                            source,
                            json.dumps({"field": field, "old": old.get(field, ""), "new": getattr(row, field)}, ensure_ascii=False),
                            now_iso(),
                            now_iso(),
                        ),
                    )
                    all_new_event_hashes.add(event_hash)
                conn.commit()

        conn.execute("DELETE FROM emitents_snapshot")
        for row in emitents:
            conn.execute(
                """
                INSERT INTO emitents_snapshot (inn, company_name, scoring, scoring_date, nra_rate, acra_rate, nkr_rate, raex_rate, snapshot_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (row.inn, row.company_name, row.scoring, row.scoring_date, row.nra_rate, row.acra_rate, row.nkr_rate, row.raex_rate, now_iso()),
            )
        conn.commit()
        save_emitents_snapshot_excel(emitents)

    _, elapsed = timed(stage_ratings)
    stage_times["Этап 3: События по рейтингам"] = elapsed

    print("Этап 4: Загрузка портфеля")

    def stage_portfolio() -> list[dict[str, str]]:
        src = find_portfolio_file()
        items = load_portfolio_items(src, logger)
        conn.execute("DELETE FROM portfolio_items")
        for item in items:
            conn.execute(
                """
                INSERT INTO portfolio_items (instrument_type, instrument_code, inn, company_name, source_file, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_type, instrument_code) DO UPDATE SET
                inn=excluded.inn, company_name=excluded.company_name, source_file=excluded.source_file, loaded_at=excluded.loaded_at
                """,
                (
                    item.get("instrument_type", ""),
                    item.get("instrument_code", ""),
                    item.get("inn", ""),
                    item.get("company_name", ""),
                    str(src) if src else "",
                    now_iso(),
                ),
            )
        conn.commit()

        wb = Workbook()
        ws = wb.active
        ws.title = "portfolio_snapshot"
        ws.append(["instrument_type", "instrument_code", "inn", "company_name"])
        for item in items:
            ws.append([item.get("instrument_type", ""), item.get("instrument_code", ""), item.get("inn", ""), item.get("company_name", "")])
        wb.save(config.PORTFOLIO_SNAPSHOT_XLSX)
        return items

    portfolio_items, elapsed = timed(stage_portfolio)
    stage_times["Этап 4: Загрузка портфеля"] = elapsed

    print("Этап 5: Новости портфеля")

    def stage_news() -> list[dict[str, str]]:
        collector = SmartlabNewsCollector(logger)
        cache = NewsCacheManager(config.CACHE_DIR / "news" / "news_cache.csv")
        rows: list[dict[str, str]] = []
        for item in tqdm(portfolio_items, desc="Сбор новостей", position=0):
            try:
                for news in collector.collect(item):
                    h = md5_short(f"{news['url']}_{sanitize_str(news['title'])[:50]}_{news['news_date']}", 16)
                    is_new = cache.is_new(h)
                    if is_new:
                        cache.add(
                            {
                                "hash": h,
                                "company_name": item.get("company_name", ""),
                                "company_inn": item.get("inn", ""),
                                "date": news["news_date"],
                                "title": news["title"],
                                "source": "Smartlab",
                                "url": news["url"],
                                "added_date": today_iso(),
                            }
                        )
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO news_events (
                            event_hash, instrument_type, instrument_code, inn, company_name, news_date, title, url, source, first_seen_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            h,
                            item.get("instrument_type", ""),
                            item.get("instrument_code", ""),
                            item.get("inn", ""),
                            item.get("company_name", ""),
                            news["news_date"],
                            news["title"],
                            news["url"],
                            "Smartlab",
                            now_iso(),
                        ),
                    )
                    conn.commit()
                    rows.append(
                        {
                            "event_hash": h,
                            "instrument_type": item.get("instrument_type", ""),
                            "instrument_code": item.get("instrument_code", ""),
                            "inn": item.get("inn", ""),
                            "company_name": item.get("company_name", ""),
                            "news_date": news["news_date"],
                            "title": news["title"],
                            "url": news["url"],
                            "source": "Smartlab",
                            "is_new": is_new,
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception("News failed for %s: %s", item, exc)
        cache.save()
        return rows

    news_rows, elapsed = timed(stage_news)
    stage_times["Этап 5: Новости портфеля"] = elapsed

    print("Этап 6: Экспорт витрин")

    def stage_export() -> None:
        report_events = [dict(r) for r in conn.execute("SELECT event_hash, inn, company_name, scoring_date, event_date, event_type, event_url FROM report_events").fetchall()]
        for row in report_events:
            row["is_new"] = row.get("event_hash") in all_new_event_hashes
        export_reports(report_events)

        latest_event_by_inn: dict[str, dict[str, str]] = {}
        for row in sorted(report_events, key=lambda x: x.get("event_date", ""), reverse=True):
            latest_event_by_inn.setdefault(row.get("inn", ""), row)

        latest_news_by_key: dict[tuple[str, str], dict[str, str]] = {}
        for row in sorted(news_rows, key=lambda x: x.get("news_date", ""), reverse=True):
            latest_news_by_key.setdefault((row.get("instrument_type", ""), row.get("instrument_code", "")), row)

        export_portfolio(portfolio_items, latest_event_by_inn, latest_news_by_key, news_rows)

    _, elapsed = timed(stage_export)
    stage_times["Этап 6: Экспорт витрин"] = elapsed

    conn.close()

    print("=====\nSummary")
    total = sum(stage_times.values())
    for stage, sec in stage_times.items():
        print(f"- {stage}: {sec:.2f} сек")
    print(f"- Итого: {total:.2f} сек")


if __name__ == "__main__":
    run_monitoring()
