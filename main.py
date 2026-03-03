from __future__ import annotations

import json
import logging
import math
import re
import signal
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
EXPERT_RA_BASE_URL = "https://raexpert.ru"
ACRA_BASE_URL = "https://www.acra-ratings.ru"
ACRA_PROXY_BASE_URL = "https://r.jina.ai/http://www.acra-ratings.ru"
OUTPUT_DIR = Path(__file__).resolve().parent
LOG_FILE = OUTPUT_DIR / "main.log"
SHARES_FILE = OUTPUT_DIR / "moex_shares.xlsx"
BONDS_FILE = OUTPUT_DIR / "moex_bonds.xlsx"
EMITTERS_FILE = OUTPUT_DIR / "moex_emitters.xlsx"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 24
CACHE_FILE = OUTPUT_DIR / "emitter_cache.json"
HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ZEBRA_FILL = PatternFill(fill_type="solid", fgColor="E8F2FF")
THIN_BORDER = Border(
    left=Side(style="thin", color="000000"),
    right=Side(style="thin", color="000000"),
    top=Side(style="thin", color="000000"),
    bottom=Side(style="thin", color="000000"),
)
CENTERED_WRAP_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)


def progress(total: int, desc: str, unit: str):
    return tqdm(total=total, desc=desc, unit=unit, position=0, leave=False, dynamic_ncols=True)


def load_cache(logger: logging.Logger) -> dict[str, dict[str, Any]]:
    if not CACHE_FILE.exists():
        return {"secid_to_emitter": {}, "emitters": {}}

    try:
        with CACHE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            return {
                "secid_to_emitter": data.get("secid_to_emitter", {}),
                "emitters": data.get("emitters", {}),
            }
    except Exception as error:
        logger.exception("Cache load failed: %s", error)

    return {"secid_to_emitter": {}, "emitters": {}}


def save_cache(cache: dict[str, dict[str, Any]], logger: logging.Logger) -> None:
    try:
        with CACHE_FILE.open("w", encoding="utf-8") as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)
    except Exception as error:
        logger.exception("Cache save failed: %s", error)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("moex_export")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


class MoexClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self.logger.info("GET %s params=%s status=%s", url, params, response.status_code)
        return response.json()

    def fetch_market_securities(self, market: str, columns: list[str]) -> pd.DataFrame:
        with progress(total=1, desc=f"MOEX {market}", unit="запрос") as pbar:
            data = self._get(
                f"/engines/stock/markets/{market}/securities.json",
                params={"iss.meta": "off", "iss.only": "securities", "securities.columns": ",".join(columns)},
            )
            pbar.update(1)

        return pd.DataFrame(data.get("securities", {}).get("data", []), columns=data.get("securities", {}).get("columns", []))

    def fetch_emitter_id_by_secid(self, secid: str) -> int | None:
        data = self._get(
            f"/securities/{secid}.json",
            params={"iss.meta": "off", "iss.only": "description"},
        )
        rows = data.get("description", {}).get("data", [])
        mapping = {row[0]: row[2] for row in rows if len(row) >= 3}
        emitter_id = mapping.get("EMITTER_ID") or mapping.get("EMITENT_ID")
        try:
            return int(emitter_id) if emitter_id is not None else None
        except (TypeError, ValueError):
            return None

    def fetch_emitter_info(self, emitter_id: int) -> dict[str, Any]:
        data = self._get(
            f"/emitters/{emitter_id}.json",
            params={"iss.meta": "off", "iss.only": "emitter", "emitter.columns": "EMITTER_ID,SHORT_TITLE,INN"},
        )
        row = data.get("emitter", {}).get("data", [])
        if not row:
            return {"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None}
        return {"EMITTER_ID": int(row[0][0]), "EMITTER_NAME": row[0][1], "INN": row[0][2]}


class ExpertRaClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})

    def _normalize_inn(self, value: Any) -> str | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
        return digits or None

    def _clean_text(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    def _format_date(self, value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d.%m.%Y")
        if isinstance(value, date):
            return value.strftime("%d.%m.%Y")
        text = str(value).strip()
        if not text:
            return ""
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.strftime("%d.%m.%Y")
        return text

    def _fetch_export_paths(self) -> list[str]:
        response = self.session.get(f"{EXPERT_RA_BASE_URL}/ratings/", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        found_paths = set(re.findall(r'data-path="/([^/"]+)/"', response.text))
        paths = sorted(path for path in found_paths if path)
        self.logger.info("Expert RA export paths resolved: %s", len(paths))
        return paths

    def _download_ratings_workbook(self, paths: list[str]) -> bytes:
        labels = [f"Категория {path}" for path in paths]
        payload = {"all": {"labels": labels, "paths": paths}}
        virtual_date = date.today().strftime("%d.%m.%Y")
        response = self.session.post(
            f"{EXPERT_RA_BASE_URL}/ratings/ratings-xlsx-export",
            params={"isSinglePage": 1, "virtual_date": virtual_date},
            json=payload,
            timeout=REQUEST_TIMEOUT * 3,
        )
        response.raise_for_status()
        self.logger.info(
            "Expert RA export downloaded: status=%s size=%s",
            response.status_code,
            len(response.content),
        )
        return response.content

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(inn) for inn in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        paths = self._fetch_export_paths()
        if not paths:
            self.logger.warning("Expert RA export paths not found")
            return {}

        workbook_bytes = self._download_ratings_workbook(paths)
        workbook = pd.read_excel(BytesIO(workbook_bytes), header=5)
        workbook.columns = [str(col).strip() for col in workbook.columns]

        required_columns = {"ИНН", "Рейтинг", "Прогноз", "Дата присвоения/актуализации/изменения рейтинга"}
        missing_columns = required_columns - set(workbook.columns)
        if missing_columns:
            self.logger.warning("Expert RA missing columns in export: %s", sorted(missing_columns))
            return {}

        ratings_by_inn: dict[str, dict[str, Any]] = {}

        with progress(total=len(workbook), desc="Парсинг Эксперт РА", unit="строка") as pbar:
            for _, row in workbook.iterrows():
                inn = self._normalize_inn(row.get("ИНН"))
                if not inn or inn not in normalized_inns:
                    pbar.update(1)
                    continue

                row_date = pd.to_datetime(row.get("Дата присвоения/актуализации/изменения рейтинга"), errors="coerce", dayfirst=True)
                row_date_for_sort = row_date if pd.notna(row_date) else pd.Timestamp.min
                current_best = ratings_by_inn.get(inn)

                if current_best is None or row_date_for_sort > current_best["_sort_date"]:
                    rating = self._clean_text(row.get("Рейтинг"))
                    if not rating or not rating.lower().startswith("ru"):
                        pbar.update(1)
                        continue

                    forecast = self._clean_text(row.get("Прогноз"))
                    date_text = self._format_date(row.get("Дата присвоения/актуализации/изменения рейтинга"))
                    rating_parts = [part for part in [rating, forecast, date_text] if part]
                    ratings_by_inn[inn] = {
                        "_sort_date": row_date_for_sort,
                        "value": "\n".join(rating_parts),
                    }

                pbar.update(1)

        result = {inn: payload["value"] for inn, payload in ratings_by_inn.items()}
        self.logger.info("Expert RA ratings matched by INN: %s", len(result))
        return result


class AcraClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})

    def _normalize_inn(self, value: Any) -> str | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
        return digits or None

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    def _extract_total_issuers(self, text: str) -> int | None:
        match = re.search(r"Найдено:\s*(\d+)", text)
        return int(match.group(1)) if match else None

    def _parse_ru_date(self, raw_value: str) -> str:
        month_map = {
            "янв": "01",
            "фев": "02",
            "мар": "03",
            "апр": "04",
            "мая": "05",
            "май": "05",
            "июн": "06",
            "июл": "07",
            "авг": "08",
            "сен": "09",
            "окт": "10",
            "ноя": "11",
            "дек": "12",
        }
        normalized = self._clean_text(raw_value.lower())
        match = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", normalized)
        if not match:
            return self._clean_text(raw_value)

        day, month_text, year = match.groups()
        month = month_map.get(month_text[:3])
        if not month:
            return self._clean_text(raw_value)
        return f"{int(day):02d}.{month}.{year}"

    def _get_page_text(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{ACRA_BASE_URL}{path}"
        try:
            response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            self.logger.info("GET ACRA %s params=%s status=%s", url, params, response.status_code)
            return response.text
        except requests.RequestException as error:
            self.logger.warning("ACRA direct request failed, fallback to jina proxy: %s", error)
            proxy_response = self.session.get(f"{ACRA_PROXY_BASE_URL}{path}", params=params or {}, timeout=REQUEST_TIMEOUT * 2)
            proxy_response.raise_for_status()
            self.logger.info("GET ACRA proxy %s params=%s status=%s", path, params, proxy_response.status_code)
            return proxy_response.text

    def _extract_issuer_links(self, text: str) -> list[str]:
        matches = re.findall(r"/ratings/issuers/(\d+)/", text)
        unique_ids = sorted(set(matches), key=lambda value: int(value))
        return [f"/ratings/issuers/{issuer_id}/" for issuer_id in unique_ids]

    def _parse_issuer_card(self, text: str) -> tuple[str | None, str | None]:
        lines = [self._clean_text(line) for line in text.splitlines() if self._clean_text(line)]
        if not lines:
            return None, None

        inn: str | None = None
        for index, line in enumerate(lines):
            if line.upper() == "ИНН" and index + 1 < len(lines):
                candidate = self._normalize_inn(lines[index + 1])
                if candidate:
                    inn = candidate
                    break

        if not inn:
            return None, None

        current_start: int | None = None
        for index, line in enumerate(lines):
            if line.lower() == "текущий рейтинг":
                current_start = index
                break

        if current_start is None:
            return inn, None

        end_index = len(lines)
        for index in range(current_start + 1, len(lines)):
            if lines[index].lower() == "история рейтингов":
                end_index = index
                break

        current_block = lines[current_start:end_index]
        rating = next((line for line in current_block if re.fullmatch(r"[A-Z]{1,4}[+-]?\(RU\)", line)), None)

        forecast: str | None = None
        for line in current_block:
            lower_line = line.lower()
            if lower_line.startswith("прогноз "):
                forecast = self._clean_text(line[8:])
                break
            if "под наблюдением" in lower_line or lower_line in {"позитивный", "стабильный", "негативный", "развивающийся"}:
                forecast = line
                break

        date_value: str | None = None
        for line in current_block:
            if re.search(r"\b\d{1,2}\s+[а-я]+\s+\d{4}\b", line.lower()):
                date_value = self._parse_ru_date(line)
                break

        if not rating:
            return inn, None

        rating_parts = [part for part in [rating, forecast, date_value] if part]
        return inn, "\n".join(rating_parts) if rating_parts else None

    def fetch_latest_ratings_by_inn(self, inns: set[str]) -> dict[str, str]:
        normalized_inns = {self._normalize_inn(value) for value in inns}
        normalized_inns = {inn for inn in normalized_inns if inn}
        if not normalized_inns:
            return {}

        first_page = self._get_page_text("/ratings/issuers/", params={"page": 1})
        total_issuers = self._extract_total_issuers(first_page)
        total_pages = max(1, math.ceil(total_issuers / 10)) if total_issuers else 100

        issuer_links = set(self._extract_issuer_links(first_page))
        with progress(total=max(total_pages - 1, 0), desc="Сканирование страниц АКРА", unit="страница") as pbar:
            for page in range(2, total_pages + 1):
                page_text = self._get_page_text("/ratings/issuers/", params={"page": page})
                page_links = self._extract_issuer_links(page_text)
                if not page_links and not total_issuers:
                    pbar.update(total_pages - page + 1)
                    break
                issuer_links.update(page_links)
                pbar.update(1)

        ratings_by_inn: dict[str, str] = {}
        sorted_links = sorted(issuer_links, key=lambda link: int(link.rstrip("/").split("/")[-1]))
        with progress(total=len(sorted_links), desc="Парсинг АКРА", unit="эмитент") as pbar:
            for issuer_path in sorted_links:
                try:
                    issuer_text = self._get_page_text(issuer_path)
                    inn, value = self._parse_issuer_card(issuer_text)
                    if inn and inn in normalized_inns and value:
                        ratings_by_inn[inn] = value
                except requests.RequestException as error:
                    self.logger.exception("ACRA issuer fetch failed path=%s: %s", issuer_path, error)
                except Exception as error:
                    self.logger.exception("ACRA issuer parse failed path=%s: %s", issuer_path, error)
                pbar.update(1)

        self.logger.info("ACRA ratings matched by INN: %s", len(ratings_by_inn))
        return ratings_by_inn


def enrich_emitters(
    client: MoexClient,
    shares: pd.DataFrame,
    bonds: pd.DataFrame,
    logger: logging.Logger,
    cache: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "EMITTER_ID" not in shares.columns:
        shares["EMITTER_ID"] = pd.NA
    if "EMITTER_ID" not in bonds.columns:
        bonds["EMITTER_ID"] = pd.NA

    existing_pairs = pd.concat([shares[["SECID", "EMITTER_ID"]], bonds[["SECID", "EMITTER_ID"]]], ignore_index=True)
    cached_pairs = pd.DataFrame(
        [{"SECID": secid, "EMITTER_ID": emitter_id} for secid, emitter_id in cache.get("secid_to_emitter", {}).items()]
    )
    existing_pairs = pd.concat([existing_pairs, cached_pairs], ignore_index=True)
    existing_pairs = existing_pairs.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["SECID"], keep="first")
    existing_secids = set(existing_pairs["SECID"].tolist())

    secids = sorted((set(shares["SECID"].tolist()) | set(bonds["SECID"].tolist())) - existing_secids)
    logger.info("Emitter enrichment start for secids=%s", len(secids))

    secid_rows: list[dict[str, Any]] = existing_pairs.to_dict("records")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_id_by_secid, secid): secid for secid in secids}
        with progress(total=len(futures), desc="Определение EMITTER_ID", unit="бумага") as pbar:
            for future in as_completed(futures):
                secid = futures[future]
                try:
                    emitter_id = future.result()
                except requests.RequestException as error:
                    logger.exception("Emitter id fetch failed secid=%s: %s", secid, error)
                    emitter_id = None
                except Exception as error:
                    logger.exception("Unexpected emitter id error secid=%s: %s", secid, error)
                    emitter_id = None
                secid_rows.append({"SECID": secid, "EMITTER_ID": emitter_id})
                if emitter_id is not None:
                    cache.setdefault("secid_to_emitter", {})[secid] = int(emitter_id)
                pbar.update(1)

    secid_map = pd.DataFrame(secid_rows).drop_duplicates(subset=["SECID"], keep="first")
    secid_map["EMITTER_ID"] = pd.to_numeric(secid_map["EMITTER_ID"], errors="coerce")
    emitter_ids = sorted({int(x) for x in secid_map["EMITTER_ID"].dropna().tolist()})
    logger.info("Resolved emitter ids=%s", len(emitter_ids))

    cached_emitters = cache.get("emitters", {})
    emitter_rows: list[dict[str, Any]] = []
    missing_emitter_ids = []
    for emitter_id in emitter_ids:
        cached = cached_emitters.get(str(emitter_id))
        if cached:
            emitter_rows.append(cached)
        else:
            missing_emitter_ids.append(emitter_id)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_info, emitter_id): emitter_id for emitter_id in missing_emitter_ids}
        with progress(total=len(futures), desc="Дозагрузка эмитентов", unit="эмитент") as pbar:
            for future in as_completed(futures):
                emitter_id = futures[future]
                try:
                    emitter_info = future.result()
                    emitter_rows.append(emitter_info)
                    cache.setdefault("emitters", {})[str(emitter_id)] = emitter_info
                except requests.RequestException as error:
                    logger.exception("Emitter info failed id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                except Exception as error:
                    logger.exception("Unexpected emitter info error id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                pbar.update(1)

    emitters_df = pd.DataFrame(emitter_rows).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    secid_map = secid_map.rename(columns={"EMITTER_ID": "EMITTER_ID_RESOLVED"})

    shares = shares.merge(secid_map, on="SECID", how="left")
    shares_existing = pd.to_numeric(shares["EMITTER_ID"], errors="coerce")
    shares_resolved = pd.to_numeric(shares["EMITTER_ID_RESOLVED"], errors="coerce")
    shares["EMITTER_ID"] = shares_existing.where(shares_existing.notna(), shares_resolved)
    shares = shares.drop(columns=["EMITTER_ID_RESOLVED"])

    bonds = bonds.merge(secid_map, on="SECID", how="left")
    bonds_existing = pd.to_numeric(bonds["EMITTER_ID"], errors="coerce")
    bonds_resolved = pd.to_numeric(bonds["EMITTER_ID_RESOLVED"], errors="coerce")
    bonds["EMITTER_ID"] = bonds_existing.where(bonds_existing.notna(), bonds_resolved)
    bonds = bonds.drop(columns=["EMITTER_ID_RESOLVED"])

    shares = shares.merge(emitters_df, on="EMITTER_ID", how="left")
    bonds = bonds.merge(emitters_df, on="EMITTER_ID", how="left")

    logger.info(
        "Emitter fill ratio: shares(name=%s inn=%s), bonds(name=%s inn=%s)",
        shares["EMITTER_NAME"].notna().mean(),
        shares["INN"].notna().mean(),
        bonds["EMITTER_NAME"].notna().mean(),
        bonds["INN"].notna().mean(),
    )
    return shares, bonds


def build_emitters_table(shares: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    shares_grouped = (
        shares.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_SHARES")
    )
    bonds_grouped = (
        bonds.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_BONDS")
    )

    emitters = shares_grouped.merge(bonds_grouped, on="EMITTER_ID", how="outer")
    details = pd.concat([shares[["EMITTER_ID", "EMITTER_NAME", "INN"]], bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]]], ignore_index=True)
    details = details.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    emitters = emitters.merge(details, on="EMITTER_ID", how="left")
    return emitters[["EMITTER_NAME", "INN", "TRADED_SHARES", "TRADED_BONDS", "EMITTER_ID"]].sort_values(
        by=["EMITTER_NAME", "EMITTER_ID"], na_position="last"
    )


def apply_expert_ra_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = "".join(ch for ch in str(inn) if ch.isdigit())
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["Рейтинг Эксперт РА"] = result["INN"].map(rating_for_row)
    return result


def apply_acra_ratings(emitters: pd.DataFrame, ratings_by_inn: dict[str, str]) -> pd.DataFrame:
    result = emitters.copy()

    def rating_for_row(inn: Any) -> Any:
        if pd.isna(inn):
            return pd.NA
        normalized = "".join(ch for ch in str(inn) if ch.isdigit())
        if not normalized:
            return pd.NA
        return ratings_by_inn.get(normalized, pd.NA)

    result["Рейтинг Акра"] = result["INN"].map(rating_for_row)
    return result


def _fit_column_widths(worksheet: Any, df: pd.DataFrame) -> None:
    for col_idx, column_name in enumerate(df.columns, start=1):
        values = df[column_name]
        if values.empty:
            max_len = len(str(column_name))
        else:
            series_len = values.map(lambda value: len(str(value)) if pd.notna(value) else 0)
            max_len = max(len(str(column_name)), int(series_len.max()))

        adjusted_width = min(max_len + 2, 80)
        worksheet.column_dimensions[get_column_letter(col_idx)].width = max(10, adjusted_width)


def save_to_excel(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        worksheet = writer.sheets["Data"]

        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, min_col=1, max_col=worksheet.max_column):
            for cell in row:
                cell.alignment = CENTERED_WRAP_ALIGNMENT
                cell.border = THIN_BORDER

        for cell in worksheet[1]:
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT

        if worksheet.max_row >= 2:
            max_col_letter = get_column_letter(worksheet.max_column)
            zebra_range = f"A2:{max_col_letter}{worksheet.max_row}"
            zebra_rule = FormulaRule(formula=["MOD(ROW(),2)=0"], fill=ZEBRA_FILL)
            worksheet.conditional_formatting.add(zebra_range, zebra_rule)

        _fit_column_widths(worksheet, df)

    logger.info("Saved %s rows=%s", path, len(df))


def run() -> None:
    logger = setup_logging()
    logger.info("Script started")

    interrupted = {"value": False}
    stage_times: dict[str, float] = {}
    script_started_at = perf_counter()

    def handle_sigint(signum: int, frame: Any) -> None:
        _ = (signum, frame)
        interrupted["value"] = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)
    client = MoexClient(logger)
    expert_ra_client = ExpertRaClient(logger)
    acra_client = AcraClient(logger)
    cache = load_cache(logger)

    try:
        print("=====\nЭтап 1: Сбор акций")
        stage_started_at = perf_counter()
        shares = client.fetch_market_securities("shares", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "LISTLEVEL", "STATUS", "EMITTER_ID"])
        shares = shares[(shares["BOARDID"] == "TQBR") & (shares["STATUS"].fillna("") != "N")].copy()
        stage_times["Этап 1: Сбор акций"] = perf_counter() - stage_started_at

        print("Этап 2: Сбор облигаций")
        stage_started_at = perf_counter()
        bonds = client.fetch_market_securities("bonds", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL", "STATUS", "EMITTER_ID"])
        bonds = bonds[bonds["BOARDID"].isin(["TQCB", "TQOB", "TQOD", "TQIR", "TQOE"])].copy()
        bonds = bonds[bonds["STATUS"].fillna("") != "N"].copy()
        bonds["MATDATE"] = pd.to_datetime(bonds["MATDATE"], errors="coerce").dt.date
        bonds = bonds[(bonds["MATDATE"].isna()) | (bonds["MATDATE"] >= date.today())].copy()
        stage_times["Этап 2: Сбор облигаций"] = perf_counter() - stage_started_at

        print("Этап 3: Обогащение эмитентов")
        stage_started_at = perf_counter()
        shares, bonds = enrich_emitters(client, shares, bonds, logger, cache)
        stage_times["Этап 3: Обогащение эмитентов"] = perf_counter() - stage_started_at

        print("Этап 4: Получение рейтингов Эксперт РА")
        stage_started_at = perf_counter()
        emitters = build_emitters_table(shares, bonds)
        inns = set(emitters["INN"].dropna().astype(str).tolist())
        expert_ra_ratings = expert_ra_client.fetch_latest_ratings_by_inn(inns)
        emitters = apply_expert_ra_ratings(emitters, expert_ra_ratings)
        stage_times["Этап 4: Получение рейтингов Эксперт РА"] = perf_counter() - stage_started_at

        print("Этап 5: Получение рейтингов АКРА")
        stage_started_at = perf_counter()
        acra_ratings = acra_client.fetch_latest_ratings_by_inn(inns)
        emitters = apply_acra_ratings(emitters, acra_ratings)
        stage_times["Этап 5: Получение рейтингов АКРА"] = perf_counter() - stage_started_at

        print("Этап 6: Формирование Excel")
        stage_started_at = perf_counter()

        excel_exports = [
            (shares, SHARES_FILE),
            (bonds, BONDS_FILE),
            (emitters, EMITTERS_FILE),
        ]
        with progress(total=len(excel_exports), desc="Экспорт Excel", unit="файл") as pbar:
            for df, output_path in excel_exports:
                save_to_excel(df, output_path, logger)
                pbar.update(1)

        stage_times["Этап 6: Формирование Excel"] = perf_counter() - stage_started_at

        print("=====\nГотово")
        logger.info("Script completed successfully")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by Ctrl+C")
        print("\nОстановлено пользователем (Ctrl+C)")
    except Exception as error:
        logger.exception("Script failed: %s", error)
        raise
    finally:
        save_cache(cache, logger)
        logger.info("Script finished. interrupted=%s", interrupted["value"])

        total_time = perf_counter() - script_started_at
        print("=====\nSummary")
        for stage_name, duration in stage_times.items():
            print(f"{stage_name}: {duration:.2f} сек")
        print(f"Всего: {total_time:.2f} сек")
        print("=====")


if __name__ == "__main__":
    run()
