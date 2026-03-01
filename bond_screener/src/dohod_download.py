from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .utils import file_is_fresh, normalize_decimal, parse_date, to_upper

logger = logging.getLogger(__name__)


@dataclass
class DohodResult:
    raw: pd.DataFrame
    norm: pd.DataFrame
    excel_path: Path


def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    lookup = {col.upper().replace(" ", ""): col for col in df.columns}
    for c in candidates:
        key = c.upper().replace(" ", "")
        if key in lookup:
            return lookup[key]
    return None


def _download_with_playwright(url: str, output_path: Path, headless: bool, timeout_s: int) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=timeout_s * 1000)

        try:
            with page.expect_download(timeout=timeout_s * 1000) as download_info:
                page.get_by_text("Скачать Excel", exact=False).first.click()
            download = download_info.value
        except PlaywrightTimeoutError:
            with page.expect_download(timeout=timeout_s * 1000) as download_info:
                page.locator("text=/Excel/i").first.click()
            download = download_info.value

        temp_path = Path(download.path())
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(temp_path, output_path)
        browser.close()


def load_or_download_dohod_excel(
    url: str,
    output_path: Path,
    ttl_hours: int,
    use_playwright: bool,
    headless: bool,
    timeout_s: int,
) -> DohodResult:
    if file_is_fresh(output_path, ttl_hours):
        logger.info("Using cached DOHOD excel: %s", output_path)
    else:
        logger.info("Downloading DOHOD excel via Playwright")
        if not use_playwright:
            raise RuntimeError("DOHOD download requires Playwright by config")
        _download_with_playwright(url, output_path, headless, timeout_s)

    raw = pd.read_excel(output_path, dtype=object)

    isin_col = _find_col(raw, "ISIN")
    secid_col = _find_col(raw, "SECID", "ТИКЕР")
    matdate_col = _find_col(raw, "ПОГАШЕНИЕ", "MATDATE")
    price_col = _find_col(raw, "ЦЕНА", "PRICE")
    nkd_col = _find_col(raw, "НКД", "ACCRUED")
    margin_col = _find_col(raw, "СПРЕД", "PREMIUM", "НАДБАВКА")
    floater_col = _find_col(raw, "ФЛОАТЕР", "FRN")
    linker_col = _find_col(raw, "ИНФЛЯЦ", "LINKER", "ОФЗ-ИН")

    norm = raw.copy()
    norm["isin_norm"] = raw[isin_col].map(to_upper) if isin_col else None
    norm["secid_norm"] = raw[secid_col].map(to_upper) if secid_col else None
    norm["matdate_norm"] = raw[matdate_col].map(parse_date) if matdate_col else None
    norm["price_norm"] = raw[price_col].map(normalize_decimal) if price_col else None
    norm["nkd_norm"] = raw[nkd_col].map(normalize_decimal) if nkd_col else None
    norm["frn_margin_norm"] = raw[margin_col].map(normalize_decimal) if margin_col else None
    norm["is_floater_norm"] = raw[floater_col].astype(str).str.contains("да|frn|float", case=False, na=False) if floater_col else False
    norm["is_linker_norm"] = raw[linker_col].astype(str).str.contains("да|ин|link", case=False, na=False) if linker_col else False

    return DohodResult(raw=raw, norm=norm, excel_path=output_path)
