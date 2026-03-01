from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Dict
from urllib.parse import urljoin

import httpx
import pandas as pd
from playwright.sync_api import TimeoutError as PWTimeoutError
from playwright.sync_api import sync_playwright

from .utils import normalize_isin, parse_date, to_float


def _fresh(path: Path, ttl_h: float) -> bool:
    return path.exists() and (time.time() - path.stat().st_mtime) / 3600 <= ttl_h


def _download_dohod_excel_http_fallback(url: str, export_path: Path, timeout_s: int, logger) -> Path:
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        page = client.get(url)
        page.raise_for_status()
        html = page.text

        links = re.findall(r'href=["\']([^"\']+\.(?:xlsx?|XLSX?))(?:\?[^"\']*)?["\']', html)
        if not links:
            links = re.findall(r'["\'](https?://[^"\']+\.(?:xlsx?|XLSX?))(?:\?[^"\']*)?["\']', html)
        if not links:
            raise RuntimeError("DOHOD fallback: Excel link not found in page")

        excel_url = urljoin(url, links[0])
        resp = client.get(excel_url)
        resp.raise_for_status()
        export_path.write_bytes(resp.content)
        logger.info("Downloaded DOHOD Excel via HTTP fallback: %s", excel_url)
        return export_path


def download_dohod_excel(config: Dict, export_path: Path, logger) -> Path:
    ttl = config["ttl_hours"]["dohod_excel"]
    if _fresh(export_path, ttl):
        logger.info("DOHOD Excel is fresh, skip download")
        return export_path

    export_path.parent.mkdir(parents=True, exist_ok=True)
    url = config["sources"]["dohod_url"]
    headless = config["dohod"]["headless"]
    timeout_s = int(config["dohod"]["timeout_s"])
    timeout_ms = timeout_s * 1000

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                locator = page.get_by_role("button", name=re.compile("Скачать Excel", re.IGNORECASE))
                if locator.count() == 0:
                    locator = page.locator("text=Скачать Excel")
                with page.expect_download(timeout=timeout_ms) as dl:
                    locator.first.click()
                download = dl.value
                download.save_as(str(export_path))
            except PWTimeoutError:
                logger.exception("Failed to download DOHOD Excel via Playwright timeout")
                raise
            finally:
                context.close()
                browser.close()
        logger.info("Downloaded DOHOD Excel to %s", export_path)
        return export_path
    except Exception as exc:
        logger.warning("Playwright DOHOD download failed (%s), trying HTTP fallback", exc)
        try:
            return _download_dohod_excel_http_fallback(url, export_path, timeout_s=timeout_s, logger=logger)
        except Exception as fallback_exc:
            if export_path.exists():
                logger.warning("HTTP fallback failed (%s), using stale DOHOD file: %s", fallback_exc, export_path)
                return export_path
            raise


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        for lcol, orig in lower.items():
            if cand in lcol:
                return orig
    return None


def normalize_dohod(df: pd.DataFrame) -> pd.DataFrame:
    norm = df.copy()
    isin_col = _find_col(df, ["isin"])
    price_col = _find_col(df, ["цена", "price"])
    nkd_col = _find_col(df, ["нкд", "accrued"])
    freq_col = _find_col(df, ["частот", "frequency", "купон/год"])
    coupon_col = _find_col(df, ["купон", "ставк"])
    offer_col = _find_col(df, ["оферт"])
    mat_col = _find_col(df, ["погаш", "maturity"])
    base_col = _find_col(df, ["ruonia", "zcyc", "кбд", "g-curve", "баз", "индекс"])
    spread_col = _find_col(df, ["спред", "прем", "надбав", "+", "маржа"])
    nominal_col = _find_col(df, ["номин", "nominal"])
    ccy_col = _find_col(df, ["валют", "currency"])

    norm["norm_isin"] = norm[isin_col].map(normalize_isin) if isin_col else None
    norm["dohod_price"] = norm[price_col].map(to_float) if price_col else None
    norm["dohod_nkd"] = norm[nkd_col].map(to_float) if nkd_col else None
    norm["coupon_freq_per_year"] = norm[freq_col].map(to_float) if freq_col else None
    norm["dohod_coupon_rate"] = norm[coupon_col].map(to_float) if coupon_col else None
    norm["offer_date"] = norm[offer_col].map(parse_date) if offer_col else None
    norm["maturity_date"] = norm[mat_col].map(parse_date) if mat_col else None
    norm["dohod_base_index"] = norm[base_col].astype(str) if base_col else None
    norm["dohod_spread"] = norm[spread_col].map(to_float) if spread_col else None
    norm["dohod_current_nominal"] = norm[nominal_col].map(to_float) if nominal_col else None
    norm["dohod_currency"] = norm[ccy_col].astype(str).str.upper() if ccy_col else None
    return norm
