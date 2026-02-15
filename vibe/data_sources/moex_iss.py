from __future__ import annotations

import logging
from io import BytesIO

import pandas as pd

from vibe.utils.http import get_with_retries

logger = logging.getLogger(__name__)


def read_csv_with_fallbacks(csv_bytes: bytes, **kwargs) -> tuple[pd.DataFrame, str]:
    encodings = ("utf-8", "utf-8-sig", "cp1251")
    for encoding in encodings:
        try:
            return pd.read_csv(BytesIO(csv_bytes), encoding=encoding, **kwargs), encoding
        except UnicodeDecodeError:
            continue

    logger.warning("Falling back to latin1 while parsing MOEX rates CSV.")
    return pd.read_csv(BytesIO(csv_bytes), encoding="latin1", **kwargs), "latin1"


class MOEXBondRatesClient:
    def __init__(self, *, timeout: int = 30, retries: int = 3):
        self.timeout = timeout
        self.retries = retries

    def fetch_rates_csv_bytes(self, url: str) -> bytes:
        response = get_with_retries(url, timeout=self.timeout, retries=self.retries)
        logger.info(
            "Downloaded MOEX rates CSV: status=%s bytes=%s elapsed=%.3fs",
            response.status_code,
            len(response.content),
            response.elapsed_seconds,
        )
        return response.content

    def fetch_rates_df(self, url: str) -> pd.DataFrame:
        content = self.fetch_rates_csv_bytes(url)
        df, encoding = read_csv_with_fallbacks(content)
        logger.info(
            "Parsed MOEX rates CSV into DataFrame with row_count=%s encoding=%s",
            len(df),
            encoding,
        )
        return df

    def fetch_rates_df_from_bytes(self, csv_bytes: bytes) -> pd.DataFrame:
        df, encoding = read_csv_with_fallbacks(csv_bytes)
        logger.info(
            "Parsed MOEX rates CSV bytes into DataFrame with row_count=%s encoding=%s",
            len(df),
            encoding,
        )
        return df
