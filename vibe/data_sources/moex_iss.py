from __future__ import annotations

import logging
from io import BytesIO

import pandas as pd

from vibe.utils.http import get_with_retries

logger = logging.getLogger(__name__)


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
        df = pd.read_csv(BytesIO(content))
        logger.info("Parsed MOEX rates CSV into DataFrame with row_count=%s", len(df))
        return df
