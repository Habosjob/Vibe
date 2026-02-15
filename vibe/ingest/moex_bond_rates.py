from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd

from vibe.data_sources.moex_iss import MOEXBondRatesClient
from vibe.storage.excel import write_dataframe_to_excel_atomic
from vibe.utils.fs import write_bytes_atomic

logger = logging.getLogger(__name__)

KNOWN_ID_COLUMNS = {"SECID", "ISIN", "REGNUMBER", "SHORTNAME", "SECNAME"}
KNOWN_RATE_COLUMNS = {
    "LAST",
    "YIELD",
    "YIELDATWAPRICE",
    "DURATION",
    "ACCRUEDINT",
    "COUPONVALUE",
    "WAPRICE",
    "VOLUME",
    "NUMTRADES",
    "PRICE",
}
KNOWN_DATE_COLUMNS = {"MATDATE", "PREVDATE", "SETTLEDATE", "TRADEDATE", "OFFERDATE"}


@dataclass
class IngestResult:
    out_xlsx: Path
    raw_csv: Path
    rows: int
    cols: int
    downloaded_at_utc: str
    sha256_raw_csv: str


def _validate_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("MOEX rates DataFrame is empty.")

    columns = set(df.columns)
    id_hits = columns.intersection(KNOWN_ID_COLUMNS)
    rate_hits = columns.intersection(KNOWN_RATE_COLUMNS)
    if not id_hits or not rate_hits:
        raise ValueError(
            "Unexpected MOEX rates schema: no required identifier/rate columns found. "
            f"identifier_matches={sorted(id_hits)}, rate_matches={sorted(rate_hits)}"
        )

    numeric_candidates = [c for c in df.columns if c in KNOWN_RATE_COLUMNS]
    for col in numeric_candidates:
        before_nan = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after_nan = df[col].isna().sum()
        if after_nan > before_nan:
            logger.info(
                "Numeric coercion introduced NaN values: col=%s added_nan=%s",
                col,
                after_nan - before_nan,
            )

    date_candidates = [c for c in df.columns if c in KNOWN_DATE_COLUMNS]
    for col in date_candidates:
        before_nan = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        after_nan = df[col].isna().sum()
        if after_nan > before_nan:
            logger.info(
                "Date coercion introduced NaN values: col=%s added_nan=%s",
                col,
                after_nan - before_nan,
            )

    return df


def run_moex_bond_rates_ingest(out_xlsx: Path, raw_csv: Path, url: str, *, timeout: int = 30, retries: int = 3) -> IngestResult:
    client = MOEXBondRatesClient(timeout=timeout, retries=retries)
    raw_bytes = client.fetch_rates_csv_bytes(url)

    write_bytes_atomic(raw_bytes, raw_csv)

    df = pd.read_csv(BytesIO(raw_bytes))
    df = _validate_and_cast(df)

    downloaded_at_utc = datetime.now(timezone.utc).isoformat()
    sha256_raw_csv = hashlib.sha256(raw_bytes).hexdigest()

    meta = {
        "downloaded_at_utc": downloaded_at_utc,
        "source_url": url,
        "rows": len(df),
        "cols": len(df.columns),
        "sha256_raw_csv": sha256_raw_csv,
    }
    write_dataframe_to_excel_atomic(df=df, out_path=out_xlsx, sheet_name="rates", meta=meta)

    return IngestResult(
        out_xlsx=out_xlsx,
        raw_csv=raw_csv,
        rows=len(df),
        cols=len(df.columns),
        downloaded_at_utc=downloaded_at_utc,
        sha256_raw_csv=sha256_raw_csv,
    )
