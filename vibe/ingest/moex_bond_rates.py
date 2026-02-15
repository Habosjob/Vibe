from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vibe.data_sources.moex_iss import MOEXBondRatesClient
from vibe.storage.excel import write_dataframe_to_excel_atomic
from vibe.utils.fs import ensure_parent_dir, write_bytes_atomic

logger = logging.getLogger(__name__)

DROP_COLUMNS = [
    "RTL1",
    "RTH1",
    "RTL2",
    "RTH2",
    "RTL3",
    "RTH3",
    "DISCOUNT1",
    "LIMIT1",
    "DISCOUNT2",
    "LIMIT2",
    "DISCOUNT3",
    "DISCOUNTL0",
    "DISCOUNTH0",
    "FULLCOVERED",
    "FULL_COVERED_LIMIT",
    "REGISTRYCLOSEDATE",
    "DIVIDENDVALUE",
    "DIVIDENDYIELD",
    "REGISTRYCLOSETYPE",
    "SUSPENSION_LISTING",
    "EVENINGSESSION",
    "MORNINGSESSION",
    "WEEKENDSESSION",
    "S_RII",
    "INCLUDEDBYMOEX",
    "PRIMARY_BOARD_TITLE",
    "PRIMARY_BOARDID",
    "IS_COLLATERAL",
    "IS_EXTERNAL",
]

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

DEFAULT_OUT_XLSX = Path("data/curated/moex/bond_rates.xlsx")
DEFAULT_RAW_DIR = Path("data/raw/moex")
DEFAULT_RAW_BASENAME = "bond_rates"
DEFAULT_MAX_PRINT = 200
DEFAULT_KEEP_ID = "ISIN"


@dataclass
class IngestResult:
    out_xlsx: Path
    raw_csv: Path
    rows: int
    cols: int
    downloaded_at_utc: str
    sha256_raw_csv: str


def _normalize_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"^\s*$", pd.NA, regex=True)


def _select_identifier_column(df: pd.DataFrame, keep_id: str = DEFAULT_KEEP_ID) -> str:
    if keep_id not in {"ISIN", "SECID"}:
        raise ValueError(f"Unsupported keep_id='{keep_id}'. Use 'ISIN' or 'SECID'.")

    if keep_id == "SECID":
        if "SECID" in df.columns:
            return "SECID"
        logger.warning("SECID column is missing. Falling back to ISIN as identifier.")
        return "ISIN"

    if "ISIN" not in df.columns:
        logger.warning("ISIN column is missing. Falling back to SECID as identifier.")
        return "SECID"

    isin_blank_ratio = df["ISIN"].isna().mean()
    if isin_blank_ratio > 0.5:
        logger.warning(
            "ISIN has too many empty values (%.1f%%). Falling back to SECID as identifier.",
            isin_blank_ratio * 100,
        )
        return "SECID"
    return "ISIN"


def _drop_duplicate_by_key(df: pd.DataFrame, key: str) -> pd.DataFrame:
    if key not in df.columns:
        raise ValueError(f"Identifier column '{key}' is missing in DataFrame.")

    df = df.dropna(subset=[key]).copy()
    if df.empty:
        return df

    completeness = df.notna().sum(axis=1)
    df = (
        df.assign(_completeness=completeness)
        .sort_values(by=[key, "_completeness"], ascending=[True, False], kind="stable")
        .drop_duplicates(subset=[key], keep="first")
        .drop(columns=["_completeness"])
    )

    if df[key].duplicated().sum() != 0:
        raise ValueError(f"Duplicate rows by '{key}' still exist after deduplication.")

    return df


def clean_bond_rates_dataframe(df: pd.DataFrame, keep_id: str = DEFAULT_KEEP_ID) -> tuple[pd.DataFrame, str]:
    cleaned = _normalize_empty_strings(df.copy())
    cleaned = cleaned.drop(columns=DROP_COLUMNS, errors="ignore")

    key = _select_identifier_column(cleaned, keep_id=keep_id)
    if key == "ISIN":
        cleaned = cleaned.drop(columns=["SECID"], errors="ignore")
    else:
        cleaned = cleaned.drop(columns=["ISIN"], errors="ignore")

    cleaned = _drop_duplicate_by_key(cleaned, key)
    return cleaned, key


def _snapshot_path_for_date(base_path: Path, date_tag: str, suffix: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{date_tag}.{suffix}")


def _load_previous_snapshot(curated_dir: Path, stem: str, date_tag: str) -> pd.DataFrame | None:
    candidates = sorted(curated_dir.glob(f"{stem}_*.parquet"))
    previous = [path for path in candidates if path.stem.rsplit("_", 1)[-1] < date_tag]
    if not previous:
        return None
    return pd.read_parquet(previous[-1])


def _print_changes(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame | None,
    key: str,
    max_print: int = DEFAULT_MAX_PRINT,
) -> None:
    if previous_df is None:
        print("Предыдущий снапшот не найден — сравнение пропущено")
        return

    if key not in current_df.columns or key not in previous_df.columns:
        logger.warning("Comparison skipped: identifier column '%s' is absent in one of snapshots.", key)
        return

    current = set(current_df[key].dropna())
    previous = set(previous_df[key].dropna())

    added = sorted(current - previous)
    expired = sorted(previous - current)

    def _shortname_map(df: pd.DataFrame) -> dict[str, str]:
        if "SHORTNAME" not in df.columns:
            return {}
        series = df[[key, "SHORTNAME"]].dropna(subset=[key]).drop_duplicates(subset=[key], keep="first")
        return dict(zip(series[key], series["SHORTNAME"]))

    current_names = _shortname_map(current_df)
    previous_names = _shortname_map(previous_df)

    print(f"Добавлено {len(added)} бумаг:")
    for value in added[:max_print]:
        print(f"{value} | {current_names.get(value, '')}")
    if len(added) > max_print:
        print(f"... and {len(added) - max_print} more")

    print(f"Погашено {len(expired)} бумаг:")
    for value in expired[:max_print]:
        print(f"{value} | {previous_names.get(value, '')}")
    if len(expired) > max_print:
        print(f"... and {len(expired) - max_print} more")


def _resolve_ingest_paths(out_xlsx: Path | None, raw_path: Path | None) -> tuple[Path, Path]:
    resolved_out = out_xlsx or DEFAULT_OUT_XLSX
    if raw_path is None:
        resolved_raw_csv = DEFAULT_RAW_DIR / f"{DEFAULT_RAW_BASENAME}.csv"
    elif raw_path.suffix.lower() == ".csv":
        resolved_raw_csv = raw_path
    else:
        resolved_raw_csv = raw_path / f"{DEFAULT_RAW_BASENAME}.csv"

    ensure_parent_dir(resolved_out)
    ensure_parent_dir(resolved_raw_csv)
    return resolved_out, resolved_raw_csv


def _validate_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("MOEX rates DataFrame is empty.")
    if df.shape[1] < 5:
        raise ValueError(f"MOEX rates DataFrame has too few columns: {df.shape[1]}.")

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


def run_moex_bond_rates_ingest(
    out_xlsx: Path | None,
    raw_csv: Path | None,
    url: str,
    *,
    timeout: int = 30,
    retries: int = 3,
    no_cache: bool = False,
    max_print: int = DEFAULT_MAX_PRINT,
    keep_id: str = DEFAULT_KEEP_ID,
) -> IngestResult:
    out_xlsx, raw_csv = _resolve_ingest_paths(out_xlsx, raw_csv)
    date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    daily_raw_csv = _snapshot_path_for_date(raw_csv, date_tag, "csv")
    daily_parquet = _snapshot_path_for_date(out_xlsx.with_suffix(""), date_tag, "parquet")

    source_url = url
    ensure_parent_dir(daily_parquet)

    raw_bytes: bytes | None = None
    if daily_parquet.exists() and not no_cache:
        logger.info("Cache hit for %s", date_tag)
        df = pd.read_parquet(daily_parquet)
        sha256_raw_csv = ""
    else:
        client = MOEXBondRatesClient(timeout=timeout, retries=retries)
        raw_bytes = client.fetch_rates_csv_bytes(url)
        write_bytes_atomic(raw_bytes, daily_raw_csv)

        df = client.fetch_rates_df_from_bytes(raw_bytes)
        df = _validate_and_cast(df)
        df, key = clean_bond_rates_dataframe(df, keep_id=keep_id)
        df.to_parquet(daily_parquet, index=False)
        sha256_raw_csv = hashlib.sha256(raw_bytes).hexdigest()

        previous_snapshot = _load_previous_snapshot(daily_parquet.parent, out_xlsx.stem, date_tag)
        _print_changes(df, previous_snapshot, key, max_print=max_print)

    if raw_bytes is None:
        # cache branch
        df, key = clean_bond_rates_dataframe(df, keep_id=keep_id)
        previous_snapshot = _load_previous_snapshot(daily_parquet.parent, out_xlsx.stem, date_tag)
        _print_changes(df, previous_snapshot, key, max_print=max_print)

    downloaded_at_utc = datetime.now(timezone.utc).isoformat()

    meta = {
        "downloaded_at_utc": downloaded_at_utc,
        "source_url": source_url,
        "rows": len(df),
        "cols": len(df.columns),
        "sha256_raw_csv": sha256_raw_csv,
    }
    write_dataframe_to_excel_atomic(df=df, out_path=out_xlsx, sheet_name="rates", meta=meta)

    return IngestResult(
        out_xlsx=out_xlsx,
        raw_csv=daily_raw_csv,
        rows=len(df),
        cols=len(df.columns),
        downloaded_at_utc=downloaded_at_utc,
        sha256_raw_csv=sha256_raw_csv,
    )
