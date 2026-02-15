from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vibe.ingest.moex_bond_rates import DROP_COLUMNS, clean_bond_rates_dataframe, run_moex_bond_rates_ingest


def test_clean_bond_rates_dataframe_drops_columns_and_dedups() -> None:
    df = pd.DataFrame(
        {
            "ISIN": ["RU000A", "RU000A", "RU000B", ""],
            "SECID": ["SEC1", "SEC1", "SEC2", "SEC3"],
            "SHORTNAME": ["A", "A", "B", "C"],
            "LAST": [100, 101, 102, 103],
            "RTL1": [1, 2, 3, 4],
            "LIMIT1": [5, 6, 7, 8],
            "MATDATE": ["01.01.2030", "01.01.2030", "01.01.2031", "01.01.2032"],
        }
    )

    cleaned, key = clean_bond_rates_dataframe(df)

    assert key == "ISIN"
    assert "SECID" not in cleaned.columns
    assert cleaned["ISIN"].duplicated().sum() == 0
    assert len(cleaned) == 2
    for col in DROP_COLUMNS:
        assert col not in cleaned.columns


def test_clean_bond_rates_dataframe_fallback_to_secid(caplog) -> None:
    caplog.set_level("WARNING")
    df = pd.DataFrame(
        {
            "ISIN": [None, None, "RU000A"],
            "SECID": ["SEC1", "SEC2", "SEC3"],
            "SHORTNAME": ["A", "B", "C"],
            "LAST": [100, 101, 102],
        }
    )

    cleaned, key = clean_bond_rates_dataframe(df)

    assert key == "SECID"
    assert "ISIN" not in cleaned.columns
    assert "too many empty values" in caplog.text


def test_run_ingest_uses_daily_parquet_cache(tmp_path: Path, caplog) -> None:
    caplog.set_level("INFO")
    date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_xlsx = tmp_path / "bond_rates.xlsx"
    raw_csv = tmp_path / "bond_rates.csv"
    daily_parquet = tmp_path / f"bond_rates_{date_tag}.parquet"

    pd.DataFrame(
        {
            "ISIN": ["RU000A"],
            "SHORTNAME": ["Bond A"],
            "LAST": [100.0],
            "YIELD": [10.0],
            "MATDATE": [pd.Timestamp("2030-01-01")],
        }
    ).to_parquet(daily_parquet, index=False)

    result = run_moex_bond_rates_ingest(
        out_xlsx=out_xlsx,
        raw_csv=raw_csv,
        url="https://example.com/rates.csv",
    )

    assert "Cache hit for" in caplog.text
    assert result.raw_csv == tmp_path / f"bond_rates_{date_tag}.csv"
    assert out_xlsx.exists()
