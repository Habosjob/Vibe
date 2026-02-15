from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vibe.app.cli import build_parser
from vibe.ingest.moex_bond_rates import (
    DEFAULT_OUT_XLSX,
    DEFAULT_RAW_BASENAME,
    DEFAULT_RAW_DIR,
    run_moex_bond_rates_ingest,
)


def test_cli_parser_accepts_command_without_out_and_raw() -> None:
    parser = build_parser()

    args = parser.parse_args(["moex-bond-rates"])

    assert args.out is None
    assert args.raw is None


def test_run_ingest_uses_default_paths_when_out_and_raw_are_none(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    daily_parquet = (DEFAULT_OUT_XLSX.with_suffix("")).with_name(f"{DEFAULT_OUT_XLSX.stem}_{date_tag}.parquet")

    daily_parquet.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "ISIN": ["RU000A"],
            "SHORTNAME": ["Bond A"],
            "LAST": [100.0],
            "YIELD": [10.0],
        }
    ).to_parquet(daily_parquet, index=False)

    result = run_moex_bond_rates_ingest(
        out_xlsx=None,
        raw_csv=None,
        url="https://example.com/rates.csv",
    )

    assert result.out_xlsx == DEFAULT_OUT_XLSX
    assert result.raw_csv == DEFAULT_RAW_DIR / f"{DEFAULT_RAW_BASENAME}_{date_tag}.csv"
    assert DEFAULT_OUT_XLSX.exists()


def test_cli_parser_probe_defaults() -> None:
    parser = build_parser()

    args = parser.parse_args(["moex-bond-endpoints-probe"])

    assert args.n_static == 10
    assert args.n_random == 10
    assert args.interval == 24
    assert args.max_rows_per_sheet == 200_000
