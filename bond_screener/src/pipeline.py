from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd

from .cache import HTTPCache
from .checkpoint import CheckpointStore
from .db import Database
from .dohod_download import load_or_download_dohod_excel
from .excel_export import export_screener
from .merge import merge_all
from .moex_bondization import fetch_bondization
from .moex_rates import fetch_moex_rates
from .ytm import calc_ytm_for_row

logger = logging.getLogger(__name__)


def run_pipeline(config: dict, project_root: Path) -> pd.DataFrame:
    started = time.time()
    v2 = config["v2"]
    today = date.today()

    db = Database(project_root / "data" / "bonds.db")
    http_cache = HTTPCache(project_root / "cache" / "http")
    checkpoints = CheckpointStore(project_root / "cache" / "checkpoints")

    moex_res = fetch_moex_rates(v2["sources"]["moex_rates_csv_url"], v2["ttl_hours"]["moex_rates"], http_cache)

    dohod_path = project_root / "source" / "dohod_export.xlsx"
    dohod_res = load_or_download_dohod_excel(
        url=v2["sources"]["dohod_url"],
        output_path=dohod_path,
        ttl_hours=v2["ttl_hours"]["dohod_excel"],
        use_playwright=v2["dohod"]["use_playwright"],
        headless=v2["dohod"]["headless"],
        timeout_s=v2["dohod"]["timeout_s"],
    )

    moex_isins = set(moex_res.norm["isin"].dropna().tolist())
    dohod_isins = set(dohod_res.norm["isin_norm"].dropna().tolist())
    intersect = moex_isins.intersection(dohod_isins)

    if intersect:
        universe = moex_res.norm[moex_res.norm["isin"].isin(intersect)].copy()
    else:
        universe = moex_res.norm.copy()

    bondization_res = fetch_bondization(
        universe=universe[["isin", "secid"]].drop_duplicates(),
        ttl_hours=v2["ttl_hours"]["moex_bondization"],
        cache=http_cache,
        checkpoints=checkpoints,
        concurrency=v2["moex"]["concurrency"],
        today=today,
    )

    merged = merge_all(
        moex_norm=moex_res.norm,
        dohod_norm=dohod_res.norm,
        amort_start=bondization_res.amort_start,
        min_days_to_amort=v2["filters"]["min_days_to_amort"],
    )

    ytm_values = []
    warnings = []
    for _, row in merged.iterrows():
        result = calc_ytm_for_row(
            row=row,
            coupons=bondization_res.coupons,
            amortizations=bondization_res.amortizations,
            today=today,
            key_rate=v2["scenario"]["key_rate_avg_percent"],
            linker_inf=v2["scenario"]["linker_inflation_percent"],
        )
        ytm_values.append(result.ytm_calc)
        warnings.append(result.warning_text)

    merged["ytm_calc"] = ytm_values
    merged["warning_text"] = warnings

    with db.connect() as conn:
        db.write_df(conn, "moex_rates_raw", moex_res.raw)
        db.write_df(conn, "moex_rates_norm", moex_res.norm)
        db.write_df(conn, "dohod_raw", dohod_res.raw)
        db.write_df(conn, "dohod_norm", dohod_res.norm)
        db.write_df(conn, "moex_coupons", bondization_res.coupons)
        db.write_df(conn, "moex_amortizations", bondization_res.amortizations)
        db.write_df(conn, "moex_amort_start", bondization_res.amort_start)
        db.write_df(conn, "screener", merged)

    output_path = project_root / "source" / "Screener.xlsx"
    export_screener(merged, output_path)

    elapsed = time.time() - started
    logger.info(
        "Summary: rows total=%s, has_amortization=%s, filter_amort_ok=%s, ytm_calc_not_null=%s, total_time_sec=%.2f",
        len(merged),
        int(merged["has_amortization"].fillna(False).sum()) if "has_amortization" in merged else 0,
        int(merged["filter_amort_ok"].fillna(False).sum()),
        int(merged["ytm_calc"].notna().sum()),
        elapsed,
    )

    return merged
