from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import yaml

from .cache import HTTPCache
from .checkpoint import CheckpointStore
from .db import Database
from .dohod_download import download_dohod_excel, normalize_dohod
from .excel_export import export_screener
from .logging_setup import setup_logging
from .market_indices import load_market_indices
from .merge import merge_all
from .moex_bondization import build_amort_start, load_bondization
from .moex_rates import load_moex_rates
from .ytm import compute_ytm


class Pipeline:
    def __init__(self, root: Path):
        self.root = root
        self.config = yaml.safe_load((root / "config" / "config.yaml").read_text(encoding="utf-8"))["v2"]
        self.logger = setup_logging(root / "logs" / "app.log")
        self.db = Database(root / "data" / "bonds.db")
        self.cache = HTTPCache(root / "cache" / "http")
        self.checkpoints = CheckpointStore(root / "cache" / "checkpoints" / "bondization.json")

    def run(self) -> None:
        t0 = time.time()
        self.logger.info("Pipeline started")

        moex_raw, moex_norm = load_moex_rates(self.config, self.cache)
        self.db.write_df("moex_rates_raw", moex_raw)
        self.db.write_df("moex_rates_norm", moex_norm)

        dohod_file = download_dohod_excel(self.config, self.root / "source" / "dohod_export.xlsx", self.logger)
        dohod_raw = pd.read_excel(dohod_file)
        dohod_norm = normalize_dohod(dohod_raw)
        self.db.write_df("dohod_raw", dohod_raw)
        self.db.write_df("dohod_norm", dohod_norm)

        m_ruonia, m_zcyc = load_market_indices(self.config, self.cache)
        self.db.write_df("market_ruonia", m_ruonia)
        self.db.write_df("market_zcyc", m_zcyc)

        universe = moex_norm[["norm_isin", "norm_secid"]].rename(columns={"norm_isin": "isin", "norm_secid": "secid"}).dropna(subset=["secid"])
        inter = set(moex_norm["norm_isin"].dropna()) & set(dohod_norm["norm_isin"].dropna())
        if inter:
            universe = universe[universe["isin"].isin(inter)]
        if universe.empty:
            universe = moex_norm[["norm_isin", "norm_secid"]].rename(columns={"norm_isin": "isin", "norm_secid": "secid"}).dropna(subset=["secid"])

        coupons_df, amort_df = load_bondization(
            universe,
            self.config,
            self.checkpoints,
            ttl_hours=self.config["ttl_hours"]["moex_bondization"],
            logger=self.logger,
        )
        self.db.write_df("moex_coupons", coupons_df)
        self.db.write_df("moex_amortizations", amort_df)
        amort_start = build_amort_start(universe, amort_df)
        self.db.write_df("moex_amort_start", amort_start)

        merged = merge_all(moex_norm, dohod_norm, amort_start, self.config["filters"]["min_days_to_amort"])
        result = compute_ytm(merged, coupons_df, amort_df, m_ruonia, m_zcyc, self.config)
        self.db.write_df("merged_all", result)

        export_screener(result, self.root / "source" / "Screener.xlsx")

        summary = {
            "rows total": len(result),
            "offer_count": int(result["dohod_offer_date"].notna().sum()) if "dohod_offer_date" in result else 0,
            "has_amort_count": int(result["has_amortization"].fillna(False).sum()) if "has_amortization" in result else 0,
            "filter_ok_count": int(result["filter_amort_ok"].fillna(False).sum()),
            "ytm_calc_count": int(result["ytm_calc"].notna().sum()),
            "zero_coupon_count": int((result["ytm_method"] == "zero_coupon").sum()),
            "floater_count": int((result["ytm_method"] == "floater_scenario").sum()),
            "perpetual_count": int((result["ytm_method"] == "perpetual_compounded").sum()),
            "total_time": round(time.time() - t0, 2),
        }
        self.logger.info("Summary: %s", summary)
        self.db.close()
