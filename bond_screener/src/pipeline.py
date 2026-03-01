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
        moex_sample_cols = [c for c in ["norm_isin", "norm_secid", "norm_name"] if c in moex_norm.columns]
        self.logger.info(
            "Loaded moex_rates_norm rows=%s isin_notnull=%s secid_notnull=%s sample=%s",
            len(moex_norm),
            int(moex_norm.get("norm_isin", pd.Series(dtype=object)).notna().sum()),
            int(moex_norm.get("norm_secid", pd.Series(dtype=object)).notna().sum()),
            moex_norm[moex_sample_cols].head(5).to_dict("records") if moex_sample_cols else [],
        )
        self.db.write_df("moex_rates_raw", moex_raw)
        self.db.write_df("moex_rates_norm", moex_norm)

        dohod_file = download_dohod_excel(self.config, self.root / "source" / "dohod_export.xlsx", self.logger)
        dohod_raw = pd.read_excel(dohod_file)
        dohod_norm = normalize_dohod(dohod_raw)
        dohod_sample_cols = [c for c in ["norm_isin", "dohod_price", "dohod_base_index"] if c in dohod_norm.columns]
        self.logger.info(
            "Loaded dohod_norm rows=%s isin_notnull=%s sample=%s",
            len(dohod_norm),
            int(dohod_norm.get("norm_isin", pd.Series(dtype=object)).notna().sum()),
            dohod_norm[dohod_sample_cols].head(5).to_dict("records") if dohod_sample_cols else [],
        )
        self.db.write_df("dohod_raw", dohod_raw)
        self.db.write_df("dohod_norm", dohod_norm)

        m_ruonia, m_zcyc = load_market_indices(self.config, self.cache)
        self.db.write_df("market_ruonia", m_ruonia)
        self.db.write_df("market_zcyc", m_zcyc)

        universe_src = moex_norm[["norm_isin", "norm_secid"]].rename(columns={"norm_isin": "isin", "norm_secid": "secid"}).copy()
        inter = set(moex_norm.get("norm_isin", pd.Series(dtype=object)).dropna()) & set(dohod_norm.get("norm_isin", pd.Series(dtype=object)).dropna())
        universe = universe_src[universe_src["secid"].notna()].drop_duplicates()
        reason = "moex_norm_secid"
        if inter:
            inter_universe = universe[universe["isin"].isin(inter)]
            if not inter_universe.empty:
                universe = inter_universe
                reason = "isin_intersection"
            else:
                self.logger.warning("ISIN intersection exists but secid universe by intersection is empty; fallback to full moex SECID universe")
        elif not universe.empty:
            self.logger.warning("ISIN intersection empty; fallback to full moex SECID universe")

        if universe.empty:
            fallback = universe_src[universe_src["isin"].notna()].copy()
            fallback["secid"] = fallback["isin"]
            universe = fallback.drop_duplicates()
            reason = "isin_as_secid_fallback"

        fallback_count = int(((universe["secid"] == universe["isin"]) & universe["isin"].notna()).sum()) if not universe.empty else 0
        self.logger.info(
            "Before bondization universe_rows=%s reason=%s secid_count=%s fallback_isin_as_secid=%s secid_sample=%s",
            len(universe),
            reason,
            int(universe["secid"].notna().sum()) if not universe.empty else 0,
            fallback_count,
            universe["secid"].dropna().astype(str).head(10).tolist() if not universe.empty else [],
        )
        if universe.empty:
            self.logger.error(
                "Bondization universe is empty. Possible reasons: missing moex norm_secid and missing norm_isin fallback after filters/intersection"
            )

        coupons_df, amort_df = load_bondization(
            universe,
            self.config,
            self.checkpoints,
            ttl_hours=self.config["ttl_hours"]["moex_bondization"],
            logger=self.logger,
        )
        self.db.write_df("moex_coupons", coupons_df)
        self.db.write_df("moex_amortizations", amort_df)
        if amort_df.empty:
            self.logger.error("Bondization returned empty moex_amortizations")
        amort_start = build_amort_start(universe, amort_df)
        self.db.write_df("moex_amort_start", amort_start)

        merged = merge_all(moex_norm, dohod_norm, amort_start, self.config["filters"]["min_days_to_amort"])
        result = compute_ytm(merged, coupons_df, amort_df, m_ruonia, m_zcyc, self.config)
        self.db.write_df("merged_all", result)

        export_screener(result, self.root / "source" / "Screener.xlsx")

        result = result.infer_objects(copy=False)

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
        self.logger.info(
            "Self-check top ytm rows: %s",
            result.sort_values("ytm_calc", ascending=False, na_position="last")[
                [c for c in ["isin", "moex_norm_name", "ytm_calc", "price_unit", "dirty_price_amt", "dohod_dohod_current_nominal", "target_date"] if c in result.columns]
            ].head(10).to_dict("records"),
        )
        self.logger.info(
            "Self-check has_amortization_true=%s days_to_amort_notnull=%s floater_base_known=%s",
            int(result.get("has_amortization", pd.Series(dtype=bool)).fillna(False).sum()),
            int(result.get("days_to_amort", pd.Series(dtype=object)).notna().sum()),
            int((result.get("floater_base", pd.Series(dtype=object)).fillna("UNKNOWN") != "UNKNOWN").sum()),
        )
        self.logger.info(
            "Floater base distribution: %s",
            result.get("floater_base", pd.Series(dtype=object)).fillna("UNKNOWN").value_counts().to_dict(),
        )
        self.logger.info("Summary: %s", summary)
        self.db.close()
