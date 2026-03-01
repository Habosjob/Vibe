from __future__ import annotations

import asyncio
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
from .moex_bondization import build_amort_agg, fetch_bondization_bulk
from .moex_rates import load_moex_rates
from .smartlab import fetch_smartlab
from .sorter import apply_sorter_with_dropped
from .writer_queue import AsyncWriter
from .ytm import compute_ytm


class Pipeline:
    def __init__(self, root: Path):
        self.root = root
        self.config = yaml.safe_load((root / "config" / "config.yaml").read_text(encoding="utf-8"))["v2"]
        self.logger = setup_logging(root / "logs" / "app.log")
        self.db = Database(root / "data" / "bonds.db")
        self.db.ensure_source_tables()
        self.cache = HTTPCache(root / "cache" / "http")
        self.cp_bulk = CheckpointStore(root / "cache" / "checkpoints" / "moex_bondization_bulk.json")
        self.cp_smartlab = CheckpointStore(root / "cache" / "checkpoints" / "smartlab_items.json")

    async def _run_network_stage(self, universe: pd.DataFrame) -> dict:
        writer = AsyncWriter(self.db, heartbeat_s=7, commit_rows=2000, commit_every_s=2.0)
        writer_task = asyncio.create_task(writer.run(self.logger))

        moex_task = asyncio.create_task(fetch_bondization_bulk(self.config, writer, self.cp_bulk, self.logger))
        smart_task = asyncio.create_task(fetch_smartlab(universe, self.config, writer, self.cp_smartlab, self.logger))

        moex_stats, smart_stats = await asyncio.gather(moex_task, smart_task)
        await writer.stop()
        await writer_task
        return {"moex": moex_stats, "smartlab": smart_stats, "writer_rows": writer.total_rows}

    def run(self) -> None:
        t0 = time.time()
        self.logger.info("Pipeline started")

        moex_raw, moex_norm = load_moex_rates(self.config, self.cache, self.logger)
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

        universe = moex_norm[["norm_isin", "norm_secid"]].rename(columns={"norm_isin": "isin", "norm_secid": "secid"}).drop_duplicates()
        universe = universe[universe["secid"].notna()]

        network_stats = asyncio.run(self._run_network_stage(universe))

        coupons_df = self.db.read_df("SELECT * FROM moex_coupons")
        amort_df = self.db.read_df("SELECT * FROM moex_amortizations")
        smartlab_df = self.db.read_df("SELECT * FROM smartlab_bond")

        amort_agg = build_amort_agg(amort_df)
        if not amort_agg.empty:
            self.db.upsert_many("moex_amort_agg", amort_agg.to_dict("records"))

        merged = merge_all(moex_norm, dohod_norm, amort_agg, smartlab_df, self.config["filters"]["min_days_to_amort"])
        result = compute_ytm(merged, coupons_df, amort_df, m_ruonia, m_zcyc, self.config, logger=self.logger)
        result["smartlab_status"] = network_stats["smartlab"]["status"]
        result = apply_sorter_with_dropped(result, self.db, self.logger)
        self.db.write_df("merged_all", result)

        export_screener(result, self.root / "source" / "Screener.xlsx")

        dropped_stats = (
            result[result["dropped_flag"]].groupby("dropped_reason_code").size().sort_values(ascending=False).head(10).to_dict()
            if "dropped_reason_code" in result
            else {}
        )

        ytm_nonnull = result["ytm_calc"].notna() if "ytm_calc" in result else pd.Series(dtype=bool)
        metrics = {
            "ytm_calc_count": int(ytm_nonnull.sum()) if len(ytm_nonnull) else 0,
            "zero_coupon_count": int((result.get("ytm_method", pd.Series(dtype=str)) == "zero_coupon").sum()),
            "floater_count": int((result.get("ytm_method", pd.Series(dtype=str)) == "floater_scenario").sum()),
            "perpetual_count": int((result.get("ytm_method", pd.Series(dtype=str)) == "perpetual_compounded").sum()),
        }

        self.logger.info(
            "Summary total_rows=%s bondization_bulk=%s smartlab_done=%s smartlab_failed=%s smartlab_skipped=%s smartlab_avg_rps=%s has_amort_count=%s amort_started_count=%s dropped=%s total_time=%ss",
            len(result),
            network_stats["moex"],
            network_stats["smartlab"].get("done", 0),
            network_stats["smartlab"].get("failed", 0),
            network_stats["smartlab"].get("skipped", 0),
            network_stats["smartlab"].get("avg_rps", 0),
            int(result.get("has_amortization", pd.Series(dtype=float)).fillna(0).sum()),
            int(result.get("amort_started_flag", pd.Series(dtype=bool)).fillna(False).sum()),
            dropped_stats,
            round(time.time() - t0, 2),
        )
        self.logger.info("YTM counts: %s", metrics)
        self.logger.info(
            "Unparsable date counts: coupons=%s amortizations=%s",
            int(result.get("unparsable_coupon_dates_count", pd.Series([0])).max()),
            int(result.get("unparsable_amort_dates_count", pd.Series([0])).max()),
        )

        top_cols = [
            c
            for c in ["isin", "secid", "ytm_calc", "dirty_price_amt", "nominal_used", "horizon_date", "ytm_method"]
            if c in result.columns
        ]
        self.logger.info("Top-10 ytm_calc: %s", result.sort_values("ytm_calc", ascending=False, na_position="last")[top_cols].head(10).to_dict("records"))
        self.db.close()
