from __future__ import annotations

import pandas as pd


def merge_all(moex_norm: pd.DataFrame, dohod_norm: pd.DataFrame, amort_start: pd.DataFrame, min_days_to_amort: int) -> pd.DataFrame:
    moex = moex_norm.copy()
    dohod = dohod_norm.copy()
    moex = moex.add_prefix("moex_")
    dohod = dohod.add_prefix("dohod_")

    if "moex_norm_isin" in moex.columns:
        merged = moex.merge(dohod, left_on="moex_norm_isin", right_on="dohod_norm_isin", how="outer")
    else:
        merged = moex.join(dohod, how="outer")

    merged["isin"] = merged.get("moex_norm_isin").combine_first(merged.get("dohod_norm_isin"))
    merged["secid"] = merged.get("moex_norm_secid")
    merged = merged.merge(amort_start, on=["isin", "secid"], how="left")

    offer = merged.get("dohod_offer_date")
    mat = merged.get("dohod_maturity_date").combine_first(merged.get("moex_norm_maturity_date"))
    merged["target_date"] = offer.combine_first(mat)
    merged["filter_amort_ok"] = merged["days_to_amort"].isna() | (merged["days_to_amort"] >= min_days_to_amort)
    return merged
