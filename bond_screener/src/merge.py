from __future__ import annotations

import pandas as pd


def merge_all(
    moex_norm: pd.DataFrame,
    dohod_norm: pd.DataFrame,
    amort_start: pd.DataFrame,
    min_days_to_amort: int,
) -> pd.DataFrame:
    moex_pref = moex_norm.add_prefix("moex_")
    dohod_pref = dohod_norm.add_prefix("dohod_")

    merged = pd.merge(
        moex_pref,
        dohod_pref,
        left_on="moex_isin",
        right_on="dohod_isin_norm",
        how="outer",
    )

    merged["isin"] = merged["moex_isin"].combine_first(merged["dohod_isin_norm"])
    merged["secid"] = merged["moex_secid"].combine_first(merged.get("dohod_secid_norm"))

    if not amort_start.empty:
        merged = merged.merge(amort_start, on=["isin", "secid"], how="left")
    else:
        merged["amort_start_date"] = None
        merged["days_to_amort"] = None
        merged["has_amortization"] = False

    merged["filter_amort_ok"] = merged["days_to_amort"].isna() | (merged["days_to_amort"] >= min_days_to_amort)
    return merged
