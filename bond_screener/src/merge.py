from __future__ import annotations

from datetime import date

import pandas as pd


def merge_all(
    moex_norm: pd.DataFrame,
    dohod_norm: pd.DataFrame,
    amort_agg: pd.DataFrame,
    smartlab_df: pd.DataFrame,
    min_days_to_amort: int,
) -> pd.DataFrame:
    moex = moex_norm.copy().add_prefix("moex_")
    dohod = dohod_norm.copy().add_prefix("dohod_")
    sl = smartlab_df.copy().add_prefix("smartlab_") if not smartlab_df.empty else pd.DataFrame(columns=["smartlab_secid"])

    merged = moex.merge(dohod, left_on="moex_norm_isin", right_on="dohod_norm_isin", how="outer")
    merged["isin"] = merged.get("moex_norm_isin").combine_first(merged.get("dohod_norm_isin"))
    merged["secid"] = merged.get("moex_norm_secid")

    if not amort_agg.empty:
        aa = amort_agg.rename(columns={"first_amort_date": "amort_start_date_iso"})
        merged = merged.merge(aa, left_on="secid", right_on="secid", how="left")
    else:
        merged["amort_start_date_iso"] = None
        merged["has_amortization"] = 0

    if not sl.empty:
        merged = merged.merge(sl, left_on="secid", right_on="smartlab_secid", how="left")

    merged["offer_date"] = merged.get("dohod_offer_date")
    sl_offer = pd.to_datetime(merged.get("smartlab_sl_offer_date_ddmmyyyy"), dayfirst=True, errors="coerce").dt.date
    merged["offer_date"] = merged["offer_date"].combine_first(sl_offer)

    merged["maturity_date"] = merged.get("dohod_maturity_date").combine_first(merged.get("moex_norm_maturity_date"))
    sl_mat = pd.to_datetime(merged.get("smartlab_sl_maturity_date_ddmmyyyy"), dayfirst=True, errors="coerce").dt.date
    merged["maturity_date"] = merged["maturity_date"].combine_first(sl_mat)

    merged["target_date"] = merged["offer_date"].combine_first(merged["maturity_date"])

    merged["current_nominal"] = merged.get("dohod_dohod_current_nominal").combine_first(merged.get("moex_norm_facevalue"))
    merged["initial_nominal"] = merged.get("moex_norm_facevalue").combine_first(merged["current_nominal"])

    amort_date = pd.to_datetime(merged.get("amort_start_date_iso"), errors="coerce").dt.date
    today = date.today()
    merged["days_to_amort"] = amort_date.map(lambda d: (d - today).days if pd.notna(d) else None)
    merged["days_to_offer"] = merged["offer_date"].map(lambda d: (d - today).days if pd.notna(d) else None)
    merged["days_to_maturity"] = merged["maturity_date"].map(lambda d: (d - today).days if pd.notna(d) else None)

    merged["filter_amort_ok"] = merged["days_to_amort"].isna() | (merged["days_to_amort"] >= min_days_to_amort)
    return merged
