# moex_excel.py
from __future__ import annotations

from typing import Dict

import pandas as pd

from moex_parsers import ensure_single_secid, parse_date_utc_safe


def build_pivot_description(description_df: pd.DataFrame, emitents_df: pd.DataFrame) -> pd.DataFrame:
    if description_df is None or description_df.empty:
        base = pd.DataFrame(columns=["SECID"])
    else:
        df = description_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = ensure_single_secid(df)
        if "SECID" not in df.columns:
            df["SECID"] = None
        key_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
        if key_col is None or "VALUE" not in df.columns:
            base = pd.DataFrame({"SECID": sorted(df["SECID"].dropna().astype(str).unique().tolist())})
        else:
            wide = df.pivot_table(index="SECID", columns=key_col, values="VALUE", aggfunc="first")
            if wide.index.name == "SECID" and "SECID" in wide.columns:
                wide = wide.drop(columns=["SECID"])
            base = wide.reset_index()
            base.columns = [str(c) for c in base.columns]

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        e = ensure_single_secid(e)
        if "SECID" in e.columns:
            keep = [
                c
                for c in [
                    "SECID",
                    "EMITTER_ID",
                    "INN",
                    "TITLE",
                    "SHORT_TITLE",
                    "OGRN",
                    "OKPO",
                    "KPP",
                    "OKVED",
                    "ADDRESS",
                    "PHONE",
                    "SITE",
                    "EMAIL",
                ]
                if c in e.columns
            ]
            if keep:
                base = base.merge(e[keep].drop_duplicates(), on="SECID", how="left")
    return base


def build_summary(
    sample_bonds: pd.DataFrame, emitents_df: pd.DataFrame, offers_df: pd.DataFrame, coupons_df: pd.DataFrame
) -> pd.DataFrame:
    out = sample_bonds.copy()
    out.columns = [str(c).upper() for c in out.columns]
    out = ensure_single_secid(out)

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        e = ensure_single_secid(e)
        keep = [c for c in ["SECID", "EMITTER_ID", "INN", "TITLE", "SHORT_TITLE", "OGRN", "OKPO", "KPP", "OKVED"] if c in e.columns]
        if keep:
            out = out.merge(e[keep].drop_duplicates(), on="SECID", how="left")

    next_offer: Dict[str, str] = {}
    if offers_df is not None and not offers_df.empty:
        df = offers_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = ensure_single_secid(df)
        date_col = "OFFERDATE" if "OFFERDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(parse_date_utc_safe)
            now = pd.Timestamp.now(tz="UTC")
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                pick = future.iloc[0] if len(future) else g.sort_values("_DT").iloc[-1]
                next_offer[str(secid)] = pick["_DT"].date().isoformat()

    next_coupon: Dict[str, str] = {}
    if coupons_df is not None and not coupons_df.empty:
        df = coupons_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = ensure_single_secid(df)
        date_col = "COUPONDATE" if "COUPONDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(parse_date_utc_safe)
            now = pd.Timestamp.now(tz="UTC")
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                if len(future):
                    next_coupon[str(secid)] = future.iloc[0]["_DT"].date().isoformat()

    if "SECID" in out.columns:
        out["NEXT_OFFER_DATE"] = out["SECID"].astype(str).map(next_offer)
        out["NEXT_COUPON_DATE"] = out["SECID"].astype(str).map(next_coupon)

    preferred = [
        "SECID",
        "ISIN",
        "REGNUMBER",
        "SHORTNAME",
        "NAME",
        "EMITTER_ID",
        "INN",
        "TITLE",
        "ISSUEDATE",
        "MATDATE",
        "FACEVALUE",
        "FACEUNIT",
        "COUPONPERCENT",
        "COUPONVALUE",
        "COUPONPERIOD",
        "NEXT_OFFER_DATE",
        "NEXT_COUPON_DATE",
        "LISTLEVEL",
        "PRIMARY_BOARDID",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]