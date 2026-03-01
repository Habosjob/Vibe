from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from .utils import date_ddmmyyyy


def apply_sorter_with_dropped(df: pd.DataFrame, db, logger) -> pd.DataFrame:
    out = df.copy()
    today = date.today()
    now_iso = datetime.utcnow().isoformat()
    eps = 0.01
    records: list[dict] = []

    out["amort_started_flag"] = None
    out["amort_lt_1y"] = out.get("days_to_amort").map(lambda x: pd.notna(x) and float(x) < 365 if x is not None else False)
    out["mat_lt_1y"] = out.get("days_to_maturity").map(lambda x: pd.notna(x) and float(x) < 365 if x is not None else False)
    out["offer_lt_1y"] = out.get("days_to_offer").map(lambda x: pd.notna(x) and float(x) < 365 if x is not None else False)

    for i, row in out.iterrows():
        key = row.get("isin") or row.get("secid")
        if not key:
            continue
        key_type = "ISIN" if row.get("isin") else "SECID"
        reasons = []

        ini = row.get("initial_nominal")
        cur = row.get("current_nominal")
        if pd.notna(ini) and pd.notna(cur):
            started = float(cur) < float(ini) - eps
            out.at[i, "amort_started_flag"] = bool(started)
            if started:
                reasons.append(("AMORT_STARTED", "Амортизация уже началась", None, 1))

        if bool(row.get("amort_lt_1y")):
            reasons.append(("AMORT_LT_1Y", "Амортизация начнется менее чем через год", None, 1))
        if bool(row.get("mat_lt_1y")):
            reasons.append(("MAT_LT_1Y", "Погашение менее чем через год", None, 1))
        if bool(row.get("offer_lt_1y")):
            offer = row.get("offer_date")
            until = (offer + timedelta(days=1)) if pd.notna(offer) else None
            reasons.append(("OFFER_LT_1Y", "Оферта менее чем через год", until, 0))

        for code, text, until, is_perm in reasons:
            records.append(
                {
                    "key": str(key),
                    "key_type": key_type,
                    "reason_code": code,
                    "reason_text": text,
                    "dropped_at": date_ddmmyyyy(today),
                    "until": date_ddmmyyyy(until) if until else None,
                    "is_permanent": is_perm,
                    "updated_at": now_iso,
                }
            )

    if records:
        db.upsert_many("dropped_bonds", records)

    dropped = db.read_df("SELECT * FROM dropped_bonds")
    active = dropped[(dropped["is_permanent"] == 1) | (pd.to_datetime(dropped["until"], dayfirst=True, errors="coerce").dt.date >= today)]

    reason_map = active.groupby("key")["reason_code"].apply(lambda s: ",".join(sorted(set(s)))).to_dict() if not active.empty else {}
    until_map = active.groupby("key")["until"].first().to_dict() if not active.empty else {}
    perm_map = active.groupby("key")["is_permanent"].max().to_dict() if not active.empty else {}

    out["drop_key"] = out["isin"].fillna(out["secid"])
    out["dropped_reason_code"] = out["drop_key"].map(reason_map)
    out["dropped_flag"] = out["dropped_reason_code"].notna()
    out["dropped_until"] = out["drop_key"].map(until_map)
    out["dropped_is_permanent"] = out["drop_key"].map(perm_map).fillna(0).astype(int)
    out = out.drop(columns=["drop_key"])

    logger.info("SORTER dropped active=%s", int(out["dropped_flag"].sum()))
    return out
