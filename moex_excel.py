# moex_excel.py
from __future__ import annotations

import pandas as pd

from moex_parsers import ensure_single_secid


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    pandas иногда тащит дубликаты имен колонок (особенно после merge/concat).
    Эта функция делает имена уникальными: SECID, SECID__2, SECID__3 ...
    """
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        s = str(c)
        if s not in seen:
            seen[s] = 1
            new_cols.append(s)
        else:
            seen[s] += 1
            new_cols.append(f"{s}__{seen[s]}")
    out = df.copy()
    out.columns = new_cols
    return out


def build_pivot_description(description_df: pd.DataFrame, emitents_df: pd.DataFrame) -> pd.DataFrame:
    # ---- base from description ----
    if description_df is None or description_df.empty:
        base = pd.DataFrame(columns=["SECID"])
    else:
        df = description_df.copy()
        df = _dedupe_columns(df)
        df.columns = [str(c).upper() for c in df.columns]

        # гарантируем один SECID
        df = ensure_single_secid(df)
        df = _dedupe_columns(df)

        # если SECID по какой-то причине пропал
        if "SECID" not in df.columns:
            df["SECID"] = None

        key_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
        if key_col is None or "VALUE" not in df.columns:
            base = pd.DataFrame({"SECID": sorted(df["SECID"].dropna().astype(str).unique().tolist())})
        else:
            # pivot
            wide = df.pivot_table(index="SECID", columns=key_col, values="VALUE", aggfunc="first")

            # !!! КЛЮЧЕВОЙ ФИКС !!!
            # wide может содержать колонку "SECID" (или дубликаты) — тогда reset_index упадёт
            if isinstance(wide, pd.DataFrame) and "SECID" in wide.columns:
                wide = wide.drop(columns=["SECID"])

            # если columns внезапно MultiIndex — приводим к строкам
            if isinstance(wide.columns, pd.MultiIndex):
                wide.columns = ["|".join([str(x) for x in tup if x is not None]) for tup in wide.columns.tolist()]
            else:
                wide.columns = [str(c) for c in wide.columns]

            # безопасный reset_index
            base = wide.reset_index(drop=False)

            # ещё раз обезопасимся от дублей
            base = _dedupe_columns(base)

    # ---- merge emitents ----
    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e = _dedupe_columns(e)
        e.columns = [str(c).upper() for c in e.columns]
        e = ensure_single_secid(e)
        e = _dedupe_columns(e)

        if "SECID" in e.columns and "SECID" in base.columns:
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
                    "UPDATED_UTC",
                ]
                if c in e.columns
            ]
            if keep:
                base = base.merge(e[keep].drop_duplicates(subset=["SECID"]), on="SECID", how="left")

    return base


def build_summary(sample_bonds: pd.DataFrame, emitents_df: pd.DataFrame) -> pd.DataFrame:
    out = sample_bonds.copy()
    out = _dedupe_columns(out)
    out.columns = [str(c).upper() for c in out.columns]
    out = ensure_single_secid(out)
    out = _dedupe_columns(out)

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e = _dedupe_columns(e)
        e.columns = [str(c).upper() for c in e.columns]
        e = ensure_single_secid(e)
        e = _dedupe_columns(e)

        if "SECID" in e.columns and "SECID" in out.columns:
            keep = [
                c for c in ["SECID", "EMITTER_ID", "INN", "TITLE", "SHORT_TITLE", "OGRN", "OKPO", "KPP", "OKVED"]
                if c in e.columns
            ]
            if keep:
                out = out.merge(e[keep].drop_duplicates(subset=["SECID"]), on="SECID", how="left")

    preferred = [
        "SECID", "ISIN", "REGNUMBER", "SHORTNAME", "NAME",
        "EMITTER_ID", "INN", "TITLE",
        "ISSUEDATE", "MATDATE", "FACEVALUE", "FACEUNIT",
        "COUPONPERCENT", "COUPONVALUE", "COUPONPERIOD",
        "LISTLEVEL", "PRIMARY_BOARDID",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]