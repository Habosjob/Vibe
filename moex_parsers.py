# moex_parsers.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import pandas as pd


def ensure_single_secid(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    secid_idx = [i for i, c in enumerate(cols) if c == "SECID"]
    if len(secid_idx) <= 1:
        return df

    sec_cols = df.loc[:, ["SECID"]]
    secid_series = None
    for j in range(sec_cols.shape[1]):
        s = sec_cols.iloc[:, j]
        if secid_series is None:
            secid_series = s
        else:
            secid_series = secid_series.fillna(s)

    out = df.copy()
    out = out.loc[:, [c for c in out.columns if c != "SECID"]]
    out.insert(0, "SECID", secid_series)
    return out


def parse_iss_json_tables_safe(
    payload_text: str,
    *,
    logger=None,
    url: str = "",
    content_type: str = "",
    snippet_chars: int = 800,
) -> Dict[str, pd.DataFrame]:
    try:
        obj = json.loads(payload_text)
    except Exception as e:
        if logger:
            snip = (payload_text or "")[: max(0, int(snippet_chars))]
            logger.warning(
                f"ISS parse failed | err={e} | content_type={content_type!r} | url={url}\n"
                f"ISS response snippet:\n{snip}"
            )
        return {}

    out: Dict[str, pd.DataFrame] = {}
    if not isinstance(obj, dict):
        return out

    for block, content in obj.items():
        if not isinstance(content, dict):
            continue
        cols = content.get("columns")
        data = content.get("data")
        if isinstance(cols, list) and isinstance(data, list):
            out[block] = pd.DataFrame(data, columns=cols)
    return out


def pick_first(d: Dict[str, Any], keys: list[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, "", "nan", "None"):
            v = d[k]
            s = str(v).strip()
            if s and s.lower() not in ("nan", "none"):
                return s
    return None


def description_to_kv(description_df: pd.DataFrame) -> Dict[str, str]:
    if description_df is None or description_df.empty:
        return {}
    df = description_df.copy()
    df.columns = [str(c).upper() for c in df.columns]
    name_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
    if name_col is None or "VALUE" not in df.columns:
        return {}
    out: Dict[str, str] = {}
    for _, r in df.iterrows():
        k = str(r.get(name_col) or "").strip()
        v = r.get("VALUE")
        if not k or v is None:
            continue
        vs = str(v).strip()
        if not vs or vs.lower() in ("nan", "none"):
            continue
        out[k.upper()] = vs
    return out


def parse_date_utc_safe(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None