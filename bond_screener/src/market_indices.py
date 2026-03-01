from __future__ import annotations

from datetime import date
from io import StringIO
from typing import Dict

import pandas as pd

from .cache import HTTPCache
from .utils import parse_date, to_float


def _read_tables(data: bytes) -> list[pd.DataFrame]:
    text = data.decode("utf-8", errors="ignore")
    return pd.read_html(StringIO(text))


def load_market_indices(config: Dict, cache: HTTPCache) -> tuple[pd.DataFrame, pd.DataFrame]:
    ttl = config["ttl_hours"]["market_indices"]
    ruonia_bytes = cache.fetch("ruonia", config["sources"]["cbr_ruonia_url"], ttl)
    zcyc_bytes = cache.fetch("zcyc", config["sources"]["cbr_zcyc_url"], ttl)

    ru_tables = _read_tables(ruonia_bytes)
    ru = ru_tables[0].copy()
    ru.columns = [str(c).strip() for c in ru.columns]
    date_col = next((c for c in ru.columns if "дат" in c.lower()), ru.columns[0])
    val_col = next((c for c in ru.columns if "ruonia" in c.lower() or "%" in c.lower()), ru.columns[-1])
    ru["date"] = ru[date_col].map(parse_date)
    ru["ruonia_percent"] = ru[val_col].map(to_float)
    ru = ru.dropna(subset=["date", "ruonia_percent"]).sort_values("date")
    ru_last = ru.tail(1)
    market_ruonia = pd.DataFrame(
        {
            "date_ddmmyyyy": ru_last["date"].dt.strftime("%d.%m.%Y"),
            "ruonia_percent": ru_last["ruonia_percent"],
        }
    )

    z_tables = _read_tables(zcyc_bytes)
    z = z_tables[0].copy()
    z.columns = [str(c).strip() for c in z.columns]
    date_col = next((c for c in z.columns if "дат" in c.lower()), z.columns[0])
    tenor_col = next((c for c in z.columns if "срок" in c.lower() or "tenor" in c.lower()), z.columns[1])
    y_col = next((c for c in z.columns if "%" in c.lower() or "yield" in c.lower() or "став" in c.lower()), z.columns[-1])
    z["date"] = z[date_col].map(parse_date)
    z["tenor_years"] = z[tenor_col].map(to_float)
    z["yield_percent"] = z[y_col].map(to_float)
    z = z.dropna(subset=["date", "tenor_years", "yield_percent"]).sort_values(["date", "tenor_years"])
    latest_date = z["date"].max()
    z = z[z["date"] == latest_date]
    market_zcyc = pd.DataFrame(
        {
            "date_ddmmyyyy": z["date"].dt.strftime("%d.%m.%Y"),
            "tenor_years": z["tenor_years"],
            "yield_percent": z["yield_percent"],
        }
    )
    return market_ruonia, market_zcyc


def rolling_value(values: list[float], target_date: date, today: date) -> float:
    idx = max(0, min(len(values) - 1, target_date.year - today.year))
    return values[idx]


def interpolate_zcyc(zcyc_df: pd.DataFrame, tenor: float) -> float | None:
    if zcyc_df.empty:
        return None
    z = zcyc_df.sort_values("tenor_years")
    return float(pd.Series(index=z["tenor_years"], data=z["yield_percent"]).interpolate(method="index").reindex([tenor], method="nearest").iloc[0])
