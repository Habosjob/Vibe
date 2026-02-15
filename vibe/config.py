from __future__ import annotations

import os
from pathlib import Path

DEFAULT_MOEX_RATES_URL = (
    "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?"
    "sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,"
    "stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&"
    "iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&"
    "limit=unlimited&lang=ru"
)

DEFAULT_OUT_XLSX = Path("data/curated/moex/bond_rates.xlsx")
DEFAULT_RAW_CSV = Path("data/raw/moex/bond_rates.csv")
DEFAULT_HTTP_TIMEOUT_SECONDS = 30
DEFAULT_HTTP_RETRIES = 3

# Допущение: сохраняем rates "как есть" (без сложной нормализации),
# ограничиваясь базовой валидацией, приведением типов и метаданными загрузки.


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)
