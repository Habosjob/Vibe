from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlencode

import pandas as pd

from vibe.utils.http import get_with_retries

logger = logging.getLogger(__name__)

MOEX_ISS_BASE_URL = "https://iss.moex.com"
BOARD_FALLBACKS = ("TQCB", "TQOB", "TQOD")


@dataclass(frozen=True)
class BondEndpointSpec:
    name: str
    url_template: str

    def build_url(self, isin: str, board: str) -> str:
        return self.url_template.format(SECID=isin, BOARD=board)


class MoexBondEndpointsClient:
    def __init__(self, *, timeout: int = 30, retries: int = 3):
        self.timeout = timeout
        self.retries = retries

    def fetch_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{MOEX_ISS_BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"

        response = get_with_retries(url, timeout=self.timeout, retries=self.retries)
        return json.loads(response.content.decode("utf-8"))

    def resolve_board(self, isin: str) -> str:
        payload = self.fetch_json(f"/iss/securities/{isin}.json")
        boards_df = iss_json_to_frames(payload).get("boards")
        if boards_df is None or boards_df.empty:
            return BOARD_FALLBACKS[0]

        normalized = {str(col).upper(): col for col in boards_df.columns}

        def _value(frame: pd.DataFrame, key: str) -> pd.Series:
            col = normalized.get(key)
            if col is None:
                return pd.Series([None] * len(frame), index=frame.index)
            return frame[col]

        filtered = boards_df.copy()
        filtered = filtered[
            _value(filtered, "ENGINE").astype(str).str.lower().eq("stock")
            & _value(filtered, "MARKET").astype(str).str.lower().eq("bonds")
            & pd.to_numeric(_value(filtered, "IS_TRADED"), errors="coerce").fillna(0).astype(int).eq(1)
        ]

        if filtered.empty:
            filtered = boards_df

        primary_flags = ["IS_PRIMARY", "PRIMARY", "IS_DEFAULT"]
        primary_mask = pd.Series([False] * len(filtered), index=filtered.index)
        for key in primary_flags:
            series = _value(filtered, key)
            primary_mask = primary_mask | series.fillna(0).astype(str).isin(["1", "True", "true"])

        if primary_mask.any():
            filtered = filtered[primary_mask]

        board_col = normalized.get("BOARDID") or normalized.get("BOARD")
        if board_col is None or filtered.empty:
            return BOARD_FALLBACKS[0]

        return str(filtered.iloc[0][board_col])

    def fetch_endpoint(
        self,
        isin: str,
        board: str,
        spec: BondEndpointSpec,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        path = spec.build_url(isin=isin, board=board)
        return self.fetch_json(path=path, params=params)


def iss_json_to_frames(payload: dict[str, Any]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for table_name, table_payload in payload.items():
        if not isinstance(table_payload, dict):
            continue
        columns = table_payload.get("columns")
        data = table_payload.get("data")
        if not isinstance(columns, list) or not isinstance(data, list):
            continue
        frames[str(table_name)] = pd.DataFrame(data, columns=columns)
    return frames


def iss_json_to_single_frame(payload: dict[str, Any]) -> pd.DataFrame:
    frames = iss_json_to_frames(payload)
    if not frames:
        return pd.DataFrame()

    merged_parts: list[pd.DataFrame] = []
    for table_name, frame in frames.items():
        part = frame.copy()
        part.insert(0, "__table", table_name)
        merged_parts.append(part)
    return pd.concat(merged_parts, ignore_index=True, sort=False)


def default_endpoint_specs() -> list[BondEndpointSpec]:
    return [
        BondEndpointSpec("securities", "/iss/securities/{SECID}.json"),
        BondEndpointSpec("marketdata", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}.json"),
        BondEndpointSpec("history", "/iss/history/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}.json"),
        BondEndpointSpec("candles", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}/candles.json"),
        BondEndpointSpec("trades", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}/trades.json"),
        BondEndpointSpec("orderbook", "/iss/engines/stock/markets/bonds/boards/{BOARD}/securities/{SECID}/orderbook.json"),
        BondEndpointSpec("bondization", "/iss/statistics/engines/stock/markets/bonds/bondization/{SECID}.json"),
    ]


def default_endpoint_params(from_date: date, till_date: date, interval: int) -> dict[str, dict[str, Any]]:
    from_value = from_date.isoformat()
    till_value = till_date.isoformat()
    return {
        "securities": {},
        "marketdata": {},
        "history": {"from": from_value, "till": till_value},
        "candles": {"from": from_value, "till": till_value, "interval": interval},
        "trades": {"from": from_value, "till": till_value},
        "orderbook": {},
        "bondization": {
            "from": from_value,
            "iss.only": "coupons,amortizations,offers",
            "iss.meta": "off",
        },
    }
