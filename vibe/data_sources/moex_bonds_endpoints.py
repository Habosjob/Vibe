from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlencode

import pandas as pd

from vibe.utils.fs import ensure_parent_dir
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


@dataclass(frozen=True)
class FetchMeta:
    status_code: int | None
    from_cache: bool
    elapsed_ms: int
    url: str
    params: dict[str, Any]
    error: str | None = None


def build_probe_cache_key(*, isin: str, endpoint_name: str, url: str, params: dict[str, Any] | None) -> str:
    raw = json.dumps(
        {
            "isin": isin,
            "endpoint_name": endpoint_name,
            "url": url,
            "params": params or {},
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class MoexBondEndpointsClient:
    def __init__(
        self,
        *,
        timeout: int = 30,
        retries: int = 3,
        cache_dir: Path | None = None,
        use_cache: bool = True,
    ):
        self.timeout = timeout
        self.retries = retries
        self.cache_dir = cache_dir
        self.use_cache = use_cache

    def fetch_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        payload, meta = self._fetch_json_with_meta(path=path, params=params)
        if payload is None:
            raise RuntimeError(meta.error or f"Failed to fetch {meta.url}")
        return payload

    def _build_url(self, path: str, params: dict[str, Any] | None = None) -> str:
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{MOEX_ISS_BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"
        return url

    def _cache_file_path(self, *, isin: str, endpoint_name: str, url: str, params: dict[str, Any] | None) -> Path | None:
        if not self.cache_dir:
            return None
        cache_key = build_probe_cache_key(isin=isin, endpoint_name=endpoint_name, url=url, params=params)
        return self.cache_dir / f"{cache_key}.json"

    def _fetch_json_with_meta(
        self,
        *,
        path: str,
        params: dict[str, Any] | None = None,
        isin: str = "",
        endpoint_name: str = "generic",
    ) -> tuple[dict[str, Any] | None, FetchMeta]:
        url = self._build_url(path=path, params=params)
        cache_path = self._cache_file_path(isin=isin, endpoint_name=endpoint_name, url=url, params=params)

        if self.use_cache and cache_path and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                payload = cached.get("payload")
                cached_meta = cached.get("meta", {})
                logger.info("Cache hit: endpoint=%s isin=%s url=%s", endpoint_name, isin, url)
                return payload, FetchMeta(
                    status_code=cached_meta.get("status_code"),
                    from_cache=True,
                    elapsed_ms=int(cached_meta.get("elapsed_ms", 0)),
                    url=url,
                    params=params or {},
                    error=cached_meta.get("error"),
                )
            except Exception as exc:
                logger.warning("Cache read failed for %s: %s", cache_path, exc)

        start = perf_counter()
        try:
            response = get_with_retries(url, timeout=self.timeout, retries=self.retries)
            payload = json.loads(response.content.decode("utf-8"))
            elapsed_ms = int((perf_counter() - start) * 1000)
            meta = FetchMeta(
                status_code=response.status_code,
                from_cache=False,
                elapsed_ms=elapsed_ms,
                url=url,
                params=params or {},
                error=None,
            )
            if self.use_cache and cache_path is not None:
                ensure_parent_dir(cache_path)
                cache_path.write_text(
                    json.dumps(
                        {
                            "payload": payload,
                            "meta": {
                                "status_code": meta.status_code,
                                "elapsed_ms": meta.elapsed_ms,
                                "fetched_at": datetime.now(timezone.utc).isoformat(),
                                "error": meta.error,
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
            logger.info(
                "Fetched endpoint=%s isin=%s status=%s elapsed_ms=%s",
                endpoint_name,
                isin,
                response.status_code,
                elapsed_ms,
            )
            return payload, meta
        except Exception as exc:
            elapsed_ms = int((perf_counter() - start) * 1000)
            error = str(exc)
            logger.error("Fetch failed: endpoint=%s isin=%s error=%s", endpoint_name, isin, error)
            return None, FetchMeta(
                status_code=None,
                from_cache=False,
                elapsed_ms=elapsed_ms,
                url=url,
                params=params or {},
                error=error,
            )

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
        is_traded = pd.to_numeric(_value(filtered, "IS_TRADED"), errors="coerce").fillna(0).astype(int)
        filtered = filtered[
            _value(filtered, "ENGINE").astype(str).str.lower().eq("stock")
            & _value(filtered, "MARKET").astype(str).str.lower().eq("bonds")
            & is_traded.eq(1)
        ]

        if filtered.empty:
            filtered = boards_df

        primary_flags = ["IS_PRIMARY", "PRIMARY", "IS_DEFAULT"]
        primary_mask = pd.Series([False] * len(filtered), index=filtered.index)
        for key in primary_flags:
            series = _value(filtered, key).astype("string").fillna("0")
            primary_mask = primary_mask | series.isin(["1", "True", "true"])

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
    ) -> tuple[dict[str, Any] | None, FetchMeta]:
        path = spec.build_url(isin=isin, board=board)
        return self._fetch_json_with_meta(path=path, params=params, isin=isin, endpoint_name=spec.name)


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
        part = frame.copy().dropna(axis=1, how="all")
        if part.empty:
            continue
        part.insert(0, "__table", table_name)
        merged_parts.append(part)
    if not merged_parts:
        return pd.DataFrame()
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
