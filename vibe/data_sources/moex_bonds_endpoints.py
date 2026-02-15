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
PREFERRED_BONDS_BOARDS = ["TQCB", "TQOB", "TQOD", "TQIR", "TQOY"]


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
    content_type: str | None = None
    response_head: str | None = None
    error: str | None = None
    final_url: str | None = None
    headers_subset: dict[str, str] | None = None


def _extract_headers_subset(headers: dict[str, str]) -> dict[str, str]:
    wanted = {"content-type", "server", "location"}
    subset: dict[str, str] = {}
    for key, value in headers.items():
        key_lc = str(key).lower()
        if key_lc in wanted or key_lc.startswith("cf-"):
            subset[str(key)] = str(value)
    return subset


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
                    content_type=cached_meta.get("content_type"),
                    from_cache=True,
                    elapsed_ms=int(cached_meta.get("elapsed_ms", 0)),
                    url=url,
                    params=params or {},
                    response_head=cached_meta.get("response_head"),
                    error=cached_meta.get("error"),
                    final_url=cached_meta.get("final_url"),
                    headers_subset=cached_meta.get("headers_subset"),
                )
            except Exception as exc:
                logger.warning("Cache read failed for %s: %s", cache_path, exc)

        start = perf_counter()
        try:
            response = get_with_retries(url, timeout=self.timeout, retries=self.retries)
            elapsed_ms = int((perf_counter() - start) * 1000)
            content_type = response.headers.get("Content-Type")
            headers_subset = _extract_headers_subset(response.headers)
            final_url = getattr(response, "final_url", None) or getattr(response, "url", None) or url
            response_text = response.content.decode("utf-8", errors="replace")
            try:
                payload = json.loads(response_text)
            except json.JSONDecodeError as exc:
                response_head = response_text[:200]
                is_html = (content_type or "").lower().startswith("text/html") and "<html" in response_head.lower()
                error = "HTML_INSTEAD_OF_JSON" if is_html else f"invalid_json: {exc}"
                meta = FetchMeta(
                    status_code=response.status_code,
                    content_type=content_type,
                    from_cache=False,
                    elapsed_ms=elapsed_ms,
                    url=url,
                    params=params or {},
                    response_head=response_head,
                    error=error,
                    final_url=final_url,
                    headers_subset=headers_subset,
                )
                if self.use_cache and cache_path is not None:
                    ensure_parent_dir(cache_path)
                    cache_path.write_text(
                        json.dumps(
                            {
                                "payload": None,
                                "meta": {
                                    "endpoint_name": endpoint_name,
                                    "isin": isin,
                                    "url": url,
                                    "params": params or {},
                                    "status_code": meta.status_code,
                                    "content_type": meta.content_type,
                                    "elapsed_ms": meta.elapsed_ms,
                                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                                    "response_head": meta.response_head,
                                    "error": meta.error,
                                    "final_url": meta.final_url,
                                    "headers_subset": meta.headers_subset,
                                },
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                logger.warning(
                    "Fetch non-JSON response: endpoint=%s isin=%s status=%s content_type=%s url=%s final_url=%s response_head=%r",
                    endpoint_name,
                    isin,
                    response.status_code,
                    content_type,
                    url,
                    meta.final_url if meta.final_url and meta.final_url != url else "",
                    response_text[:200],
                )
                return None, meta

            meta = FetchMeta(
                status_code=response.status_code,
                content_type=content_type,
                from_cache=False,
                elapsed_ms=elapsed_ms,
                url=url,
                params=params or {},
                error=None,
                final_url=final_url,
                headers_subset=headers_subset,
            )
            if self.use_cache and cache_path is not None:
                ensure_parent_dir(cache_path)
                cache_path.write_text(
                    json.dumps(
                        {
                            "payload": payload,
                            "meta": {
                                "endpoint_name": endpoint_name,
                                "isin": isin,
                                "url": url,
                                "params": params or {},
                                "status_code": meta.status_code,
                                "content_type": meta.content_type,
                                "elapsed_ms": meta.elapsed_ms,
                                "fetched_at": datetime.now(timezone.utc).isoformat(),
                                "response_head": meta.response_head,
                                "error": meta.error,
                                "final_url": meta.final_url,
                                "headers_subset": meta.headers_subset,
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
                content_type=None,
                from_cache=False,
                elapsed_ms=elapsed_ms,
                url=url,
                params=params or {},
                response_head=None,
                error=error,
                final_url=None,
                headers_subset=None,
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

        def _pick_board(frame: pd.DataFrame, *, bonds_only: bool) -> str | None:
            working = frame.copy()
            engine_series = _value(working, "ENGINE").astype("string").fillna("").str.lower()
            market_series = _value(working, "MARKET").astype("string").fillna("").str.lower()
            traded_series = pd.to_numeric(_value(working, "IS_TRADED"), errors="coerce")
            has_is_traded = normalized.get("IS_TRADED") is not None

            mask = engine_series.eq("stock")
            if bonds_only:
                mask = mask & market_series.eq("bonds")
            if has_is_traded:
                mask = mask & traded_series.eq(1)

            working = working[mask]
            if working.empty:
                return None

            primary_flags = ["IS_PRIMARY", "PRIMARY", "IS_DEFAULT"]
            primary_mask = pd.Series([False] * len(working), index=working.index)
            for key in primary_flags:
                series = _value(working, key).astype("string").fillna("0")
                primary_mask = primary_mask | series.isin(["1", "True", "true"])

            if primary_mask.any():
                primary = working[primary_mask]
                if not primary.empty:
                    working = primary

            board_col = normalized.get("BOARDID") or normalized.get("BOARD")
            if board_col is None or working.empty:
                return None

            boards = working[board_col].astype("string").fillna("")
            for preferred_board in PREFERRED_BONDS_BOARDS:
                matched = working[boards.eq(preferred_board)]
                if not matched.empty:
                    return str(matched.iloc[0][board_col])

            return str(working.iloc[0][board_col])

        board = _pick_board(boards_df, bonds_only=True)
        if board:
            return board

        logger.warning("resolve_board: no bonds boards found for %s, using fallback behavior", isin)
        board = _pick_board(boards_df, bonds_only=False)
        if board:
            return board

        board_col = normalized.get("BOARDID") or normalized.get("BOARD")
        if board_col is None:
            logger.warning("resolve_board: no bonds boards found for %s, using fallback behavior", isin)
            return BOARD_FALLBACKS[0]
        if boards_df.empty:
            return BOARD_FALLBACKS[0]
        return str(boards_df.iloc[0][board_col])

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
