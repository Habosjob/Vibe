from __future__ import annotations

import json
import logging
import random
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from vibe.data_sources.moex_bonds_endpoints import (
    BOARD_FALLBACKS,
    FetchMeta,
    MoexBondEndpointsClient,
    default_endpoint_params,
    default_endpoint_specs,
    iss_json_to_frames,
    iss_json_to_single_frame,
)
from vibe.ingest.moex_bond_rates import DEFAULT_OUT_XLSX
from vibe.utils.fs import atomic_replace_with_retry, ensure_parent_dir
from vibe.utils.retention import cleanup_old_dirs

logger = logging.getLogger(__name__)


@dataclass
class ProbeResult:
    output_dir: Path
    files_written: int
    total_isins: int
    orderbook_blocked_html: int = 0


def pick_isins(
    bond_rates_df: pd.DataFrame,
    *,
    n_static: int = 10,
    n_random: int = 10,
    seed: int | None = None,
) -> tuple[list[str], list[str]]:
    id_col = "ISIN" if "ISIN" in bond_rates_df.columns else "SECID"
    values = sorted({str(v).strip() for v in bond_rates_df[id_col].dropna() if str(v).strip()})

    static = values[: min(n_static, len(values))]
    remaining = [value for value in values if value not in static]

    random_count = min(n_random, len(remaining))
    if random_count == 0:
        return static, []

    rng = random.Random(seed)
    return static, sorted(rng.sample(remaining, random_count))


def _load_latest_bond_rates_snapshot() -> tuple[pd.DataFrame, Path]:
    parquet_candidates = sorted(DEFAULT_OUT_XLSX.parent.glob(f"{DEFAULT_OUT_XLSX.stem}_*.parquet"))
    if parquet_candidates:
        latest = parquet_candidates[-1]
        return pd.read_parquet(latest), latest
    return pd.read_excel(DEFAULT_OUT_XLSX, sheet_name="rates"), DEFAULT_OUT_XLSX


def _pick_bondization_frames(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if frame.empty:
        return {
            "bondization_coupons": frame.copy(),
            "bondization_amort": frame.copy(),
            "bondization_offers": frame.copy(),
        }

    if "__table" not in frame.columns:
        return {"bondization": frame}

    names_map = {
        "coupons": "bondization_coupons",
        "amortizations": "bondization_amort",
        "offers": "bondization_offers",
    }

    result: dict[str, pd.DataFrame] = {}
    for table, sheet_name in names_map.items():
        result[sheet_name] = frame[frame["__table"].astype(str).str.lower().eq(table)].copy()
    return result


def _choose_working_fallback_board(client: MoexBondEndpointsClient, isin: str) -> str:
    marketdata_spec = next(spec for spec in default_endpoint_specs() if spec.name == "marketdata")
    for board in BOARD_FALLBACKS:
        payload, _meta = client.fetch_endpoint(isin=isin, board=board, spec=marketdata_spec, params={})
        if not payload:
            continue
        frame = iss_json_to_single_frame(payload)
        if not frame.empty:
            logger.info("Fallback board selected for %s: %s", isin, board)
            return board
    return BOARD_FALLBACKS[0]

def _pick_first_value(frame: pd.DataFrame, candidates: list[str]) -> Any | None:
    normalized = {str(col).upper(): col for col in frame.columns}
    if frame.empty:
        return None
    row = frame.iloc[0]
    for candidate in candidates:
        column = normalized.get(candidate.upper())
        if column is None:
            continue
        value = row.get(column)
        if pd.notna(value):
            return value
    return None


def _extract_top_of_book_from_marketdata(frame: pd.DataFrame) -> dict[str, Any] | None:
    if frame.empty:
        return None
    bestbid = _pick_first_value(frame, ["BESTBID", "BID", "BIDPRICE"])
    bestoffer = _pick_first_value(frame, ["BESTOFFER", "OFFER", "OFFERPRICE"])
    if bestbid is None and bestoffer is None:
        return None

    spread: float | None = None
    bid_value = pd.to_numeric(pd.Series([bestbid]), errors="coerce").iloc[0]
    offer_value = pd.to_numeric(pd.Series([bestoffer]), errors="coerce").iloc[0]
    if pd.notna(bid_value) and pd.notna(offer_value):
        spread = float(offer_value - bid_value)

    return {
        "bestbid": bestbid,
        "bestoffer": bestoffer,
        "spread": spread,
        "biddepth": _pick_first_value(frame, ["BIDDEPTH", "NUMBIDS", "BIDQTY"]),
        "offerdepth": _pick_first_value(frame, ["OFFERDEPTH", "NUMOFFERS", "OFFERQTY"]),
    }


def _build_orderbook_fallback_frame(top_of_book: dict[str, Any]) -> pd.DataFrame:
    row = dict(top_of_book)
    row["top_of_book_source"] = "marketdata_fallback"
    return pd.DataFrame([row])


def build_probe_summary_df(
    *,
    meta: FetchMeta,
    payload: dict[str, Any] | None,
    board: str,
    from_date: date,
    till_date: date,
    interval: int,
    status_override: str | None = None,
    reason_override: str | None = None,
) -> pd.DataFrame:
    tables = []
    rows_by_table: dict[str, int] = {}
    if payload:
        tables_payload = iss_json_to_frames(payload)
        tables = sorted(tables_payload.keys())
        rows_by_table = {name: len(df) for name, df in tables_payload.items()}

    if status_override:
        status = status_override
        reason = reason_override or ""
    elif meta.error:
        status = "ERROR"
        reason = "request_error"
    elif not payload:
        status = "NO_DATA"
        reason = "no_tables_in_payload"
    elif not tables:
        status = "NO_DATA"
        reason = "no_tables_in_payload"
    elif all(rows == 0 for rows in rows_by_table.values()):
        status = "NO_DATA"
        reason = "empty_table"
    else:
        status = "OK"
        reason = ""

    return pd.DataFrame(
        [
            {
                "__status": status,
                "reason": reason,
                "http_status": meta.status_code,
                "content_type": meta.content_type or "",
                "from_cache": meta.from_cache,
                "elapsed_ms": meta.elapsed_ms,
                "tables_returned": ",".join(tables),
                "rows_by_table": json.dumps(rows_by_table, ensure_ascii=False, sort_keys=True),
                "board": board,
                "params_from": from_date.isoformat(),
                "params_till": till_date.isoformat(),
                "interval": interval,
                "params": json.dumps(meta.params, ensure_ascii=False, sort_keys=True),
                "error": meta.error or "",
                "response_head": (meta.response_head or "")[:200],
                "final_url": meta.final_url or "",
                "headers_subset": json.dumps(meta.headers_subset or {}, ensure_ascii=False, sort_keys=True),
            }
        ]
    )


def write_isin_workbook(
    isin: str,
    endpoint_frames_map: dict[str, pd.DataFrame],
    endpoint_summaries_map: dict[str, pd.DataFrame],
    meta: dict[str, str],
    out_path: Path,
    max_rows_per_sheet: int = 200_000,
) -> None:
    ensure_parent_dir(out_path)

    with tempfile.NamedTemporaryFile(dir=out_path.parent, suffix=".xlsx", delete=False) as tmp:
        temp_path = Path(tmp.name)

    try:
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            for sheet_name, frame in endpoint_frames_map.items():
                safe_name = sheet_name[:31]
                summary_df = endpoint_summaries_map[sheet_name]
                summary_df.to_excel(writer, sheet_name=safe_name, index=False)

                data_df = frame.head(max_rows_per_sheet)
                if not data_df.empty:
                    data_df.to_excel(writer, sheet_name=safe_name, index=False, startrow=len(summary_df) + 2)

            pd.DataFrame([meta]).to_excel(writer, sheet_name="meta", index=False)
        atomic_replace_with_retry(temp_path, out_path)
    finally:
        if temp_path.exists() and temp_path != out_path:
            temp_path.unlink(missing_ok=True)


def run_probe(
    isins: list[str],
    out_dir: Path,
    from_date: date,
    till_date: date,
    interval: int,
    *,
    timeout: int = 30,
    retries: int = 3,
    max_rows_per_sheet: int = 200_000,
    cache_dir: Path | None = None,
    use_cache: bool = True,
    max_workers: int = 4,
) -> ProbeResult:
    ensure_parent_dir(out_dir / "placeholder")
    cleanup_old_dirs(Path("data/curated/moex/endpoints_probe"), keep_days=7)
    logger.info(
        "Probe started: isins=%s out_dir=%s from=%s till=%s interval=%s",
        len(isins),
        out_dir,
        from_date,
        till_date,
        interval,
    )

    endpoint_specs = default_endpoint_specs()
    params_map = default_endpoint_params(from_date=from_date, till_date=till_date, interval=interval)
    files_written = 0
    orderbook_blocked_html = 0
    counter_lock = threading.Lock()
    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    source_snapshot = str(_load_latest_bond_rates_snapshot()[1])

    def _process_isin(isin: str) -> int:
        nonlocal orderbook_blocked_html
        endpoint_sheets: dict[str, pd.DataFrame] = {}
        endpoint_summaries: dict[str, pd.DataFrame] = {}
        ok: list[str] = []
        failed: list[str] = []
        orderbook_status = "ok"
        marketdata_top_of_book: dict[str, Any] | None = None
        orderbook_fallback_pending = False
        client = MoexBondEndpointsClient(timeout=timeout, retries=retries, cache_dir=cache_dir, use_cache=use_cache)

        try:
            board = client.resolve_board(isin)
            logger.info("Resolved board for %s: %s", isin, board)
        except Exception as exc:
            board = _choose_working_fallback_board(client, isin)
            failed.append(f"board_resolve={exc}")
            logger.warning("Board resolve failed for %s, fallback board: %s", isin, board)

        for spec in endpoint_specs:
            worker_name = threading.current_thread().name
            params = params_map.get(spec.name, {})
            payload, fetch_meta = client.fetch_endpoint(isin=isin, board=board, spec=spec, params=params)
            if spec.name == "bondization" and payload and "offers" not in str(payload).lower():
                fallback_params = dict(params)
                fallback_params["iss.only"] = "coupons,amortizations"
                payload, fetch_meta = client.fetch_endpoint(
                    isin=isin,
                    board=board,
                    spec=spec,
                    params=fallback_params,
                )

            frame = iss_json_to_single_frame(payload or {})
            status_override: str | None = None
            reason_override: str | None = None

            if spec.name == "marketdata" and payload and not frame.empty:
                marketdata_top_of_book = _extract_top_of_book_from_marketdata(frame)
                if orderbook_fallback_pending and marketdata_top_of_book:
                    endpoint_sheets["orderbook"] = _build_orderbook_fallback_frame(marketdata_top_of_book)
                    orderbook_fallback_pending = False
                    logger.info("Orderbook fallback from marketdata applied (deferred): isin=%s", isin)

            if spec.name == "orderbook" and fetch_meta.error == "HTML_INSTEAD_OF_JSON":
                orderbook_status = "blocked_html"
                status_override = "BLOCKED_HTML"
                reason_override = "html_instead_of_json"
                if marketdata_top_of_book:
                    frame = _build_orderbook_fallback_frame(marketdata_top_of_book)
                    logger.info("Orderbook fallback from marketdata applied: isin=%s", isin)
                else:
                    orderbook_fallback_pending = True

            summary_df = build_probe_summary_df(
                meta=fetch_meta,
                payload=payload,
                board=board,
                from_date=from_date,
                till_date=till_date,
                interval=interval,
                status_override=status_override,
                reason_override=reason_override,
            )

            if spec.name == "bondization":
                for sheet_name, split_frame in _pick_bondization_frames(frame).items():
                    endpoint_sheets[sheet_name] = split_frame
                    endpoint_summaries[sheet_name] = summary_df.copy()
            else:
                endpoint_sheets[spec.name] = frame
                endpoint_summaries[spec.name] = summary_df

            if fetch_meta.error:
                failed.append(f"{spec.name}={fetch_meta.error}")
            else:
                ok.append(spec.name)

            logger.info(
                "Probe worker=%s isin=%s endpoint=%s status=%s cache_hit=%s rows=%s",
                worker_name,
                isin,
                spec.name,
                fetch_meta.status_code,
                fetch_meta.from_cache,
                len(frame),
            )

        if orderbook_status == "blocked_html":
            with counter_lock:
                orderbook_blocked_html += 1

        out_path = out_dir / f"{isin}.xlsx"
        meta = {
            "isin": isin,
            "board": board,
            "run_date": run_date,
            "endpoints_ok": ",".join(ok),
            "endpoints_failed": "; ".join(failed),
            "orderbook_status": orderbook_status,
            "source_snapshot": source_snapshot,
        }
        write_isin_workbook(
            isin=isin,
            endpoint_frames_map=endpoint_sheets,
            endpoint_summaries_map=endpoint_summaries,
            meta=meta,
            out_path=out_path,
            max_rows_per_sheet=max_rows_per_sheet,
        )
        return 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for written in executor.map(_process_isin, isins):
            files_written += written

    cleanup_old_dirs(Path("data/curated/moex/endpoints_probe"), keep_days=7)
    logger.info("Probe counters: orderbook_blocked_html=%s", orderbook_blocked_html)

    return ProbeResult(
        output_dir=out_dir,
        files_written=files_written,
        total_isins=len(isins),
        orderbook_blocked_html=orderbook_blocked_html,
    )


def run_probe_for_latest_bond_rates(
    *,
    n_static: int = 10,
    n_random: int = 10,
    from_date: date | None = None,
    till_date: date | None = None,
    interval: int = 24,
    out_dir: Path | None = None,
    seed: int | None = None,
    timeout: int = 30,
    retries: int = 3,
    max_rows_per_sheet: int = 200_000,
    cache_dir: Path | None = None,
    use_cache: bool = True,
) -> ProbeResult:
    today = datetime.now(timezone.utc).date()
    from_date = from_date or (today - timedelta(days=30))
    till_date = till_date or today
    seed = seed if seed is not None else int(today.strftime("%Y%m%d"))

    rates_df, _snapshot = _load_latest_bond_rates_snapshot()
    static_isins, random_isins = pick_isins(rates_df, n_static=n_static, n_random=n_random, seed=seed)
    selected = static_isins + random_isins

    if out_dir is None:
        out_dir = Path("data/curated/moex/endpoints_probe") / today.strftime("%Y%m%d")

    if cache_dir is None:
        cache_dir = Path("data/cache/moex_iss/endpoint_probe") / today.strftime("%Y%m%d")

    return run_probe(
        isins=selected,
        out_dir=out_dir,
        from_date=from_date,
        till_date=till_date,
        interval=interval,
        timeout=timeout,
        retries=retries,
        max_rows_per_sheet=max_rows_per_sheet,
        cache_dir=cache_dir,
        use_cache=use_cache,
    )
