from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from vibe.data_sources.moex_bonds_endpoints import (
    BOARD_FALLBACKS,
    MoexBondEndpointsClient,
    default_endpoint_params,
    default_endpoint_specs,
    iss_json_to_single_frame,
)
from vibe.ingest.moex_bond_rates import DEFAULT_OUT_XLSX
from vibe.storage.excel import write_workbook_multi_sheet_atomic
from vibe.utils.fs import ensure_parent_dir

logger = logging.getLogger(__name__)


@dataclass
class ProbeResult:
    output_dir: Path
    files_written: int
    total_isins: int


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
        try:
            payload = client.fetch_endpoint(isin=isin, board=board, spec=marketdata_spec, params={})
            frame = iss_json_to_single_frame(payload)
            if not frame.empty:
                logger.info("Fallback board selected for %s: %s", isin, board)
                return board
        except Exception:
            continue
    return BOARD_FALLBACKS[0]


def write_isin_workbook(
    isin: str,
    endpoint_frames_map: dict[str, pd.DataFrame],
    meta: dict[str, str],
    out_path: Path,
    max_rows_per_sheet: int = 200_000,
) -> None:
    normalized_sheets: dict[str, pd.DataFrame] = {}
    for sheet_name, frame in endpoint_frames_map.items():
        safe_name = sheet_name[:31]
        normalized_sheets[safe_name] = frame.head(max_rows_per_sheet)

    meta_df = pd.DataFrame([meta])
    write_workbook_multi_sheet_atomic(normalized_sheets, meta_df, out_path=out_path)


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
) -> ProbeResult:
    ensure_parent_dir(out_dir / "placeholder")

    endpoint_specs = default_endpoint_specs()
    params_map = default_endpoint_params(from_date=from_date, till_date=till_date, interval=interval)
    client = MoexBondEndpointsClient(timeout=timeout, retries=retries)

    files_written = 0
    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    source_snapshot = str(_load_latest_bond_rates_snapshot()[1])

    for isin in isins:
        endpoint_sheets: dict[str, pd.DataFrame] = {}
        ok: list[str] = []
        failed: list[str] = []

        try:
            board = client.resolve_board(isin)
        except Exception as exc:
            board = _choose_working_fallback_board(client, isin)
            failed.append(f"board_resolve={exc}")

        for spec in endpoint_specs:
            params = params_map.get(spec.name, {})
            try:
                payload = client.fetch_endpoint(isin=isin, board=board, spec=spec, params=params)
                if spec.name == "bondization" and "offers" not in str(payload).lower():
                    fallback_params = dict(params)
                    fallback_params["iss.only"] = "coupons,amortizations"
                    payload = client.fetch_endpoint(isin=isin, board=board, spec=spec, params=fallback_params)

                frame = iss_json_to_single_frame(payload)
                if spec.name == "bondization":
                    endpoint_sheets.update(_pick_bondization_frames(frame))
                else:
                    endpoint_sheets[spec.name] = frame
                ok.append(spec.name)
            except Exception as exc:
                failed.append(f"{spec.name}={exc}")
                if spec.name == "bondization":
                    for name in ["bondization_coupons", "bondization_amort", "bondization_offers"]:
                        endpoint_sheets.setdefault(name, pd.DataFrame())
                else:
                    endpoint_sheets.setdefault(spec.name, pd.DataFrame())

        out_path = out_dir / f"{isin}.xlsx"
        meta = {
            "isin": isin,
            "board": board,
            "run_date": run_date,
            "endpoints_ok": ",".join(ok),
            "endpoints_failed": "; ".join(failed),
            "source_snapshot": source_snapshot,
        }
        write_isin_workbook(
            isin=isin,
            endpoint_frames_map=endpoint_sheets,
            meta=meta,
            out_path=out_path,
            max_rows_per_sheet=max_rows_per_sheet,
        )
        files_written += 1

    return ProbeResult(output_dir=out_dir, files_written=files_written, total_isins=len(isins))


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

    return run_probe(
        isins=selected,
        out_dir=out_dir,
        from_date=from_date,
        till_date=till_date,
        interval=interval,
        timeout=timeout,
        retries=retries,
        max_rows_per_sheet=max_rows_per_sheet,
    )
