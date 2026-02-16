from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from vibe.config import (
    DEFAULT_HTTP_RETRIES,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    DEFAULT_MOEX_RATES_URL,
)
from vibe.ingest.moex_bond_rates import (
    DEFAULT_KEEP_ID,
    DEFAULT_MAX_PRINT,
    run_moex_bond_rates_ingest,
)
from vibe.ingest.moex_bonds_endpoints_probe import run_probe_for_latest_bond_rates
from vibe.utils.logging import setup_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vibe")
    subparsers = parser.add_subparsers(dest="command", required=True)

    moex = subparsers.add_parser("moex-bond-rates", help="Download MOEX bond rates CSV and save Excel")
    moex.add_argument("--out", type=Path, default=None, help="Optional output xlsx path")
    moex.add_argument("--raw", type=Path, default=None, help="Optional raw csv path or directory")
    moex.add_argument("--url", default=DEFAULT_MOEX_RATES_URL)
    moex.add_argument("--timeout", type=int, default=DEFAULT_HTTP_TIMEOUT_SECONDS)
    moex.add_argument("--retries", type=int, default=DEFAULT_HTTP_RETRIES)
    moex.add_argument("--max-print", type=int, default=DEFAULT_MAX_PRINT)
    moex.add_argument("--keep-id", choices=["ISIN", "SECID"], default=DEFAULT_KEEP_ID)
    moex.add_argument("--keep-days", type=int, default=7)
    moex.add_argument("--no-cache", action="store_true", help="Bypass daily parquet cache and force download")

    probe = subparsers.add_parser(
        "moex-bond-endpoints-probe",
        help="Probe MOEX ISS bond endpoints for selected ISINs and save one workbook per ISIN",
    )
    today = datetime.now(timezone.utc).date()
    probe.add_argument("--n-static", type=int, default=10)
    probe.add_argument("--n-random", type=int, default=10)
    probe.add_argument("--from", dest="from_date", default=(today - timedelta(days=30)).isoformat())
    probe.add_argument("--till", dest="till_date", default=today.isoformat())
    probe.add_argument("--interval", type=int, default=24)
    probe.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/curated/moex/endpoints_probe") / today.strftime("%Y%m%d"),
    )
    probe.add_argument("--seed", type=int, default=int(today.strftime("%Y%m%d")))
    probe.add_argument("--max-rows-per-sheet", type=int, default=200_000)
    probe.add_argument("--keep-days", type=int, default=7)
    probe.add_argument("--timeout", type=int, default=DEFAULT_HTTP_TIMEOUT_SECONDS)
    probe.add_argument("--retries", type=int, default=DEFAULT_HTTP_RETRIES)
    probe.add_argument(
        "--log-file",
        type=Path,
        default=Path("logs") / f"moex_endpoints_probe_{today.strftime('%Y%m%d')}.log",
    )
    probe.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/cache/moex_iss/endpoint_probe") / today.strftime("%Y%m%d"),
    )
    probe.add_argument(
        "--no-cache",
        action="store_true",
        help="Do not read endpoint probe responses from cache (responses are still captured when --capture is enabled)",
    )
    probe.add_argument("--capture", dest="capture", action="store_true", default=True, help="Save raw endpoint responses for diagnostics")
    probe.add_argument("--no-capture", dest="capture", action="store_false", help="Do not save raw endpoint responses")

    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging(log_path=None, level="INFO")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "moex-bond-rates":
        try:
            result = run_moex_bond_rates_ingest(
                out_xlsx=args.out,
                raw_csv=args.raw,
                url=args.url,
                timeout=args.timeout,
                retries=args.retries,
                no_cache=args.no_cache,
                max_print=args.max_print,
                keep_id=args.keep_id,
                keep_days=args.keep_days,
            )
            logging.info(
                "Ingest complete: out=%s raw=%s rows=%s cols=%s",
                result.out_xlsx,
                result.raw_csv,
                result.rows,
                result.cols,
            )
            return 0
        except Exception as exc:
            logging.error("MOEX bond rates ingest failed: %s", exc)
            return 1

    if args.command == "moex-bond-endpoints-probe":
        try:
            setup_logging(log_path=args.log_file, level="INFO")
            result = run_probe_for_latest_bond_rates(
                n_static=args.n_static,
                n_random=args.n_random,
                from_date=datetime.fromisoformat(args.from_date).date(),
                till_date=datetime.fromisoformat(args.till_date).date(),
                interval=args.interval,
                out_dir=args.out_dir,
                seed=args.seed,
                timeout=args.timeout,
                retries=args.retries,
                max_rows_per_sheet=args.max_rows_per_sheet,
                cache_dir=args.cache_dir,
                use_cache=not args.no_cache,
                capture=args.capture,
                keep_days=args.keep_days,
            )
            logging.info(
                "Probe complete: out_dir=%s run_dir=%s files_written=%s total_isins=%s endpoints_checked=%s successful_endpoints=%s rows_effective_total=%s orderbook_blocked_html=%s results_xlsx=%s results_json=%s results_csv=%s",
                result.output_dir,
                result.run_dir,
                result.files_written,
                result.total_isins,
                result.endpoints_checked,
                result.successful_endpoints,
                result.rows_effective_total,
                result.orderbook_blocked_html,
                result.results_xlsx,
                result.results_json,
                result.results_csv,
            )
            if result.rows_effective_total == 0:
                logging.warning(
                    "Probe produced 0 effective rows: endpoints_checked=%s successful=%s http_errors=%s parse_errors=%s filtered_out=%s artifacts=%s",
                    result.endpoints_checked,
                    result.successful_endpoints,
                    result.http_errors,
                    result.parse_errors,
                    result.filtered_out,
                    result.run_dir,
                )
            return 0
        except Exception:
            logging.exception("MOEX endpoints probe failed")
            return 1
        finally:
            logging.shutdown()

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
