from __future__ import annotations

import argparse
import logging
import sys
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
    moex.add_argument("--no-cache", action="store_true", help="Bypass daily parquet cache and force download")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
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

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
