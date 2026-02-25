"""CLI-отчет по последним запускам скринера из SQLite-таблицы runs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from moex_bond_screener.config import load_config
from moex_bond_screener.state_store import ScreenerStateStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Показать последние запуски из state/runs")
    parser.add_argument("--limit", type=int, default=10, help="Сколько запусков показать (по умолчанию: 10)")
    parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Формат вывода: table или json",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    config = load_config()
    store = ScreenerStateStore(
        config.exclusions_state_dir,
        storage_backend=config.storage_backend,
        sqlite_db_path=config.sqlite_db_path,
    )

    rows = store.load_runs(limit=args.limit)
    if config.storage_backend != "sqlite":
        print("История запусков доступна только для storage_backend=sqlite")
        return

    if args.format == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return

    if not rows:
        print("История запусков пуста.")
        return

    print("id | started_at | elapsed_s | processed | filtered | errors | backend")
    print("-" * 86)
    for row in rows:
        print(
            f"{row['id']} | {row['started_at']} | {row['elapsed_seconds']:.2f} | "
            f"{row['bonds_processed']} | {row['bonds_filtered']} | {row['errors_count']} | {row['backend']}"
        )


if __name__ == "__main__":
    main()
