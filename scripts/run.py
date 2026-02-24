from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bond_screener.runtime import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Локальный запуск bond_screener")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=PROJECT_ROOT,
        help="Базовая директория проекта (опция для продвинутого режима)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = args.base_dir.resolve()

    summary, elapsed = run(base_dir)
    print("\nГотово.")
    print(
        "Сводка: обработано бумаг={processed}, отфильтровано={filtered}, ошибок={errors}.".format(
            processed=summary.processed,
            filtered=summary.filtered,
            errors=summary.errors,
        )
    )
    print(f"Время выполнения: {elapsed:.2f} сек.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
