from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import screen_basic, sync_moex_cashflows, sync_moex_universe


@dataclass(slots=True)
class StepResult:
    name: str
    code: int
    elapsed: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Локальный запуск bond_screener")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=PROJECT_ROOT,
        help="Базовая директория проекта (опция для продвинутого режима)",
    )
    parser.add_argument(
        "--skip-universe",
        action="store_true",
        help="Пропустить sync_moex_universe.py",
    )
    parser.add_argument(
        "--skip-cashflows",
        action="store_true",
        help="Пропустить sync_moex_cashflows.py",
    )
    parser.add_argument(
        "--skip-screen",
        action="store_true",
        help="Пропустить screen_basic.py",
    )
    return parser.parse_args()


def _run_step(name: str, func) -> StepResult:
    started = time.perf_counter()
    code = func()
    elapsed = time.perf_counter() - started
    return StepResult(name=name, code=code, elapsed=elapsed)


def main() -> int:
    args = parse_args()

    if args.base_dir.resolve() != PROJECT_ROOT:
        print(
            "Внимание: --base-dir сейчас не используется напрямую, "
            "скрипты запускаются из корня проекта.",
        )

    print("Этап 1/3: sync_moex_universe")
    results: list[StepResult] = []
    if not args.skip_universe:
        results.append(_run_step("sync_moex_universe", sync_moex_universe.main))

    print("Этап 2/3: sync_moex_cashflows")
    if not args.skip_cashflows:
        results.append(_run_step("sync_moex_cashflows", sync_moex_cashflows.main))

    print("Этап 3/3: screen_basic")
    if not args.skip_screen:
        results.append(_run_step("screen_basic", screen_basic.main))

    errors = sum(1 for result in results if result.code != 0)

    print("\nГотово.")
    for result in results:
        status = "OK" if result.code == 0 else "ERROR"
        print(f"- {result.name}: {status}, время={result.elapsed:.2f} сек.")
    print(f"Сводка: обработано этапов={len(results)}, отфильтровано=0, ошибок={errors}.")
    print(f"Время выполнения: {sum(item.elapsed for item in results):.2f} сек.")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
