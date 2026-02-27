#!/usr/bin/env python3
"""Общий запуск пайплайна: Moex_Bonds -> Python_Sorter."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path("config/moex_bonds.yaml")


class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    CYAN = "\033[36m"


class ConsoleProgress:
    def __init__(self, total_steps: int) -> None:
        self.total_steps = total_steps
        self.width = 28

    def update(self, step: int, message: str) -> None:
        ratio = max(0.0, min(1.0, step / self.total_steps))
        filled = int(self.width * ratio)
        bar = f"{Ansi.GREEN}{'█' * filled}{Ansi.DIM}{'-' * (self.width - filled)}{Ansi.RESET}"
        pct = int(ratio * 100)
        print(
            f"\r{Ansi.BOLD}[{bar}{Ansi.BOLD}] {step:>2}/{self.total_steps} ({pct:>3}%) {Ansi.CYAN}{message:60}{Ansi.RESET}",
            end="",
            flush=True,
        )
        if step == self.total_steps:
            print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Запуск полного пайплайна по облигациям")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к YAML-конфигу")
    return parser.parse_args()


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("MainPipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def resolve_pipeline_log(config_path: Path) -> Path:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        return Path("logs/main.log")
    path = loaded.get("pipeline", {}).get("logging", {}).get("path", "logs/main.log")
    return Path(str(path))


def run_step(label: str, command: list[str], logger: logging.Logger) -> None:
    logger.info("Запуск шага '%s': %s", label, " ".join(command))
    started = time.perf_counter()
    result = subprocess.run(command, check=False)
    elapsed = time.perf_counter() - started
    if result.returncode != 0:
        logger.error("Шаг '%s' завершился с кодом %s за %.2f сек", label, result.returncode, elapsed)
        raise RuntimeError(f"Шаг '{label}' завершился с кодом {result.returncode}")
    logger.info("Шаг '%s' выполнен успешно за %.2f сек", label, elapsed)


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    progress = ConsoleProgress(total_steps=3)

    if not config_path.exists():
        print(f"{Ansi.RED}Файл конфига не найден: {config_path}{Ansi.RESET}")
        return 1

    logger = build_logger(resolve_pipeline_log(config_path))
    run_started = time.perf_counter()

    try:
        progress.update(1, "Шаг 1/2: запуск Moex_Bonds.py")
        run_step("Moex_Bonds", [sys.executable, "Moex_Bonds.py", "--config", str(config_path)], logger)

        progress.update(2, "Шаг 2/2: запуск Python_Sorter.py")
        run_step("Python_Sorter", [sys.executable, "Python_Sorter.py", "--config", str(config_path)], logger)

        elapsed = time.perf_counter() - run_started
        progress.update(3, f"Пайплайн завершен за {elapsed:0.1f}с")
        logger.info("Полный пайплайн завершен успешно за %.2f сек", elapsed)
        print(f"{Ansi.GREEN}Пайплайн завершен. Лог: {resolve_pipeline_log(config_path)}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка пайплайна: %s", exc)
        print(f"\n{Ansi.RED}Ошибка пайплайна. См. лог: {resolve_pipeline_log(config_path)}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

