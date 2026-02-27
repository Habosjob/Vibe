#!/usr/bin/env python3
"""Скачивание таблицы Анализа облигаций с dohod.ru через Playwright."""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("config/moex_bonds.yaml")


class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"


class ConsoleProgress:
    """Интерактивный прогресс-бар со спиннером для долгих операций."""

    def __init__(self, total_steps: int) -> None:
        self.total_steps = total_steps
        self.width = 28
        self.spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.spin_idx = 0

    def update(self, step: int, message: str) -> None:
        ratio = max(0.0, min(1.0, step / self.total_steps))
        filled = int(self.width * ratio)
        bar = f"{Ansi.GREEN}{'█' * filled}{Ansi.DIM}{'-' * (self.width - filled)}{Ansi.RESET}"
        pct = int(ratio * 100)
        print(
            f"\r{Ansi.BOLD}[{bar}{Ansi.BOLD}] {step:>2}/{self.total_steps} ({pct:>3}%) {Ansi.CYAN}{message:64}{Ansi.RESET}",
            end="",
            flush=True,
        )
        if step == self.total_steps:
            print()

    def pulse(self, message: str) -> None:
        spin = self.spinner[self.spin_idx % len(self.spinner)]
        self.spin_idx += 1
        print(f"\r{Ansi.CYAN}{spin}{Ansi.RESET} {message:95}", end="", flush=True)

    @staticmethod
    def done_line() -> None:
        print()


@dataclass
class DohodConfig:
    enabled: bool
    page_url: str
    download_button_text: str
    output_excel_path: Path
    timeout_sec: int
    download_timeout_sec: int
    headless: bool
    log_path: Path
    state_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Скачать excel-таблицу Анализ облигаций с dohod.ru")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к YAML-конфигу")
    return parser.parse_args()


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def load_config(path: Path) -> DohodConfig:
    if not path.exists():
        raise FileNotFoundError(f"Не найден YAML-конфиг: {path}")

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError("YAML-конфиг должен быть словарём (mapping).")

    output_path = Path(str(_deep_get(loaded, "dohod", "output", "excel_path", default="Dohod_Bonds.xlsx")))

    return DohodConfig(
        enabled=bool(_deep_get(loaded, "dohod", "enabled", default=True)),
        page_url=str(_deep_get(loaded, "dohod", "source", "url", default="https://www.dohod.ru/analytic/bonds")),
        download_button_text=str(_deep_get(loaded, "dohod", "source", "download_button_text", default="СКАЧАТЬ EXCEL")),
        output_excel_path=output_path,
        timeout_sec=int(_deep_get(loaded, "dohod", "network", "timeout_sec", default=90)),
        download_timeout_sec=int(_deep_get(loaded, "dohod", "network", "download_timeout_sec", default=120)),
        headless=bool(_deep_get(loaded, "dohod", "browser", "headless", default=True)),
        log_path=Path(str(_deep_get(loaded, "dohod", "logging", "path", default="logs/DOHOD_Bonds.log"))),
        state_path=Path(str(_deep_get(loaded, "dohod", "cache", "state_path", default="logs/cache/dohod_bonds_state.json"))),
    )


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("DOHOD_Bonds")
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


def _already_downloaded_today(output_path: Path) -> bool:
    if not output_path.exists():
        return False
    file_day = datetime.fromtimestamp(output_path.stat().st_mtime).date()
    return file_day == datetime.now().date()


def _save_state(config: DohodConfig, payload: dict[str, Any], logger: logging.Logger) -> None:
    config.state_path.parent.mkdir(parents=True, exist_ok=True)
    config.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Состояние обновлено: %s", config.state_path)


def _wait_for_button(page: Any, label: str, timeout_sec: int, progress: ConsoleProgress, logger: logging.Logger) -> Any:
    locator = page.get_by_text(label, exact=False)
    started = time.perf_counter()

    while (time.perf_counter() - started) <= timeout_sec:
        if locator.count() > 0 and locator.first.is_visible():
            logger.info("Кнопка '%s' найдена", label)
            progress.done_line()
            return locator.first
        progress.pulse("Ожидание появления кнопки скачивания Excel...")
        time.sleep(0.2)

    progress.done_line()
    raise TimeoutError(f"Не удалось найти кнопку '{label}' за {timeout_sec} сек")


def download_bonds_excel(config: DohodConfig, logger: logging.Logger, progress: ConsoleProgress) -> str:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Не установлен Playwright. Установите зависимости: pip install playwright && playwright install chromium"
        ) from exc

    progress.update(1, "Проверка локального файла Dohod_Bonds.xlsx")
    if _already_downloaded_today(config.output_excel_path):
        logger.info("Файл уже скачан сегодня, пропускаю: %s", config.output_excel_path)
        progress.update(5, "Файл уже актуален на сегодня, повторное скачивание не требуется")
        return "skipped"

    progress.update(2, "Запуск браузера и открытие dohod.ru")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=config.headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        logger.info("Открываю страницу: %s", config.page_url)
        page.goto(config.page_url, timeout=config.timeout_sec * 1000, wait_until="domcontentloaded")

        progress.update(3, "Поиск кнопки 'Скачать Excel'")
        button = _wait_for_button(page, config.download_button_text, config.timeout_sec, progress, logger)

        progress.update(4, "Ожидание и сохранение файла")
        try:
            with page.expect_download(timeout=config.download_timeout_sec * 1000) as download_info:
                button.click()
            download = download_info.value
        except PlaywrightTimeoutError as exc:
            raise TimeoutError("Не удалось получить download-событие от кнопки 'Скачать Excel'") from exc

        config.output_excel_path.parent.mkdir(parents=True, exist_ok=True)
        download.save_as(str(config.output_excel_path))
        suggested_name = download.suggested_filename

        logger.info("Файл успешно сохранен: %s (suggested=%s)", config.output_excel_path, suggested_name)
        browser.close()

    _save_state(
        config,
        {
            "last_downloaded_at": datetime.now().isoformat(timespec="seconds"),
            "output_excel_path": str(config.output_excel_path),
            "source_page": config.page_url,
        },
        logger,
    )
    progress.update(5, "Готово: Dohod_Bonds.xlsx обновлен")
    return "downloaded"


def main() -> int:
    args = parse_args()
    run_started = time.perf_counter()

    try:
        config = load_config(Path(args.config))
    except Exception as exc:  # noqa: BLE001
        print(f"{Ansi.RED}Ошибка загрузки конфига: {exc}{Ansi.RESET}")
        return 1

    logger = build_logger(config.log_path)
    progress = ConsoleProgress(total_steps=5)

    if not config.enabled:
        logger.info("DOHOD_Bonds отключен в YAML (dohod.enabled=false), запуск пропущен")
        print(f"{Ansi.YELLOW}DOHOD_Bonds отключен в YAML (dohod.enabled=false).{Ansi.RESET}")
        return 0

    try:
        result = download_bonds_excel(config, logger, progress)
        elapsed = time.perf_counter() - run_started
        logger.info("Завершено успешно (%s) за %.2f сек", result, elapsed)
        print(f"{Ansi.GREEN}DOHOD_Bonds завершен ({result}). Лог: {config.log_path}{Ansi.RESET}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка DOHOD_Bonds: %s", exc)
        print(f"\n{Ansi.RED}Ошибка DOHOD_Bonds. См. лог: {config.log_path}{Ansi.RESET}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
