"""Точка входа: запуск парсинга облигаций MOEX без аргументов."""

from __future__ import annotations

import time

from moex_bond_screener.config import load_config
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.moex_client import MoexClient
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.writer import save_bonds_file


def main() -> None:
    started = time.time()
    logger = setup_logging()

    print("[Этап] Загрузка конфигурации...")
    config = load_config()

    print("[Этап] Подготовка raw-хранилища...")
    raw_store = RawStore("raw")
    raw_store.cleanup(config.raw_ttl_hours, config.raw_max_size_mb)

    print("[Этап] Загрузка облигаций с MOEX...")
    client = MoexClient(config=config, logger=logger, raw_store=raw_store)
    bonds, errors = client.fetch_all_bonds()

    print("[Этап] Сохранение итогового файла...")
    elapsed_before_export = time.time() - started
    save_bonds_file(
        config.output_file,
        bonds,
        summary={
            "bonds_count": len(bonds),
            "errors_count": errors,
            "elapsed_seconds": elapsed_before_export,
        },
    )

    elapsed = time.time() - started
    filtered = 0
    print("\nГотово.")
    print(f"Обработано бумаг: {len(bonds)}")
    print(f"Отфильтровано: {filtered}")
    print(f"Ошибок: {errors}")
    print(f"Время выполнения: {elapsed:.2f} сек")


if __name__ == "__main__":
    main()
