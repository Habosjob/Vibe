"""Точка входа: запуск парсинга облигаций MOEX без аргументов."""

from __future__ import annotations

import time

from moex_bond_screener.config import load_config
from moex_bond_screener.exclusion_rules import BondExclusionFilter
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.moex_client import MoexClient
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.state_store import ScreenerStateStore
from moex_bond_screener.writer import save_bonds_file


def main() -> None:
    started = time.time()
    logger = setup_logging()

    print("[Этап] Загрузка конфигурации...")
    config = load_config()

    print("[Этап] Подготовка raw-хранилища...")
    raw_store = RawStore("raw")
    raw_store.cleanup(config.raw_ttl_hours, config.raw_max_size_mb)

    print("[Этап] Загрузка состояния сортировщика...")
    state_store = ScreenerStateStore(config.exclusions_state_dir)
    previous_exclusions = state_store.load_exclusions()

    print("[Этап] Загрузка облигаций с MOEX...")
    client = MoexClient(config=config, logger=logger, raw_store=raw_store)
    bonds, errors = client.fetch_all_bonds()

    print("[Этап] Применение правил исключения...")
    exclusion_filter = BondExclusionFilter(days_threshold=config.exclusion_window_days)
    exclusion_result = exclusion_filter.apply(bonds=bonds, previous_exclusions=previous_exclusions)

    state_store.save_exclusions(exclusion_result.active_exclusions)

    print("[Этап] Инкрементальное обновление итоговых данных...")
    incremental_stats = state_store.update_eligible_bonds(exclusion_result.eligible_bonds)

    print("[Этап] Сохранение итогового файла...")
    elapsed_before_export = time.time() - started
    summary = {
        "bonds_count": len(exclusion_result.eligible_bonds),
        "errors_count": errors,
        "elapsed_seconds": elapsed_before_export,
        "filtered_total": len(bonds) - len(exclusion_result.eligible_bonds),
        "excluded_by_active_exclusion": exclusion_result.skipped_by_active_exclusion,
        "excluded_buyback_lt_1y": exclusion_result.excluded_by_rule["buyback_lt_1y"],
        "excluded_offer_lt_1y": exclusion_result.excluded_by_rule["offer_lt_1y"],
        "excluded_calloption_lt_1y": exclusion_result.excluded_by_rule["calloption_lt_1y"],
        "excluded_mat_lt_1y": exclusion_result.excluded_by_rule["mat_lt_1y"],
    }
    save_bonds_file(config.output_file, exclusion_result.eligible_bonds, summary=summary)

    elapsed = time.time() - started
    filtered_total = len(bonds) - len(exclusion_result.eligible_bonds)

    print("\nГотово.")
    print(f"Обработано бумаг: {len(bonds)}")
    print(f"Отфильтровано: {filtered_total}")
    print(f"  - уже исключены по активному сроку: {exclusion_result.skipped_by_active_exclusion}")
    print(f"  - BUYBACKDATE < {config.exclusion_window_days} дней: {exclusion_result.excluded_by_rule['buyback_lt_1y']}")
    print(f"  - OFFERDATE < {config.exclusion_window_days} дней: {exclusion_result.excluded_by_rule['offer_lt_1y']}")
    print(f"  - CALLOPTIONDATE < {config.exclusion_window_days} дней: {exclusion_result.excluded_by_rule['calloption_lt_1y']}")
    print(f"  - MATDATE < {config.exclusion_window_days} дней: {exclusion_result.excluded_by_rule['mat_lt_1y']}")
    print(f"Ошибок: {errors}")
    print(
        "Инкрементальные изменения: "
        f"+{incremental_stats.inserted} / ~{incremental_stats.updated} / = {incremental_stats.unchanged} / -{incremental_stats.removed}"
    )
    print(f"Время выполнения: {elapsed:.2f} сек")


if __name__ == "__main__":
    main()
