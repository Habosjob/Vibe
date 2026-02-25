"""Точка входа: запуск парсинга облигаций MOEX без аргументов."""

from __future__ import annotations

import time
from typing import Any

from moex_bond_screener.config import load_config
from moex_bond_screener.exclusion_rules import BondExclusionFilter
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.moex_client import AMORTIZATION_CHECKPOINT_VERSION, MoexClient
from moex_bond_screener.progress import PipelineProgress
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.state_store import ScreenerStateStore
from moex_bond_screener.writer import save_bonds_file


def main() -> None:
    started = time.time()
    logger = setup_logging()
    progress = PipelineProgress(total_stages=8)

    progress.start_stage(1, "Загрузка конфигурации")
    config = load_config()

    progress.start_stage(2, "Подготовка raw-хранилища")
    raw_store = RawStore("raw")
    raw_store.cleanup(config.raw_ttl_hours, config.raw_max_size_mb)

    progress.start_stage(3, "Загрузка состояния сортировщика и чекпоинтов")
    state_store = ScreenerStateStore(config.exclusions_state_dir)
    previous_exclusions = state_store.load_exclusions()
    bonds_checkpoint = state_store.load_checkpoint("bonds_fetch")
    raw_amortization_checkpoint = state_store.load_checkpoint("amortization")
    amortization_checkpoint, amortization_checkpoint_invalidated = _prepare_amortization_checkpoint(
        raw_amortization_checkpoint
    )
    if amortization_checkpoint_invalidated:
        state_store.clear_checkpoint("amortization")

    progress.start_stage(4, "Загрузка облигаций с MOEX")
    client = MoexClient(config=config, logger=logger, raw_store=raw_store)

    if bonds_checkpoint and not bonds_checkpoint.get("completed", False):
        progress.tick("Найден чекпоинт списка облигаций — продолжаем с последнего успешного шага")

    bonds, errors, fetch_completed = client.fetch_all_bonds(
        checkpoint_data=bonds_checkpoint,
        checkpoint_saver=lambda payload: state_store.save_checkpoint("bonds_fetch", payload),
        progress_callback=lambda data: _print_fetch_progress(data, progress),
    )
    if fetch_completed:
        state_store.clear_checkpoint("bonds_fetch")

    progress.start_stage(5, "Применение правил исключения")
    exclusion_filter = BondExclusionFilter(days_threshold=config.exclusion_window_days)
    exclusion_result = exclusion_filter.apply(bonds=bonds, previous_exclusions=previous_exclusions)

    progress.start_stage(6, "Обогащение датой начала амортизации")
    if amortization_checkpoint_invalidated:
        progress.tick("Найден устаревший чекпоинт амортизации — старый кэш сброшен, пересчитываем этап")
    elif amortization_checkpoint and not amortization_checkpoint.get("completed", False):
        progress.tick("Найден чекпоинт амортизации — пропускаем уже обработанные SECID")

    amortization_errors = client.enrich_amortization_start_dates(
        exclusion_result.eligible_bonds,
        checkpoint_data=amortization_checkpoint,
        checkpoint_saver=lambda payload: state_store.save_checkpoint("amortization", payload),
        progress_callback=lambda data: _print_amortization_progress(data, progress),
    )
    state_store.clear_checkpoint("amortization")

    progress.start_stage(7, "Сохранение инкрементального состояния")
    state_store.save_exclusions(exclusion_result.active_exclusions)
    incremental_stats = state_store.update_eligible_bonds(exclusion_result.eligible_bonds)

    progress.start_stage(8, "Сохранение итогового файла")
    elapsed_before_export = time.time() - started
    summary = {
        "bonds_count": len(exclusion_result.eligible_bonds),
        "errors_count": errors + amortization_errors,
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
    print(f"Ошибок: {errors + amortization_errors}")
    print(f"  - ошибки загрузки списка бумаг: {errors}")
    print(f"  - ошибки запроса амортизации: {amortization_errors}")
    print(
        "Инкрементальные изменения: "
        f"+{incremental_stats.inserted} / ~{incremental_stats.updated} / = {incremental_stats.unchanged} / -{incremental_stats.removed}"
    )
    if not fetch_completed:
        print("[Внимание] Загрузка MOEX завершилась с ошибкой сети. Чекпоинт сохранен, следующий запуск продолжит с места остановки.")
    print(f"Время выполнения: {elapsed:.2f} сек")


def _prepare_amortization_checkpoint(checkpoint: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not checkpoint:
        return {}, False

    version = checkpoint.get("version")
    if version != AMORTIZATION_CHECKPOINT_VERSION:
        return {}, True

    processed = checkpoint.get("processed")
    if not isinstance(processed, dict):
        return {}, True

    normalized = {str(secid): str(value or "") for secid, value in processed.items() if str(secid).strip()}
    return {
        "version": AMORTIZATION_CHECKPOINT_VERSION,
        "processed": normalized,
        "completed": bool(checkpoint.get("completed", False)),
    }, False


def _print_fetch_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    fetched = int(data.get("fetched", 0))
    new_items = int(data.get("new_items", 0))
    start = int(data.get("start", 0))
    message = data.get("message")
    if message:
        progress.tick(str(message))
    progress.report_counter(fetched, f"загружено бумаг: {fetched} (+{new_items}, start={start})")


def _print_amortization_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    processed = int(data.get("processed", 0))
    total = int(data.get("total", 0))
    progress.report_fraction(processed, total, "обработано амортизаций")


if __name__ == "__main__":
    main()
