"""Точка входа: запуск парсинга облигаций MOEX без аргументов."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from moex_bond_screener.config import load_config
from moex_bond_screener.data_quality import attach_data_status
from moex_bond_screener.dohod_enrichment import DohodEnricher
from moex_bond_screener.emitents import build_emitents_reference
from moex_bond_screener.exclusion_rules import (
    AMORTIZATION_RULE_NAME,
    HASDEFAULT_RULE_NAME,
    QUALIFIED_ONLY_RULE_NAME,
    STRUCTURAL_BOND_RULE_NAME,
    BondExclusionFilter,
)
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.moex_client import AMORTIZATION_CHECKPOINT_VERSION, MoexClient
from moex_bond_screener.progress import PipelineProgress
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.state_store import ScreenerStateStore
from moex_bond_screener.writer import save_bonds_file
from moex_bond_screener.writer import save_emitents_excel
from moex_bond_screener.ytm import enrich_ytm


def main() -> None:
    started = time.time()
    started_dt = datetime.now(timezone.utc)
    logger = setup_logging()
    progress = PipelineProgress(total_stages=11)
    stage_started = time.perf_counter()
    stage_durations: dict[str, float] = {}

    progress.start_stage(1, "Загрузка конфигурации")
    config = load_config()

    stage_durations["config_load_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(2, "Подготовка raw-хранилища")
    stage_started = time.perf_counter()
    raw_store = RawStore("raw")
    raw_store.cleanup(config.raw_ttl_hours, config.raw_max_size_mb)

    stage_durations["raw_cleanup_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(3, "Загрузка состояния сортировщика и чекпоинтов")
    stage_started = time.perf_counter()
    state_store = ScreenerStateStore(
        config.exclusions_state_dir,
        storage_backend=config.storage_backend,
        sqlite_db_path=config.sqlite_db_path,
    )
    previous_exclusions = state_store.load_exclusions()
    bonds_checkpoint = state_store.load_checkpoint("bonds_fetch")
    raw_amortization_checkpoint = state_store.load_checkpoint("amortization")
    amortization_checkpoint, amortization_checkpoint_invalidated, amortization_cache_fresh = _prepare_amortization_checkpoint(
        raw_amortization_checkpoint
    )
    if amortization_checkpoint_invalidated:
        state_store.clear_checkpoint("amortization")

    rebuild_lock_path = Path(config.exclusions_state_dir) / "checkpoints" / "amortization_rebuild.lock"

    if config.force_cache_refresh:
        progress.tick("Принудительное обновление кэша: удаляем чекпоинты и market cache")
        for checkpoint_name in ("bonds_fetch", "amortization", "dohod_enrichment", "market_cache_bonds", "market_cache_shares"):
            state_store.clear_checkpoint(checkpoint_name)
        previous_exclusions = {}
        bonds_checkpoint = {}
        raw_amortization_checkpoint = {}
        amortization_checkpoint = {}
        amortization_checkpoint_invalidated = False
        amortization_cache_fresh = False


    stage_durations["state_load_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(4, "Загрузка облигаций с MOEX")
    stage_started = time.perf_counter()
    client = MoexClient(config=config, logger=logger, raw_store=raw_store)

    if rebuild_lock_path.exists():
        progress.tick("Найден lock-файл полного пересбора амортизаций: выполняем только этап амортизации")
        _run_amortization_rebuild_mode(
            state_store=state_store,
            client=client,
            progress=progress,
            lock_path=rebuild_lock_path,
            started=started,
            started_dt=started_dt,
            backend=config.storage_backend,
        )
        return

    if bonds_checkpoint and not bonds_checkpoint.get("completed", False):
        progress.tick("Найден чекпоинт списка облигаций — продолжаем с последнего успешного шага")

    bonds, errors, fetch_completed = client.fetch_all_bonds(
        checkpoint_data=bonds_checkpoint,
        checkpoint_saver=lambda payload: state_store.save_checkpoint("bonds_fetch", payload),
        progress_callback=lambda data: _print_fetch_progress(data, progress),
    )
    if fetch_completed:
        state_store.clear_checkpoint("bonds_fetch")

    _sanitize_date_fields(bonds)

    stage_durations["fetch_bonds_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(5, "Применение правил исключения")
    stage_started = time.perf_counter()
    exclusion_filter = BondExclusionFilter(days_threshold=config.exclusion_window_days, qualified_investor_days=config.qualified_investor_exclusion_days)
    exclusion_result = exclusion_filter.apply(bonds=bonds, previous_exclusions=previous_exclusions)

    stage_durations["first_filter_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(6, "Обогащение датой начала амортизации")
    stage_started = time.perf_counter()
    if amortization_checkpoint_invalidated:
        progress.tick("Найден устаревший кэш амортизации — старые данные сброшены, пересчитываем этап")
    elif amortization_cache_fresh:
        progress.tick("Кэш амортизации свежий (до 24 часов) — повторные запросы выполняются только для новых SECID")
    elif amortization_checkpoint and not amortization_checkpoint.get("completed", False):
        progress.tick("Найден чекпоинт амортизации — пропускаем уже обработанные SECID")
    elif amortization_checkpoint:
        progress.tick("Кэш амортизации старше 24 часов — обновляем данные")

    amortization_errors = client.enrich_amortization_start_dates(
        exclusion_result.eligible_bonds,
        checkpoint_data=amortization_checkpoint,
        checkpoint_saver=lambda payload: state_store.save_checkpoint("amortization", payload),
        progress_callback=lambda data: _print_amortization_progress(data, progress),
    )
    amortization_checkpoint_latest = state_store.load_checkpoint("amortization")

    post_amortization_exclusion_result = exclusion_filter.apply(
        bonds=exclusion_result.eligible_bonds,
        previous_exclusions={},
    )

    stage_durations["amortization_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(7, "Обогащение данных через ДОХОД")
    stage_started = time.perf_counter()
    dohod_client = DohodEnricher(config=config, logger=logger, raw_store=raw_store)
    dohod_checkpoint = state_store.load_checkpoint("dohod_enrichment")
    dohod_errors = dohod_client.enrich_bonds(
        post_amortization_exclusion_result.eligible_bonds,
        checkpoint_data=dohod_checkpoint,
        checkpoint_saver=lambda payload: state_store.save_checkpoint("dohod_enrichment", payload),
        progress_callback=lambda data: _print_dohod_progress(data, progress),
    )
    dohod_stats = dohod_client.last_stats

    stage_durations["dohod_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(8, "Повторная фильтрация после обогащения")
    stage_started = time.perf_counter()
    post_dohod_exclusion_result = exclusion_filter.apply(
        bonds=post_amortization_exclusion_result.eligible_bonds,
        previous_exclusions={},
    )

    eligible_bonds = post_dohod_exclusion_result.eligible_bonds
    ytm_stats = enrich_ytm(eligible_bonds)
    attach_data_status(eligible_bonds)
    active_exclusions = dict(exclusion_result.active_exclusions)
    active_exclusions.update(post_amortization_exclusion_result.active_exclusions)
    active_exclusions.update(post_dohod_exclusion_result.active_exclusions)
    excluded_by_rule = dict(exclusion_result.excluded_by_rule)
    for rule_name, count in post_amortization_exclusion_result.excluded_by_rule.items():
        excluded_by_rule[rule_name] = excluded_by_rule.get(rule_name, 0) + count
    for rule_name, count in post_dohod_exclusion_result.excluded_by_rule.items():
        excluded_by_rule[rule_name] = excluded_by_rule.get(rule_name, 0) + count
    skipped_by_active_exclusion = (
        exclusion_result.skipped_by_active_exclusion
        + post_amortization_exclusion_result.skipped_by_active_exclusion
        + post_dohod_exclusion_result.skipped_by_active_exclusion
    )

    stage_durations["second_filter_and_scoring_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(9, "Сохранение инкрементального состояния")
    stage_started = time.perf_counter()
    state_store.save_exclusions(active_exclusions)
    state_store.update_exclusions_history(active_exclusions)
    incremental_stats = state_store.update_eligible_bonds(eligible_bonds)

    stage_durations["incremental_state_seconds"] = round(time.perf_counter() - stage_started, 2)
    progress.start_stage(10, "Сохранение итогового файла")

    progress.start_stage(11, "Формирование справочника эмитентов")
    stage_started = time.perf_counter()
    forced_blacklist_emitters = {
        str(bond.get("EMITTER_ID") or bond.get("ISSUER_ID") or "").strip()
        for bond in bonds
        if str(bond.get("HASDEFAULT") or "").strip() == "1"
    }
    forced_blacklist_emitters.discard("")

    emitents_result = build_emitents_reference(
        eligible_bonds=eligible_bonds,
        client=client,
        state_store=state_store,
        progress_callback=lambda data: _print_emitents_progress(data, progress),
        forced_blacklist_emitters=forced_blacklist_emitters,
    )
    save_emitents_excel(config.emitents_output_file, emitents_result.rows)
    stage_durations["emitents_seconds"] = round(time.perf_counter() - stage_started, 2)

    scorerate_emoji = {"Greenlist": "🟢", "Yellowlist": "🟡", "Redlist": "🔴"}
    annotated_bonds: list[dict[str, Any]] = []
    for bond in eligible_bonds:
        emitter_id = str(bond.get("EMITTER_ID") or bond.get("ISSUER_ID") or "").strip()
        scorerate = emitents_result.scorerate_by_emitter.get(emitter_id, "")
        if scorerate == "Blacklist":
            continue
        bond["Scorerate"] = scorerate
        bond["ScoreColor"] = scorerate_emoji.get(scorerate, "")
        annotated_bonds.append(bond)
    eligible_bonds = annotated_bonds

    elapsed = time.time() - started
    summary = {
        "bonds_count": len(eligible_bonds),
        "errors_count": errors + amortization_errors + dohod_errors + emitents_result.errors,
        "elapsed_seconds": elapsed,
        "filtered_total": len(bonds) - len(eligible_bonds),
        "excluded_by_active_exclusion": skipped_by_active_exclusion,
        "excluded_amortization_permanent_reused": exclusion_result.skipped_by_active_rule.get(AMORTIZATION_RULE_NAME, 0),
        "excluded_buyback_lt_1y": excluded_by_rule["buyback_lt_1y"],
        "excluded_offer_lt_1y": excluded_by_rule["offer_lt_1y"],
        "excluded_calloption_lt_1y": excluded_by_rule["calloption_lt_1y"],
        "excluded_mat_lt_1y": excluded_by_rule["mat_lt_1y"],
        "excluded_amortization_started_or_lt_1y_permanent": excluded_by_rule[AMORTIZATION_RULE_NAME],
        "excluded_hasdefault_permanent": excluded_by_rule[HASDEFAULT_RULE_NAME],
        "excluded_qualified_investors_temp": excluded_by_rule[QUALIFIED_ONLY_RULE_NAME],
        "amortization_cache_hits": int(amortization_checkpoint_latest.get("cache_stats", {}).get("hits", 0)),
        "amortization_cache_misses": int(amortization_checkpoint_latest.get("cache_stats", {}).get("misses", 0)),
        "dohod_realprice_added": dohod_stats.realprice_added,
        "dohod_realprice_updated": dohod_stats.realprice_updated,
        "dohod_coupon_added": dohod_stats.coupon_added,
        "dohod_coupon_updated": dohod_stats.coupon_updated,
        "dohod_offer_added": dohod_stats.offer_added,
        "dohod_offer_updated": dohod_stats.offer_updated,
        "dohod_cache_hits": dohod_stats.cache_hits,
        "dohod_requested": dohod_stats.requested,
        "dohod_parse_empty_payloads": dohod_stats.parse_empty_payloads,
        "corpbonds_realprice_added": dohod_stats.corpbonds_realprice_added,
        "corpbonds_coupontype_added": dohod_stats.corpbonds_coupontype_added,
        "corpbonds_lesenka_added": dohod_stats.corpbonds_lesenka_added,
        "ytm_calculated": ytm_stats.calculated,
        "ytm_skipped": ytm_stats.skipped,
    }
    summary.update(emitents_result.stage_durations)
    summary.update(stage_durations)
    save_bonds_file(config.output_file, eligible_bonds, summary=summary)
    filtered_total = len(bonds) - len(eligible_bonds)

    print("\nГотово.")
    print(f"Обработано бумаг: {len(bonds)}")
    print(f"Отфильтровано: {filtered_total}")
    print(f"  - уже исключены по активному сроку: {skipped_by_active_exclusion}")
    print(f"  - BUYBACKDATE < {config.exclusion_window_days} дней: {excluded_by_rule['buyback_lt_1y']}")
    print(f"  - OFFERDATE < {config.exclusion_window_days} дней: {excluded_by_rule['offer_lt_1y']}")
    print(f"  - CALLOPTIONDATE < {config.exclusion_window_days} дней: {excluded_by_rule['calloption_lt_1y']}")
    print(f"  - MATDATE < {config.exclusion_window_days} дней: {excluded_by_rule['mat_lt_1y']}")
    print(f"  - Структурные облигации (BONDTYPE): {excluded_by_rule[STRUCTURAL_BOND_RULE_NAME]}")
    print(f"  - HASDEFAULT=1 (пожизненно): {excluded_by_rule[HASDEFAULT_RULE_NAME]}")
    print(f"  - ISQUALIFIEDINVESTORS=1 (на {config.qualified_investor_exclusion_days} дней): {excluded_by_rule[QUALIFIED_ONLY_RULE_NAME]}")
    print(
        "  - Amortization_start_date < "
        f"{config.exclusion_window_days} дней (включая начавшуюся): {excluded_by_rule[AMORTIZATION_RULE_NAME]}"
    )
    print(f"Ошибок: {errors + amortization_errors + dohod_errors + emitents_result.errors}")
    print(f"  - ошибки загрузки списка бумаг: {errors}")
    print(f"  - ошибки запроса амортизации: {amortization_errors}")
    print(f"  - ошибки обогащения ДОХОД: {dohod_errors}")
    print(f"  - ошибки этапа эмитентов: {emitents_result.errors}")
    print(
        "ДОХОД (добавлено/обновлено): "
        f"RealPrice +{dohod_stats.realprice_added}/~{dohod_stats.realprice_updated}, "
        f"COUPONPERCENT +{dohod_stats.coupon_added}/~{dohod_stats.coupon_updated}, "
        f"OFFERDATE +{dohod_stats.offer_added}/~{dohod_stats.offer_updated}"
    )
    print(f"YTM: рассчитано {ytm_stats.calculated}, пропущено {ytm_stats.skipped}")
    print(f"ДОХОД: пустых payload после парсинга: {dohod_stats.parse_empty_payloads}")
    print(
        "CorpBonds: "
        f"RealPrice +{dohod_stats.corpbonds_realprice_added}, "
        f"CouponType +{dohod_stats.corpbonds_coupontype_added}, "
        f"Lesenka +{dohod_stats.corpbonds_lesenka_added}"
    )
    print(
        "Инкрементальные изменения: "
        f"+{incremental_stats.inserted} / ~{incremental_stats.updated} / = {incremental_stats.unchanged} / -{incremental_stats.removed}"
    )
    if not fetch_completed:
        print("[Внимание] Загрузка MOEX завершилась с ошибкой сети. Чекпоинт сохранен, следующий запуск продолжит с места остановки.")
    print(
        "Справочник эмитентов: "
        f"{emitents_result.processed_emitters} эмитентов, новых: {emitents_result.new_emitters}, "
        f"файл: {config.emitents_output_file}"
    )
    print(f"Время выполнения: {elapsed:.2f} сек")

    state_store.save_run_metrics(
        {
            "started_at": started_dt.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": elapsed,
            "bonds_processed": len(bonds),
            "bonds_filtered": filtered_total,
            "errors_count": errors + amortization_errors + dohod_errors + emitents_result.errors,
            "backend": config.storage_backend,
            "notes": {
                "eligible_bonds": len(eligible_bonds),
                "new_emitters": emitents_result.new_emitters,
                "excluded_amortization_permanent_reused": exclusion_result.skipped_by_active_rule.get(
                    AMORTIZATION_RULE_NAME, 0
                ),
                "amortization_cache_hits": int(amortization_checkpoint_latest.get("cache_stats", {}).get("hits", 0)),
                "amortization_cache_misses": int(
                    amortization_checkpoint_latest.get("cache_stats", {}).get("misses", 0)
                ),
                "dohod_realprice_added": dohod_stats.realprice_added,
                "dohod_realprice_updated": dohod_stats.realprice_updated,
                "dohod_coupon_added": dohod_stats.coupon_added,
                "dohod_coupon_updated": dohod_stats.coupon_updated,
                "dohod_offer_added": dohod_stats.offer_added,
                "dohod_offer_updated": dohod_stats.offer_updated,
                "dohod_cache_hits": dohod_stats.cache_hits,
                "dohod_requested": dohod_stats.requested,
                "dohod_parse_empty_payloads": dohod_stats.parse_empty_payloads,
                "ytm_calculated": ytm_stats.calculated,
                "ytm_skipped": ytm_stats.skipped,
                "corpbonds_realprice_added": dohod_stats.corpbonds_realprice_added,
                "corpbonds_coupontype_added": dohod_stats.corpbonds_coupontype_added,
                "corpbonds_lesenka_added": dohod_stats.corpbonds_lesenka_added,
                "stage_durations": stage_durations,
            },
        }
    )


def _run_amortization_rebuild_mode(
    state_store: ScreenerStateStore,
    client: MoexClient,
    progress: PipelineProgress,
    lock_path: Path,
    started: float,
    started_dt: datetime,
    backend: str,
) -> None:
    eligible_bonds = state_store.load_eligible_bonds()
    if not eligible_bonds:
        print("Режим пересбора амортизаций: eligible_bonds пуст, пересбор не требуется.")
        lock_path.unlink(missing_ok=True)
        return

    progress.start_stage(5, "Пересбор кэша амортизаций")
    amortization_errors = client.enrich_amortization_start_dates(
        eligible_bonds,
        checkpoint_data={},
        checkpoint_saver=lambda payload: state_store.save_checkpoint("amortization", payload),
        progress_callback=lambda data: _print_amortization_progress(data, progress),
    )
    state_store.update_eligible_bonds(eligible_bonds)
    checkpoint = state_store.load_checkpoint("amortization")
    cache_stats = checkpoint.get("cache_stats", {}) if isinstance(checkpoint, dict) else {}
    lock_path.unlink(missing_ok=True)

    elapsed = time.time() - started
    print("\nГотово.")
    print("Выполнен режим полного пересбора только амортизаций.")
    print(f"Обработано бумаг: {len(eligible_bonds)}")
    print(f"Ошибок амортизации: {amortization_errors}")
    print(f"cache_hits/cache_misses: {int(cache_stats.get('hits', 0))}/{int(cache_stats.get('misses', 0))}")
    print(f"Время выполнения: {elapsed:.2f} сек")

    state_store.save_run_metrics(
        {
            "started_at": started_dt.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": elapsed,
            "bonds_processed": len(eligible_bonds),
            "bonds_filtered": 0,
            "errors_count": amortization_errors,
            "backend": backend,
            "notes": {
                "mode": "amortization_rebuild",
                "amortization_cache_hits": int(cache_stats.get("hits", 0)),
                "amortization_cache_misses": int(cache_stats.get("misses", 0)),
            },
        }
    )


def _prepare_amortization_checkpoint(checkpoint: dict[str, Any]) -> tuple[dict[str, Any], bool, bool]:
    if not checkpoint:
        return {}, False, False

    version = checkpoint.get("version")
    if version != AMORTIZATION_CHECKPOINT_VERSION:
        return {}, True, False

    processed = checkpoint.get("processed")
    if not isinstance(processed, dict):
        return {}, True, False

    updated_at_raw = checkpoint.get("updated_at")
    if not isinstance(updated_at_raw, str):
        return {}, True, False

    try:
        updated_at = datetime.fromisoformat(updated_at_raw)
    except ValueError:
        return {}, True, False

    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    is_fresh = datetime.now(timezone.utc) - updated_at <= timedelta(hours=24)
    if not is_fresh:
        return {}, True, False

    normalized = {str(secid): value for secid, value in processed.items() if str(secid).strip()}
    cache_stats_raw = checkpoint.get("cache_stats", {})
    cache_stats = {"date": "", "hits": 0, "misses": 0}
    if isinstance(cache_stats_raw, dict):
        cache_stats = {
            "date": str(cache_stats_raw.get("date") or ""),
            "hits": int(cache_stats_raw.get("hits") or 0),
            "misses": int(cache_stats_raw.get("misses") or 0),
        }
    return {
        "version": AMORTIZATION_CHECKPOINT_VERSION,
        "processed": normalized,
        "cache_stats": cache_stats,
        "updated_at": updated_at.isoformat(),
        "completed": bool(checkpoint.get("completed", False)),
    }, False, True


def _sanitize_date_fields(bonds: list[dict[str, Any]]) -> None:
    date_fields = ("MATDATE", "Amortization_start_date", "BUYBACKDATE", "OFFERDATE", "CALLOPTIONDATE", "PUTOPTIONDATE")
    for bond in bonds:
        for field in date_fields:
            raw_value = str(bond.get(field) or "").strip()
            if not raw_value:
                continue
            match = re.search(r"(\d{4}-\d{2}-\d{2})", raw_value)
            if match:
                bond[field] = match.group(1)
                continue
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", raw_value):
                day, month, year = raw_value.split(".")
                bond[field] = f"{year}-{month}-{day}"


def _print_fetch_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    fetched = int(data.get("fetched", 0))
    new_items = int(data.get("new_items", 0))
    start = int(data.get("start", 0))
    message = data.get("message")
    if message:
        progress.tick(str(message))
    progress.report_counter(fetched, f"загружено бумаг: {fetched} (+{new_items}, start={start})")



def _print_emitents_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    message = data.get("message")
    if message:
        progress.tick(str(message))

    phase = str(data.get("phase") or "")
    if phase == "sample_descriptions":
        processed = int(data.get("processed", 0))
        total = int(data.get("total", 0))
        progress.report_fraction(processed, total, "обработано карточек эмитентов")
    elif phase == "emitter_profiles":
        processed = int(data.get("processed", 0))
        total = int(data.get("total", 0))
        progress.report_fraction(processed, total, "обработано профилей эмитентов")
    elif phase == "market_descriptions":
        processed = int(data.get("processed", 0))
        total = int(data.get("total", 0))
        progress.report_fraction(processed, total, "обработано market-description карточек")



def _print_dohod_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    message = data.get("message")
    if message:
        progress.tick(str(message))
    processed = int(data.get("processed", 0))
    total = int(data.get("total", 0))
    progress.report_fraction(processed, total, "обработано карточек ДОХОД")

def _print_amortization_progress(data: dict[str, Any], progress: PipelineProgress) -> None:
    processed = int(data.get("processed", 0))
    total = int(data.get("total", 0))
    progress.report_fraction(processed, total, "обработано амортизаций")


if __name__ == "__main__":
    main()
