"""Формирование справочника эмитентов по итоговым облигациям."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Callable

from .moex_client import MoexClient
from .state_store import ScreenerStateStore



SCORE_VALUES = {"", "Blacklist", "Redlist", "Yellowlist", "Greenlist"}

@dataclass(slots=True)
class EmitentsBuildResult:
    rows: list[dict[str, str]]
    errors: int
    processed_emitters: int
    new_emitters: int
    stage_durations: dict[str, float]
    scorerate_by_emitter: dict[str, str]


def build_emitents_reference(
    eligible_bonds: list[dict[str, Any]],
    client: MoexClient,
    state_store: ScreenerStateStore,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    forced_blacklist_emitters: set[str] | None = None,
    manual_overrides: dict[str, dict[str, str]] | None = None,
) -> EmitentsBuildResult:
    """Собирает справочник эмитентов для итоговых облигаций.

    Полное наименование и ИНН кэшируются в состоянии и обновляются
    только для новых эмитентов. Списки торгуемых акций/облигаций
    пересчитываются на каждом запуске.
    """

    registry = state_store.load_emitents_registry()
    registry_before_overrides = {
        emitter_id: {
            "scorerate": str((details or {}).get("scorerate") or "").strip(),
            "datescore": str((details or {}).get("datescore") or "").strip(),
        }
        for emitter_id, details in registry.items()
    }
    for emitter_id, override in (manual_overrides or {}).items():
        details = registry.get(emitter_id, {})
        scorerate = str((override or {}).get("scorerate") or "").strip()
        datescore = str((override or {}).get("datescore") or "").strip()
        if scorerate not in SCORE_VALUES:
            scorerate = str(details.get("scorerate") or "").strip()
        registry[emitter_id] = {
            "full_name": str(details.get("full_name") or "").strip(),
            "inn": str(details.get("inn") or "").strip(),
            "scorerate": scorerate,
            "datescore": datescore or str(details.get("datescore") or "").strip(),
        }
    secid_to_emitter = state_store.load_secid_to_emitter_map()
    secid_samples = _pick_emitter_samples(eligible_bonds, secid_to_emitter)
    total_descriptions = len(secid_samples.unknown_emitters)
    discovered_emitters: set[str] = set(registry.keys())
    errors = 0
    new_emitters = 0
    registry_changed = False
    secid_cache_changed = False
    stage_durations = {
        "emitents_cards_seconds": 0.0,
        "emitents_market_descriptions_seconds": 0.0,
        "emitents_market_bonds_seconds": 0.0,
        "emitents_market_shares_seconds": 0.0,
    }
    workers = max(1, int(client.config.amortization_workers))

    emitters_needing_details: set[str] = set()
    for emitter_id in secid_samples.known_emitters:
        cached = registry.get(emitter_id)
        if cached and cached.get("full_name") and cached.get("inn"):
            discovered_emitters.add(emitter_id)
            continue
        emitters_needing_details.add(emitter_id)

    if progress_callback:
        progress_callback(
            {
                "phase": "sample_descriptions",
                "processed": 0,
                "total": total_descriptions,
                "message": "Сопоставление SECID -> EMITTER_ID по карточкам бумаг",
            }
        )

    processed_descriptions = 0
    pending_secids = sorted(secid_samples.unknown_emitters)

    cards_started = perf_counter()
    if pending_secids:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(client.fetch_security_description, secid): secid
                for secid in pending_secids
            }
            for future in as_completed(futures):
                secid = futures[future]
                try:
                    details, fetch_errors = future.result()
                except Exception:  # noqa: BLE001
                    details, fetch_errors = {}, 1
                errors += fetch_errors
                processed_descriptions += 1

                if progress_callback:
                    progress_callback(
                        {
                            "phase": "sample_descriptions",
                            "processed": processed_descriptions,
                            "total": total_descriptions,
                        }
                    )

                if fetch_errors:
                    continue

                resolved_emitter_id = str(details.get("EMITTER_ID") or details.get("ISSUER_ID") or "").strip()
                if not resolved_emitter_id:
                    continue

                if secid_to_emitter.get(secid) != resolved_emitter_id:
                    secid_to_emitter[secid] = resolved_emitter_id
                    secid_cache_changed = True

                discovered_emitters.add(resolved_emitter_id)
                cached = registry.get(resolved_emitter_id)
                if not (cached and cached.get("full_name") and cached.get("inn")):
                    emitters_needing_details.add(resolved_emitter_id)

    pending_emitters = sorted(emitters_needing_details)
    if progress_callback:
        progress_callback(
            {
                "phase": "emitter_profiles",
                "processed": 0,
                "total": len(pending_emitters),
                "message": "Загрузка карточек эмитентов по EMITTER_ID",
            }
        )

    processed_emitters = 0
    if pending_emitters:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(client.fetch_emitter_details, emitter_id): emitter_id
                for emitter_id in pending_emitters
            }
            for future in as_completed(futures):
                emitter_id = futures[future]
                try:
                    details, fetch_errors = future.result()
                except Exception:  # noqa: BLE001
                    details, fetch_errors = {}, 1
                errors += fetch_errors
                processed_emitters += 1

                if progress_callback:
                    progress_callback(
                        {
                            "phase": "emitter_profiles",
                            "processed": processed_emitters,
                            "total": len(pending_emitters),
                        }
                    )

                if fetch_errors:
                    continue

                cached = registry.get(emitter_id, {})
                full_name = str(details.get("TITLE") or details.get("SHORT_TITLE") or "").strip()
                inn = str(details.get("INN") or "").strip()
                if not full_name:
                    full_name = str(cached.get("full_name") or "").strip()
                if not inn:
                    inn = str(cached.get("inn") or "").strip()
                if not full_name and not inn:
                    continue

                if emitter_id not in registry:
                    new_emitters += 1
                registry_changed = True
                registry[emitter_id] = {
                    "full_name": full_name,
                    "inn": inn,
                    "scorerate": str(cached.get("scorerate") or ""),
                    "datescore": str(cached.get("datescore") or ""),
                }
                discovered_emitters.add(emitter_id)

    stage_durations["emitents_cards_seconds"] = round(perf_counter() - cards_started, 2)

    if registry_changed:
        state_store.save_emitents_registry(registry)
    if secid_cache_changed:
        state_store.save_secid_to_emitter_map(secid_to_emitter)

    bonds_market: list[dict[str, Any]] = []
    bonds_errors = 0
    bonds_started = perf_counter()
    unresolved_bonds = _count_bonds_without_emitter(eligible_bonds, secid_to_emitter)
    bonds_without_isin = _count_bonds_without_isin(eligible_bonds)
    need_bonds_market = True
    if need_bonds_market:
        bonds_market = state_store.load_market_cache("bonds") or []
        if not bonds_market:
            if progress_callback:
                progress_callback(
                    {
                        "phase": "market_data",
                        "message": (
                            "Загрузка рыночных инструментов bonds "
                            f"(без EMITTER_ID: {unresolved_bonds}, без ISIN: {bonds_without_isin})"
                        ),
                    }
                )
            bonds_market, bonds_errors = client.fetch_market_securities("bonds")
            if bonds_market:
                state_store.save_market_cache("bonds", bonds_market)
        elif progress_callback:
            progress_callback({"phase": "market_data", "message": "Используем кэш рыночных инструментов bonds"})
    elif progress_callback:
        progress_callback(
            {
                "phase": "market_data",
                "message": "Пропускаем рынок bonds: EMITTER_ID/ISSUER_ID и ISIN уже заполнены в итоговых бумагах",
            }
        )
    stage_durations["emitents_market_bonds_seconds"] = round(perf_counter() - bonds_started, 2)

    shares_started = perf_counter()
    shares_market = state_store.load_market_cache("shares") or []
    shares_errors = 0
    if not shares_market:
        if progress_callback:
            progress_callback({"phase": "market_data", "message": "Загрузка рыночных инструментов shares"})
        shares_market, shares_errors = client.fetch_market_securities("shares")
        if shares_market:
            state_store.save_market_cache("shares", shares_market)
    elif progress_callback:
        progress_callback({"phase": "market_data", "message": "Используем кэш рыночных инструментов shares"})
    stage_durations["emitents_market_shares_seconds"] = round(perf_counter() - shares_started, 2)
    errors += bonds_errors + shares_errors

    market_description_started = perf_counter()
    secid_cache_before_market = dict(secid_to_emitter)
    market_description_errors = _resolve_market_emitter_ids(
        rows=[*bonds_market, *shares_market],
        client=client,
        secid_to_emitter=secid_to_emitter,
        workers=workers,
        progress_callback=progress_callback,
    )
    stage_durations["emitents_market_descriptions_seconds"] = round(perf_counter() - market_description_started, 2)
    errors += market_description_errors
    if secid_to_emitter != secid_cache_before_market:
        state_store.save_secid_to_emitter_map(secid_to_emitter)

    market_emitters = _extract_emitter_ids_from_market(bonds_market) | _extract_emitter_ids_from_market(shares_market)
    discovered_emitters.update(market_emitters)
    late_emitters = sorted(emitter_id for emitter_id in market_emitters if _needs_emitter_details(registry.get(emitter_id)))
    if progress_callback:
        progress_callback(
            {
                "phase": "emitter_profiles",
                "processed": 0,
                "total": len(late_emitters),
                "message": "Дозагрузка карточек эмитентов, найденных через markets/*/securities",
            }
        )

    if late_emitters:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(client.fetch_emitter_details, emitter_id): emitter_id
                for emitter_id in late_emitters
            }
            late_processed = 0
            for future in as_completed(futures):
                emitter_id = futures[future]
                try:
                    details, fetch_errors = future.result()
                except Exception:  # noqa: BLE001
                    details, fetch_errors = {}, 1
                errors += fetch_errors
                late_processed += 1

                if progress_callback:
                    progress_callback(
                        {
                            "phase": "emitter_profiles",
                            "processed": late_processed,
                            "total": len(late_emitters),
                        }
                    )

                if fetch_errors:
                    continue

                cached = registry.get(emitter_id, {})
                full_name = str(details.get("TITLE") or details.get("SHORT_TITLE") or cached.get("full_name") or "").strip()
                inn = str(details.get("INN") or cached.get("inn") or "").strip()
                if not full_name and not inn:
                    continue

                if emitter_id not in registry:
                    new_emitters += 1
                registry[emitter_id] = {
                    "full_name": full_name,
                    "inn": inn,
                    "scorerate": str(cached.get("scorerate") or ""),
                    "datescore": str(cached.get("datescore") or ""),
                }

        state_store.save_emitents_registry(registry)

    bond_map = _collect_bond_isins_by_emitter(eligible_bonds, secid_to_emitter)
    if bonds_market:
        market_bond_map = _collect_market_instruments(bonds_market, instrument_key="ISIN")
        for emitter_id, isins in market_bond_map.items():
            existing = set(bond_map.get(emitter_id, []))
            existing.update(isins)
            bond_map[emitter_id] = sorted(existing)
    share_map = _collect_market_instruments(shares_market, instrument_key="SECID")
    discovered_emitters.update(_infer_emitters_from_market(eligible_bonds, bonds_market, shares_market, secid_to_emitter))

    rows: list[dict[str, str]] = []
    scorerate_by_emitter: dict[str, str] = {}
    today = datetime.now().date().isoformat()
    forced_blacklist_emitters = forced_blacklist_emitters or set()
    discovered_emitters.update(forced_blacklist_emitters)

    for emitter_id in sorted(discovered_emitters):
        details = registry.get(emitter_id, {})
        full_name = str(details.get("full_name") or "").strip()
        inn = str(details.get("inn") or "").strip()
        previous_score = str(registry_before_overrides.get(emitter_id, {}).get("scorerate") or "").strip()
        if previous_score not in SCORE_VALUES:
            previous_score = ""

        scorerate = str(details.get("scorerate") or "").strip()
        if scorerate not in SCORE_VALUES:
            scorerate = ""
        if emitter_id in forced_blacklist_emitters:
            scorerate = "Blacklist"

        datescore = str(details.get("datescore") or "").strip()
        if scorerate != previous_score:
            datescore = today

        registry[emitter_id] = {
            "full_name": full_name,
            "inn": inn,
            "scorerate": scorerate,
            "datescore": datescore,
        }
        scorerate_by_emitter[emitter_id] = scorerate

        missing_full_name = "1" if not full_name else "0"
        missing_inn = "1" if not inn else "0"
        quality_flag = "ok" if missing_full_name == "0" and missing_inn == "0" else "warning"
        rows.append(
            {
                "Полное наименование": full_name,
                "ИНН": inn,
                "Scorerate": scorerate,
                "DateScore": datescore,
                "Тикеры акций": ", ".join(share_map.get(emitter_id, [])),
                "ISIN облигаций": ", ".join(bond_map.get(emitter_id, [])),
                "EMITTER_ID": emitter_id,
                "missing_full_name": missing_full_name,
                "missing_inn": missing_inn,
                "Флаг качества": quality_flag,
            }
        )

    rows.sort(key=lambda item: (item["Полное наименование"], item["ИНН"]))
    state_store.save_emitents_registry(registry)
    return EmitentsBuildResult(
        rows=rows,
        errors=errors,
        processed_emitters=len(discovered_emitters),
        new_emitters=new_emitters,
        stage_durations=stage_durations,
        scorerate_by_emitter=scorerate_by_emitter,
    )


@dataclass(slots=True)
class EmitterSamples:
    known_emitters: dict[str, str]
    unknown_emitters: set[str]


def _pick_emitter_samples(eligible_bonds: list[dict[str, Any]], secid_to_emitter: dict[str, str]) -> EmitterSamples:
    known_emitters: dict[str, str] = {}
    unknown_emitters: set[str] = set()
    for bond in eligible_bonds:
        secid = str(bond.get("SECID") or "").strip()
        emitter_id = _normalize_emitter_id(
            bond.get("EMITTER_ID")
            or bond.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or ""
        )
        if not secid:
            continue

        if not emitter_id:
            unknown_emitters.add(secid)
            continue

        if emitter_id in known_emitters:
            continue

        known_emitters[emitter_id] = secid
    return EmitterSamples(known_emitters=known_emitters, unknown_emitters=unknown_emitters)


def _collect_market_instruments(rows: list[dict[str, Any]], instrument_key: str) -> dict[str, list[str]]:
    instruments: dict[str, set[str]] = {}
    for row in rows:
        emitter_id = _normalize_emitter_id(
            row.get("EMITTER_ID")
            or row.get("ISSUER_ID")
            or row.get("EMITTERID")
            or row.get("ISSUERID")
            or ""
        )
        instrument = str(row.get(instrument_key) or "").strip()
        if not emitter_id or not instrument:
            continue
        instruments.setdefault(emitter_id, set()).add(instrument)

    return {key: sorted(values) for key, values in instruments.items()}


def _extract_emitter_ids_from_market(rows: list[dict[str, Any]]) -> set[str]:
    emitter_ids: set[str] = set()
    for row in rows:
        emitter_id = _normalize_emitter_id(
            row.get("EMITTER_ID")
            or row.get("ISSUER_ID")
            or row.get("EMITTERID")
            or row.get("ISSUERID")
            or ""
        )
        if emitter_id:
            emitter_ids.add(emitter_id)
    return emitter_ids


def _needs_emitter_details(cached: dict[str, Any] | None) -> bool:
    if not cached:
        return True
    return not str(cached.get("full_name") or "").strip() or not str(cached.get("inn") or "").strip()


def _resolve_market_emitter_ids(
    rows: list[dict[str, Any]],
    client: MoexClient,
    secid_to_emitter: dict[str, str],
    workers: int,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> int:
    pending_secids: set[str] = set()
    for row in rows:
        secid = str(row.get("SECID") or "").strip()
        if not secid:
            continue
        cached_emitter_id = _normalize_emitter_id(secid_to_emitter.get(secid, ""))
        if cached_emitter_id:
            row["EMITTER_ID"] = cached_emitter_id
            continue
        pending_secids.add(secid)

    if progress_callback:
        progress_callback(
            {
                "phase": "market_descriptions",
                "processed": 0,
                "total": len(pending_secids),
                "message": "Запрашиваем description для market SECID без EMITTER_ID",
            }
        )

    errors = 0
    if not pending_secids:
        return errors

    resolved = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(client.fetch_security_description, secid): secid for secid in sorted(pending_secids)}
        for future in as_completed(futures):
            secid = futures[future]
            try:
                payload, fetch_errors = future.result()
            except Exception:  # noqa: BLE001
                payload, fetch_errors = {}, 1
            errors += fetch_errors
            resolved += 1

            if progress_callback:
                progress_callback(
                    {
                        "phase": "market_descriptions",
                        "processed": resolved,
                        "total": len(pending_secids),
                    }
                )

            if fetch_errors:
                continue

            emitter_id = _normalize_emitter_id(payload.get("EMITTER_ID") or payload.get("ISSUER_ID") or "")
            if not emitter_id:
                continue
            secid_to_emitter[secid] = emitter_id

    for row in rows:
        secid = str(row.get("SECID") or "").strip()
        if not secid:
            continue
        emitter_id = _normalize_emitter_id(
            row.get("EMITTER_ID")
            or row.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or ""
        )
        if emitter_id:
            row["EMITTER_ID"] = emitter_id

    return errors


def _infer_emitters_from_market(
    eligible_bonds: list[dict[str, Any]],
    bonds_market: list[dict[str, Any]],
    shares_market: list[dict[str, Any]],
    secid_to_emitter: dict[str, str],
) -> set[str]:
    by_secid: dict[str, str] = {}
    by_isin: dict[str, str] = {}

    for row in [*bonds_market, *shares_market]:
        emitter_id = _normalize_emitter_id(
            row.get("EMITTER_ID")
            or row.get("ISSUER_ID")
            or row.get("EMITTERID")
            or row.get("ISSUERID")
            or ""
        )
        if not emitter_id:
            continue
        secid = str(row.get("SECID") or "").strip()
        isin = str(row.get("ISIN") or "").strip()
        if secid:
            by_secid[secid] = emitter_id
        if isin:
            by_isin[isin] = emitter_id

    inferred: set[str] = set()
    for bond in eligible_bonds:
        secid = str(bond.get("SECID") or "").strip()
        isin = str(bond.get("ISIN") or "").strip()
        emitter_id = _normalize_emitter_id(
            bond.get("EMITTER_ID")
            or bond.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or by_secid.get(secid, "")
            or by_isin.get(isin, "")
            or ""
        )
        if emitter_id:
            inferred.add(emitter_id)

    return inferred


def _collect_bond_isins_by_emitter(eligible_bonds: list[dict[str, Any]], secid_to_emitter: dict[str, str]) -> dict[str, list[str]]:
    by_emitter: dict[str, set[str]] = {}
    for bond in eligible_bonds:
        secid = str(bond.get("SECID") or "").strip()
        isin = str(bond.get("ISIN") or "").strip()
        emitter_id = _normalize_emitter_id(
            bond.get("EMITTER_ID")
            or bond.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or ""
        )
        if not emitter_id or not isin:
            continue
        by_emitter.setdefault(emitter_id, set()).add(isin)
    return {emitter_id: sorted(isins) for emitter_id, isins in by_emitter.items()}


def _count_bonds_without_emitter(eligible_bonds: list[dict[str, Any]], secid_to_emitter: dict[str, str]) -> int:
    unresolved = 0
    for bond in eligible_bonds:
        secid = str(bond.get("SECID") or "").strip()
        emitter_id = _normalize_emitter_id(
            bond.get("EMITTER_ID")
            or bond.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or ""
        )
        if not emitter_id:
            unresolved += 1
    return unresolved


def _count_bonds_without_isin(eligible_bonds: list[dict[str, Any]]) -> int:
    return sum(1 for bond in eligible_bonds if not str(bond.get("ISIN") or "").strip())


def _normalize_emitter_id(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if value.endswith(".0"):
        integer_part = value[:-2]
        if integer_part.isdigit():
            return integer_part
    return value
