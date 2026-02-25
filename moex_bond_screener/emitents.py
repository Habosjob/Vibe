"""Формирование справочника эмитентов по итоговым облигациям."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable

from .moex_client import MoexClient
from .state_store import ScreenerStateStore


@dataclass(slots=True)
class EmitentsBuildResult:
    rows: list[dict[str, str]]
    errors: int
    processed_emitters: int
    new_emitters: int
    stage_durations: dict[str, float]


def build_emitents_reference(
    eligible_bonds: list[dict[str, Any]],
    client: MoexClient,
    state_store: ScreenerStateStore,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> EmitentsBuildResult:
    """Собирает справочник эмитентов для итоговых облигаций.

    Полное наименование и ИНН кэшируются в состоянии и обновляются
    только для новых эмитентов. Списки торгуемых акций/облигаций
    пересчитываются на каждом запуске.
    """

    registry = state_store.load_emitents_registry()
    secid_to_emitter = state_store.load_secid_to_emitter_map()
    secid_samples = _pick_emitter_samples(eligible_bonds, secid_to_emitter)
    total_samples = len(secid_samples.known_emitters) + len(secid_samples.unknown_emitters)
    discovered_emitters: set[str] = set()
    errors = 0
    new_emitters = 0
    registry_changed = False
    secid_cache_changed = False
    stage_durations = {
        "emitents_cards_seconds": 0.0,
        "emitents_market_bonds_seconds": 0.0,
        "emitents_market_shares_seconds": 0.0,
    }
    workers = max(1, int(client.config.amortization_workers))

    if progress_callback:
        progress_callback(
            {
                "phase": "sample_descriptions",
                "processed": 0,
                "total": total_samples,
                "message": "Сбор описаний эмитентов по итоговым бумагам",
            }
        )

    processed_samples = 0
    pending_samples: list[tuple[str, str]] = []

    for emitter_id, secid in sorted(secid_samples.known_emitters.items()):
        cached = registry.get(emitter_id)
        if cached and cached.get("full_name") and cached.get("inn"):
            discovered_emitters.add(emitter_id)
            processed_samples += 1
            if progress_callback:
                progress_callback(
                    {
                        "phase": "sample_descriptions",
                        "processed": processed_samples,
                        "total": total_samples,
                    }
                )
            continue

        pending_samples.append((secid, emitter_id))

    pending_samples.extend((secid, "") for secid in sorted(secid_samples.unknown_emitters))

    cards_started = perf_counter()
    if pending_samples:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(client.fetch_security_description, secid): (secid, emitter_id)
                for secid, emitter_id in pending_samples
            }
            for future in as_completed(futures):
                secid, emitter_id = futures[future]
                try:
                    details, fetch_errors = future.result()
                except Exception:  # noqa: BLE001
                    details, fetch_errors = {}, 1
                errors += fetch_errors
                processed_samples += 1

                if fetch_errors:
                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "sample_descriptions",
                                "processed": processed_samples,
                                "total": total_samples,
                            }
                        )
                    continue

                resolved_emitter_id = emitter_id or str(details.get("EMITTER_ID") or details.get("ISSUER_ID") or "").strip()
                if not resolved_emitter_id:
                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "sample_descriptions",
                                "processed": processed_samples,
                                "total": total_samples,
                            }
                        )
                    continue

                if secid_to_emitter.get(secid) != resolved_emitter_id:
                    secid_to_emitter[secid] = resolved_emitter_id
                    secid_cache_changed = True
                discovered_emitters.add(resolved_emitter_id)

                cached = registry.get(resolved_emitter_id)
                if cached and cached.get("full_name") and cached.get("inn"):
                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "sample_descriptions",
                                "processed": processed_samples,
                                "total": total_samples,
                            }
                        )
                    continue

                full_name = str(details.get("EMITTER_FULL_NAME") or "").strip()
                inn = str(details.get("EMITTER_INN") or details.get("INN") or "").strip()
                if not full_name and cached:
                    full_name = str(cached.get("full_name") or "")
                if not inn and cached:
                    inn = str(cached.get("inn") or "")

                if not full_name and not inn:
                    if progress_callback:
                        progress_callback(
                            {
                                "phase": "sample_descriptions",
                                "processed": processed_samples,
                                "total": total_samples,
                            }
                        )
                    continue

                if resolved_emitter_id not in registry:
                    new_emitters += 1
                registry_changed = True
                registry[resolved_emitter_id] = {
                    "full_name": full_name,
                    "inn": inn,
                }

                if progress_callback:
                    progress_callback(
                        {
                            "phase": "sample_descriptions",
                            "processed": processed_samples,
                            "total": total_samples,
                        }
                    )

    stage_durations["emitents_cards_seconds"] = round(perf_counter() - cards_started, 2)

    if registry_changed:
        state_store.save_emitents_registry(registry)
    if secid_cache_changed:
        state_store.save_secid_to_emitter_map(secid_to_emitter)

    bonds_started = perf_counter()
    bonds_market = state_store.load_market_cache("bonds") or []
    bonds_errors = 0
    if not bonds_market:
        if progress_callback:
            progress_callback({"phase": "market_data", "message": "Загрузка рыночных инструментов bonds"})
        bonds_market, bonds_errors = client.fetch_market_securities("bonds")
        if bonds_market:
            state_store.save_market_cache("bonds", bonds_market)
    elif progress_callback:
        progress_callback({"phase": "market_data", "message": "Используем кэш рыночных инструментов bonds"})
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

    bond_map = _collect_market_instruments(bonds_market, instrument_key="ISIN")
    share_map = _collect_market_instruments(shares_market, instrument_key="SECID")

    rows: list[dict[str, str]] = []
    for emitter_id in sorted(discovered_emitters):
        details = registry.get(emitter_id, {})
        rows.append(
            {
                "Полное наименование": str(details.get("full_name") or ""),
                "ИНН": str(details.get("inn") or ""),
                "Тикеры акций": ", ".join(share_map.get(emitter_id, [])),
                "ISIN облигаций": ", ".join(bond_map.get(emitter_id, [])),
            }
        )

    rows.sort(key=lambda item: (item["Полное наименование"], item["ИНН"]))
    return EmitentsBuildResult(
        rows=rows,
        errors=errors,
        processed_emitters=len(discovered_emitters),
        new_emitters=new_emitters,
        stage_durations=stage_durations,
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
        emitter_id = str(
            bond.get("EMITTER_ID")
            or bond.get("ISSUER_ID")
            or secid_to_emitter.get(secid, "")
            or ""
        ).strip()
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
        emitter_id = str(row.get("EMITTER_ID") or "").strip()
        instrument = str(row.get(instrument_key) or "").strip()
        if not emitter_id or not instrument:
            continue
        instruments.setdefault(emitter_id, set()).add(instrument)

    return {key: sorted(values) for key, values in instruments.items()}
