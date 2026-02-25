"""Формирование справочника эмитентов по итоговым облигациям."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .moex_client import MoexClient
from .state_store import ScreenerStateStore


@dataclass(slots=True)
class EmitentsBuildResult:
    rows: list[dict[str, str]]
    errors: int
    processed_emitters: int
    new_emitters: int


def build_emitents_reference(
    eligible_bonds: list[dict[str, Any]],
    client: MoexClient,
    state_store: ScreenerStateStore,
) -> EmitentsBuildResult:
    """Собирает справочник эмитентов для итоговых облигаций.

    Полное наименование и ИНН кэшируются в состоянии и обновляются
    только для новых эмитентов. Списки торгуемых акций/облигаций
    пересчитываются на каждом запуске.
    """

    registry = state_store.load_emitents_registry()
    secid_samples = _pick_emitter_samples(eligible_bonds)
    discovered_emitters: set[str] = set()
    errors = 0
    new_emitters = 0
    registry_changed = False

    for secid in sorted(secid_samples):
        emitter_id = secid_samples[secid]
        cached = registry.get(emitter_id) if emitter_id else None
        if emitter_id and cached and cached.get("full_name") and cached.get("inn"):
            discovered_emitters.add(emitter_id)
            continue

        details, fetch_errors = client.fetch_security_description(secid)
        errors += fetch_errors
        if fetch_errors:
            continue

        if not emitter_id:
            emitter_id = str(details.get("EMITTER_ID") or details.get("ISSUER_ID") or "").strip()
        if not emitter_id:
            continue
        discovered_emitters.add(emitter_id)

        cached = registry.get(emitter_id)
        if cached and cached.get("full_name") and cached.get("inn"):
            continue

        full_name = str(details.get("EMITTER_FULL_NAME") or "").strip()
        inn = str(details.get("EMITTER_INN") or details.get("INN") or "").strip()
        if not full_name and cached:
            full_name = str(cached.get("full_name") or "")
        if not inn and cached:
            inn = str(cached.get("inn") or "")

        if not full_name and not inn:
            continue

        if emitter_id not in registry:
            new_emitters += 1
        registry_changed = True
        registry[emitter_id] = {
            "full_name": full_name,
            "inn": inn,
        }

    if registry_changed:
        state_store.save_emitents_registry(registry)

    bonds_market, bonds_errors = client.fetch_market_securities("bonds")
    shares_market, shares_errors = client.fetch_market_securities("shares")
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
    )


def _pick_emitter_samples(eligible_bonds: list[dict[str, Any]]) -> dict[str, str]:
    secid_to_emitter: dict[str, str] = {}
    sampled_emitters: set[str] = set()
    for bond in eligible_bonds:
        secid = str(bond.get("SECID") or "").strip()
        emitter_id = str(bond.get("EMITTER_ID") or bond.get("ISSUER_ID") or "").strip()
        if not secid:
            continue

        if not emitter_id:
            secid_to_emitter[secid] = ""
            continue

        if emitter_id in sampled_emitters:
            continue

        secid_to_emitter[secid] = emitter_id
        sampled_emitters.add(emitter_id)
    return secid_to_emitter


def _collect_market_instruments(rows: list[dict[str, Any]], instrument_key: str) -> dict[str, list[str]]:
    instruments: dict[str, set[str]] = {}
    for row in rows:
        emitter_id = str(row.get("EMITTER_ID") or "").strip()
        instrument = str(row.get(instrument_key) or "").strip()
        if not emitter_id or not instrument:
            continue
        instruments.setdefault(emitter_id, set()).add(instrument)

    return {key: sorted(values) for key, values in instruments.items()}
