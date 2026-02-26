"""Отдельный запуск формирования справочника эмитентов MOEX."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from moex_bond_screener.config import load_config
from moex_bond_screener.emitents import build_emitents_reference
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.exclusion_rules import HASDEFAULT_RULE_NAME
from moex_bond_screener.moex_client import MoexClient
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.state_store import ScreenerStateStore
from moex_bond_screener.writer import load_emitents_manual_overrides, save_emitents_excel


def main() -> None:
    logger = setup_logging()
    config = load_config()
    raw_store = RawStore("raw")
    state_store = ScreenerStateStore(
        config.exclusions_state_dir,
        storage_backend=config.storage_backend,
        sqlite_db_path=config.sqlite_db_path,
    )
    client = MoexClient(config=config, logger=logger, raw_store=raw_store)

    eligible_bonds = state_store.load_eligible_bonds()
    if not eligible_bonds:
        print("Нет данных в хранилище eligible_bonds. Сначала запустите run.py")
        return

    manual_overrides = load_emitents_manual_overrides(config.emitents_output_file)
    secid_to_emitter_map = state_store.load_secid_to_emitter_map()
    forced_blacklist_emitters, secid_to_emitter_map = _collect_forced_blacklist_emitters_from_history(
        exclusions_history=state_store.load_exclusions_history(),
        secid_to_emitter_map=secid_to_emitter_map,
        client=client,
    )
    state_store.save_secid_to_emitter_map(secid_to_emitter_map)

    result = build_emitents_reference(
        eligible_bonds=eligible_bonds,
        client=client,
        state_store=state_store,
        manual_overrides=manual_overrides,
        forced_blacklist_emitters=forced_blacklist_emitters,
    )
    save_emitents_excel(config.emitents_output_file, result.rows)

    print("Готово: сформирован справочник эмитентов")
    print(f"Эмитентов: {result.processed_emitters}, новых: {result.new_emitters}, ошибок: {result.errors}")
    print(f"Файл: {config.emitents_output_file}")


def _collect_forced_blacklist_emitters_from_history(
    exclusions_history: dict[str, dict[str, str]],
    secid_to_emitter_map: dict[str, str],
    client: MoexClient,
) -> tuple[set[str], dict[str, str]]:
    forced_blacklist_emitters: set[str] = set()
    updated_map = dict(secid_to_emitter_map)
    unresolved_hasdefault_secids: set[str] = set()

    for secid, details in exclusions_history.items():
        if str((details or {}).get("last_rule") or "").strip() != HASDEFAULT_RULE_NAME:
            continue

        safe_secid = str(secid or "").strip()
        if not safe_secid:
            continue

        emitter_id = str(updated_map.get(safe_secid, "") or "").strip()
        if emitter_id.endswith(".0") and emitter_id[:-2].isdigit():
            emitter_id = emitter_id[:-2]
        if emitter_id:
            forced_blacklist_emitters.add(emitter_id)
            continue

        unresolved_hasdefault_secids.add(safe_secid)

    for secid in sorted(unresolved_hasdefault_secids):
        details, _errors = client.fetch_security_description(secid)
        emitter_id = str(details.get("EMITTER_ID") or details.get("ISSUER_ID") or "").strip()
        if emitter_id.endswith(".0") and emitter_id[:-2].isdigit():
            emitter_id = emitter_id[:-2]
        if not emitter_id:
            continue
        forced_blacklist_emitters.add(emitter_id)
        updated_map[secid] = emitter_id

    return forced_blacklist_emitters, updated_map


if __name__ == "__main__":
    main()
