"""Отдельный запуск формирования справочника эмитентов MOEX."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from moex_bond_screener.config import load_config
from moex_bond_screener.emitents import build_emitents_reference
from moex_bond_screener.logging_utils import setup_logging
from moex_bond_screener.moex_client import MoexClient
from moex_bond_screener.raw_store import RawStore
from moex_bond_screener.state_store import ScreenerStateStore
from moex_bond_screener.writer import save_emitents_excel


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

    result = build_emitents_reference(eligible_bonds=eligible_bonds, client=client, state_store=state_store)
    save_emitents_excel(config.emitents_output_file, result.rows)

    print("Готово: сформирован справочник эмитентов")
    print(f"Эмитентов: {result.processed_emitters}, новых: {result.new_emitters}, ошибок: {result.errors}")
    print(f"Файл: {config.emitents_output_file}")


if __name__ == "__main__":
    main()
