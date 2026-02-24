from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bond_screener.db import init_db, make_session_factory
from bond_screener.runtime import ensure_default_configs, ensure_runtime_dirs, load_config, setup_logging
from bond_screener.screening import build_screen_rows, export_screen_to_excel


def main() -> int:
    started = time.perf_counter()

    dirs = ensure_runtime_dirs(PROJECT_ROOT)
    ensure_default_configs(dirs["config"])
    config = load_config(dirs["config"])

    log_file = PROJECT_ROOT / str(config.get("logging", {}).get("file", "logs/latest.log"))
    logger = setup_logging(log_file, config.get("logging", {}).get("level", "INFO"))

    database_path = PROJECT_ROOT / str(config.get("database", {}).get("path", "data/bond_screener.sqlite"))
    screen_output = PROJECT_ROOT / str(config.get("output", {}).get("screen_basic_excel", "out/screen_basic.xlsx"))

    processed = 0
    filtered = 0
    errors = 0

    logger.info("Запуск basic-скринера")
    logger.info("Этап 1/3: инициализация БД")
    init_db(database_path)

    try:
        logger.info("Этап 2/3: расчет screen_pass / screen_drop")
        session_factory = make_session_factory(database_path)
        pass_rows, drop_rows = build_screen_rows(session_factory)
        processed = len(pass_rows) + len(drop_rows)
        filtered = len(drop_rows)

        logger.info("Этап 3/3: выгрузка Excel в %s", screen_output)
        export_screen_to_excel(pass_rows, drop_rows, screen_output)
    except Exception:
        errors += 1
        logger.exception("Скрининг завершился ошибкой")
        return 1
    finally:
        elapsed = time.perf_counter() - started
        print("\nГотово.")
        print(f"Сводка: обработано бумаг={processed}, отфильтровано={filtered}, ошибок={errors}.")
        print(f"Время выполнения: {elapsed:.2f} сек.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
