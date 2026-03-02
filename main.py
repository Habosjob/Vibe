from __future__ import annotations

import asyncio
import logging
import sys

import config
from app.bootstrap import ensure_directories, setup_logging, validate_config
from app.database import Database
from app.pipeline import run_pipeline


def main() -> int:
    ensure_directories()
    errors = validate_config()
    if errors:
        print("Ошибка конфигурации. Исправьте config.py:")
        for err in errors:
            print(f"- {err}")
        return 1

    log_path = setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Запуск скрипта скрининга облигаций")

    db = None
    try:
        db = Database(config.get_db_path())
        summary = asyncio.run(run_pipeline(db))
    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        print("Произошла критическая ошибка. Подробности смотрите в логах.")
        return 1
    finally:
        if db is not None:
            db.close()

    print()
    print(f"Получено облигаций: {summary.fetched_count}")
    print(f"Отобрано по правилам: {summary.selected_count}")
    print(f"Сохранено в Excel: {summary.saved_count}")
    print(f"Скрипт завершил работу за {summary.duration_total:.1f} сек.")
    print("Из них:")
    print(f"  - Загрузка данных: {summary.duration_load:.1f} сек")
    print(f"  - Расчёты: {summary.duration_calc:.1f} сек")
    print(f"  - Сохранение: {summary.duration_save:.1f} сек")
    print(f"Ошибок при загрузке: {summary.errors_count} (подробности в логах)")
    print(f"Итоговый файл: {summary.output_path}")
    print(f"Лог-файл: {log_path}")
    print(f"Взято из кэша/инкрементально: {summary.from_cache_count}")

    logger.info("Скрипт завершен успешно")
    return 0


if __name__ == "__main__":
    sys.exit(main())
