from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bond_screener.db import init_db, make_session_factory
from bond_screener.http_client import AsyncHttpClient, DomainPolicy
from bond_screener.providers.moex_iss import MoexIssProvider, save_instruments_to_db
from bond_screener.runtime import ensure_default_configs, ensure_runtime_dirs, load_config, setup_logging


async def _run_sync(base_dir: Path, logger: logging.Logger) -> tuple[int, int]:
    config = load_config(base_dir / "config")
    moex_cfg = config.get("providers", {}).get("moex_iss", {})
    database_path = base_dir / str(config.get("database", {}).get("path", "data/bond_screener.sqlite"))

    out_xlsx = base_dir / "out" / "universe.xlsx"
    out_csv = base_dir / "out" / "universe.csv"

    limit = int(moex_cfg.get("limit", 100))
    search_query = moex_cfg.get("q")

    logger.info("Этап 1/3: загрузка списка облигаций из MOEX ISS")
    async with AsyncHttpClient(
        cache_db_path=base_dir / "cache" / "http_cache.sqlite",
        cache_ttl_seconds=int(moex_cfg.get("cache_ttl_seconds", 1800)),
        domain_policies={"iss.moex.com": DomainPolicy(rate_limit_per_sec=2.0, max_concurrency=1)},
        debug_raw_enabled=bool(config.get("raw", {}).get("enabled", False)),
        raw_dir=base_dir / "raw",
    ) as http_client:
        provider = MoexIssProvider(http_client)

        def progress(current_start: int, total: int) -> None:
            logger.info("MOEX ISS: загружено строк=%s (start=%s)", total, current_start)

        instruments = await provider.fetch_all(limit=limit, q=search_query, progress_cb=progress)

    logger.info("Этап 2/3: сохранение в out/universe.xlsx и out/universe.csv")
    records = [item.as_dict() for item in instruments]
    frame = pd.DataFrame(records, columns=["isin", "secid", "shortname", "primary_boardid", "board", "currency"])
    frame.to_csv(out_csv, index=False, encoding="utf-8-sig")
    frame.to_excel(out_xlsx, index=False)

    logger.info("Этап 3/3: сохранение instruments в SQLite")
    init_db(database_path)
    session_factory = make_session_factory(database_path)
    saved = save_instruments_to_db(session_factory, instruments)
    return len(instruments), saved


def main() -> int:
    started = time.perf_counter()

    dirs = ensure_runtime_dirs(PROJECT_ROOT)
    ensure_default_configs(dirs["config"])
    config = load_config(dirs["config"])

    log_file = PROJECT_ROOT / str(config.get("logging", {}).get("file", "logs/latest.log"))
    logger = setup_logging(log_file, config.get("logging", {}).get("level", "INFO"))

    logger.info("Запуск sync_moex_universe")
    processed = 0
    saved = 0
    errors = 0
    try:
        processed, saved = asyncio.run(_run_sync(PROJECT_ROOT, logger))
    except Exception:
        errors += 1
        logger.exception("Синхронизация MOEX ISS завершилась ошибкой")
        return 1
    finally:
        elapsed = time.perf_counter() - started
        print("\nГотово.")
        print(f"Сводка: обработано бумаг={processed}, отфильтровано=0, ошибок={errors}, сохранено={saved}.")
        print(f"Время выполнения: {elapsed:.2f} сек.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
