from __future__ import annotations

import asyncio
import logging
import random
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bond_screener.db import Cashflow, Instrument, InstrumentField, Offer, init_db, make_session_factory
from bond_screener.http_client import AsyncHttpClient, DomainPolicy
from bond_screener.providers.moex_cashflows import (
    MoexCashflowProvider,
    apply_offer_fields,
    derive_fields,
    save_cashflows_batch_to_db,
    save_derived_fields_batch_to_db,
    save_offers_batch_to_db,
)
from bond_screener.runtime import ensure_default_configs, ensure_runtime_dirs, load_config, setup_logging


async def _run_sync(base_dir: Path, logger: logging.Logger) -> tuple[int, int, int]:
    config = load_config(base_dir / "config")
    provider_cfg = config.get("providers", {}).get("moex_iss", {})
    database_path = base_dir / str(config.get("database", {}).get("path", "data/bond_screener.sqlite"))

    init_db(database_path)
    session_factory = make_session_factory(database_path)

    with session_factory() as session:
        instruments = session.execute(select(Instrument.isin, Instrument.secid)).all()

    logger.info("Этап 1/4: подготовка списка бумаг (%s шт)", len(instruments))
    concurrency = max(1, int(provider_cfg.get("cashflows_concurrency", 5)))
    rate_limit = float(provider_cfg.get("rate_limit_per_sec", 2.0))

    collected: dict[str, list] = {}
    collected_offers: dict[str, list] = {}
    errors = 0
    processed = 0

    logger.info("Этап 2/4: загрузка расписания платежей MOEX ISS")
    async with AsyncHttpClient(
        cache_db_path=base_dir / "cache" / "http_cache.sqlite",
        cache_ttl_seconds=int(provider_cfg.get("cashflows_cache_ttl_seconds", 86400)),
        domain_policies={"iss.moex.com": DomainPolicy(rate_limit_per_sec=rate_limit, max_concurrency=concurrency)},
        debug_raw_enabled=bool(config.get("raw", {}).get("enabled", False)),
        raw_dir=base_dir / "raw",
    ) as http_client:
        provider = MoexCashflowProvider(http_client)
        semaphore = asyncio.Semaphore(concurrency)

        async def worker(isin: str, secid: str | None) -> None:
            nonlocal errors, processed
            security_id = (secid or isin).strip()
            try:
                async with semaphore:
                    rows, offer_rows = await provider.fetch_cashflows_and_offers(secid=security_id, isin=isin)
                collected[isin] = rows
                collected_offers[isin] = offer_rows
            except Exception:
                errors += 1
                logger.exception("Ошибка загрузки cashflows: isin=%s secid=%s", isin, security_id)
            finally:
                processed += 1
                if processed % 100 == 0 or processed == len(instruments):
                    logger.info("Прогресс: обработано=%s/%s", processed, len(instruments))

        await asyncio.gather(*(worker(isin, secid) for isin, secid in instruments))

    logger.info("Этап 3/4: сохранение cashflows и derived полей в SQLite")
    saved_cashflows = save_cashflows_batch_to_db(session_factory, cashflows_by_isin=collected, source="moex_iss")
    saved_offers = save_offers_batch_to_db(session_factory, offers_by_isin=collected_offers, source="moex_iss")
    derived_by_isin = {
        isin: apply_offer_fields(derive_fields(rows), collected_offers.get(isin, [])) for isin, rows in collected.items()
    }
    saved_derived = save_derived_fields_batch_to_db(
        session_factory,
        derived_by_isin=derived_by_isin,
        source="derived_from_moex_cashflows",
    )
    logger.info("Сохранено: cashflows=%s offers=%s derived=%s", saved_cashflows, saved_offers, saved_derived)

    logger.info("Этап 4/4: формирование sample Excel-файлов")
    sample_isins = sorted(collected.keys())
    random.shuffle(sample_isins)
    sample_isins = sample_isins[:5]

    with session_factory() as session:
        cf_rows = session.execute(
            select(Cashflow).where(Cashflow.isin.in_(sample_isins)).order_by(Cashflow.isin, Cashflow.date, Cashflow.kind)
        ).scalars()
        cashflow_records = [
            {
                "isin": row.isin,
                "date": row.date.isoformat(),
                "kind": row.kind,
                "amount": row.amount,
                "rate": row.rate,
                "source": row.source,
            }
            for row in cf_rows
        ]
        derived_rows = session.execute(
            select(InstrumentField)
            .where(InstrumentField.isin.in_(sample_isins), InstrumentField.field.in_([
                "maturity_date",
                "next_coupon_date",
                "next_offer_date",
                "amort_start_date",
                "has_amortization",
            ]))
            .order_by(InstrumentField.isin, InstrumentField.field)
        ).scalars()
        derived_records = [{"isin": r.isin, "field": r.field, "value": r.value, "source": r.source} for r in derived_rows]
        offer_rows = session.execute(
            select(Offer).where(Offer.isin.in_(sample_isins)).order_by(Offer.isin, Offer.offer_date, Offer.offer_type)
        ).scalars()
        offer_records = [
            {
                "isin": row.isin,
                "offer_date": row.offer_date.isoformat(),
                "offer_type": row.offer_type,
                "offer_price": row.offer_price,
                "source": row.source,
            }
            for row in offer_rows
        ]

    (base_dir / "out").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cashflow_records).to_excel(base_dir / "out" / "cashflows_sample.xlsx", index=False)
    pd.DataFrame(derived_records).to_excel(base_dir / "out" / "derived_sample.xlsx", index=False)
    pd.DataFrame(offer_records).to_excel(base_dir / "out" / "offers_sample.xlsx", index=False)

    return len(instruments), saved_cashflows, errors


def main() -> int:
    started = time.perf_counter()

    dirs = ensure_runtime_dirs(PROJECT_ROOT)
    ensure_default_configs(dirs["config"])
    config = load_config(dirs["config"])

    log_file = PROJECT_ROOT / str(config.get("logging", {}).get("file", "logs/latest.log"))
    logger = setup_logging(log_file, config.get("logging", {}).get("level", "INFO"))

    logger.info("Запуск sync_moex_cashflows")
    processed = 0
    saved = 0
    errors = 0
    try:
        processed, saved, errors = asyncio.run(_run_sync(PROJECT_ROOT, logger))
    except Exception:
        logger.exception("Синхронизация MOEX cashflows завершилась ошибкой")
        return 1
    finally:
        elapsed = time.perf_counter() - started
        print("\nГотово.")
        print(f"Сводка: обработано бумаг={processed}, отфильтровано=0, ошибок={errors}, сохранено cashflows={saved}.")
        print(f"Время выполнения: {elapsed:.2f} сек.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
