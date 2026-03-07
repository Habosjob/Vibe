from __future__ import annotations

import time
from datetime import datetime, timedelta

from tqdm import tqdm

from . import config, db
from .edisclosure import EDisclosureClient
from .excel_export import export_reports_xlsx, export_simple_snapshot
from .helpers import md5_short, parse_date, sanitize_str, setup_logger, timed, today_iso
from .news import NewsCacheManager, SmartlabNewsCollector, build_news_hash
from .portfolio_export import export_portfolio_xlsx
from .portfolio_loader import find_portfolio_file, load_portfolio_items
from .ratings_snapshot import EmitentRow, build_rating_events, load_emitents_xlsx, save_emitents_snapshot_xlsx


def _is_mapping_fresh(last_checked_at: str) -> bool:
    dt = parse_date(last_checked_at)
    if not dt:
        return False
    return dt >= datetime.now() - timedelta(days=config.COMPANY_MAP_TTL_DAYS)


def _build_report_events(conn, logger, emitents: list[EmitentRow]) -> list[dict[str, str]]:
    client = EDisclosureClient(logger)
    events: list[dict[str, str]] = []

    for row in tqdm(emitents, desc="Сбор отчетности", position=0, leave=False):
        inn = sanitize_str(row.inn)
        if not inn:
            continue
        try:
            mapping = db.get_company_map(conn, inn)
            company = None
            if mapping and _is_mapping_fresh(mapping["last_checked_at"]):
                company = {"id": mapping["company_id"], "name": mapping["company_name"], "url": mapping["company_url"]}
            else:
                candidates = client.search_company_by_inn(inn)
                company = client.choose_best_candidate(inn, candidates, row.company_name)
                if company:
                    db.upsert_company_map(conn, inn, company)

            if not company or not company.get("id"):
                logger.info("No company_id for INN=%s", inn)
                continue

            reports = client.get_financial_reports(company["id"])
            if not reports:
                logger.info("No reports for INN=%s company=%s", inn, company.get("id"))
                continue

            latest_report_date = ""
            for rep in reports:
                event_date = rep.get("placement_date") or rep.get("foundation_date")
                if event_date and (not latest_report_date or event_date > latest_report_date):
                    latest_report_date = event_date
                event = {
                    "event_hash": rep["hash"],
                    "inn": inn,
                    "company_name": row.company_name or company.get("name", ""),
                    "scoring_date": row.scoring_date,
                    "event_date": event_date,
                    "event_type": "Опубликована новая отчетность",
                    "event_url": rep.get("file_url") or rep.get("page_url", ""),
                    "source": "e-disclosure",
                    "payload": rep,
                }
                is_new = db.upsert_report_event(conn, event)
                event["is_new"] = is_new
                events.append(event)

            if latest_report_date:
                stale_dt = parse_date(latest_report_date)
                if stale_dt and stale_dt < datetime.now() - timedelta(days=config.REPORT_STALE_DAYS):
                    stale_hash = md5_short(f"stale_{inn}_{latest_report_date}", 16)
                    stale_event = {
                        "event_hash": stale_hash,
                        "inn": inn,
                        "company_name": row.company_name,
                        "scoring_date": row.scoring_date,
                        "event_date": today_iso(),
                        "event_type": "Нет новой отчетности дольше порога",
                        "event_url": company.get("url", ""),
                        "source": "stale-alert",
                        "payload": {"latest_report_date": latest_report_date},
                    }
                    is_new = db.upsert_report_event(conn, stale_event)
                    stale_event["is_new"] = is_new
                    events.append(stale_event)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed INN=%s: %s", inn, exc)
            continue
    return events


def _run() -> dict[str, float]:
    logger = setup_logger()
    conn = db.connect()
    db.bootstrap(conn)

    stage_times: dict[str, float] = {}

    print("=====\nЭтап 1: Загрузка эмитентов")
    emitents, elapsed = timed("emitents", lambda: load_emitents_xlsx(config.EMITENTS_SOURCE_FILE))
    stage_times["Этап 1: Загрузка эмитентов"] = elapsed

    print("Этап 2: Отчетные события")
    report_events, elapsed = timed("reports", lambda: _build_report_events(conn, logger, emitents))
    stage_times["Этап 2: Отчетные события"] = elapsed

    print("Этап 3: Рейтинговые события")
    prev = db.read_emitents_snapshot(conn)
    rating_events, elapsed = timed(
        "ratings",
        lambda: build_rating_events(emitents, {k: dict(v) for k, v in prev.items()}),
    )
    for event in tqdm(rating_events, desc="Сохранение рейтингов", position=0, leave=False):
        event["is_new"] = db.upsert_report_event(conn, event)
    stage_times["Этап 3: Рейтинговые события"] = elapsed

    print("Этап 4: Snapshot эмитентов")
    _, elapsed = timed(
        "snapshot_emitents",
        lambda: (
            db.replace_emitents_snapshot(conn, [e.__dict__ for e in emitents]),
            save_emitents_snapshot_xlsx(emitents),
        ),
    )
    stage_times["Этап 4: Snapshot эмитентов"] = elapsed

    print("Этап 5: Портфель")
    portfolio_file = find_portfolio_file()
    items, elapsed = timed("portfolio_load", lambda: load_portfolio_items(portfolio_file, logger))
    db.save_portfolio_items(conn, items, str(portfolio_file) if portfolio_file else "")
    export_simple_snapshot(
        config.PORTFOLIO_SNAPSHOT_XLSX,
        "portfolio_snapshot",
        ["instrument_type", "instrument_code", "inn", "company_name"],
        [[x.get("instrument_type", ""), x.get("instrument_code", ""), x.get("inn", ""), x.get("company_name", "")] for x in items],
    )
    stage_times["Этап 5: Портфель"] = elapsed

    print("Этап 6: Новости портфеля")

    def _news_stage() -> list[dict[str, str]]:
        news_cache = NewsCacheManager(config.CACHE_DIR / "news" / "news_cache.csv")
        collector = SmartlabNewsCollector(logger)
        _news_rows: list[dict[str, str]] = []
        for item in tqdm(items, desc="Сбор новостей", position=0, leave=False):
            try:
                found = collector.collect_for_item(item)
                for row in found:
                    h = build_news_hash(row["url"], row["title"], row["news_date"])
                    is_new = news_cache.is_new(h)
                    if is_new:
                        news_cache.add(
                            {
                                "hash": h,
                                "company_name": item.get("company_name", ""),
                                "company_inn": item.get("inn", ""),
                                "date": row["news_date"],
                                "title": row["title"],
                                "source": "Smartlab",
                                "url": row["url"],
                                "added_date": today_iso(),
                            }
                        )
                    payload = {
                        "event_hash": h,
                        "instrument_type": item.get("instrument_type", ""),
                        "instrument_code": item.get("instrument_code", ""),
                        "inn": item.get("inn", ""),
                        "company_name": item.get("company_name", ""),
                        "news_date": row["news_date"],
                        "title": row["title"],
                        "url": row["url"],
                        "source": "Smartlab",
                    }
                    db.save_news_event(conn, payload)
                    payload["is_new"] = is_new
                    _news_rows.append(payload)
            except Exception as exc:  # noqa: BLE001
                logger.exception("News failed for item=%s: %s", item, exc)
                continue
        news_cache.save()
        return _news_rows

    news_rows, elapsed = timed("news", _news_stage)
    stage_times["Этап 6: Новости портфеля"] = elapsed

    print("Этап 7: Экспорт Excel")

    def _export_stage() -> None:
        all_events = [dict(r) for r in db.list_report_events(conn)]
        new_hashes = {e["event_hash"] for e in report_events + rating_events if e.get("is_new")}
        for row in all_events:
            row["is_new"] = row.get("event_hash") in new_hashes
        export_reports_xlsx(all_events)

        latest_event_by_inn: dict[str, dict[str, str]] = {}
        for row in sorted(all_events, key=lambda x: x.get("event_date", ""), reverse=True):
            latest_event_by_inn.setdefault(row.get("inn", ""), row)

        latest_news_by_key: dict[tuple[str, str], dict[str, str]] = {}
        for row in sorted(news_rows, key=lambda x: x.get("news_date", ""), reverse=True):
            key = (row.get("instrument_type", ""), row.get("instrument_code", ""))
            latest_news_by_key.setdefault(key, row)

        export_portfolio_xlsx(items, latest_event_by_inn, latest_news_by_key, news_rows)

    _, elapsed = timed("export", _export_stage)
    stage_times["Этап 7: Экспорт Excel"] = elapsed

    conn.close()
    return stage_times


def run_monitoring() -> None:
    started = time.perf_counter()
    stage_times = _run()
    total = time.perf_counter() - started
    print("=====\nSummary")
    for stage, sec in stage_times.items():
        print(f"- {stage}: {sec:.2f} сек")
    print(f"- Итого: {total:.2f} сек")


if __name__ == "__main__":
    run_monitoring()
