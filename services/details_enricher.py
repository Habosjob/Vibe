from datetime import datetime

from MOEX_API import (
    build_retry_session,
    init_db,
    run_details_enricher,
    run_rates_ingest,
    setup_logging,
    _resolve_details_worker_processes,
)


def main() -> int:
    logger = setup_logging()
    init_db()
    session = build_retry_session()
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    dataframe, source, export_date = run_rates_ingest(session, logger, run_id=run_id, debug=False)
    workers = _resolve_details_worker_processes(None, logger)
    run_details_enricher(
        dataframe,
        session,
        logger,
        run_id=run_id,
        source=source,
        export_date=export_date,
        details_worker_processes=workers,
        debug=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
