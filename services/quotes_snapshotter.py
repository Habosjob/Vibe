from datetime import datetime

from MOEX_API import build_retry_session, init_db, run_quotes_snapshotter, setup_logging


def main() -> int:
    logger = setup_logging()
    init_db()
    session = build_retry_session()
    run_quotes_snapshotter(
        session,
        logger,
        run_id=datetime.now().strftime("%Y%m%dT%H%M%S"),
        source="snapshotter",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
