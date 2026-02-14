from MOEX_API import build_retry_session, init_db, run_rates_ingest, setup_logging
from datetime import datetime


def main() -> int:
    logger = setup_logging()
    init_db()
    session = build_retry_session()
    run_rates_ingest(session, logger, run_id=datetime.now().strftime("%Y%m%dT%H%M%S"), debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
