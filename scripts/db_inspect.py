from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bond_screener.db import Base, create_sqlite_engine


DEFAULT_DB_PATH = Path("data/bond_screener.sqlite")


def inspect_db(db_path: Path) -> list[tuple[str, int]]:
    engine = create_sqlite_engine(db_path)
    Base.metadata.create_all(engine)

    stats: list[tuple[str, int]] = []
    with engine.connect() as conn:
        for table_name in sorted(Base.metadata.tables):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            stats.append((table_name, int(count.scalar_one())))
    return stats


def main() -> int:
    db_path = DEFAULT_DB_PATH
    print(f"SQLite DB: {db_path.resolve()}")
    for table, rows in inspect_db(db_path):
        print(f"- {table}: {rows} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
