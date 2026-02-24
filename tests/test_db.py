from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import select

from bond_screener.db import (
    Cashflow,
    Instrument,
    InstrumentField,
    Issuer,
    IssuerField,
    Offer,
    Publication,
    Rating,
    Snapshot,
    init_db,
    make_session_factory,
)


def test_db_create_insert_read(tmp_path: Path) -> None:
    db_path = tmp_path / "bond_screener.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    with session_factory() as session:
        session.add(
            Instrument(
                isin="RU000A000001",
                secid="BOND1",
                name="Тестовая облигация",
                currency="RUB",
                issuer_key="issuer-1",
                tags_json='["corp", "test"]',
                updated_at=datetime(2026, 2, 24, 10, 0, 0),
            )
        )
        session.add(
            InstrumentField(
                isin="RU000A000001",
                field="duration",
                value="3.5",
                source="manual",
                confidence=0.9,
                fetched_at=datetime(2026, 2, 24, 10, 1, 0),
            )
        )
        session.add(
            Issuer(
                issuer_key="issuer-1",
                inn="7700000000",
                name="ООО Тест",
                group_key="group-1",
                updated_at=datetime(2026, 2, 24, 10, 2, 0),
            )
        )
        session.add(
            IssuerField(
                issuer_key="issuer-1",
                field="leverage",
                value="1.2",
                source="manual",
                confidence=0.8,
                fetched_at=datetime(2026, 2, 24, 10, 3, 0),
            )
        )
        session.add(
            Cashflow(
                isin="RU000A000001",
                date=date(2026, 12, 31),
                kind="coupon",
                amount=35.5,
                rate=10.5,
                source="moex",
                fetched_at=datetime(2026, 2, 24, 10, 4, 0),
            )
        )
        session.add(
            Offer(
                isin="RU000A000001",
                offer_date=date(2027, 1, 15),
                offer_type="put",
                offer_price=100.0,
                source="moex",
                fetched_at=datetime(2026, 2, 24, 10, 5, 0),
            )
        )
        session.add(
            Rating(
                scope="bond",
                key="RU000A000001",
                agency="ACRA",
                rating="A",
                outlook="stable",
                rating_date=date(2026, 2, 20),
                source="agency",
            )
        )
        session.add(
            Publication(
                scope="issuer",
                key="issuer-1",
                kind="news",
                published_at=datetime(2026, 2, 21, 12, 0, 0),
                title="Новости эмитента",
                url="https://example.com/news/1",
                hash="news_hash_1",
                source="news_feed",
            )
        )
        session.add(
            Snapshot(
                run_id="run-001",
                isin="RU000A000001",
                computed_fields_json='{"ytm": 12.3}',
            )
        )
        session.commit()

    with session_factory() as session:
        instrument = session.scalar(select(Instrument).where(Instrument.isin == "RU000A000001"))
        rating = session.scalar(select(Rating).where(Rating.key == "RU000A000001"))
        snapshot = session.scalar(select(Snapshot).where(Snapshot.run_id == "run-001"))

    assert instrument is not None
    assert instrument.name == "Тестовая облигация"
    assert rating is not None
    assert rating.agency == "ACRA"
    assert snapshot is not None
    assert snapshot.computed_fields_json == '{"ytm": 12.3}'


def test_init_db_migrates_legacy_instruments_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE instruments (
            isin VARCHAR(12) PRIMARY KEY,
            secid VARCHAR(64),
            name VARCHAR(512),
            currency VARCHAR(16),
            issuer_key VARCHAR(128),
            tags_json TEXT,
            updated_at DATETIME
        )
        """
    )
    conn.execute("INSERT INTO instruments(isin, secid, name) VALUES ('RU000A000999', 'OLD', 'legacy')")
    conn.commit()
    conn.close()

    init_db(db_path)

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(instruments)").fetchall()}
    conn.close()

    assert "shortname" in columns
    assert "primary_boardid" in columns
    assert "board" in columns

    session_factory = make_session_factory(db_path)
    with session_factory() as session:
        existing = session.scalar(select(Instrument).where(Instrument.isin == "RU000A000999"))

    assert existing is not None
