from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from sqlalchemy import Date, DateTime, Float, PrimaryKeyConstraint, String, Text, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    """Базовый класс SQLAlchemy моделей."""


class Instrument(Base):
    __tablename__ = "instruments"

    isin: Mapped[str] = mapped_column(String(12), primary_key=True)
    secid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    shortname: Mapped[str | None] = mapped_column(String(512), nullable=True)
    name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    primary_boardid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    board: Mapped[str | None] = mapped_column(String(64), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    issuer_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tags_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class InstrumentField(Base):
    __tablename__ = "instrument_fields"
    __table_args__ = (PrimaryKeyConstraint("isin", "field", name="pk_instrument_fields"),)

    isin: Mapped[str] = mapped_column(String(12), nullable=False)
    field: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class Issuer(Base):
    __tablename__ = "issuers"

    issuer_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    inn: Mapped[str | None] = mapped_column(String(12), nullable=True)
    name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    group_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class IssuerField(Base):
    __tablename__ = "issuer_fields"
    __table_args__ = (PrimaryKeyConstraint("issuer_key", "field", name="pk_issuer_fields"),)

    issuer_key: Mapped[str] = mapped_column(String(128), nullable=False)
    field: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class Cashflow(Base):
    __tablename__ = "cashflows"
    __table_args__ = (PrimaryKeyConstraint("isin", "date", "kind", name="pk_cashflows"),)

    isin: Mapped[str] = mapped_column(String(12), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class Offer(Base):
    __tablename__ = "offers"
    __table_args__ = (PrimaryKeyConstraint("isin", "offer_date", "offer_type", name="pk_offers"),)

    isin: Mapped[str] = mapped_column(String(12), nullable=False)
    offer_date: Mapped[date] = mapped_column(Date, nullable=False)
    offer_type: Mapped[str] = mapped_column(String(64), nullable=False)
    offer_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class Rating(Base):
    __tablename__ = "ratings"
    __table_args__ = (PrimaryKeyConstraint("scope", "key", "agency", name="pk_ratings"),)

    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    agency: Mapped[str] = mapped_column(String(64), nullable=False)
    rating: Mapped[str | None] = mapped_column(String(32), nullable=True)
    outlook: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rating_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)


class Publication(Base):
    __tablename__ = "publications"
    __table_args__ = (PrimaryKeyConstraint("scope", "key", "kind", "hash", name="pk_publications"),)

    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    title: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    hash: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)


class Snapshot(Base):
    __tablename__ = "snapshots"
    __table_args__ = (PrimaryKeyConstraint("run_id", "isin", name="pk_snapshots"),)

    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    isin: Mapped[str] = mapped_column(String(12), nullable=False)
    computed_fields_json: Mapped[str | None] = mapped_column(Text, nullable=True)


def create_sqlite_engine(db_path: Path | str):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite+pysqlite:///{db_path}", future=True)


def init_db(db_path: Path | str) -> None:
    engine = create_sqlite_engine(db_path)
    Base.metadata.create_all(engine)
    _migrate_sqlite_schema(engine)


def _migrate_sqlite_schema(engine) -> None:
    """Лёгкие миграции для уже существующих SQLite-файлов без Alembic."""
    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='instruments'")
        ).fetchone()
        if not table_exists:
            return

        rows = conn.execute(text("PRAGMA table_info(instruments)")).fetchall()
        existing_columns = {str(row[1]) for row in rows}

        missing_column_defs = {
            "shortname": "VARCHAR(512)",
            "primary_boardid": "VARCHAR(64)",
            "board": "VARCHAR(64)",
        }

        for column, sql_type in missing_column_defs.items():
            if column in existing_columns:
                continue
            conn.execute(text(f"ALTER TABLE instruments ADD COLUMN {column} {sql_type}"))


def make_session_factory(db_path: Path | str) -> sessionmaker:
    engine = create_sqlite_engine(db_path)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
