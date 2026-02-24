from __future__ import annotations

from datetime import date
from pathlib import Path

from sqlalchemy import select

from bond_screener.db import Cashflow, InstrumentField, Offer, init_db, make_session_factory
from bond_screener.providers.moex_cashflows import (
    apply_offer_fields,
    CashflowRecord,
    OfferRecord,
    derive_fields,
    parse_cashflows_payload,
    parse_offers_payload,
    save_cashflows_batch_to_db,
    save_cashflows_to_db,
    save_derived_fields_batch_to_db,
    save_derived_fields_to_db,
    save_offers_batch_to_db,
    save_offers_to_db,
)


def test_parse_cashflows_payload_and_derive_fields() -> None:
    payload = {
        "coupons": {
            "columns": ["coupondate", "value", "valueprc"],
            "data": [
                ["2026-05-15", 34.5, 11.2],
                ["2026-11-15", 30.0, 10.8],
            ],
        },
        "amortizations": {
            "columns": ["amortdate", "valueprc", "value"],
            "data": [
                ["2027-01-01", 30.0, 300.0],
                ["2028-01-01", 100.0, 1000.0],
            ],
        },
        "offers": {
            "columns": ["offerdate", "offertype", "price"],
            "data": [["2026-04-10", "put", 100.0]],
        },
    }

    rows = parse_cashflows_payload(payload, isin="RU000A000001")
    offers = parse_offers_payload(payload, isin="RU000A000001")

    assert [row.kind for row in rows] == ["coupon", "coupon", "amort", "redemption"]
    assert offers[0].offer_date == date(2026, 4, 10)

    derived = apply_offer_fields(derive_fields(rows, today=date(2026, 1, 1)), offers, today=date(2026, 1, 1))
    assert derived.maturity_date == date(2028, 1, 1)
    assert derived.next_coupon_date == date(2026, 5, 15)
    assert derived.next_offer_date == date(2026, 4, 10)
    assert derived.amort_start_date == date(2027, 1, 1)
    assert derived.has_amortization is True


def test_derive_fields_uses_amortization_for_maturity() -> None:
    rows = [
        CashflowRecord(isin="RU000A000001", date=date(2025, 8, 28), kind="amort", amount=50.0, rate=5.0),
        CashflowRecord(isin="RU000A000001", date=date(2033, 11, 1), kind="coupon", amount=20.0, rate=8.0),
    ]
    derived = derive_fields(rows, today=date(2025, 1, 1))
    assert derived.maturity_date == date(2025, 8, 28)



def test_save_batch_cashflows_and_derived_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "bond.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    cashflows_by_isin = {
        "RU000A000001": [
            CashflowRecord(isin="RU000A000001", date=date(2026, 5, 1), kind="coupon", amount=20.0, rate=8.0),
            CashflowRecord(isin="RU000A000001", date=date(2027, 5, 1), kind="redemption", amount=1000.0, rate=100.0),
        ],
        "RU000A000002": [
            CashflowRecord(isin="RU000A000002", date=date(2026, 6, 1), kind="coupon", amount=22.0, rate=8.2),
        ],
    }
    offers_by_isin = {
        "RU000A000001": [OfferRecord(isin="RU000A000001", offer_date=date(2026, 4, 15), offer_type="put", offer_price=100.0)],
        "RU000A000002": [],
    }

    saved = save_cashflows_batch_to_db(session_factory, cashflows_by_isin=cashflows_by_isin, source="moex")
    offers_saved = save_offers_batch_to_db(session_factory, offers_by_isin=offers_by_isin, source="moex")
    derived_by_isin = {
        isin: apply_offer_fields(derive_fields(rows, today=date(2026, 1, 1)), offers_by_isin.get(isin, []), today=date(2026, 1, 1))
        for isin, rows in cashflows_by_isin.items()
    }
    fields_saved = save_derived_fields_batch_to_db(session_factory, derived_by_isin=derived_by_isin, source="derived")

    assert saved == 3
    assert offers_saved == 1
    assert fields_saved == 10

    # Повторный запуск с теми же данными не должен размножать строки.
    save_cashflows_batch_to_db(session_factory, cashflows_by_isin=cashflows_by_isin, source="moex")
    save_offers_batch_to_db(session_factory, offers_by_isin=offers_by_isin, source="moex")
    save_derived_fields_batch_to_db(session_factory, derived_by_isin=derived_by_isin, source="derived")

    with session_factory() as session:
        assert len(session.execute(select(Cashflow)).scalars().all()) == 3
        assert len(session.execute(select(Offer)).scalars().all()) == 1
        assert len(session.execute(select(InstrumentField)).scalars().all()) == 10

def test_save_cashflows_and_derived_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "bond.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    cashflows = [
        CashflowRecord(isin="RU000A000001", date=date(2026, 5, 1), kind="coupon", amount=20.0, rate=8.0),
        CashflowRecord(isin="RU000A000001", date=date(2027, 5, 1), kind="redemption", amount=1000.0, rate=100.0),
    ]
    offers = [OfferRecord(isin="RU000A000001", offer_date=date(2026, 4, 15), offer_type="put", offer_price=100.0)]

    saved = save_cashflows_to_db(session_factory, isin="RU000A000001", cashflows=cashflows, source="moex")
    assert saved == 2
    offers_saved = save_offers_to_db(
        session_factory,
        isin="RU000A000001",
        offers=offers,
        source="moex",
    )
    assert offers_saved == 1

    derived = apply_offer_fields(derive_fields(cashflows, today=date(2026, 1, 1)), offers, today=date(2026, 1, 1))
    fields_saved = save_derived_fields_to_db(
        session_factory,
        isin="RU000A000001",
        derived=derived,
        source="derived",
    )
    assert fields_saved == 5

    with session_factory() as session:
        db_cashflows = session.execute(select(Cashflow).where(Cashflow.isin == "RU000A000001")).scalars().all()
        db_fields = session.execute(select(InstrumentField).where(InstrumentField.isin == "RU000A000001")).scalars().all()
        db_offers = session.execute(select(Offer).where(Offer.isin == "RU000A000001")).scalars().all()

    assert len(db_cashflows) == 2
    assert len(db_offers) == 1
    by_field = {item.field: item.value for item in db_fields}
    assert by_field["maturity_date"] == "2027-05-01"
    assert by_field["next_coupon_date"] == "2026-05-01"
    assert by_field["next_offer_date"] == "2026-04-15"
    assert by_field["amort_start_date"] is None
    assert by_field["has_amortization"] == "0"


def test_parse_cashflows_payload_keeps_single_partial_amortization() -> None:
    payload = {
        "amortizations": {
            "columns": ["amortdate", "valueprc", "value"],
            "data": [["2026-02-25", 5.0, 50.0]],
        }
    }

    rows = parse_cashflows_payload(payload, isin="RU000A0ZYAP9")
    assert len(rows) == 1
    assert rows[0].kind == "amort"

    derived = derive_fields(rows, today=date(2026, 1, 1))
    assert derived.amort_start_date == date(2026, 2, 25)
    assert derived.has_amortization is True


def test_parse_offers_payload_supports_datetime_format() -> None:
    payload = {
        "offers": {
            "columns": ["offerdate", "offertype", "price"],
            "data": [["2026-04-10 00:00:00", "put", 100.0]],
        }
    }

    offers = parse_offers_payload(payload, isin="RU000A000001")
    assert offers[0].offer_date == date(2026, 4, 10)
