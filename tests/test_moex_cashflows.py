from __future__ import annotations

from datetime import date
from pathlib import Path

from sqlalchemy import select

from bond_screener.db import Cashflow, InstrumentField, init_db, make_session_factory
from bond_screener.providers.moex_cashflows import (
    CashflowRecord,
    derive_fields,
    parse_cashflows_payload,
    save_cashflows_to_db,
    save_derived_fields_to_db,
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
    }

    rows = parse_cashflows_payload(payload, isin="RU000A000001")

    assert [row.kind for row in rows] == ["coupon", "coupon", "amort", "redemption"]

    derived = derive_fields(rows, today=date(2026, 1, 1))
    assert derived.maturity_date == date(2028, 1, 1)
    assert derived.next_coupon_date == date(2026, 5, 15)
    assert derived.amort_start_date == date(2027, 1, 1)
    assert derived.has_amortization is True


def test_save_cashflows_and_derived_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "bond.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    cashflows = [
        CashflowRecord(isin="RU000A000001", date=date(2026, 5, 1), kind="coupon", amount=20.0, rate=8.0),
        CashflowRecord(isin="RU000A000001", date=date(2027, 5, 1), kind="redemption", amount=1000.0, rate=100.0),
    ]

    saved = save_cashflows_to_db(session_factory, isin="RU000A000001", cashflows=cashflows, source="moex")
    assert saved == 2

    derived = derive_fields(cashflows, today=date(2026, 1, 1))
    fields_saved = save_derived_fields_to_db(
        session_factory,
        isin="RU000A000001",
        derived=derived,
        source="derived",
    )
    assert fields_saved == 4

    with session_factory() as session:
        db_cashflows = session.execute(select(Cashflow).where(Cashflow.isin == "RU000A000001")).scalars().all()
        db_fields = session.execute(select(InstrumentField).where(InstrumentField.isin == "RU000A000001")).scalars().all()

    assert len(db_cashflows) == 2
    by_field = {item.field: item.value for item in db_fields}
    assert by_field["maturity_date"] == "2027-05-01"
    assert by_field["next_coupon_date"] == "2026-05-01"
    assert by_field["amort_start_date"] is None
    assert by_field["has_amortization"] == "0"
