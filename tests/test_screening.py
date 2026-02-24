from __future__ import annotations

from datetime import date
from pathlib import Path

import openpyxl
from sqlalchemy import select

from bond_screener.db import Instrument, InstrumentField, init_db, make_session_factory
from bond_screener.screening import build_screen_rows, classify_bond_class, export_screen_to_excel


def test_classify_bond_class_for_ofz_and_corp() -> None:
    assert classify_bond_class("SU26238RMFS4", "ОФЗ-ПД 26238", '["government"]') == "OFZ"
    assert classify_bond_class("RU000A10", "ПАО Тест БО-01", '["corp"]') == "Corp"


def test_build_screen_rows_excludes_ofz_pk_and_short_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    with session_factory() as session:
        session.add(
            Instrument(
                isin="RU000A000001",
                secid="SU29012RMFS0",
                name="ОФЗ-ПК 29012",
                tags_json='["ofz", "floating_coupon"]',
            )
        )
        session.add(
            Instrument(
                isin="RU000A000002",
                secid="SU52001RMFS2",
                name="ОФЗ-ИН 52001",
                tags_json='["ofz", "inflation"]',
            )
        )
        session.add(
            Instrument(
                isin="RU000A000003",
                secid="CORP1",
                name="ООО Ромашка 1P1",
                tags_json='["corp"]',
            )
        )
        session.add_all(
            [
                InstrumentField(isin="RU000A000001", field="maturity_date", value="2026-12-31"),
                InstrumentField(isin="RU000A000002", field="maturity_date", value="2028-01-01"),
                InstrumentField(isin="RU000A000003", field="maturity_date", value="2026-04-01"),
                InstrumentField(isin="RU000A000003", field="next_offer_date", value="2026-03-01"),
                InstrumentField(isin="RU000A000003", field="amort_start_date", value="2026-02-15"),
            ]
        )
        session.commit()

    pass_rows, drop_rows = build_screen_rows(session_factory, today=date(2026, 1, 1))

    assert [row.isin for row in pass_rows] == ["RU000A000002"]

    drop_by_isin = {row.isin: row.reasons for row in drop_rows}
    assert "ofz_pk_excluded" in drop_by_isin["RU000A000001"]
    assert set(drop_by_isin["RU000A000003"]) == {"maturity_lt_365", "offer_lt_365", "amort_lt_365"}

    with session_factory() as session:
        fields = session.scalars(
            select(InstrumentField).where(InstrumentField.field == "bond_class").order_by(InstrumentField.isin)
        ).all()

    assert [f.value for f in fields] == ["OFZ", "OFZ", "Corp"]


def test_export_screen_to_excel_creates_sheets(tmp_path: Path) -> None:
    output = tmp_path / "screen.xlsx"
    export_screen_to_excel([], [], output)

    wb = openpyxl.load_workbook(output)
    assert wb.sheetnames == ["screen_pass", "screen_drop"]


def test_build_screen_rows_marks_maturity_in_past(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    with session_factory() as session:
        session.add(
            Instrument(
                isin="RU000A000010",
                secid="CORP10",
                name="ПАО Тест 1P10",
                tags_json='["corp"]',
            )
        )
        session.add(InstrumentField(isin="RU000A000010", field="maturity_date", value="2024-01-01"))
        session.commit()

    _, drop_rows = build_screen_rows(session_factory, today=date(2026, 1, 1))
    assert drop_rows[0].reasons == ["maturity_in_past", "maturity_lt_365"]


def test_non_pk_ofz_is_not_excluded_by_secid_pattern(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    with session_factory() as session:
        session.add(
            Instrument(
                isin="RU000A000020",
                secid="SU26207RMFS9",
                name="ОФЗ 26207",
                tags_json='["ofz"]',
            )
        )
        session.add(InstrumentField(isin="RU000A000020", field="maturity_date", value="2030-01-01"))
        session.commit()

    pass_rows, drop_rows = build_screen_rows(session_factory, today=date(2026, 1, 1))
    assert [row.isin for row in pass_rows] == ["RU000A000020"]
    assert drop_rows == []
