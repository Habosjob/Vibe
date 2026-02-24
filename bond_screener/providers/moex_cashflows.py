from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker

from bond_screener.db import Cashflow, InstrumentField
from bond_screener.http_client import AsyncHttpClient

MOEX_BONDIZATION_URL = "https://iss.moex.com/iss/securities/{secid}/bondization.json"


@dataclass(slots=True)
class CashflowRecord:
    isin: str
    date: date
    kind: str
    amount: float | None
    rate: float | None


@dataclass(slots=True)
class DerivedFields:
    maturity_date: date | None
    next_coupon_date: date | None
    amort_start_date: date | None
    has_amortization: bool


class MoexCashflowProvider:
    def __init__(self, http_client: AsyncHttpClient) -> None:
        self.http_client = http_client

    async def fetch_cashflows(self, *, secid: str, isin: str) -> list[CashflowRecord]:
        response = await self.http_client.request(
            "GET",
            MOEX_BONDIZATION_URL.format(secid=secid),
            params={"iss.meta": "off"},
            provider="moex_cashflows",
        )
        response.raise_for_status()
        payload = response.json()
        return parse_cashflows_payload(payload, isin=isin)


def parse_cashflows_payload(payload: dict[str, Any], *, isin: str) -> list[CashflowRecord]:
    rows: list[CashflowRecord] = []

    coupon_rows = _parse_block(payload, block_name="coupons", default_kind="coupon", isin=isin)
    amort_rows = _parse_block(payload, block_name="amortizations", default_kind="amort", isin=isin)
    redemption_rows = _parse_block(payload, block_name="redemptions", default_kind="redemption", isin=isin)

    rows.extend(coupon_rows)
    rows.extend(_split_amort_and_redemption(amort_rows))
    rows.extend(redemption_rows)

    unique: dict[tuple[str, date, str], CashflowRecord] = {}
    for row in rows:
        unique[(row.isin, row.date, row.kind)] = row

    return sorted(unique.values(), key=lambda x: (x.date, x.kind))


def _parse_block(payload: dict[str, Any], *, block_name: str, default_kind: str, isin: str) -> list[CashflowRecord]:
    block = payload.get(block_name) or {}
    columns = block.get("columns") or []
    data = block.get("data") or []

    result: list[CashflowRecord] = []
    for raw_row in data:
        item = dict(zip(columns, raw_row, strict=False))
        cf_date = _parse_date(item, ["coupondate", "amortdate", "redemptiondate", "paydate", "date"])
        if cf_date is None:
            continue
        amount = _parse_float(item, ["value", "valueprc", "facevalue", "amortvalue", "redemptionvalue"])
        rate = _parse_float(item, ["valueprc", "couponpercent", "rate"])
        result.append(CashflowRecord(isin=isin, date=cf_date, kind=default_kind, amount=amount, rate=rate))

    return result


def _split_amort_and_redemption(amort_rows: list[CashflowRecord]) -> list[CashflowRecord]:
    if not amort_rows:
        return []

    max_date = max(row.date for row in amort_rows)
    result: list[CashflowRecord] = []
    for row in amort_rows:
        inferred_kind = "redemption" if row.date == max_date and _looks_like_redemption(row) else "amort"
        result.append(CashflowRecord(isin=row.isin, date=row.date, kind=inferred_kind, amount=row.amount, rate=row.rate))
    return result


def _looks_like_redemption(row: CashflowRecord) -> bool:
    if row.rate is not None and abs(row.rate - 100.0) < 1e-6:
        return True
    if row.amount is not None and row.amount >= 90:
        return True
    return False


def derive_fields(cashflows: list[CashflowRecord], *, today: date | None = None) -> DerivedFields:
    today = today or date.today()
    future = [row for row in cashflows if row.date >= today]
    maturity_candidates = [row.date for row in cashflows if row.kind == "redemption"] or [row.date for row in cashflows]
    coupon_candidates = sorted({row.date for row in future if row.kind == "coupon"})
    amort_candidates = sorted({row.date for row in cashflows if row.kind == "amort"})

    return DerivedFields(
        maturity_date=max(maturity_candidates) if maturity_candidates else None,
        next_coupon_date=coupon_candidates[0] if coupon_candidates else None,
        amort_start_date=amort_candidates[0] if amort_candidates else None,
        has_amortization=bool(amort_candidates),
    )


def save_cashflows_to_db(session_factory: sessionmaker, *, isin: str, cashflows: list[CashflowRecord], source: str) -> int:
    now = datetime.utcnow()
    with session_factory() as session:
        session.execute(delete(Cashflow).where(Cashflow.isin == isin))
        for row in cashflows:
            session.add(
                Cashflow(
                    isin=row.isin,
                    date=row.date,
                    kind=row.kind,
                    amount=row.amount,
                    rate=row.rate,
                    source=source,
                    fetched_at=now,
                )
            )
        session.commit()
    return len(cashflows)


def save_derived_fields_to_db(session_factory: sessionmaker, *, isin: str, derived: DerivedFields, source: str) -> int:
    now = datetime.utcnow()
    payload = {
        "maturity_date": derived.maturity_date.isoformat() if derived.maturity_date else None,
        "next_coupon_date": derived.next_coupon_date.isoformat() if derived.next_coupon_date else None,
        "amort_start_date": derived.amort_start_date.isoformat() if derived.amort_start_date else None,
        "has_amortization": "1" if derived.has_amortization else "0",
    }

    with session_factory() as session:
        for field, value in payload.items():
            db_obj = session.scalar(
                select(InstrumentField).where(InstrumentField.isin == isin, InstrumentField.field == field)
            )
            if db_obj is None:
                db_obj = InstrumentField(isin=isin, field=field)
                session.add(db_obj)
            db_obj.value = value
            db_obj.source = source
            db_obj.confidence = 1.0
            db_obj.fetched_at = now
        session.commit()

    return len(payload)


def _parse_date(item: dict[str, Any], keys: list[str]) -> date | None:
    for key in keys:
        value = item.get(key.upper()) if key.upper() in item else item.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    return None


def _parse_float(item: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = item.get(key.upper()) if key.upper() in item else item.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip().replace(",", ".")
        try:
            return float(text)
        except ValueError:
            continue
    return None
