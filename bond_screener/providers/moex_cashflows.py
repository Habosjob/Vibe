from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

from bond_screener.db import Cashflow, InstrumentField, Offer
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
    next_offer_date: date | None
    amort_start_date: date | None
    has_amortization: bool


@dataclass(slots=True)
class OfferRecord:
    isin: str
    offer_date: date
    offer_type: str
    offer_price: float | None


class MoexCashflowProvider:
    def __init__(self, http_client: AsyncHttpClient) -> None:
        self.http_client = http_client

    async def fetch_cashflows(self, *, secid: str, isin: str) -> list[CashflowRecord]:
        payload = await self._fetch_bondization_payload(secid=secid)
        return parse_cashflows_payload(payload, isin=isin)

    async def fetch_offers(self, *, secid: str, isin: str) -> list[OfferRecord]:
        payload = await self._fetch_bondization_payload(secid=secid)
        return parse_offers_payload(payload, isin=isin)

    async def fetch_cashflows_and_offers(self, *, secid: str, isin: str) -> tuple[list[CashflowRecord], list[OfferRecord]]:
        payload = await self._fetch_bondization_payload(secid=secid)
        return parse_cashflows_payload(payload, isin=isin), parse_offers_payload(payload, isin=isin)

    async def _fetch_bondization_payload(self, *, secid: str) -> dict[str, Any]:
        response = await self.http_client.request(
            "GET",
            MOEX_BONDIZATION_URL.format(secid=secid),
            params={"iss.meta": "off"},
            provider="moex_cashflows",
        )
        response.raise_for_status()
        return response.json()


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


def parse_offers_payload(payload: dict[str, Any], *, isin: str) -> list[OfferRecord]:
    rows: list[OfferRecord] = []
    for block_name in ("offers", "putoffers"):
        block = payload.get(block_name) or {}
        columns = block.get("columns") or []
        data = block.get("data") or []
        for raw_row in data:
            item = dict(zip(columns, raw_row, strict=False))
            offer_date = _parse_date(item, ["offerdate", "buybackdate", "acceptedate", "date"])
            if offer_date is None:
                continue
            rows.append(
                OfferRecord(
                    isin=isin,
                    offer_date=offer_date,
                    offer_type=_parse_text(item, ["offertype", "type", "offerkind"]) or "put",
                    offer_price=_parse_float(item, ["price", "offerprice", "priceprc", "valueprc"]),
                )
            )

    unique: dict[tuple[str, date, str], OfferRecord] = {}
    for row in rows:
        unique[(row.isin, row.offer_date, row.offer_type)] = row
    return sorted(unique.values(), key=lambda x: (x.offer_date, x.offer_type))


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

    result: list[CashflowRecord] = []
    for row in amort_rows:
        inferred_kind = "redemption" if _looks_like_redemption(row) else "amort"
        result.append(CashflowRecord(isin=row.isin, date=row.date, kind=inferred_kind, amount=row.amount, rate=row.rate))

    # Если не удалось определить погашение по признакам и есть несколько записей,
    # считаем последнюю запись погашением.
    if len(result) > 1 and all(item.kind != "redemption" for item in result):
        last_idx = max(range(len(result)), key=lambda i: result[i].date)
        last = result[last_idx]
        result[last_idx] = CashflowRecord(
            isin=last.isin,
            date=last.date,
            kind="redemption",
            amount=last.amount,
            rate=last.rate,
        )
    return result


def _looks_like_redemption(row: CashflowRecord) -> bool:
    if row.rate is not None and abs(row.rate - 100.0) < 1e-6:
        return True
    return False


def derive_fields(cashflows: list[CashflowRecord], *, today: date | None = None) -> DerivedFields:
    today = today or date.today()
    future = [row for row in cashflows if row.date >= today]
    maturity_candidates = [row.date for row in cashflows if row.kind in {"redemption", "amort"}] or [row.date for row in cashflows]
    coupon_candidates = sorted({row.date for row in future if row.kind == "coupon"})
    amort_candidates = sorted({row.date for row in cashflows if row.kind == "amort"})

    return DerivedFields(
        maturity_date=max(maturity_candidates) if maturity_candidates else None,
        next_coupon_date=coupon_candidates[0] if coupon_candidates else None,
        next_offer_date=None,
        amort_start_date=amort_candidates[0] if amort_candidates else None,
        has_amortization=bool(amort_candidates),
    )


def apply_offer_fields(derived: DerivedFields, offers: list[OfferRecord], *, today: date | None = None) -> DerivedFields:
    today = today or date.today()
    next_offer_candidates = sorted({row.offer_date for row in offers if row.offer_date >= today})
    return DerivedFields(
        maturity_date=derived.maturity_date,
        next_coupon_date=derived.next_coupon_date,
        next_offer_date=next_offer_candidates[0] if next_offer_candidates else None,
        amort_start_date=derived.amort_start_date,
        has_amortization=derived.has_amortization,
    )


def save_cashflows_to_db(session_factory: sessionmaker, *, isin: str, cashflows: list[CashflowRecord], source: str) -> int:
    return save_cashflows_batch_to_db(session_factory, cashflows_by_isin={isin: cashflows}, source=source)


def save_cashflows_batch_to_db(
    session_factory: sessionmaker, *, cashflows_by_isin: dict[str, list[CashflowRecord]], source: str
) -> int:
    now = datetime.utcnow()
    isins = list(cashflows_by_isin.keys())
    if not isins:
        return 0

    rows_to_insert: list[Cashflow] = []
    for isin, cashflows in cashflows_by_isin.items():
        for row in cashflows:
            rows_to_insert.append(
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

    with session_factory() as session:
        session.execute(delete(Cashflow).where(Cashflow.isin.in_(isins)))
        session.add_all(rows_to_insert)
        session.commit()
    return len(rows_to_insert)


def save_derived_fields_to_db(session_factory: sessionmaker, *, isin: str, derived: DerivedFields, source: str) -> int:
    return save_derived_fields_batch_to_db(session_factory, derived_by_isin={isin: derived}, source=source)


def save_derived_fields_batch_to_db(
    session_factory: sessionmaker, *, derived_by_isin: dict[str, DerivedFields], source: str
) -> int:
    now = datetime.utcnow()
    if not derived_by_isin:
        return 0

    isins = list(derived_by_isin.keys())
    fields = ("maturity_date", "next_coupon_date", "next_offer_date", "amort_start_date", "has_amortization")
    rows_to_insert: list[InstrumentField] = []
    for isin, derived in derived_by_isin.items():
        payload = {
            "maturity_date": derived.maturity_date.isoformat() if derived.maturity_date else None,
            "next_coupon_date": derived.next_coupon_date.isoformat() if derived.next_coupon_date else None,
            "next_offer_date": derived.next_offer_date.isoformat() if derived.next_offer_date else None,
            "amort_start_date": derived.amort_start_date.isoformat() if derived.amort_start_date else None,
            "has_amortization": "1" if derived.has_amortization else "0",
        }
        for field, value in payload.items():
            rows_to_insert.append(
                InstrumentField(
                    isin=isin,
                    field=field,
                    value=value,
                    source=source,
                    confidence=1.0,
                    fetched_at=now,
                )
            )

    with session_factory() as session:
        session.execute(delete(InstrumentField).where(InstrumentField.isin.in_(isins), InstrumentField.field.in_(fields)))
        session.add_all(rows_to_insert)
        session.commit()

    return len(rows_to_insert)


def save_offers_to_db(session_factory: sessionmaker, *, isin: str, offers: list[OfferRecord], source: str) -> int:
    return save_offers_batch_to_db(session_factory, offers_by_isin={isin: offers}, source=source)


def save_offers_batch_to_db(session_factory: sessionmaker, *, offers_by_isin: dict[str, list[OfferRecord]], source: str) -> int:
    now = datetime.utcnow()
    isins = list(offers_by_isin.keys())
    if not isins:
        return 0

    rows_to_insert: list[Offer] = []
    for isin, offers in offers_by_isin.items():
        for row in offers:
            rows_to_insert.append(
                Offer(
                    isin=row.isin,
                    offer_date=row.offer_date,
                    offer_type=row.offer_type,
                    offer_price=row.offer_price,
                    source=source,
                    fetched_at=now,
                )
            )

    with session_factory() as session:
        session.execute(delete(Offer).where(Offer.isin.in_(isins)))
        session.add_all(rows_to_insert)
        session.commit()
    return len(rows_to_insert)


def _parse_date(item: dict[str, Any], keys: list[str]) -> date | None:
    for key in keys:
        value = item.get(key.upper()) if key.upper() in item else item.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y"):
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


def _parse_text(item: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = item.get(key.upper()) if key.upper() in item else item.get(key)
        if value in (None, ""):
            continue
        return str(value).strip().lower()
    return None
