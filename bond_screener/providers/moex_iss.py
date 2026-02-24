from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from bond_screener.db import Instrument
from bond_screener.http_client import AsyncHttpClient

MOEX_SECURITIES_URL = "https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"


@dataclass(slots=True)
class MoexInstrument:
    isin: str
    secid: str | None
    shortname: str | None
    primary_boardid: str | None
    board: str | None
    currency: str | None

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


class MoexIssProvider:
    def __init__(self, http_client: AsyncHttpClient) -> None:
        self.http_client = http_client

    async def fetch_page(self, *, start: int = 0, limit: int = 100, q: str | None = None) -> list[MoexInstrument]:
        params: dict[str, Any] = {
            "iss.meta": "off",
            "iss.only": "securities",
            "start": max(0, start),
            "limit": max(1, limit),
            "securities.start": max(0, start),
            "securities.limit": max(1, limit),
        }
        if q:
            params["q"] = q

        response = await self.http_client.request(
            "GET",
            MOEX_SECURITIES_URL,
            params=params,
            provider="moex_iss",
        )
        response.raise_for_status()
        payload = response.json()

        securities = payload.get("securities") or {}
        columns = securities.get("columns") or []
        rows = securities.get("data") or []

        result: list[MoexInstrument] = []
        for row in rows:
            item = dict(zip(columns, row, strict=False))
            isin = (item.get("ISIN") or "").strip()
            if not isin:
                continue
            result.append(
                MoexInstrument(
                    isin=isin,
                    secid=_clean(item.get("SECID")),
                    shortname=_clean(item.get("SHORTNAME") or item.get("SECNAME")),
                    primary_boardid=_clean(item.get("PRIMARYBOARDID")),
                    board=_clean(item.get("BOARDID")),
                    currency=_clean(item.get("FACEUNIT") or item.get("CURRENCYID")),
                )
            )
        return result

    async def fetch_all(self, *, limit: int = 100, q: str | None = None, progress_cb: Any | None = None) -> list[MoexInstrument]:
        start = 0
        all_items: list[MoexInstrument] = []
        unique_isins: set[str] = set()

        while True:
            page = await self.fetch_page(start=start, limit=limit, q=q)
            if not page:
                break

            prev_unique = len(unique_isins)
            for row in page:
                unique_isins.add(row.isin)
            all_items.extend(page)
            start += limit
            if progress_cb:
                progress_cb(start, len(unique_isins))
            if len(unique_isins) == prev_unique or len(page) < limit:
                break

        return _deduplicate(all_items)


def save_instruments_to_db(session_factory: sessionmaker, instruments: list[MoexInstrument]) -> int:
    now = datetime.utcnow()
    updated = 0

    with session_factory() as session:
        for instrument in instruments:
            db_obj = session.scalar(select(Instrument).where(Instrument.isin == instrument.isin))
            if db_obj is None:
                db_obj = Instrument(isin=instrument.isin)
                session.add(db_obj)

            db_obj.secid = instrument.secid
            db_obj.shortname = instrument.shortname
            db_obj.name = instrument.shortname
            db_obj.primary_boardid = instrument.primary_boardid
            db_obj.board = instrument.board
            db_obj.currency = instrument.currency
            db_obj.updated_at = now
            updated += 1

        session.commit()

    return updated


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _deduplicate(items: list[MoexInstrument]) -> list[MoexInstrument]:
    unique: dict[str, MoexInstrument] = {}
    for item in items:
        unique[item.isin] = item
    return list(unique.values())
