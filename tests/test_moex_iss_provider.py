from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from bond_screener.db import Instrument, init_db, make_session_factory
from bond_screener.http_client import AsyncHttpClient
from bond_screener.providers.moex_iss import MoexInstrument, MoexIssProvider, save_instruments_to_db


@pytest.mark.asyncio
async def test_fetch_all_uses_pagination_and_q(tmp_path: Path) -> None:
    starts: list[int] = []
    received_q: list[str | None] = []

    def payload(rows: list[list[str | None]]) -> dict[str, object]:
        return {
            "securities": {
                "columns": ["ISIN", "SECID", "SHORTNAME", "PRIMARYBOARDID", "BOARDID", "FACEUNIT"],
                "data": rows,
            }
        }

    async def handler(request: httpx.Request) -> httpx.Response:
        start = int(request.url.params.get("start", "0"))
        starts.append(start)
        received_q.append(request.url.params.get("q"))

        if start == 0:
            return httpx.Response(200, json=payload([
                ["RU000A000001", "BOND1", "Облигация 1", "TQCB", "TQCB", "RUB"],
                ["RU000A000002", "BOND2", "Облигация 2", "TQCB", "TQCB", "RUB"],
            ]))
        if start == 2:
            return httpx.Response(200, json=payload([
                ["RU000A000003", "BOND3", "Облигация 3", "TQOB", "TQOB", "USD"],
            ]))
        return httpx.Response(200, json=payload([]))

    transport = httpx.MockTransport(handler)

    async with AsyncHttpClient(cache_db_path=tmp_path / "cache.sqlite", cache_ttl_seconds=0, transport=transport) as client:
        provider = MoexIssProvider(client)
        items = await provider.fetch_all(limit=2, q="RU000")

    assert starts == [0, 2]
    assert received_q == ["RU000", "RU000"]
    assert [item.isin for item in items] == ["RU000A000001", "RU000A000002", "RU000A000003"]


def test_save_instruments_to_db(tmp_path: Path) -> None:
    db_path = tmp_path / "bond.sqlite"
    init_db(db_path)
    session_factory = make_session_factory(db_path)

    saved = save_instruments_to_db(
        session_factory,
        [
            MoexInstrument(
                isin="RU000A000001",
                secid="BOND1",
                shortname="Облигация 1",
                primary_boardid="TQCB",
                board="TQCB",
                currency="RUB",
            )
        ],
    )
    assert saved == 1

    with session_factory() as session:
        row = session.scalar(select(Instrument).where(Instrument.isin == "RU000A000001"))

    assert row is not None
    assert row.secid == "BOND1"
    assert row.shortname == "Облигация 1"
    assert row.primary_boardid == "TQCB"
    assert row.board == "TQCB"
    assert row.currency == "RUB"
