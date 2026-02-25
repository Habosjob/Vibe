from __future__ import annotations

import logging

import requests

from moex_bond_screener.config import AppConfig
from moex_bond_screener.moex_client import MoexClient


class DummyResponse:
    def __init__(self, payload: dict, text: str = "{}") -> None:
        self._payload = payload
        self.text = text

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_fetch_all_bonds_pagination(monkeypatch):
    config = AppConfig(page_size=2, retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    responses = [
        DummyResponse({"securities": {"columns": ["SECID", "SHORTNAME"], "data": [["A", "Bond A"], ["B", "Bond B"]]}}),
        DummyResponse({"securities": {"columns": ["SECID", "SHORTNAME"], "data": [["C", "Bond C"]]}}),
        DummyResponse({"securities": {"columns": ["SECID", "SHORTNAME"], "data": []}}),
    ]

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(client.session, "get", fake_get)
    bonds, errors = client.fetch_all_bonds()

    assert errors == 0
    assert [item["SECID"] for item in bonds] == ["A", "B", "C"]


def test_fetch_page_retries_and_reports_error(monkeypatch):
    config = AppConfig(retries=2, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def always_fail(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr(client.session, "get", always_fail)
    page, errors = client._fetch_page(0)

    assert page == []
    assert errors == 1
