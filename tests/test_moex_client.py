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
    bonds, errors, completed = client.fetch_all_bonds()

    assert errors == 0
    assert completed is True
    assert [item["SECID"] for item in bonds] == ["A", "B", "C"]


def test_fetch_page_retries_and_reports_error(monkeypatch):
    config = AppConfig(retries=2, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def always_fail(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr(client.session, "get", always_fail)
    page, errors, failed = client._fetch_page(0)

    assert page == []
    assert errors == 1
    assert failed is True


def test_fetch_all_bonds_stops_when_pagination_repeats_data(monkeypatch):
    config = AppConfig(page_size=2, retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    response = DummyResponse(
        {"securities": {"columns": ["SECID", "SHORTNAME"], "data": [["A", "Bond A"], ["B", "Bond B"]]}}
    )
    calls = {"count": 0}

    def fake_get(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] > 2:
            raise AssertionError("Вероятный бесконечный цикл пагинации")
        return response

    monkeypatch.setattr(client.session, "get", fake_get)
    bonds, errors, completed = client.fetch_all_bonds()

    assert errors == 0
    assert completed is True
    assert calls["count"] == 2
    assert [item["SECID"] for item in bonds] == ["A", "B"]


def test_fetch_all_bonds_stops_when_moex_returns_all_rows_at_once(monkeypatch):
    config = AppConfig(page_size=2, retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    response = DummyResponse(
        {
            "securities": {
                "columns": ["SECID", "SHORTNAME"],
                "data": [["A", "Bond A"], ["B", "Bond B"], ["C", "Bond C"]],
            }
        }
    )
    calls = {"count": 0}

    def fake_get(*args, **kwargs):
        calls["count"] += 1
        return response

    monkeypatch.setattr(client.session, "get", fake_get)
    bonds, errors, completed = client.fetch_all_bonds()

    assert errors == 0
    assert completed is True
    assert calls["count"] == 1
    assert [item["SECID"] for item in bonds] == ["A", "B", "C"]


def test_fetch_page_requests_all_columns_without_securities_columns(monkeypatch):
    config = AppConfig(page_size=10, retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    captured: dict[str, object] = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return DummyResponse(
            {
                "securities": {
                    "columns": ["SECID", "SHORTNAME", "MATDATE"],
                    "data": [["A", "Bond A", "2030-01-01"]],
                }
            }
        )

    monkeypatch.setattr(client.session, "get", fake_get)

    page, errors, failed = client._fetch_page(0)

    assert errors == 0
    assert failed is False
    assert page == [{"SECID": "A", "SHORTNAME": "Bond A", "MATDATE": "2030-01-01"}]
    assert captured["url"] == config.base_url
    assert captured["timeout"] == config.timeout_seconds
    assert captured["params"]["iss.only"] == "securities"
    assert captured["params"]["iss.meta"] == "off"
    assert "securities.columns" not in captured["params"]


def test_extract_earliest_amortization_date_returns_min_date():
    payload = {
        "amortizations": {
            "columns": ["amortdate", "value"],
            "data": [["2028-04-01", 100], ["2026-09-15", 50], ["0000-00-00", 0]],
        }
    }

    result = MoexClient._extract_earliest_amortization_date(payload)

    assert result == "2026-09-15"


def test_enrich_amortization_start_dates_sets_empty_if_no_data(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def fake_get(url, params, timeout):
        assert "bondization.json" in url
        assert params["iss.only"] == "amortizations"
        return DummyResponse({"amortizations": {"columns": ["amortdate"], "data": []}})

    monkeypatch.setattr(client.session, "get", fake_get)
    bonds = [{"SECID": "SU26218RMFS6", "SHORTNAME": "ОФЗ 26218"}]

    errors = client.enrich_amortization_start_dates(bonds)

    assert errors == 0
    assert bonds[0]["Amortization_start_date"] == ""


def test_enrich_amortization_start_dates_fills_earliest_date(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def fake_get(url, params, timeout):
        assert url.endswith("/A/bondization.json")
        return DummyResponse(
            {
                "amortizations": {
                    "columns": ["amortdate", "valueprc"],
                    "data": [["2027-12-01", 10], ["2025-06-01", 5]],
                }
            }
        )

    monkeypatch.setattr(client.session, "get", fake_get)
    monkeypatch.setattr(client, "_get_thread_session", lambda: client.session)
    bonds = [{"SECID": "A", "MATDATE": "2028-01-01"}]

    errors = client.enrich_amortization_start_dates(bonds)

    assert errors == 0
    assert bonds[0]["Amortization_start_date"] == "2025-06-01"


def test_extract_earliest_amortization_date_ignores_single_full_redemption_with_matdate():
    payload = {
        "amortizations": {
            "columns": ["amortdate", "valueprc"],
            "data": [["2030-05-01", 100]],
        }
    }

    result = MoexClient._extract_earliest_amortization_date(payload, matdate="2030-05-01")

    assert result is None


def test_extract_earliest_amortization_date_returns_first_partial_payment():
    payload = {
        "amortizations": {
            "columns": ["amortdate", "valueprc"],
            "data": [["2030-05-01", 100], ["2028-05-01", 20]],
        }
    }

    result = MoexClient._extract_earliest_amortization_date(payload, matdate="2030-05-01")

    assert result == "2028-05-01"


def test_enrich_amortization_start_dates_deduplicates_secid_requests(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0, amortization_workers=4)
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    calls = {"count": 0}

    def fake_get(url, params, timeout):
        calls["count"] += 1
        return DummyResponse(
            {
                "amortizations": {
                    "columns": ["amortdate", "valueprc"],
                    "data": [["2027-12-01", 10], ["2030-01-01", 100]],
                }
            }
        )

    monkeypatch.setattr(client.session, "get", fake_get)
    monkeypatch.setattr(client, "_get_thread_session", lambda: client.session)
    bonds = [
        {"SECID": "A", "MATDATE": "2030-01-01"},
        {"SECID": "A", "MATDATE": "2030-01-01"},
    ]

    errors = client.enrich_amortization_start_dates(bonds)

    assert errors == 0
    assert calls["count"] == 1
    assert bonds[0]["Amortization_start_date"] == "2027-12-01"
    assert bonds[1]["Amortization_start_date"] == "2027-12-01"




def test_extract_earliest_amortization_date_single_partial_valueprc_is_amortization():
    payload = {
        "amortizations": {
            "columns": ["amortdate", "valueprc"],
            "data": [["2029-02-01", 15]],
        }
    }

    result = MoexClient._extract_earliest_amortization_date(payload, matdate="2030-05-01")

    assert result == "2029-02-01"


def test_client_uses_separate_delays_for_pages_and_amortizations(monkeypatch):
    config = AppConfig(
        retries=1,
        request_delay_seconds=0.15,
        amortization_request_delay_seconds=0.02,
    )
    client = MoexClient(config=config, logger=logging.getLogger("test"))
    captured: list[float] = []

    def fake_rate_limited_get(url, params, timeout, delay_seconds):
        captured.append(delay_seconds)
        if "bondization" in url:
            return DummyResponse({"amortizations": {"columns": ["amortdate"], "data": []}})
        return DummyResponse({"securities": {"columns": ["SECID"], "data": []}})

    monkeypatch.setattr(client, "_get_with_rate_limit", fake_rate_limited_get)

    client._fetch_page(0)
    client._fetch_amortization_start_date("A")

    assert captured == [0.15, 0.02]



def test_enrich_amortization_continues_on_worker_exception(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0, amortization_request_delay_seconds=0, amortization_workers=2)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def fake_fetch(secid: str, matdate: str = ""):
        if secid == "B":
            raise ValueError("bad payload")
        return "2026-01-01", 0

    monkeypatch.setattr(client, "_fetch_amortization_start_date", fake_fetch)

    bonds = [{"SECID": "A"}, {"SECID": "B"}]
    errors = client.enrich_amortization_start_dates(bonds)

    assert errors == 1
    assert bonds[0]["Amortization_start_date"] == "2026-01-01"
    assert bonds[1]["Amortization_start_date"] == ""


def test_get_thread_session_returns_dedicated_session_for_worker_thread():
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=1) as pool:
        worker_session = pool.submit(client._get_thread_session).result()

    assert worker_session is not client.session

def test_fetch_all_bonds_resumes_from_checkpoint(monkeypatch):
    config = AppConfig(page_size=2, retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    captured: list[dict] = []

    def fake_get(url, params, timeout):
        assert params["start"] == 2
        return DummyResponse({"securities": {"columns": ["SECID"], "data": [["C"]]}})

    monkeypatch.setattr(client.session, "get", fake_get)

    bonds, errors, completed = client.fetch_all_bonds(
        checkpoint_data={"bonds": [{"SECID": "A"}, {"SECID": "B"}], "next_start": 2, "seen_secids": ["A", "B"]},
        checkpoint_saver=lambda payload: captured.append(payload),
    )

    assert errors == 0
    assert completed is True
    assert [bond["SECID"] for bond in bonds] == ["A", "B", "C"]
    assert captured[-1]["completed"] is True


def test_enrich_amortization_uses_checkpoint_without_requests(monkeypatch):
    config = AppConfig(retries=1, request_delay_seconds=0)
    client = MoexClient(config=config, logger=logging.getLogger("test"))

    def should_not_be_called(*args, **kwargs):
        raise AssertionError("HTTP запрос не должен выполняться для уже обработанного SECID")

    monkeypatch.setattr(client.session, "get", should_not_be_called)
    bonds = [{"SECID": "A"}]

    errors = client.enrich_amortization_start_dates(
        bonds,
        checkpoint_data={"processed": {"A": "2025-06-01"}, "completed": False},
    )

    assert errors == 0
    assert bonds[0]["Amortization_start_date"] == "2025-06-01"
