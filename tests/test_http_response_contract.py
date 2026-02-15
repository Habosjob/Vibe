from __future__ import annotations

from vibe.utils.http import get_with_retries


class _ResponseMock:
    def __init__(self) -> None:
        self.content = b'{"ok": true}'
        self.status_code = 200
        self.headers = {"Content-Type": "application/json", "Server": "mock"}
        self.url = "https://example.test/final"

    def raise_for_status(self) -> None:
        return None


def test_http_response_contract_exposes_final_url_and_headers(monkeypatch) -> None:
    monkeypatch.setattr("vibe.utils.http.requests.get", lambda *args, **kwargs: _ResponseMock())

    response = get_with_retries("https://example.test/original")

    assert response.final_url == "https://example.test/final"
    assert response.headers == {"Content-Type": "application/json", "Server": "mock"}
