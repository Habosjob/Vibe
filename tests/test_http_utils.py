from __future__ import annotations

from vibe.utils.http import get_with_retries


class _ResponseMock:
    def __init__(self) -> None:
        self.content = b'{"ok": true}'
        self.status_code = 200
        self.headers = {"Content-Type": "application/json; charset=utf-8"}

    def raise_for_status(self) -> None:
        return None


def test_get_with_retries_returns_headers(monkeypatch) -> None:
    monkeypatch.setattr("vibe.utils.http.requests.get", lambda *args, **kwargs: _ResponseMock())

    response = get_with_retries("https://example.test")

    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
