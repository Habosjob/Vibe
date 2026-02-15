from __future__ import annotations

from vibe.utils.http import get_with_retries


class _ResponseMock:
    def __init__(self, *, content: bytes, status_code: int, headers: dict[str, str], url: str = "https://example.test") -> None:
        self.content = content
        self.status_code = status_code
        self.headers = headers
        self.url = url

    def raise_for_status(self) -> None:
        return None


def test_get_with_retries_json_response_contains_headers_and_default_request_headers(monkeypatch) -> None:
    observed_headers: dict[str, str] = {}

    def _fake_get(url: str, **kwargs):
        observed_headers.update(kwargs.get("headers", {}))
        return _ResponseMock(
            content=b'{"ok": true}',
            status_code=200,
            headers={"Content-Type": "application/json"},
            url=url,
        )

    monkeypatch.setattr("vibe.utils.http.requests.get", _fake_get)

    response = get_with_retries("https://example.test")

    assert response.headers["Content-Type"] == "application/json"
    assert observed_headers["User-Agent"]
    assert observed_headers["Accept"] == "application/json,text/plain,*/*"


def test_get_with_retries_html_response_content_type_visible(monkeypatch) -> None:
    monkeypatch.setattr(
        "vibe.utils.http.requests.get",
        lambda url, **kwargs: _ResponseMock(
            content=b"<html>ok</html>",
            status_code=200,
            headers={"Content-Type": "text/html"},
            url=url,
        ),
    )

    response = get_with_retries("https://example.test")

    assert response.headers["Content-Type"] == "text/html"
