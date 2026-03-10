from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body.decode("utf-8"))


class HTTPError(RuntimeError):
    pass


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: Any | None = None,
    timeout: int = 60,
) -> HTTPResponse:
    encoded_body: bytes | None = None
    req_headers = dict(headers or {})
    if body is not None:
        encoded_body = json.dumps(body).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=encoded_body, method=method, headers=req_headers)
    return _send(request, timeout)


def request_form(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    form: dict[str, str] | None = None,
    timeout: int = 60,
) -> HTTPResponse:
    encoded = None
    req_headers = dict(headers or {})
    if form is not None:
        encoded = urllib.parse.urlencode(form).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    request = urllib.request.Request(url, data=encoded, method=method, headers=req_headers)
    return _send(request, timeout)


def request_bytes(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: int = 120,
) -> HTTPResponse:
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    return _send(request, timeout)


def _send(request: urllib.request.Request, timeout: int) -> HTTPResponse:
    context = ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            return HTTPResponse(
                status=response.status,
                headers={key: value for key, value in response.headers.items()},
                body=response.read(),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        message = body.decode("utf-8", errors="replace")
        raise HTTPError(f"{request.method} {request.full_url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
        raise HTTPError(f"{request.method} {request.full_url} failed: {exc.reason}") from exc
