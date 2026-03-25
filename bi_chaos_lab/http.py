from __future__ import annotations

import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

_MAX_RETRIES = 5
_RETRY_BACKOFF = [5, 15, 60, 120, 300]


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
    timeout: int = 300,
) -> HTTPResponse:
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    return _send(request, timeout)


def _send(request: urllib.request.Request, timeout: int) -> HTTPResponse:
    context = ssl.create_default_context()
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            # Rebuild request for retries since urllib may consume the body
            retry_req = urllib.request.Request(
                request.full_url, data=request.data, method=request.method, headers=dict(request.headers)
            )
            with urllib.request.urlopen(retry_req, timeout=timeout, context=context) as response:
                return HTTPResponse(
                    status=response.status,
                    headers={key: value for key, value in response.headers.items()},
                    body=response.read(),
                )
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504):
                last_exc = exc
                wait = _RETRY_BACKOFF[attempt]
                if exc.code == 429:
                    # Respect Retry-After header or parse body for wait hint
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    if retry_after and retry_after.isdigit():
                        wait = max(wait, int(retry_after))
                    else:
                        # Tableau embeds "retry after N minutes" in XML body
                        try:
                            body_text = exc.read().decode("utf-8", errors="replace")
                            import re
                            match = re.search(r"retry after (\d+) minute", body_text)
                            if match:
                                wait = max(wait, int(match.group(1)) * 60 + 30)
                        except Exception:
                            wait = max(wait, 120)
                time.sleep(wait)
                continue
            body = exc.read()
            message = body.decode("utf-8", errors="replace")
            raise HTTPError(f"{request.method} {request.full_url} failed with {exc.code}: {message}") from exc
        except urllib.error.URLError as exc:
            last_exc = exc
            time.sleep(_RETRY_BACKOFF[attempt])
            continue
        except (socket.timeout, TimeoutError, OSError) as exc:
            last_exc = urllib.error.URLError(str(exc))
            time.sleep(_RETRY_BACKOFF[attempt])
            continue
    if isinstance(last_exc, urllib.error.HTTPError):
        body = last_exc.read()
        message = body.decode("utf-8", errors="replace")
        raise HTTPError(f"{request.method} {request.full_url} failed with {last_exc.code}: {message}") from last_exc
    raise HTTPError(f"{request.method} {request.full_url} failed: {last_exc}") from last_exc
