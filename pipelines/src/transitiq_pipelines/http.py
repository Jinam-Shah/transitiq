"""HTTP client with retries, built on the standard library (works inside Lambda unchanged).

Retries: 429, 500, 502, 503, 504, timeouts and dropped connections, with exponential backoff
plus jitter. A 429's Retry-After header (or the MBTA's x-ratelimit-reset) is honoured.
Other 4xx errors fail immediately because retrying cannot fix them.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from transitiq_pipelines.config import USER_AGENT

log = logging.getLogger(__name__)

RETRY_STATUSES = {429, 500, 502, 503, 504}
LOCAL_HOSTS = {"127.0.0.1", "localhost"}
CHUNK = 1024 * 1024


class HttpError(Exception):
    def __init__(self, url: str, status: int | None, message: str):
        super().__init__(f"{status or 'network error'} for {url}: {message}")
        self.url = url
        self.status = status


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self):
        return json.loads(self.body)


def build_url(url: str, params: dict | None = None) -> str:
    if not params:
        return url
    sep = "&" if urllib.parse.urlparse(url).query else "?"
    return url + sep + urllib.parse.urlencode(params, safe=",")


def check_scheme(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "https" or (parsed.scheme == "http" and parsed.hostname in LOCAL_HOSTS):
        return
    raise ValueError(f"refusing non-https URL: {url}")


class HttpClient:
    def __init__(
        self,
        policy: RetryPolicy | None = None,
        timeout: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ):
        self.policy = policy or RetryPolicy()
        self.timeout = timeout
        self.sleep = sleep
        self.clock = clock

    # ---------- retry machinery ----------
    def _delay(self, attempt: int, headers: dict[str, str] | None) -> float:
        headers = headers or {}
        retry_after = headers.get("retry-after")
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), self.policy.max_delay)
        reset = headers.get("x-ratelimit-reset")
        if reset and reset.isdigit():
            return min(max(float(reset) - self.clock(), 0.0) + 1.0, self.policy.max_delay)
        backoff = self.policy.base_delay * 2 ** (attempt - 1)
        return min(backoff + random.uniform(0, self.policy.base_delay), self.policy.max_delay)  # noqa: S311

    def _with_retries(self, url: str, fn: Callable[[], object]):
        last: HttpError | None = None
        for attempt in range(1, self.policy.max_attempts + 1):
            try:
                return fn()
            except urllib.error.HTTPError as exc:
                headers = {k.lower(): v for k, v in (exc.headers or {}).items()}
                last = HttpError(url, exc.code, exc.reason or "")
                if exc.code not in RETRY_STATUSES:
                    raise last from exc
                wait = self._delay(attempt, headers)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = HttpError(url, None, str(exc))
                wait = self._delay(attempt, None)
            if attempt < self.policy.max_attempts:
                log.warning("retrying %s in %.1fs (attempt %d): %s", url, wait, attempt, last)
                self.sleep(wait)
        if last is None:  # pragma: no cover - max_attempts is always >= 1
            raise RuntimeError("no attempts made")
        raise last

    def _open(self, url: str, method: str, headers: dict | None):
        check_scheme(url)
        req = urllib.request.Request(  # noqa: S310 (scheme checked above)
            url, method=method, headers={"User-Agent": USER_AGENT, **(headers or {})}
        )
        return urllib.request.urlopen(req, timeout=self.timeout)  # noqa: S310

    # ---------- public API ----------
    def get(self, url: str, params: dict | None = None, headers: dict | None = None) -> Response:
        full = build_url(url, params)

        def do():
            with self._open(full, "GET", headers) as resp:
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                return Response(resp.status, hdrs, resp.read())

        return self._with_retries(full, do)

    def head(self, url: str, headers: dict | None = None) -> dict[str, str]:
        def do():
            with self._open(url, "HEAD", headers) as resp:
                return {k.lower(): v for k, v in resp.headers.items()}

        return self._with_retries(url, do)

    def download(self, url: str, dest: Path, headers: dict | None = None) -> dict:
        """Stream url to dest atomically. Returns bytes, sha256 and response headers.

        Writes to dest.part first; checks Content-Length when the server sends it; only then
        renames into place, so an interrupted download never leaves a half-written file.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")

        def do():
            digest = hashlib.sha256()
            written = 0
            with self._open(url, "GET", headers) as resp, part.open("wb") as fh:
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                while chunk := resp.read(CHUNK):
                    fh.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
            expected = hdrs.get("content-length")
            if expected and expected.isdigit() and int(expected) != written:
                raise ConnectionError(f"short read: got {written} of {expected} bytes")
            return {"bytes": written, "sha256": digest.hexdigest(), "headers": hdrs}

        try:
            result = self._with_retries(url, do)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        os.replace(part, dest)
        return result
