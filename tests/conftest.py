import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest


@pytest.fixture
def aws_env(monkeypatch):
    """Fake credentials so moto never touches a real AWS account."""
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(key, value)
    for key in ("AWS_ENDPOINT_URL", "AWS_PROFILE", "S3_ENDPOINT_URL"):
        monkeypatch.delenv(key, raising=False)


@dataclass
class Reply:
    status: int = 200
    body: bytes = b""
    headers: dict = field(default_factory=dict)


@dataclass
class FakeServer:
    """Programmable HTTP server. routes[path] = list of Replies served in order (last repeats)."""

    base_url: str
    routes: dict = field(default_factory=dict)
    requests: list = field(default_factory=list)

    def add(self, path: str, *replies: Reply) -> None:
        self.routes[path] = list(replies)

    def hits(self, path: str, method: str | None = None) -> list:
        return [
            r
            for r in self.requests
            if r["path"] == path and (method is None or r["method"] == method)
        ]


@pytest.fixture
def fake_server():
    state = FakeServer(base_url="")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _serve(self, with_body: bool):
            parsed = urlparse(self.path)
            state.requests.append(
                {
                    "method": self.command,
                    "path": parsed.path,
                    "query": parse_qs(parsed.query),
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                }
            )
            replies = state.routes.get(parsed.path)
            if not replies:
                self.send_response(404)
                self.end_headers()
                return
            reply = replies.pop(0) if len(replies) > 1 else replies[0]
            self.send_response(reply.status)
            headers = {"Content-Length": str(len(reply.body)), **reply.headers}
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            if with_body:
                self.wfile.write(reply.body)

        def do_GET(self):
            self._serve(True)

        def do_HEAD(self):
            self._serve(False)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state.base_url = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def no_sleep_http():
    from transitiq_pipelines.http import HttpClient, RetryPolicy

    sleeps: list[float] = []
    client = HttpClient(policy=RetryPolicy(max_attempts=4, base_delay=0.01), sleep=sleeps.append)
    client.sleeps = sleeps
    return client


@pytest.fixture
def ctx(tmp_path, no_sleep_http):
    from datetime import UTC, datetime

    from transitiq_pipelines.sources import Context
    from transitiq_pipelines.storage import LocalBronzeStore

    (tmp_path / "tmp").mkdir()
    return Context(
        store=LocalBronzeStore(tmp_path / "bronze"),
        http=no_sleep_http,
        checkpoint_dir=tmp_path / "checkpoints",
        tmp_dir=tmp_path / "tmp",
        now=datetime(2026, 11, 6, 12, 0, 0, tzinfo=UTC),
    )
