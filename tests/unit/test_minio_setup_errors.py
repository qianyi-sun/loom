"""Storage fixture errors must not disappear into the SDK's Retry-After sleep."""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from minio import Minio
from minio.error import S3Error

from tests.support import minio as fixture


@pytest.fixture
def storage():
    state = {"exists": False, "methods": [], "fail_method": None, "code": "SlowDown", "status": 503}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond("GET")

        def do_HEAD(self):
            self.respond("HEAD")

        def do_PUT(self):
            self.respond("PUT")

        def respond(self, method):
            state["methods"].append(method)
            if method == state["fail_method"]:
                status, code = state["status"], state["code"]
            elif method != "PUT" and not state["exists"]:
                status, code = 404, "NoSuchBucket"
            else:
                status, code = 200, None
                state["exists"] = True
            body = (f"<Error><Code>{code}</Code><Message>fixture error</Message><Resource>/artifacts</Resource>"
                    "<RequestId>fixture</RequestId><HostId>fixture</HostId></Error>" if code else
                    '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>').encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            if status == 503:
                self.send_header("Retry-After", "120")
            self.end_headers()
            if method != "HEAD":
                self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    container = SimpleNamespace(get_client=lambda **kwargs: Minio(
        f"127.0.0.1:{server.server_port}", access_key="fixture", secret_key="fixture-secret", secure=False, **kwargs))
    try:
        yield container, state
    finally:
        server.shutdown()
        worker.join(timeout=2)
        server.server_close()


def ensure(container):
    assert hasattr(fixture, "ensure_test_bucket"), "missing bounded test-only bucket setup"
    fixture.ensure_test_bucket(container, "artifacts")


@pytest.mark.timeout(3)
@pytest.mark.parametrize("method,status,code", [("GET", 503, "SlowDown"), ("PUT", 503, "XMinioServerNotInitialized"),
                                               ("GET", 403, "AccessDenied")])
def test_storage_setup_surfaces_original_s3_error_without_retries(storage, method, status, code):
    container, state = storage
    state.update(fail_method=method, status=status, code=code)
    with pytest.raises(S3Error) as error:
        ensure(container)
    assert error.value.code == code
    assert state["methods"].count(method) == 1


def test_storage_setup_creates_missing_bucket_once_and_retains_existing(storage):
    container, state = storage
    ensure(container)
    assert state["exists"] and state["methods"] == ["GET", "PUT"]
    ensure(container)
    assert state["methods"] == ["GET", "PUT", "GET", "HEAD"]
