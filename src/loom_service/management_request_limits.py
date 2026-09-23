"""Pre-parse management-only request bounds; never buffer application uploads.

The in-flight budget belongs to one ASGI process/event loop. It is not a
distributed ingress rate limit. Keep configured body and concurrency limits
within the process memory budget, including copies and JSON parsing overhead.
"""

from __future__ import annotations

import asyncio

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class ManagementRequestLimitsMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int, max_inflight: int, body_timeout_sec: float) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.max_inflight = max_inflight
        self.body_timeout_sec = body_timeout_sec
        self._active = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def reject(status: int, detail: str) -> None:
            headers = {"Cache-Control": "no-store"}
            if scope.get("http_version", "1.1").startswith("1."):
                headers["Connection"] = "close"
            if status == 503:
                headers["Retry-After"] = "1"
            await JSONResponse({"detail": detail}, status_code=status, headers=headers)(scope, receive, send)

        headers = scope.get("headers", [])
        lengths = [v for k, v in headers if k.lower() == b"content-length"]
        declared: int | None = None
        if lengths:
            if (len(lengths) != 1 or not lengths[0].isdigit()
                    or any(k.lower() == b"transfer-encoding" for k, _ in headers)):
                await reject(400, "invalid request framing")
                return
            try:
                declared = int(lengths[0])
            except ValueError:
                await reject(400, "invalid request framing")
                return
            if declared > self.max_body_bytes:
                await reject(413, "management request body too large")
                return
        if any(k.lower() == b"content-encoding" and v.strip().lower() != b"identity" for k, v in headers):
            await reject(415, "encoded management request bodies are not supported")
            return
        # No await between admission test/increment: atomic on the ASGI loop.
        if self._active >= self.max_inflight:
            await reject(503, "management request capacity exhausted")
            return
        self._active += 1
        try:
            body_buffer = bytearray()
            size = 0
            error: tuple[int, str] | None = None
            try:
                async with asyncio.timeout(self.body_timeout_sec):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        if message["type"] != "http.request":
                            error = (400, "invalid request framing")
                            break
                        chunk = message.get("body", b"")
                        size += len(chunk)
                        if size > self.max_body_bytes:
                            error = (413, "management request body too large")
                            break
                        if declared is not None and size > declared:
                            error = (400, "request body length mismatch")
                            break
                        # One buffer also bounds overhead for empty/tiny frames.
                        body_buffer.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                error = (408, "management request body reception timed out")
            # Only reception is timed. Sending an error must not be cancelled
            # midway and followed by a second response from the timeout handler.
            if error is not None:
                await reject(*error)
                return
            if declared is not None and size != declared:
                await reject(400, "request body length mismatch")
                return
            body = bytes(body_buffer)
            body_buffer.clear()
            replayed = False

            async def replay() -> Message:
                nonlocal replayed
                if not replayed:
                    replayed = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await receive()

            # Admission lasts through response completion; response chunks are
            # forwarded unchanged and there is no request queue or disk spool.
            await self.app(scope, replay, send)
        finally:
            self._active -= 1
