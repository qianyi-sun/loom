from __future__ import annotations

import asyncio

from loom_pipeline_orchestrator.health import start_health_server
from loom_pipeline_orchestrator.main_loop import reconcile_error_reason


async def test_orchestrator_serves_prometheus_on_scraped_health_port() -> None:
    server = await start_health_server(host="127.0.0.1", port=0, healthy=lambda: True)
    port = server.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n")
        await writer.drain()
        response = await reader.read()
        assert b"HTTP/1.1 200 OK" in response
        assert b"loom_pipeline_controller_reconcile_errors_total" in response
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


def test_reconcile_error_reason_is_closed() -> None:
    assert reconcile_error_reason(RuntimeError("budget ledger invariant")) == "budget_ledger"
    assert reconcile_error_reason(RuntimeError("raw secret text")) == "unexpected"
