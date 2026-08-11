from __future__ import annotations

import pytest

from loom_capacity_manager.health_probe import (
    CapacityHealthProbeError,
    parse_capacity_health_response,
)


def test_health_response_accepts_only_ready_with_integer_zero() -> None:
    assert parse_capacity_health_response(
        200,
        b'{"status":"ready","executable_new_capacity_ceiling":0}',
    ) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 0,
    }


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (503, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (200, b"not-json"),
        (200, b'{"status":"ready"}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0,"extra":true}'),
        (
            200,
            b'{"status":"ready","status":"not-ready",'
            b'"executable_new_capacity_ceiling":0}',
        ),
        (200, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":1}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":false}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0.0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":"0"}'),
        (200, b"{}" + b" " * 4096),
    ],
)
def test_health_response_rejects_every_ambiguous_or_unsafe_shape(
    status_code: int,
    payload: bytes,
) -> None:
    with pytest.raises(CapacityHealthProbeError):
        parse_capacity_health_response(status_code, payload)
