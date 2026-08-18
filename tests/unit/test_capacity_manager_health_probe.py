from __future__ import annotations

import pytest

from loom_capacity_manager.health_probe import (
    CapacityHealthProbeError,
    capacity_health_probe_argv,
    parse_capacity_health_response,
    parse_observed_capacity_health_response,
)


def test_health_response_accepts_only_ready_with_integer_zero() -> None:
    assert parse_capacity_health_response(
        200,
        b'{"status":"ready","executable_new_capacity_ceiling":0}',
    ) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 0,
    }


def test_runtime_health_accepts_positive_ready_but_rejects_not_ready() -> None:
    assert parse_capacity_health_response(
        200,
        b'{"status":"ready","executable_new_capacity_ceiling":3}',
        allow_positive_ceiling=True,
    ) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 3,
    }
    with pytest.raises(CapacityHealthProbeError):
        parse_capacity_health_response(
            503,
            b'{"status":"not-ready","executable_new_capacity_ceiling":0}',
            allow_positive_ceiling=True,
        )


@pytest.mark.parametrize(
    ("status_code", "payload", "ceiling"),
    [
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0}', 0),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":3}', 3),
        (503, b'{"status":"not-ready","executable_new_capacity_ceiling":0}', 0),
    ],
)
def test_observed_health_accepts_exact_nonnegative_state(
    status_code: int,
    payload: bytes,
    ceiling: int,
) -> None:
    assert parse_observed_capacity_health_response(status_code, payload) == {
        "status": "ready" if status_code == 200 else "not-ready",
        "executable_new_capacity_ceiling": ceiling,
    }


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (201, b'{"status":"ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (503, b'{"status":"ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":-1}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":false}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0,"extra":1}'),
    ],
)
def test_observed_health_rejects_ambiguous_state(
    status_code: int,
    payload: bytes,
) -> None:
    with pytest.raises(CapacityHealthProbeError):
        parse_observed_capacity_health_response(status_code, payload)


def test_health_probe_argv_keeps_strict_default_and_explicit_observation_mode() -> None:
    strict = capacity_health_probe_argv()
    observed = capacity_health_probe_argv(observe=True)
    runtime_ready = capacity_health_probe_argv(allow_positive_ceiling=True)

    assert "--observe" not in strict
    assert observed == (*strict, "--observe")
    assert runtime_ready == (*strict, "--allow-positive-ceiling")
    with pytest.raises(ValueError):
        capacity_health_probe_argv(observe=True, allow_positive_ceiling=True)


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (503, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (200, b"not-json"),
        (200, b'{"status":"ready"}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0,"extra":true}'),
        (
            200,
            b'{"status":"ready","status":"not-ready","executable_new_capacity_ceiling":0}',
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
