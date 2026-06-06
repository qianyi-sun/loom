import json
from io import StringIO
from uuid import uuid4

import pytest
import structlog

from loom.log import bind_trial_context, configure_logging


@pytest.fixture
def captured_log_output() -> StringIO:
    buf = StringIO()
    configure_logging(level="info", out=buf)
    yield buf
    structlog.reset_defaults()


def test_log_includes_correlation_fields(captured_log_output: StringIO):
    trial_id = uuid4()
    with bind_trial_context(trial_id=trial_id, step_id="main"):
        log = structlog.get_logger("loom.test")
        log.info("trial_state_changed", from_state="queued", to_state="claimed")

    lines = [line for line in captured_log_output.getvalue().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["event"] == "trial_state_changed"
    assert record["from_state"] == "queued"
    assert record["to_state"] == "claimed"
    assert record["trial_id"] == str(trial_id)
    assert record["step_id"] == "main"


def test_nested_binds_are_not_supported_in_v1(captured_log_output: StringIO):
    """v1 semantics: exiting an inner bind clears ALL contextvars.

    Pins the documented limitation so a future refactor either preserves it
    intentionally or updates this test alongside the impl + docstring.
    """
    outer = uuid4()
    inner_worker = uuid4()
    log = structlog.get_logger("loom.test")

    with bind_trial_context(trial_id=outer, step_id="main"):
        with bind_trial_context(worker_id=inner_worker):
            log.info("inside")
        log.info("outer_after_inner")

    lines = [line for line in captured_log_output.getvalue().splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]

    # Inside the nested block: all three fields present.
    assert records[0]["trial_id"] == str(outer)
    assert records[0]["step_id"] == "main"
    assert records[0]["worker_id"] == str(inner_worker)

    # After the inner exits, the outer-scope fields are GONE — v1 semantics.
    assert "trial_id" not in records[1]
    assert "step_id" not in records[1]
    assert "worker_id" not in records[1]


def test_context_isolation_between_calls(captured_log_output: StringIO):
    trial_a = uuid4()
    trial_b = uuid4()
    log = structlog.get_logger("loom.test")

    with bind_trial_context(trial_id=trial_a, step_id="main"):
        log.info("a")
    with bind_trial_context(trial_id=trial_b, step_id="main"):
        log.info("b")

    lines = [line for line in captured_log_output.getvalue().splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    assert records[0]["trial_id"] == str(trial_a)
    assert records[1]["trial_id"] == str(trial_b)
