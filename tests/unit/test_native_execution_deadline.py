"""Local execution authority cannot outlive elapsed time or a terminal stop."""

from datetime import UTC, datetime, timedelta

import pytest

from loom_capacity_agent.build_admission import BuildExecutionPermitV1
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_native_execution_permit import execution_request


@pytest.fixture
def deadline(monkeypatch):
    from loom_capacity_executor import native_execution_deadline as module

    clock = [100_000_000_000]
    def read(clock_id):
        assert clock_id == module.time.CLOCK_BOOTTIME
        return clock[0]
    monkeypatch.setattr(module.time, "clock_gettime_ns", read)
    original = execution_request()
    return module.NativeExecutionDeadline(original.claim, source_binding_sha256=original.source_binding_sha256), clock


def receipt(request, seconds=10):
    # Deliberately unrelated wall clock: only server lifetime is relevant.
    issued = datetime(2001, 1, 1, tzinfo=UTC)
    return BuildExecutionPermitV1(request=request, request_digest=canonical_digest(request),
        issued_at=issued, not_after=issued + timedelta(seconds=seconds))


def test_permission_deadline_subtracts_all_transport_and_retry_delay(deadline):
    guard, clock = deadline
    request = guard.begin_request()
    clock[0] += 4_000_000_000
    guard.accept(receipt(request))
    assert guard.require_live() == 110_000_000_000
    clock[0] = 110_000_000_000
    with pytest.raises(RuntimeError):
        guard.require_live()
    assert guard.stopped_reason == "expired"


def test_source_shortened_lifetime_does_not_become_ten_seconds(deadline):
    guard, clock = deadline
    request = guard.begin_request()
    clock[0] += 1_000_000_000
    guard.accept(receipt(request, 2))
    assert guard.require_live() == 102_000_000_000


@pytest.mark.parametrize("phase", ["initial", "renewal", "suspend"])
def test_late_reply_cannot_bridge_expired_authority(deadline, phase):
    guard, clock = deadline
    request = guard.begin_request()
    if phase != "initial":
        guard.accept(receipt(request, 2))
        clock[0] += 1_000_000_000
        request = guard.begin_request()
    clock[0] += 100_000_000_000 if phase == "suspend" else 10_000_000_000
    with pytest.raises(RuntimeError):
        guard.accept(receipt(request))
    assert guard.stopped_reason == "expired"
    with pytest.raises(RuntimeError):
        guard.begin_request()


@pytest.mark.parametrize("reason", ["cancelled", "renewal-failed", "cleanup", "completed"])
def test_terminal_stop_rejects_late_renewal(deadline, reason):
    guard, clock = deadline
    guard.accept(receipt(guard.begin_request()))
    clock[0] += 1_000_000_000
    pending = guard.begin_request()
    guard.stop(reason)
    guard.stop("cleanup")
    assert guard.stopped_reason == reason
    with pytest.raises(RuntimeError):
        guard.accept(receipt(pending))
    with pytest.raises(RuntimeError):
        guard.require_live()


@pytest.mark.parametrize("boundary", ["duplicate", "overlap", "wrong-request", "unrequested", "no-permit"])
def test_protocol_errors_fail_closed(deadline, boundary):
    guard, _clock = deadline
    if boundary not in {"unrequested", "no-permit"}:
        request = guard.begin_request()
    with pytest.raises(RuntimeError):
        if boundary == "duplicate":
            guard.accept(receipt(request))
            guard.accept(receipt(request))
        elif boundary == "overlap":
            guard.begin_request()
        elif boundary in {"wrong-request", "unrequested"}:
            guard.accept(receipt(execution_request()))
        else:
            guard.require_live()
    assert guard.stopped_reason is not None


@pytest.mark.parametrize("boundary", ["regression", "error", "invalid"])
def test_clock_failure_irreversibly_stops_execution(deadline, monkeypatch, boundary):
    from loom_capacity_executor import native_execution_deadline as module

    guard, clock = deadline
    guard.accept(receipt(guard.begin_request()))
    if boundary == "error":
        def fail(_clock_id):
            raise OSError("clock unavailable")
        monkeypatch.setattr(module.time, "clock_gettime_ns", fail)
    else:
        clock[0] = 99_000_000_000 if boundary == "regression" else float("nan")
    with pytest.raises(RuntimeError):
        guard.require_live()
    assert guard.stopped_reason == "clock-failed"


def test_missing_boottime_never_falls_back_to_monotonic(monkeypatch):
    from loom_capacity_executor import native_execution_deadline as module

    monkeypatch.delattr(module.time, "CLOCK_BOOTTIME")
    request = execution_request()
    with pytest.raises(RuntimeError):
        module.NativeExecutionDeadline(request.claim, source_binding_sha256=request.source_binding_sha256)


def test_renewal_has_fresh_challenge_and_conservative_new_deadline(deadline):
    guard, clock = deadline
    first = guard.begin_request()
    guard.accept(receipt(first))
    clock[0] += 2_000_000_000
    second = guard.begin_request()
    assert second.challenge != first.challenge and second.claim == first.claim
    clock[0] += 3_000_000_000
    guard.accept(receipt(second))
    assert guard.require_live() == 112_000_000_000


def test_validation_time_cannot_bridge_expiry(deadline, monkeypatch):
    guard, clock = deadline
    request = guard.begin_request()
    original = BuildExecutionPermitV1.model_validate_json
    def delayed(wire):
        result = original(wire)
        clock[0] += 10_000_000_000
        return result
    monkeypatch.setattr(BuildExecutionPermitV1, "model_validate_json", delayed)
    with pytest.raises(RuntimeError):
        guard.accept(receipt(request))
    assert guard.stopped_reason == "expired"


def test_simultaneous_stop_and_renewal_always_ends_stopped(deadline):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    guard, _clock = deadline
    guard.accept(receipt(guard.begin_request()))
    reply = receipt(guard.begin_request())
    barrier = Barrier(2)
    def renew():
        barrier.wait(timeout=5)
        try:
            guard.accept(reply)
        except RuntimeError:
            assert guard.stopped_reason == "cancelled"
    def cancel():
        barrier.wait(timeout=5)
        guard.stop("cancelled")
    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(renew), executor.submit(cancel)
        first.result(timeout=5)
        second.result(timeout=5)
    assert guard.stopped_reason == "cancelled"
    with pytest.raises(RuntimeError):
        guard.require_live()
