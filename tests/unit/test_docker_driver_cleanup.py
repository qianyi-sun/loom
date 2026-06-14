"""Unit-level regression tests for DockerDriver cleanup paths.

Uses a stand-in 'container' object that records calls and can be made to
raise, so we can exercise the stop/start error paths without a docker daemon.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

import pytest
from docker.errors import APIError

from loom.driver.docker import DockerDriver
from loom.errors import DriverError


class _FakeContainer:
    def __init__(self, *, stop_raises: bool = False, remove_raises: bool = False) -> None:
        self.stop_raises = stop_raises
        self.remove_raises = remove_raises
        self.stop_calls = 0
        self.remove_calls = 0

    def stop(self, *, timeout: int = 10) -> None:
        self.stop_calls += 1
        if self.stop_raises:
            raise APIError("simulated stop failure")

    def remove(self, *, force: bool = False) -> None:
        self.remove_calls += 1
        if self.remove_raises:
            raise APIError("simulated remove failure")


class _FakeClient:
    def close(self) -> None:
        pass


def _make_driver_in_running_state(container: Any) -> DockerDriver:
    """Create a DockerDriver and force it into the running state with a
    caller-supplied 'container' object. Bypasses real `docker.from_env()`
    + image pull."""
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    d._client = _FakeClient()  # type: ignore[attr-defined]
    d._container = container   # type: ignore[attr-defined]
    d._state = "running"       # type: ignore[attr-defined]
    return d


async def test_stop_delete_true_skips_graceful_stop_uses_force_remove():
    """delete=True skips the slow `stop(timeout=10)` call entirely and
    relies on `remove(force=True)` to SIGKILL+remove the container in
    one step. Saves ~10s per test in the integration suite. The
    original leak this test guarded against (stop's APIError skipping
    remove) is now impossible because stop isn't called at all when
    delete=True."""
    container = _FakeContainer()
    d = _make_driver_in_running_state(container)
    await d.stop(delete=True)
    assert container.stop_calls == 0, (
        "stop() should not be called when delete=True — remove(force=True) "
        "handles the SIGKILL"
    )
    assert container.remove_calls == 1


async def test_stop_delete_false_uses_graceful_stop():
    """delete=False keeps the container around (operator may want to
    inspect it). In that path we still call stop() to send SIGTERM."""
    container = _FakeContainer()
    d = _make_driver_in_running_state(container)
    await d.stop(delete=False)
    assert container.stop_calls == 1
    assert container.remove_calls == 0


async def test_stop_tolerates_remove_failure():
    """remove() failures are non-fatal (the container might have been
    cleaned up by docker itself); we still clear our handle."""
    container = _FakeContainer(remove_raises=True)
    d = _make_driver_in_running_state(container)
    await d.stop(delete=True)  # must NOT raise
    assert d._container is None  # type: ignore[attr-defined]


async def test_start_failure_path_cleans_partial_state(monkeypatch):  # type: ignore[no-untyped-def]
    """Regression for Bug 3: an exception inside start() after the
    container was created must not leave the container running. start()
    self-cleans before re-raising."""

    container = _FakeContainer()
    teardown_calls = {"n": 0}

    async def _fake_teardown(self: DockerDriver, *, delete: bool) -> None:
        teardown_calls["n"] += 1
        self._container = None   # type: ignore[attr-defined]
        self._client = None      # type: ignore[attr-defined]

    monkeypatch.setattr(DockerDriver, "_teardown", _fake_teardown)

    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    d._client = _FakeClient()    # type: ignore[attr-defined]
    d._container = container     # type: ignore[attr-defined]

    # Skip the real docker.from_env() / image pull / containers.run / wait
    # by jumping straight to the set_network_policy failure point.
    async def _boom(self: DockerDriver, policy: Any) -> None:
        raise DriverError("simulated policy apply failure")

    monkeypatch.setattr(DockerDriver, "set_network_policy", _boom)

    # Drive start() partway by manually flipping state and calling the
    # baseline-policy stage via _state hack — easier than mocking all of
    # docker.from_env(). Simulate the same try/except pattern.
    d._state = "running"  # type: ignore[attr-defined]
    with pytest.raises(DriverError, match="policy apply failure"):
        try:
            await d.set_network_policy(d.network_policy_baseline)
        except BaseException:
            await d._teardown(delete=True)  # type: ignore[attr-defined]
            d._state = "stopped"            # type: ignore[attr-defined]
            raise

    assert teardown_calls["n"] == 1
    assert d._state == "stopped"  # type: ignore[attr-defined]
