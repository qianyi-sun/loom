from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from loom_worker.orphan_containers import cleanup_orphan_sandbox_containers


@dataclass
class FakeContainer:
    id: str
    labels: dict[str, str]
    attrs: dict[str, Any] = field(default_factory=dict)
    removed: bool = False
    remove_raises: Exception | None = None

    def remove(self, *, force: bool = False) -> None:
        if self.remove_raises is not None:
            raise self.remove_raises
        self.removed = True


class FakeContainers:
    def __init__(self, containers: list[FakeContainer]) -> None:
        self._containers = containers
        self.calls: list[dict[str, Any]] = []

    def list(
        self,
        *,
        all: bool = False,
        filters: dict[str, str] | None = None,
    ) -> list[FakeContainer]:
        self.calls.append({"all": all, "filters": filters})
        return list(self._containers)


class FakeDockerClient:
    def __init__(self, containers: list[FakeContainer]) -> None:
        self.containers = FakeContainers(containers)


def _started_at(delta: timedelta) -> dict[str, Any]:
    when = datetime.now(UTC) + delta
    return {"State": {"StartedAt": when.isoformat().replace("+00:00", "Z")}}


def _fresh() -> dict[str, Any]:
    return _started_at(timedelta(minutes=-5))


def _stale() -> dict[str, Any]:
    return _started_at(timedelta(days=-3))


def test_removes_terminal_trials() -> None:
    trial_a, trial_b, trial_c = uuid4(), uuid4(), uuid4()
    containers = [
        FakeContainer(id="ca", labels={"loom.trial_id": str(trial_a)}, attrs=_fresh()),
        FakeContainer(id="cb", labels={"loom.trial_id": str(trial_b)}, attrs=_fresh()),
        FakeContainer(id="cc", labels={"loom.trial_id": str(trial_c)}, attrs=_fresh()),
    ]
    client = FakeDockerClient(containers)
    states = {trial_a: "succeeded", trial_b: "failed", trial_c: "cancelled"}

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client,
        state_lookup=lambda tid: states[tid],
    )

    assert set(removed) == {trial_a, trial_b, trial_c}
    assert all(c.removed for c in containers)
    assert client.containers.calls == [
        {"all": True, "filters": {"label": "loom.trial_id"}},
    ]


def test_removes_unknown_trials() -> None:
    trial_a = uuid4()
    container = FakeContainer(
        id="c1", labels={"loom.trial_id": str(trial_a)}, attrs=_fresh(),
    )
    client = FakeDockerClient([container])

    def lookup(_tid: UUID) -> str:
        raise LookupError

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lookup,
    )
    assert removed == [trial_a]
    assert container.removed


def test_preserves_non_terminal_when_fresh() -> None:
    trial_a = uuid4()
    container = FakeContainer(
        id="c1", labels={"loom.trial_id": str(trial_a)}, attrs=_fresh(),
    )
    client = FakeDockerClient([container])

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lambda _t: "running",
    )
    assert removed == []
    assert not container.removed


def test_removes_non_terminal_when_older_than_fallback() -> None:
    trial_a = uuid4()
    container = FakeContainer(
        id="c1", labels={"loom.trial_id": str(trial_a)}, attrs=_stale(),
    )
    client = FakeDockerClient([container])

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lambda _t: "claimed",
    )
    assert removed == [trial_a]
    assert container.removed


def test_preserves_non_terminal_when_startedat_missing() -> None:
    """A container without a parseable StartedAt is treated as
    unknown-age; we skip the age-based branch and preserve. This
    avoids a stampede of accidental deletes if Docker's response
    shape ever regresses."""
    trial_a = uuid4()
    container = FakeContainer(
        id="c1", labels={"loom.trial_id": str(trial_a)}, attrs={"State": {}},
    )
    client = FakeDockerClient([container])

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lambda _t: "running",
    )
    assert removed == []
    assert not container.removed


def test_preserves_on_cp_unreachable() -> None:
    """A non-LookupError from state_lookup means transient CP failure;
    do NOT delete containers (fail-safe). Next startup retries."""
    trial_a = uuid4()
    container = FakeContainer(
        id="c1", labels={"loom.trial_id": str(trial_a)}, attrs=_stale(),
    )
    client = FakeDockerClient([container])

    def lookup(_tid: UUID) -> str:
        raise ConnectionError("cp down")

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lookup,
    )
    assert removed == []
    assert not container.removed


def test_skips_containers_with_missing_or_malformed_label() -> None:
    good_trial = uuid4()
    containers = [
        FakeContainer(id="missing", labels={}, attrs=_fresh()),
        FakeContainer(
            id="malformed",
            labels={"loom.trial_id": "not-a-uuid"},
            attrs=_fresh(),
        ),
        FakeContainer(
            id="good",
            labels={"loom.trial_id": str(good_trial)},
            attrs=_fresh(),
        ),
    ]
    client = FakeDockerClient(containers)

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lambda _t: "succeeded",
    )
    assert removed == [good_trial]
    assert containers[0].removed is False
    assert containers[1].removed is False
    assert containers[2].removed is True


def test_docker_list_failure_returns_empty() -> None:
    class BrokenContainers:
        def list(self, *, all: bool = False, filters: Any = None) -> list[Any]:
            raise RuntimeError("docker socket unreachable")

    class BrokenClient:
        containers = BrokenContainers()

    removed = cleanup_orphan_sandbox_containers(
        docker_client=BrokenClient(),
        state_lookup=lambda _t: "succeeded",
    )
    assert removed == []


def test_remove_failure_does_not_break_sweep() -> None:
    """One container's remove() raising must not skip subsequent
    containers."""
    trial_a, trial_b = uuid4(), uuid4()
    ca = FakeContainer(
        id="ca",
        labels={"loom.trial_id": str(trial_a)},
        attrs=_fresh(),
        remove_raises=RuntimeError("docker api hiccup"),
    )
    cb = FakeContainer(
        id="cb", labels={"loom.trial_id": str(trial_b)}, attrs=_fresh(),
    )
    client = FakeDockerClient([ca, cb])

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client, state_lookup=lambda _t: "succeeded",
    )
    assert removed == [trial_b]
    assert cb.removed is True


def test_uses_explicit_now_and_fallback_window() -> None:
    trial_a = uuid4()
    started_at_epoch = 1_700_000_000.0
    started_at = (
        datetime.fromtimestamp(started_at_epoch, tz=UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    container = FakeContainer(
        id="c1",
        labels={"loom.trial_id": str(trial_a)},
        attrs={"State": {"StartedAt": started_at}},
    )
    client = FakeDockerClient([container])

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client,
        state_lookup=lambda _t: "claimed",
        now_sec=started_at_epoch + 30,
        non_terminal_fallback_sec=60,
    )
    assert removed == []
    assert not container.removed

    removed = cleanup_orphan_sandbox_containers(
        docker_client=client,
        state_lookup=lambda _t: "claimed",
        now_sec=started_at_epoch + 120,
        non_terminal_fallback_sec=60,
    )
    assert removed == [trial_a]
    assert container.removed
