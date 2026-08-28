from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Literal, cast

import pytest

from loom_cli.rollout.operator import staging_mutation_guard
from loom_cli.rollout.operator.readonly_database_client import DatabaseQuery
from loom_cli.rollout.operator.staging_mutation_guard import (
    MutationGuardError,
    MutationGuardEvidence,
    MutationGuardManager,
    _resolve_candidate,
    guard_evidence_path,
    hold_request_guard,
    read_mutation_guard_evidence,
    reconcile_orphaned_guard,
)
from tests.loom_cli.rollout.operator.test_systemd import make_config

_REQUEST_ID = "req-alpha"
_CANDIDATE_SHA = "a" * 40
_CANDIDATE_TREE = "b" * 40
_GENERATION = "1" * 32
_TRY_SQL = "SELECT pg_try_advisory_lock(5498691230183247727) AS acquired"
_EPOCH_SQL = (
    "SELECT epoch AS mutation_epoch FROM staging_mutation_epochs "
    "WHERE environment = 'staging' AND namespace = 'loom-staging'"
)
_UNLOCK_SQL = "SELECT pg_advisory_unlock(5498691230183247727) AS released"
_HEALTH_SQL = (
    "SELECT pg_backend_pid() AS backend_pid, count(*) = 1 AS owns_lock "
    "FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
    "AND classid = 1280263818 AND objid = 1621151599 AND objsubid = 1 "
    "AND mode = 'ExclusiveLock' AND granted"
)
_REQUEST_ANNOTATION = "loom.carin.dev/staging-mutation-guard-request"
_CANDIDATE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-sha"
_TREE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-tree"


def _config(tmp_path: Path):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700, exist_ok=True)
    return replace(make_config(), runtime_root=runtime_root)


def test_candidate_resolution_accepts_root_owned_installer_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config()

    def root_owned_git(
        argv: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: int,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del check, capture_output, text, timeout, env
        safe_directory = ["-c", f"safe.directory={config.runner_repo}"]
        if not any(argv[index : index + 2] == safe_directory for index in range(len(argv) - 1)):
            return subprocess.CompletedProcess(
                argv,
                128,
                "",
                "fatal: detected dubious ownership in repository\n",
            )
        revision = argv[-1]
        values = {"HEAD": _CANDIDATE_SHA, "HEAD^{tree}": _CANDIDATE_TREE}
        return subprocess.CompletedProcess(argv, 0, f"{values[revision]}\n", "")

    monkeypatch.setattr(staging_mutation_guard.subprocess, "run", root_owned_git)

    assert _resolve_candidate(config) == (_CANDIDATE_SHA, _CANDIDATE_TREE)


@pytest.fixture(autouse=True)
def _trusted_reconciliation_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        staging_mutation_guard,
        "_resolve_candidate",
        lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
    )


def _cronjob(*, active: bool = False) -> dict[str, object]:
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "annotations": {"example.test/preserved": "value"},
            "labels": {"app": "loom-staging-data-lifecycle"},
            "name": "loom-staging-data-lifecycle",
            "namespace": "loom-staging",
            "resourceVersion": "10",
            "uid": "50de34f1-f12b-4dce-9f1c-e049f066bc54",
        },
        "spec": {
            "concurrencyPolicy": "Forbid",
            "schedule": "*/5 * * * *",
            "suspend": False,
        },
        "status": {
            "active": (
                [
                    {
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "loom-staging-data-lifecycle-12345",
                        "namespace": "loom-staging",
                        "uid": "4a401434-f944-4432-8ec7-62f4390ea409",
                    }
                ]
                if active
                else []
            )
        },
    }


class _Cluster:
    def __init__(self, *, active: bool = False, clear_active_after_gets: int = 0) -> None:
        self.cronjob = _cronjob(active=active)
        self.clear_active_after_gets = clear_active_after_gets
        self.post_suspend_gets = 0
        self.calls: list[list[str]] = []
        self.events: list[str] = []
        self.job_owner_uid = cast(dict[str, object], self.cronjob["metadata"])["uid"]
        self.job_active = active
        self.job_list_calls = 0
        self.job_list_resource_version = "20"

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        command = tuple(argv)
        if "get" in command and "cronjob/loom-staging-data-lifecycle" in command:
            spec = cast(dict[str, object], self.cronjob["spec"])
            if spec["suspend"] is True:
                self.post_suspend_gets += 1
                if self.post_suspend_gets > self.clear_active_after_gets:
                    cast(dict[str, object], self.cronjob["status"])["active"] = []
                    self.job_active = False
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.cronjob), "")
        if "get" in command and "jobs" in command:
            self.job_list_calls += 1
            job = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "labels": {
                        "batch.kubernetes.io/controller-uid": self.job_owner_uid,
                        "batch.kubernetes.io/job-name": "loom-staging-data-lifecycle-12345",
                    },
                    "name": "loom-staging-data-lifecycle-12345",
                    "namespace": "loom-staging",
                    "uid": "4a401434-f944-4432-8ec7-62f4390ea409",
                    "ownerReferences": [
                        {
                            "apiVersion": "batch/v1",
                            "blockOwnerDeletion": True,
                            "controller": True,
                            "kind": "CronJob",
                            "name": "loom-staging-data-lifecycle",
                            "uid": self.job_owner_uid,
                        }
                    ],
                },
                "status": {"conditions": []},
            }
            value = {
                "apiVersion": "v1",
                "items": [job] if self.job_active else [],
                "kind": "List",
                "metadata": {"resourceVersion": self.job_list_resource_version},
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        if "get" in command and "job/loom-staging-data-lifecycle-12345" in command:
            job = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "labels": {
                        "batch.kubernetes.io/controller-uid": self.job_owner_uid,
                        "batch.kubernetes.io/job-name": "loom-staging-data-lifecycle-12345",
                    },
                    "name": "loom-staging-data-lifecycle-12345",
                    "namespace": "loom-staging",
                    "uid": "4a401434-f944-4432-8ec7-62f4390ea409",
                    "ownerReferences": [
                        {
                            "apiVersion": "batch/v1",
                            "blockOwnerDeletion": True,
                            "controller": True,
                            "kind": "CronJob",
                            "name": "loom-staging-data-lifecycle",
                            "uid": self.job_owner_uid,
                        }
                    ],
                },
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(job), "")
        if "patch" in command and "cronjob/loom-staging-data-lifecycle" in command:
            payload = json.loads(argv[argv.index("--patch") + 1])
            metadata_patch = payload["metadata"]
            metadata = cast(dict[str, object], self.cronjob["metadata"])
            assert metadata_patch["resourceVersion"] == metadata["resourceVersion"]
            annotations = cast(dict[str, str], metadata.setdefault("annotations", {}))
            for key, value in metadata_patch.get("annotations", {}).items():
                if value is None:
                    annotations.pop(key, None)
                else:
                    annotations[key] = value
            cast(dict[str, object], self.cronjob["spec"])["suspend"] = payload["spec"]["suspend"]
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            self.events.append("suspend" if payload["spec"]["suspend"] is True else "restore")
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.cronjob), "")
        raise AssertionError(command)

    def apply_candidate_cronjob(self) -> None:
        metadata = cast(dict[str, object], self.cronjob["metadata"])
        cast(dict[str, object], self.cronjob["spec"])["suspend"] = False
        metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)


def _query_context(
    acquired: list[bool],
    events: list[str],
    *,
    health: list[tuple[int, bool] | BaseException] | None = None,
) -> AbstractContextManager[DatabaseQuery]:
    @contextmanager
    def opened() -> Iterator[DatabaseQuery]:
        def query(sql: str) -> tuple[dict[str, object], ...]:
            if sql == _TRY_SQL:
                events.append("try-lock")
                return ({"acquired": acquired.pop(0)},)
            if sql == _EPOCH_SQL:
                events.append("read-epoch")
                return ({"mutation_epoch": 100},)
            if sql == _HEALTH_SQL:
                events.append("lock-health")
                outcome = (health or [(4321, True)]).pop(0)
                if isinstance(outcome, BaseException):
                    raise outcome
                backend_pid, owns_lock = outcome
                return ({"backend_pid": backend_pid, "owns_lock": owns_lock},)
            if sql == _UNLOCK_SQL:
                events.append("unlock")
                return ({"released": True},)
            raise AssertionError(sql)

        yield query

    return opened()


def _hold(
    tmp_path: Path,
    cluster: _Cluster,
    *,
    acquired: list[bool] | None = None,
    stop_requested=None,
    max_active_waits: int = 4,
    max_lock_attempts: int = 4,
    owner_running=None,
    query_health: list[tuple[int, bool] | BaseException] | None = None,
    sleep=None,
    **guard_kwargs,
):
    config = _config(tmp_path)
    requested = (
        (lambda: guard_evidence_path(config, _REQUEST_ID).exists())
        if stop_requested is None
        else stop_requested
    )
    return hold_request_guard(
        config=config,
        request_id=_REQUEST_ID,
        generation=_GENERATION,
        service_uid=os.getuid(),
        run=cluster,
        query_context=lambda **_kwargs: _query_context(
            [True] if acquired is None else acquired,
            cluster.events,
            health=query_health,
        ),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
        stop_requested=requested,
        sleep=(lambda _seconds: None) if sleep is None else sleep,
        max_active_waits=max_active_waits,
        max_lock_attempts=max_lock_attempts,
        **({"owner_running": owner_running} if owner_running is not None else {}),
        **guard_kwargs,
    )


def test_guard_suspends_waits_locks_and_restores_before_unlock(tmp_path: Path) -> None:
    cluster = _Cluster(active=True, clear_active_after_gets=1)
    ready_documents: list[dict[str, object]] = []
    config = _config(tmp_path)

    def stop_requested() -> bool:
        path = guard_evidence_path(config, _REQUEST_ID)
        if not path.exists():
            return False
        ready_documents.append(
            read_mutation_guard_evidence(path, service_uid=os.getuid()).to_dict()
        )
        return True

    evidence = hold_request_guard(
        config=config,
        request_id=_REQUEST_ID,
        generation=_GENERATION,
        service_uid=os.getuid(),
        run=cluster,
        query_context=lambda **_kwargs: _query_context([False, True], cluster.events),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
        stop_requested=stop_requested,
        sleep=lambda _seconds: None,
        max_active_waits=4,
        max_lock_attempts=4,
    )

    assert ready_documents[0]["state"] == "ready"
    assert ready_documents[0]["request_id"] == _REQUEST_ID
    assert ready_documents[0]["candidate_sha"] == _CANDIDATE_SHA
    assert ready_documents[0]["candidate_tree"] == _CANDIDATE_TREE
    assert ready_documents[0]["generation"] == _GENERATION
    assert ready_documents[0]["mutation_epoch"] == 100
    assert ready_documents[0]["guard_pid"] == os.getpid()
    assert ready_documents[0]["database_backend_pid"] == 4321
    assert evidence.state == "released"
    assert evidence.generation == _GENERATION
    assert (
        read_mutation_guard_evidence(
            guard_evidence_path(config, _REQUEST_ID), service_uid=os.getuid()
        )
        == evidence
    )
    assert cluster.events == [
        "suspend",
        "try-lock",
        "try-lock",
        "lock-health",
        "lock-health",
        "read-epoch",
        "lock-health",
        "restore",
        "unlock",
    ]
    metadata = cast(dict[str, object], cluster.cronjob["metadata"])
    assert cast(dict[str, str], metadata["annotations"]) == {"example.test/preserved": "value"}
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is False
    suspend_patch = next(call for call in cluster.calls if "patch" in call)
    payload = json.loads(suspend_patch[suspend_patch.index("--patch") + 1])
    assert payload["metadata"]["resourceVersion"] == "10"
    assert "--field-manager=loom-staging-rollout" in suspend_patch


def test_guard_stop_timeout_covers_signal_during_owner_inventory(tmp_path: Path) -> None:
    from loom_cli.rollout.operator.systemd import SystemdUserManager

    cluster = _Cluster()
    config = _config(tmp_path)
    evidence_path = guard_evidence_path(config, _REQUEST_ID)
    stop_signal_pending = False

    def stop_requested() -> bool:
        nonlocal stop_signal_pending
        if not evidence_path.exists():
            return False
        if not stop_signal_pending:
            cluster.events.append("stop-check-false-then-signal")
            stop_signal_pending = True
            return False
        cluster.events.append("stop-observed")
        return True

    def owner_running(request_id: str) -> bool:
        assert request_id == _REQUEST_ID
        cluster.events.append("owner-inventory")
        return True

    def sleep(seconds: float) -> None:
        if evidence_path.exists():
            assert seconds == 1.0
            cluster.events.append("poll-sleep")

    evidence = hold_request_guard(
        config=config,
        request_id=_REQUEST_ID,
        generation=_GENERATION,
        service_uid=os.getuid(),
        run=cluster,
        query_context=lambda **_kwargs: _query_context([True], cluster.events),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
        stop_requested=stop_requested,
        sleep=sleep,
        owner_running=owner_running,
        owner_launch_grace_seconds=0,
        max_active_waits=4,
        max_lock_attempts=4,
    )
    launch = SystemdUserManager(
        config,
        service_uid=os.getuid(),
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
    ).start_mutation_guard_argv(_REQUEST_ID, _GENERATION)
    stop_timeout = int(
        next(item for item in launch if item.startswith("TimeoutStopSec="))
        .removeprefix("TimeoutStopSec=")
        .removesuffix("s")
    )
    complete_normal_release = (
        30  # in-flight owner inventory after the false stop check
        + 1  # steady-state poll sleep
        + 15  # next lock-health statement before the next stop check
        + 2 * 120  # CronJob GET plus PATCH
        + 15  # advisory unlock statement
        + 2 * 5  # graceful then forced database tunnel process teardown
        + 1  # bounded stderr-drain teardown
        + 30  # released-evidence publication margin
    )

    assert evidence.state == "released"
    assert cluster.events[-8:] == [
        "lock-health",
        "stop-check-false-then-signal",
        "owner-inventory",
        "poll-sleep",
        "lock-health",
        "stop-observed",
        "restore",
        "unlock",
    ]
    assert stop_timeout > complete_normal_release


def test_guard_lists_exact_owner_uid_jobs_and_requires_stable_empty_before_and_after_lock(
    tmp_path: Path,
) -> None:
    cluster = _Cluster(active=False)
    # CronJob.status.active is intentionally empty while the owned Job inventory
    # still contains one nonterminal Job: this is the create/status lag window.
    cluster.job_active = True

    evidence = _hold(tmp_path, cluster)

    assert evidence.state == "released"
    assert cluster.job_list_calls >= 5
    selectors = [item for call in cluster.calls for item in call if item.startswith("--selector=")]
    assert (
        selectors
        == ["--selector=batch.kubernetes.io/controller-uid=50de34f1-f12b-4dce-9f1c-e049f066bc54"]
        * cluster.job_list_calls
    )
    assert cluster.events.index("try-lock") > 0


def test_guard_accepts_live_empty_job_list_without_resource_version(tmp_path: Path) -> None:
    cluster = _Cluster()
    cluster.job_list_resource_version = ""

    evidence = _hold(tmp_path, cluster)

    assert evidence.state == "released"
    assert cluster.job_list_calls == 4


def test_guard_repeats_stable_job_inventory_after_lock_before_ready_publication(
    tmp_path: Path,
) -> None:
    class PostLockRaceCluster(_Cluster):
        inventory = iter((False, False, True, False, False))

        def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
            if "get" in argv and "jobs" in argv:
                self.job_active = next(self.inventory)
            return super().__call__(argv)

    cluster = PostLockRaceCluster()

    evidence = _hold(tmp_path, cluster)

    assert evidence.state == "released"
    assert cluster.job_list_calls == 5
    assert cluster.events == [
        "suspend",
        "try-lock",
        "lock-health",
        "lock-health",
        "read-epoch",
        "lock-health",
        "restore",
        "unlock",
    ]


def test_ready_evidence_binds_backend_and_one_absolute_deadline(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    ready: list[MutationGuardEvidence] = []

    def stop_requested() -> bool:
        path = guard_evidence_path(config, _REQUEST_ID)
        if not path.exists():
            return False
        ready.append(read_mutation_guard_evidence(path, service_uid=os.getuid()))
        return True

    _hold(
        tmp_path,
        cluster,
        stop_requested=stop_requested,
        wall_time=lambda: 2_000_000_000.0,
        monotonic=lambda: 100.0,
    )

    assert ready[0].database_backend_pid == 4321
    assert ready[0].deadline_unix_seconds > 2_000_000_000
    assert ready[0].deadline_unix_seconds < 2_001_000_000


def test_pre_readiness_elapsed_time_cannot_extend_guard_lifetime(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    evidence_path = guard_evidence_path(config, _REQUEST_ID)
    entry_monotonic = 100.0
    entry_wall_time = 2_000_000_000.0
    runtime_seconds = staging_mutation_guard.MUTATION_GUARD_RUNTIME_SECONDS
    internal_lifetime_seconds = runtime_seconds - 5 * 60

    class Clock:
        monotonic_value = entry_monotonic
        wall_value = entry_wall_time

        def monotonic(self) -> float:
            return self.monotonic_value

        def wall_time(self) -> float:
            return self.wall_value

        def advance(self, seconds: float) -> None:
            self.monotonic_value += seconds
            self.wall_value += seconds

    clock = Clock()

    def sleep(_seconds: float) -> None:
        if not evidence_path.exists():
            clock.advance(10 * 60)
            return
        internal_deadline = entry_monotonic + internal_lifetime_seconds
        if clock.monotonic_value < internal_deadline:
            clock.advance(internal_deadline - clock.monotonic_value)
            return
        raise AssertionError("guard outlived its entry-bound internal deadline")

    with pytest.raises(MutationGuardError, match="absolute deadline"):
        _hold(
            tmp_path,
            cluster,
            stop_requested=lambda: False,
            owner_running=lambda _request_id: True,
            owner_launch_grace_seconds=0,
            sleep=sleep,
            monotonic=clock.monotonic,
            wall_time=clock.wall_time,
        )

    ready = read_mutation_guard_evidence(evidence_path, service_uid=os.getuid())
    assert ready.deadline_unix_seconds == entry_wall_time + internal_lifetime_seconds
    assert clock.monotonic_value == entry_monotonic + internal_lifetime_seconds
    assert internal_lifetime_seconds < runtime_seconds
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True
    assert "restore" not in cluster.events
    assert "unlock" not in cluster.events


def test_pre_readiness_deadline_expiry_restores_without_ready_evidence(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    clock = [0.0]

    def expire_during_drain(_seconds: float) -> None:
        clock[0] = staging_mutation_guard.MUTATION_GUARD_RUNTIME_SECONDS - 5 * 60

    with pytest.raises(MutationGuardError, match="deadline expired before readiness"):
        _hold(
            tmp_path,
            cluster,
            stop_requested=lambda: False,
            sleep=expire_during_drain,
            monotonic=lambda: clock[0],
            wall_time=lambda: 2_000_000_000.0,
        )

    assert cluster.job_list_calls == 1
    assert cluster.events == ["suspend", "restore"]
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is False
    assert not guard_evidence_path(config, _REQUEST_ID).exists()


@pytest.mark.parametrize(
    "health_outcomes",
    [
        [(4321, True), (4321, True), RuntimeError("port forward lost")],
        [(4321, True), (4321, True), (9876, True)],
        [(4321, True), (4321, True), (4321, False)],
    ],
)
def test_post_ready_session_backend_or_lock_loss_is_irreversible(
    tmp_path: Path,
    health_outcomes: list[tuple[int, bool] | BaseException],
) -> None:
    cluster = _Cluster()
    sleeps = 0

    def bounded_sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 10:
            raise AssertionError("guard failed to check the exact lock session")

    with pytest.raises(MutationGuardError, match="ownership was lost"):
        _hold(
            tmp_path,
            cluster,
            stop_requested=lambda: False,
            query_health=health_outcomes,
            owner_running=lambda _request_id: True,
            owner_launch_grace_seconds=0,
            sleep=bounded_sleep,
        )

    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True
    assert "restore" not in cluster.events
    assert "unlock" not in cluster.events
    assert (
        read_mutation_guard_evidence(
            guard_evidence_path(_config(tmp_path), _REQUEST_ID),
            service_uid=os.getuid(),
        ).state
        == "ready"
    )


def test_guard_releases_normally_when_the_exact_worker_owner_disappears(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    owner_checks: list[str] = []

    evidence = _hold(
        tmp_path,
        cluster,
        stop_requested=lambda: False,
        owner_running=lambda request_id: owner_checks.append(request_id) or False,
        owner_launch_grace_seconds=0,
    )

    assert evidence.state == "released"
    assert owner_checks == [_REQUEST_ID]
    assert cluster.events[-2:] == ["restore", "unlock"]


def test_absolute_deadline_with_live_owner_fails_without_restoring(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    clock = [0.0]

    def stop_requested() -> bool:
        if guard_evidence_path(config, _REQUEST_ID).exists():
            clock[0] = 10_000_000.0
        return False

    with pytest.raises(MutationGuardError, match="deadline"):
        _hold(
            tmp_path,
            cluster,
            stop_requested=stop_requested,
            owner_running=lambda _request_id: True,
            owner_launch_grace_seconds=0,
            monotonic=lambda: clock[0],
            wall_time=lambda: 2_000_000_000.0,
        )

    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True
    assert "restore" not in cluster.events
    assert "unlock" not in cluster.events


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda item: item.__setitem__("apiVersion", "batch/v1beta1"), "identity"),
        (
            lambda item: cast(dict[str, object], item["metadata"]).__setitem__("uid", ""),
            "identity",
        ),
        (
            lambda item: cast(dict[str, object], item["spec"]).__setitem__(
                "schedule", "*/10 * * * *"
            ),
            "schedule",
        ),
        (
            lambda item: cast(dict[str, object], item["spec"]).__setitem__(
                "concurrencyPolicy", "Allow"
            ),
            "concurrency",
        ),
        (
            lambda item: cast(
                dict[str, str], cast(dict[str, object], item["metadata"])["annotations"]
            ).__setitem__(_REQUEST_ANNOTATION, "req-foreign"),
            "annotation",
        ),
    ],
)
def test_guard_rejects_cronjob_authority_drift_before_mutation(
    tmp_path: Path,
    mutation,
    message: str,
) -> None:
    cluster = _Cluster()
    mutation(cluster.cronjob)

    with pytest.raises(MutationGuardError, match=message):
        _hold(tmp_path, cluster)

    assert not any("patch" in call for call in cluster.calls)
    assert cluster.events == []


def test_guard_rejects_foreign_active_job_and_restores_freeze(tmp_path: Path) -> None:
    cluster = _Cluster(active=True, clear_active_after_gets=10)
    cluster.job_owner_uid = "e7848f1e-184b-4780-b302-ee261db2f7f4"

    with pytest.raises(MutationGuardError, match="owner"):
        _hold(tmp_path, cluster, max_active_waits=1)

    assert cluster.events == ["suspend", "restore"]
    assert "try-lock" not in cluster.events


def test_guard_lock_exhaustion_restores_without_ready_evidence(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)

    with pytest.raises(MutationGuardError, match="advisory lock"):
        hold_request_guard(
            config=config,
            request_id=_REQUEST_ID,
            generation=_GENERATION,
            service_uid=os.getuid(),
            run=cluster,
            query_context=lambda **_kwargs: _query_context([False, False], cluster.events),
            resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
            stop_requested=lambda: False,
            sleep=lambda _seconds: None,
            max_active_waits=2,
            max_lock_attempts=2,
        )

    assert cluster.events == ["suspend", "try-lock", "try-lock", "restore"]
    assert not guard_evidence_path(config, _REQUEST_ID).exists()


def test_ambiguous_suspend_response_is_read_back_and_restored(tmp_path: Path) -> None:
    class AmbiguousSuspendCluster(_Cluster):
        failed_suspend_response = False

        def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
            result = super().__call__(argv)
            if (
                not self.failed_suspend_response
                and "patch" in argv
                and json.loads(argv[argv.index("--patch") + 1])["spec"]["suspend"] is True
            ):
                self.failed_suspend_response = True
                return subprocess.CompletedProcess(argv, 1, "", "connection closed")
            return result

    cluster = AmbiguousSuspendCluster()

    with pytest.raises(MutationGuardError, match="command failed"):
        _hold(tmp_path, cluster)

    assert cluster.events == ["suspend", "restore"]
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is False


def test_guard_release_accepts_candidate_apply_already_unsuspended(tmp_path: Path) -> None:
    cluster = _Cluster()

    def stop_requested() -> bool:
        if not guard_evidence_path(_config(tmp_path), _REQUEST_ID).exists():
            return False
        cluster.apply_candidate_cronjob()
        return True

    evidence = _hold(tmp_path, cluster, stop_requested=stop_requested)

    assert evidence.state == "released"
    assert cluster.events == [
        "suspend",
        "try-lock",
        "lock-health",
        "lock-health",
        "read-epoch",
        "lock-health",
        "restore",
        "unlock",
    ]


def test_guard_stop_during_active_job_wait_restores_without_locking(tmp_path: Path) -> None:
    cluster = _Cluster(active=True, clear_active_after_gets=10)
    stop_checks = iter((False, True))

    with pytest.raises(MutationGuardError, match="stop requested"):
        _hold(
            tmp_path,
            cluster,
            stop_requested=lambda: next(stop_checks),
            max_active_waits=4,
        )

    assert cluster.events == ["suspend", "restore"]
    assert "try-lock" not in cluster.events


def test_guard_never_reacquires_an_exact_annotated_freeze(tmp_path: Path) -> None:
    cluster = _Cluster()
    metadata = cast(dict[str, object], cluster.cronjob["metadata"])
    cast(dict[str, str], metadata["annotations"]).update(
        {
            _REQUEST_ANNOTATION: _REQUEST_ID,
            _CANDIDATE_ANNOTATION: _CANDIDATE_SHA,
            _TREE_ANNOTATION: _CANDIDATE_TREE,
        }
    )
    cast(dict[str, object], cluster.cronjob["spec"])["suspend"] = True

    with pytest.raises(MutationGuardError, match="already annotated"):
        _hold(tmp_path, cluster)

    assert cluster.events == []
    assert "try-lock" not in cluster.events


def test_guard_evidence_reader_rejects_digest_drift_and_symlink(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    _hold(tmp_path, cluster)
    path = guard_evidence_path(config, _REQUEST_ID)
    original = path.read_bytes()
    value = json.loads(original)
    value["mutation_epoch"] = 101
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(0o600)

    with pytest.raises(MutationGuardError, match="digest"):
        read_mutation_guard_evidence(path, service_uid=os.getuid())

    outside = tmp_path / "outside.json"
    outside.write_bytes(original)
    outside.chmod(0o600)
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(MutationGuardError, match="evidence"):
        read_mutation_guard_evidence(path, service_uid=os.getuid())


def test_guard_evidence_round_trip_requires_exact_generation() -> None:
    evidence = MutationGuardEvidence.build(
        request_id=_REQUEST_ID,
        candidate_sha=_CANDIDATE_SHA,
        candidate_tree=_CANDIDATE_TREE,
        generation=_GENERATION,
        mutation_epoch=100,
        guard_pid=4321,
        database_backend_pid=9876,
        deadline_unix_seconds=2_000_000_000,
        cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
        suspended_resource_version="11",
        state="ready",
    )

    document = evidence.to_dict()
    assert document["generation"] == _GENERATION
    assert MutationGuardEvidence.from_dict(document) == evidence
    document.pop("generation")
    with pytest.raises(MutationGuardError, match="fields"):
        MutationGuardEvidence.from_dict(document)
    with pytest.raises(MutationGuardError, match="identity"):
        MutationGuardEvidence.build(
            request_id=_REQUEST_ID,
            candidate_sha=_CANDIDATE_SHA,
            candidate_tree=_CANDIDATE_TREE,
            generation="A" * 32,
            mutation_epoch=100,
            guard_pid=4321,
            database_backend_pid=9876,
            deadline_unix_seconds=2_000_000_000,
            cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
            suspended_resource_version="11",
            state="ready",
        )


def test_guard_evidence_is_private_regular_service_state(tmp_path: Path) -> None:
    cluster = _Cluster()
    config = _config(tmp_path)
    _hold(tmp_path, cluster)

    path = guard_evidence_path(config, _REQUEST_ID)
    metadata = path.stat()
    assert stat.S_ISREG(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert metadata.st_uid == os.getuid()
    assert metadata.st_nlink == 1


def test_mutation_guard_manager_binds_live_unit_pid_candidate_and_request(tmp_path: Path) -> None:
    config = _config(tmp_path)
    ready = MutationGuardEvidence.build(
        request_id=_REQUEST_ID,
        candidate_sha=_CANDIDATE_SHA,
        candidate_tree=_CANDIDATE_TREE,
        generation=_GENERATION,
        mutation_epoch=100,
        guard_pid=4321,
        database_backend_pid=9876,
        deadline_unix_seconds=2_000_000_000,
        cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
        suspended_resource_version="11",
        state="ready",
    )
    directory = guard_evidence_path(config, _REQUEST_ID).parent
    directory.mkdir(mode=0o700)
    path = guard_evidence_path(config, _REQUEST_ID)
    path.write_text(json.dumps(ready.to_dict(), sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(0o600)

    class Systemd:
        def start_mutation_guard(self, request_id: str) -> MutationGuardEvidence:
            assert request_id == _REQUEST_ID
            return ready

        def show_mutation_guard(self, request_id: str):  # type: ignore[no-untyped-def]
            assert request_id == _REQUEST_ID
            return type("Status", (), {"is_running": True, "main_pid": 4321})()

        def stop_mutation_guard(self, request_id: str) -> MutationGuardEvidence:
            assert request_id == _REQUEST_ID
            return MutationGuardEvidence.build(
                request_id=ready.request_id,
                candidate_sha=ready.candidate_sha,
                candidate_tree=ready.candidate_tree,
                generation=ready.generation,
                mutation_epoch=ready.mutation_epoch,
                guard_pid=ready.guard_pid,
                database_backend_pid=ready.database_backend_pid,
                deadline_unix_seconds=ready.deadline_unix_seconds,
                cronjob_uid=ready.cronjob_uid,
                suspended_resource_version=ready.suspended_resource_version,
                state="released",
            )

    manager = MutationGuardManager(
        config=config,
        service_uid=os.getuid(),
        systemd=Systemd(),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
    )

    assert manager.acquire(_REQUEST_ID) == ready
    assert manager.assert_ready(_REQUEST_ID) == ready
    assert manager.release(_REQUEST_ID).state == "released"


def test_mutation_guard_manager_releases_started_unit_when_acquired_evidence_drifts(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    drifted = MutationGuardEvidence.build(
        request_id=_REQUEST_ID,
        candidate_sha=_CANDIDATE_SHA,
        candidate_tree="c" * 40,
        generation=_GENERATION,
        mutation_epoch=100,
        guard_pid=4321,
        database_backend_pid=9876,
        deadline_unix_seconds=2_000_000_000,
        cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
        suspended_resource_version="11",
        state="ready",
    )
    stopped: list[str] = []

    class Systemd:
        def start_mutation_guard(self, request_id: str) -> MutationGuardEvidence:
            return drifted

        def show_mutation_guard(self, request_id: str):  # type: ignore[no-untyped-def]
            raise AssertionError(request_id)

        def stop_mutation_guard(self, request_id: str) -> MutationGuardEvidence:
            stopped.append(request_id)
            return MutationGuardEvidence.build(
                request_id=drifted.request_id,
                candidate_sha=drifted.candidate_sha,
                candidate_tree=drifted.candidate_tree,
                generation=drifted.generation,
                mutation_epoch=drifted.mutation_epoch,
                guard_pid=drifted.guard_pid,
                database_backend_pid=drifted.database_backend_pid,
                deadline_unix_seconds=drifted.deadline_unix_seconds,
                cronjob_uid=drifted.cronjob_uid,
                suspended_resource_version=drifted.suspended_resource_version,
                state="released",
            )

    manager = MutationGuardManager(
        config=config,
        service_uid=os.getuid(),
        systemd=Systemd(),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
    )

    with pytest.raises(MutationGuardError, match="binding drifted"):
        manager.acquire(_REQUEST_ID)

    assert stopped == [_REQUEST_ID]


def _annotate_guard(cluster: _Cluster) -> None:
    metadata = cast(dict[str, object], cluster.cronjob["metadata"])
    cast(dict[str, str], metadata["annotations"]).update(
        {
            _REQUEST_ANNOTATION: _REQUEST_ID,
            _CANDIDATE_ANNOTATION: _CANDIDATE_SHA,
            _TREE_ANNOTATION: _CANDIDATE_TREE,
        }
    )
    cast(dict[str, object], cluster.cronjob["spec"])["suspend"] = True


def _write_guard_evidence(
    config,  # type: ignore[no-untyped-def]
    *,
    guard_pid: int = 4321,
    state: Literal["ready", "released"] = "ready",
) -> None:
    evidence = MutationGuardEvidence.build(
        request_id=_REQUEST_ID,
        candidate_sha=_CANDIDATE_SHA,
        candidate_tree=_CANDIDATE_TREE,
        generation=_GENERATION,
        mutation_epoch=100,
        guard_pid=guard_pid,
        database_backend_pid=9876,
        deadline_unix_seconds=2_000_000_000,
        cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
        suspended_resource_version="11",
        state=state,
    )
    path = guard_evidence_path(config, _REQUEST_ID)
    path.parent.mkdir(mode=0o700)
    path.write_text(json.dumps(evidence.to_dict(), sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(0o600)


def _reconcile_guard(
    *,
    config,  # type: ignore[no-untyped-def]
    cluster: _Cluster,
    show_guard,  # type: ignore[no-untyped-def]
    fence_owners=lambda _request_id: None,
    owner_running=lambda _request_id: False,
):  # type: ignore[no-untyped-def]
    return reconcile_orphaned_guard(
        config=config,
        service_uid=os.getuid(),
        run=cluster,
        show_guard=show_guard,
        fence_owners=fence_owners,
        owner_running=owner_running,
        sleep=lambda _seconds: None,
    )


def test_reconcile_leaves_exact_active_guard_untouched(tmp_path: Path) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config)

    result = _reconcile_guard(
        config=config,
        cluster=cluster,
        show_guard=lambda request_id: SimpleNamespace(is_running=True, main_pid=4321),
        fence_owners=lambda _request_id: (_ for _ in ()).throw(AssertionError()),
        owner_running=lambda _request_id: (_ for _ in ()).throw(AssertionError()),
    )

    assert result == {"request_id": _REQUEST_ID, "status": "active"}
    assert cluster.events == []


def test_reconcile_restores_exact_annotated_freeze_only_when_unit_is_absent(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)

    result = _reconcile_guard(
        config=_config(tmp_path),
        cluster=cluster,
        show_guard=lambda _request_id: None,
    )

    assert result == {"request_id": _REQUEST_ID, "status": "restored"}
    assert cluster.events == ["restore"]
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is False


def test_reconcile_rejects_released_evidence_with_surviving_annotation(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config, state="released")

    with pytest.raises(MutationGuardError, match="released evidence"):
        reconcile_orphaned_guard(
            config=config,
            service_uid=os.getuid(),
            run=cluster,
            show_guard=lambda _request_id: None,
        )

    assert cluster.events == []
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True


def test_reconcile_fence_failure_keeps_freeze_for_later_timer_retry(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config)
    fence_attempts: list[str] = []

    def fail_fence(request_id: str) -> None:
        fence_attempts.append(request_id)
        raise RuntimeError("injected systemctl failure")

    with pytest.raises(MutationGuardError, match="owner fence"):
        _reconcile_guard(
            config=config,
            cluster=cluster,
            show_guard=lambda _request_id: None,
            fence_owners=fail_fence,
            owner_running=lambda _request_id: (_ for _ in ()).throw(AssertionError()),
        )

    assert fence_attempts == [_REQUEST_ID]
    assert cluster.events == []
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True


def test_reconcile_retries_fence_and_restores_after_stable_verified_absence(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config)
    attempts = 0

    def retrying_fence(_request_id: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected first fence failure")

    with pytest.raises(MutationGuardError, match="owner fence"):
        _reconcile_guard(
            config=config,
            cluster=cluster,
            show_guard=lambda _request_id: None,
            fence_owners=retrying_fence,
        )

    owner_checks: list[str] = []
    result = _reconcile_guard(
        config=config,
        cluster=cluster,
        show_guard=lambda _request_id: None,
        fence_owners=retrying_fence,
        owner_running=lambda request_id: owner_checks.append(request_id) or False,
    )

    assert result == {"request_id": _REQUEST_ID, "status": "restored"}
    assert attempts == 2
    assert owner_checks == [_REQUEST_ID, _REQUEST_ID]
    assert cluster.events == ["restore"]


def test_reconcile_keeps_freeze_when_fresh_owner_absence_is_not_stable(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config)
    observations = iter((False, True))
    owner_checks: list[str] = []

    with pytest.raises(MutationGuardError, match="owner remains live"):
        _reconcile_guard(
            config=config,
            cluster=cluster,
            show_guard=lambda _request_id: None,
            fence_owners=lambda _request_id: None,
            owner_running=lambda request_id: owner_checks.append(request_id) or next(observations),
        )

    assert owner_checks == [_REQUEST_ID, _REQUEST_ID]
    assert cluster.events == []
    assert cast(dict[str, object], cluster.cronjob["spec"])["suspend"] is True


def test_reconcile_rejects_foreign_candidate_annotations_without_restoring(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    metadata = cast(dict[str, object], cluster.cronjob["metadata"])
    cast(dict[str, str], metadata["annotations"])[_CANDIDATE_ANNOTATION] = "c" * 40

    with pytest.raises(MutationGuardError, match="candidate"):
        _reconcile_guard(
            config=_config(tmp_path),
            cluster=cluster,
            show_guard=lambda _request_id: None,
        )

    assert cluster.events == []


def test_reconcile_rejects_annotated_unsuspended_cronjob_when_unit_is_absent(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    cast(dict[str, object], cluster.cronjob["spec"])["suspend"] = False

    with pytest.raises(MutationGuardError, match="suspension"):
        _reconcile_guard(
            config=_config(tmp_path),
            cluster=cluster,
            show_guard=lambda _request_id: None,
        )

    assert cluster.events == []


def test_reconcile_rejects_annotated_unsuspended_cronjob_when_unit_is_active(
    tmp_path: Path,
) -> None:
    cluster = _Cluster()
    _annotate_guard(cluster)
    config = _config(tmp_path)
    _write_guard_evidence(config)
    cast(dict[str, object], cluster.cronjob["spec"])["suspend"] = False
    readbacks: list[str] = []

    def active_guard(request_id: str) -> SimpleNamespace:
        readbacks.append(request_id)
        return SimpleNamespace(is_running=True, main_pid=4321)

    with pytest.raises(MutationGuardError, match="suspension"):
        _reconcile_guard(
            config=config,
            cluster=cluster,
            show_guard=active_guard,
        )

    assert cluster.events == []
    assert readbacks == []


def test_reconcile_rejects_partial_annotations_and_unsafe_evidence(tmp_path: Path) -> None:
    partial = _Cluster()
    metadata = cast(dict[str, object], partial.cronjob["metadata"])
    cast(dict[str, str], metadata["annotations"])[_REQUEST_ANNOTATION] = _REQUEST_ID
    cast(dict[str, object], partial.cronjob["spec"])["suspend"] = True

    with pytest.raises(MutationGuardError, match="annotation"):
        _reconcile_guard(
            config=_config(tmp_path),
            cluster=partial,
            show_guard=lambda _request_id: None,
        )

    cluster = _Cluster()
    _annotate_guard(cluster)
    unsafe_root = tmp_path / "unsafe"
    unsafe_root.mkdir()
    config = _config(unsafe_root)
    path = guard_evidence_path(config, _REQUEST_ID)
    path.parent.mkdir(mode=0o700)
    outside = tmp_path / "outside-evidence.json"
    outside.write_text("{}")
    path.symlink_to(outside)
    fenced: list[str] = []
    owner_checks: list[str] = []
    with pytest.raises(MutationGuardError, match="evidence"):
        _reconcile_guard(
            config=config,
            cluster=cluster,
            show_guard=lambda _request_id: None,
            fence_owners=lambda request_id: fenced.append(request_id),
            owner_running=lambda request_id: owner_checks.append(request_id) or False,
        )
    assert fenced == [_REQUEST_ID]
    assert owner_checks == [_REQUEST_ID, _REQUEST_ID]
    assert cluster.events == []


def test_reconcile_is_idempotent_when_cronjob_is_already_unsuspended(tmp_path: Path) -> None:
    cluster = _Cluster()

    assert _reconcile_guard(
        config=_config(tmp_path),
        cluster=cluster,
        show_guard=lambda _request_id: (_ for _ in ()).throw(AssertionError()),
    ) == {"status": "idle"}
    assert cluster.events == []


def test_reconcile_refuses_to_claim_restore_when_exact_release_fails(tmp_path: Path) -> None:
    class _ReleaseFailureCluster(_Cluster):
        def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
            if "patch" in argv and "cronjob/loom-staging-data-lifecycle" in argv:
                return subprocess.CompletedProcess(argv, 1, "", "injected release failure")
            return super().__call__(argv)

    cluster = _ReleaseFailureCluster()
    _annotate_guard(cluster)

    with pytest.raises(MutationGuardError, match="patch"):
        _reconcile_guard(
            config=_config(tmp_path),
            cluster=cluster,
            show_guard=lambda _request_id: None,
        )
    assert cluster.events == []


def test_fence_subcommand_dispatches_only_the_exact_request_bound_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    fenced: list[str] = []
    monkeypatch.setattr(
        staging_mutation_guard.OperatorConfig,
        "load",
        classmethod(lambda _cls, _path: config),
    )
    monkeypatch.setattr(
        staging_mutation_guard,
        "fixed_operator_config_path",
        lambda: Path("/etc/loom/staging-rollout.toml"),
    )
    monkeypatch.setattr(
        staging_mutation_guard.pwd,
        "getpwnam",
        lambda _user: SimpleNamespace(pw_uid=os.geteuid()),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.fence_mutation_guard_owners",
        lambda _manager, request_id, generation: fenced.append(f"{request_id}:{generation}"),
    )

    assert (
        staging_mutation_guard.main(
            ["fence", "--request-id", _REQUEST_ID, "--generation", _GENERATION]
        )
        == 0
    )
    assert fenced == [f"{_REQUEST_ID}:{_GENERATION}"]


def test_reconcile_subcommand_wires_retry_fence_and_owner_liveness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    callbacks: list[str] = []
    monkeypatch.setattr(
        staging_mutation_guard.OperatorConfig,
        "load",
        classmethod(lambda _cls, _path: config),
    )
    monkeypatch.setattr(
        staging_mutation_guard,
        "fixed_operator_config_path",
        lambda: Path("/etc/loom/staging-rollout.toml"),
    )
    monkeypatch.setattr(
        staging_mutation_guard.pwd,
        "getpwnam",
        lambda _user: SimpleNamespace(pw_uid=os.geteuid()),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.show_mutation_guard",
        lambda _manager, request_id: callbacks.append(f"show:{request_id}"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.fence_mutation_guard_owners",
        lambda _manager, request_id: callbacks.append(f"fence:{request_id}"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.mutation_guard_owner_running",
        lambda _manager, request_id: callbacks.append(f"owner:{request_id}") or False,
    )

    def reconcile_with_callbacks(
        *,
        config,  # type: ignore[no-untyped-def]
        service_uid: int,
        run,  # type: ignore[no-untyped-def]
        show_guard,  # type: ignore[no-untyped-def]
        fence_owners,  # type: ignore[no-untyped-def]
        owner_running,  # type: ignore[no-untyped-def]
        sleep=None,  # type: ignore[no-untyped-def]
    ) -> dict[str, str]:
        assert config == _config(tmp_path)
        assert service_uid == os.geteuid()
        assert callable(run)
        assert sleep is None
        show_guard(_REQUEST_ID)
        fence_owners(_REQUEST_ID)
        assert owner_running(_REQUEST_ID) is False
        return {"status": "idle"}

    monkeypatch.setattr(
        staging_mutation_guard,
        "reconcile_orphaned_guard",
        reconcile_with_callbacks,
    )

    assert staging_mutation_guard.main(["reconcile"]) == 0
    assert callbacks == [
        f"show:{_REQUEST_ID}",
        f"fence:{_REQUEST_ID}",
        f"owner:{_REQUEST_ID}",
    ]


def test_guard_cli_preserves_kubernetes_timeout_and_tightly_bounds_systemctl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    observed: list[tuple[str, int]] = []
    monkeypatch.setattr(
        staging_mutation_guard.OperatorConfig,
        "load",
        classmethod(lambda _cls, _path: config),
    )
    monkeypatch.setattr(
        staging_mutation_guard,
        "fixed_operator_config_path",
        lambda: Path("/etc/loom/staging-rollout.toml"),
    )
    monkeypatch.setattr(
        staging_mutation_guard.pwd,
        "getpwnam",
        lambda _user: SimpleNamespace(pw_uid=os.geteuid()),
    )

    def subprocess_run(
        argv,  # type: ignore[no-untyped-def]
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: int,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert text is True
        assert env
        observed.append((argv[0], timeout))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(staging_mutation_guard.subprocess, "run", subprocess_run)

    def exercise_show(manager, request_id: str):  # type: ignore[no-untyped-def]
        manager._run(["systemctl", "show", request_id])
        return None

    def exercise_fence(manager, request_id: str) -> None:  # type: ignore[no-untyped-def]
        manager._run(["systemctl", "fence", request_id])

    def exercise_owner(manager, request_id: str) -> bool:  # type: ignore[no-untyped-def]
        manager._run(["systemctl", "owner", request_id])
        return False

    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.show_mutation_guard",
        exercise_show,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.fence_mutation_guard_owners",
        exercise_fence,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.systemd.SystemdUserManager.mutation_guard_owner_running",
        exercise_owner,
    )

    def exercise_reconcile(
        *,
        config,  # type: ignore[no-untyped-def]
        service_uid: int,
        run,  # type: ignore[no-untyped-def]
        show_guard,  # type: ignore[no-untyped-def]
        fence_owners,  # type: ignore[no-untyped-def]
        owner_running,  # type: ignore[no-untyped-def]
        sleep=None,  # type: ignore[no-untyped-def]
    ) -> dict[str, str]:
        assert service_uid == os.geteuid()
        assert sleep is None
        run(["kubectl", "get"])
        show_guard(_REQUEST_ID)
        fence_owners(_REQUEST_ID)
        assert owner_running(_REQUEST_ID) is False
        return {"status": "idle"}

    monkeypatch.setattr(
        staging_mutation_guard,
        "reconcile_orphaned_guard",
        exercise_reconcile,
    )

    assert staging_mutation_guard.main(["reconcile"]) == 0
    assert observed == [
        ("kubectl", 120),
        ("systemctl", 30),
        ("systemctl", 30),
        ("systemctl", 30),
    ]
