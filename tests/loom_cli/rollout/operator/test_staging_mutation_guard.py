from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from loom_cli.rollout.operator.readonly_database_client import DatabaseQuery
from loom_cli.rollout.operator.staging_mutation_guard import (
    MutationGuardError,
    MutationGuardEvidence,
    MutationGuardManager,
    guard_evidence_path,
    hold_request_guard,
    read_mutation_guard_evidence,
)
from tests.loom_cli.rollout.operator.test_systemd import make_config

_REQUEST_ID = "req-alpha"
_CANDIDATE_SHA = "a" * 40
_CANDIDATE_TREE = "b" * 40
_TRY_SQL = "SELECT pg_try_advisory_lock(5498691230183247727) AS acquired"
_EPOCH_SQL = (
    "SELECT epoch AS mutation_epoch FROM staging_mutation_epochs "
    "WHERE environment = 'staging' AND namespace = 'loom-staging'"
)
_UNLOCK_SQL = "SELECT pg_advisory_unlock(5498691230183247727) AS released"
_REQUEST_ANNOTATION = "loom.carin.dev/staging-mutation-guard-request"
_CANDIDATE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-sha"
_TREE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-tree"


def _config(tmp_path: Path):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700, exist_ok=True)
    return replace(make_config(), runtime_root=runtime_root)


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

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        command = tuple(argv)
        if "get" in command and "cronjob/loom-staging-data-lifecycle" in command:
            spec = cast(dict[str, object], self.cronjob["spec"])
            if spec["suspend"] is True:
                self.post_suspend_gets += 1
                if self.post_suspend_gets > self.clear_active_after_gets:
                    cast(dict[str, object], self.cronjob["status"])["active"] = []
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.cronjob), "")
        if "get" in command and "job/loom-staging-data-lifecycle-12345" in command:
            job = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
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
        service_uid=os.getuid(),
        run=cluster,
        query_context=lambda **_kwargs: _query_context(
            [True] if acquired is None else acquired,
            cluster.events,
        ),
        resolve_candidate=lambda _config: (_CANDIDATE_SHA, _CANDIDATE_TREE),
        stop_requested=requested,
        sleep=lambda _seconds: None,
        max_active_waits=max_active_waits,
        max_lock_attempts=max_lock_attempts,
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
    assert ready_documents[0]["mutation_epoch"] == 100
    assert ready_documents[0]["guard_pid"] == os.getpid()
    assert evidence.state == "released"
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
        "read-epoch",
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
    assert cluster.events == ["suspend", "try-lock", "read-epoch", "restore", "unlock"]


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


def test_guard_restart_adopts_its_exact_annotated_freeze(tmp_path: Path) -> None:
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

    evidence = _hold(tmp_path, cluster)

    assert evidence.state == "released"
    assert cluster.events == ["try-lock", "read-epoch", "restore", "unlock"]


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
        mutation_epoch=100,
        guard_pid=4321,
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
                mutation_epoch=ready.mutation_epoch,
                guard_pid=ready.guard_pid,
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
        mutation_epoch=100,
        guard_pid=4321,
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
                mutation_epoch=drifted.mutation_epoch,
                guard_pid=drifted.guard_pid,
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
