from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
    ProtectedExecutionPrerequisiteStoreError,
)
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    canonical_execution_prerequisite_bytes,
)

from .protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact as _artifact,
)


def test_store_publishes_private_immutable_prerequisite(tmp_path: Path) -> None:
    store = ProtectedExecutionPrerequisiteStore(tmp_path, service_uid=os.geteuid())
    artifact = _artifact()

    publication = store.publish(artifact)

    assert publication.path == (
        tmp_path / "execution-prerequisites" / f"{artifact.artifact_sha256}.json"
    )
    assert publication.artifact_sha256 == artifact.artifact_sha256
    assert stat.S_IMODE(publication.path.stat().st_mode) == 0o600
    assert publication.path.stat().st_nlink == 1
    assert stat.S_IMODE(publication.path.parent.stat().st_mode) == 0o700
    assert store.read(publication) == artifact
    assert store.attestation_evidence(publication) == {
        **artifact.attestation_evidence(),
        "artifact-path": str(publication.path),
    }
    assert store.publish(artifact) == publication


def test_store_rejects_tamper_and_symlinked_authority(tmp_path: Path) -> None:
    store = ProtectedExecutionPrerequisiteStore(tmp_path, service_uid=os.geteuid())
    publication = store.publish(_artifact())
    payload = json.loads(publication.path.read_text(encoding="utf-8"))
    payload["source_configuration_epoch"] = 10
    publication.path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(ProtectedExecutionPrerequisiteStoreError, match="invalid"):
        store.read(publication)

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    (state / "execution-prerequisites").symlink_to(outside, target_is_directory=True)
    unsafe = ProtectedExecutionPrerequisiteStore(state, service_uid=os.geteuid())

    with pytest.raises(ProtectedExecutionPrerequisiteStoreError, match="unsafe"):
        unsafe.publish(_artifact())
    assert list(outside.iterdir()) == []


def test_store_reads_only_while_its_private_lifecycle_lock_is_safe(
    tmp_path: Path,
) -> None:
    store = ProtectedExecutionPrerequisiteStore(tmp_path, service_uid=os.geteuid())
    publication = store.publish(_artifact())

    assert stat.S_IMODE(store.lifecycle_lock_path.stat().st_mode) == 0o600
    assert store.lifecycle_lock_path.stat().st_nlink == 1
    store.lifecycle_lock_path.unlink()
    outside = tmp_path / "outside-lock"
    outside.write_bytes(b"")
    outside.chmod(0o600)
    store.lifecycle_lock_path.symlink_to(outside)

    with pytest.raises(ProtectedExecutionPrerequisiteStoreError, match="lock is unsafe"):
        store.read(publication)


def test_store_rejects_replaced_or_symlinked_artifact(tmp_path: Path) -> None:
    store = ProtectedExecutionPrerequisiteStore(tmp_path / "replaced", service_uid=os.geteuid())
    publication = store.publish(_artifact())
    replacement = _artifact(backup_lease_sha256="0" * 64)
    publication.path.unlink()
    publication.path.write_bytes(canonical_execution_prerequisite_bytes(replacement))
    publication.path.chmod(0o600)

    with pytest.raises(ProtectedExecutionPrerequisiteStoreError, match="digest does not match"):
        store.read(publication)

    symlink_store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "symlinked",
        service_uid=os.geteuid(),
    )
    symlink_publication = symlink_store.publish(_artifact())
    outside = tmp_path / "outside-artifact"
    outside.write_bytes(canonical_execution_prerequisite_bytes(_artifact()))
    outside.chmod(0o600)
    symlink_publication.path.unlink()
    symlink_publication.path.symlink_to(outside)

    with pytest.raises(ProtectedExecutionPrerequisiteStoreError, match="invalid"):
        symlink_store.read(symlink_publication)
