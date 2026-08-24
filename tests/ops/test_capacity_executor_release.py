"""Behavioral tests for the immutable capacity-executor release artifact."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import scripts.component_ownership as component_ownership
from scripts.ops.capacity_executor_release import (
    CapacityExecutorReleaseError,
    record_release,
    verify_release,
)

_SOURCE_SHA = "1" * 40
_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_offline_release_wheels_and_hash_pins_neutral_checksum_dependency() -> None:
    """The exported offline requirements must never retain a workspace path."""
    text = (_REPO_ROOT / "deploy/Dockerfile.capacity-executor").read_text()

    assert "COPY packages/loom-bundle-checksum ./packages/loom-bundle-checksum" in text
    assert "--no-emit-workspace" in text
    checksum_wheel_command = (
        "--wheel-dir /release-inputs/wheelhouse \\\n"
        "      ./packages/loom-bundle-checksum"
    )
    assert checksum_wheel_command in text
    assert 'f"loom-bundle-checksum=={version} --hash=sha256:{digest}\\n"' in text


def _payload(root: Path) -> Path:
    payload = root / "payload"
    (payload / "wheelhouse").mkdir(parents=True)
    (payload / "units").mkdir()
    wheel = payload / "wheelhouse" / "loom-0.0.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel-bytes")
    wheel.chmod(0o444)
    unit = payload / "units" / "loom-capacity-pool-executor.service"
    unit.write_bytes(b"unit-bytes")
    unit.chmod(0o444)
    return payload


def test_record_and_verify_bind_the_exact_payload(tmp_path: Path) -> None:
    _payload(tmp_path)

    manifest = record_release(
        tmp_path,
        source_sha=_SOURCE_SHA,
        architecture="amd64",
    )

    assert manifest == {
        "architecture": "amd64",
        "component": "capacity-executor",
        "files": [
            {
                "mode": "0444",
                "path": "units/loom-capacity-pool-executor.service",
                "sha256": "c55f7cd5d0b1e4f0152599d1686bcb550987e5658e611c080cc51dc956ffdf55",
                "size": 10,
            },
            {
                "mode": "0444",
                "path": "wheelhouse/loom-0.0.0-py3-none-any.whl",
                "sha256": "9ceb18f15662bb87e54af2f5953c0484d2ef76f5444d87913360b9ef87d7296d",
                "size": 11,
            },
        ],
        "schema_version": 1,
        "source_sha": _SOURCE_SHA,
    }
    assert json.loads((tmp_path / "release-manifest.json").read_bytes()) == manifest
    assert (
        verify_release(
            tmp_path,
            expected_source_sha=_SOURCE_SHA,
            expected_architecture="amd64",
        )
        == manifest
    )


@pytest.mark.parametrize(
    "mutation",
    ("changed-content", "changed-mode", "unlisted-file", "payload-symlink"),
)
def test_verify_rejects_release_payload_drift(tmp_path: Path, mutation: str) -> None:
    payload = _payload(tmp_path)
    record_release(tmp_path, source_sha=_SOURCE_SHA, architecture="arm64")
    wheel = payload / "wheelhouse" / "loom-0.0.0-py3-none-any.whl"
    if mutation == "changed-content":
        wheel.chmod(0o644)
        wheel.write_bytes(b"different")
        wheel.chmod(0o444)
    elif mutation == "changed-mode":
        wheel.chmod(0o644)
    elif mutation == "unlisted-file":
        extra = payload / "unexpected"
        extra.write_bytes(b"extra")
        extra.chmod(0o444)
    else:
        wheel.unlink()
        wheel.symlink_to("../units/loom-capacity-pool-executor.service")

    with pytest.raises(CapacityExecutorReleaseError):
        verify_release(
            tmp_path,
            expected_source_sha=_SOURCE_SHA,
            expected_architecture="arm64",
        )


@pytest.mark.parametrize(
    ("expected_source_sha", "expected_architecture"),
    (("2" * 40, "amd64"), (_SOURCE_SHA, "arm64")),
)
def test_verify_rejects_the_wrong_release_identity(
    tmp_path: Path,
    expected_source_sha: str,
    expected_architecture: str,
) -> None:
    _payload(tmp_path)
    record_release(tmp_path, source_sha=_SOURCE_SHA, architecture="amd64")

    with pytest.raises(CapacityExecutorReleaseError):
        verify_release(
            tmp_path,
            expected_source_sha=expected_source_sha,
            expected_architecture=expected_architecture,
        )


def test_component_authority_publishes_the_executor_installation_artifact() -> None:
    manifest = component_ownership.load_manifest(_REPO_ROOT / "config/component-ownership.toml")
    components = {component.id: component for component in manifest.components}

    component = components["capacity-executor"]
    assert component.kind == "release-image"
    assert component.dockerfile == "deploy/Dockerfile.capacity-executor"
    assert component.build_context == "."
    assert component.release_digest == "loom-capacity-executor"
    assert component.runtime_policy == "conformance"
    assert component.rollout_role == "none"
    for path in (
        "deploy/dev-fleet/loom-capacity-executor.tmpfiles",
        "scripts/ops/install_capacity_executor.py",
        "src/loom/models/capabilities.py",
        "src/loom_capacity_executor/runtime.py",
        "src/loom_capacity_pool_controller/runtime.py",
        "src/loom_capacity_pool_executor/slurm_inventory.py",
        "scripts/ops/global_fleet_pool_executor_once.py",
        "scripts/ops/capacity_executor_release.py",
        "deploy/dev-fleet/loom-capacity-pool-executor-prepared.service",
    ):
        assert component in manifest.component_owners_for_path(path)


def test_executor_runtime_directory_policy_is_exact_and_nonactivating() -> None:
    assert (_REPO_ROOT / "deploy/dev-fleet/loom-capacity-executor.tmpfiles").read_bytes() == (
        b"d /run/loom-capacity-executor 0700 loom_capacity_executor loom_capacity_executor -\n"
    )
