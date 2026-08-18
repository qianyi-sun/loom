"""Behavioral tests for the immutable capacity-executor release artifact."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.ops.capacity_executor_release import (
    CapacityExecutorReleaseError,
    record_release,
    verify_release,
)

_SOURCE_SHA = "1" * 40


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
