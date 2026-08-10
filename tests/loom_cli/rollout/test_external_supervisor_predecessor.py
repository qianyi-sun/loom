from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.external_supervisor_predecessor import (
    DEFAULT_PREDECESSOR_MANIFEST,
    OLDLAB_PREDECESSOR_MANIFEST,
    PR907_PREDECESSOR_BYTES_SHA256,
    PR1197_PREDECESSOR_BYTES_SHA256,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPoolIdentity,
    ExternalSupervisorPredecessorAuthority,
    external_supervisor_unit_set_digest,
    load_predecessor_manifest,
)

_POOL_IDENTITY_TABLES = (
    "gb10_worker_node_statuses",
    "gb10_worker_pool_desired_states",
    "slurm_worker_jobs",
    "worker_pool_autoscaler_policies",
    "workers",
)


def _pool_identity(
    schema_revision: str,
    *,
    legacy_count: int,
    target_count: int,
) -> ExternalSupervisorPoolIdentity:
    return ExternalSupervisorPoolIdentity.build(
        schema_revision=schema_revision,
        legacy_rows={name: legacy_count for name in _POOL_IDENTITY_TABLES},
        target_rows={name: target_count for name in _POOL_IDENTITY_TABLES},
    )


def test_checked_in_pr907_predecessor_is_canonical_and_source_reproducible() -> None:
    manifest = load_predecessor_manifest()

    assert hashlib.sha256(DEFAULT_PREDECESSOR_MANIFEST.read_bytes()).hexdigest() == (
        PR907_PREDECESSOR_BYTES_SHA256
    )
    assert manifest.source_commit == "31714ff17797231236d0ccef8f8390d4a5e66028"
    assert manifest.source_tree == "0f65994e8dfa0304e50f99f803af157faf00c3d4"
    assert manifest.manifest_digest == (
        "debcf3a704b096c165d606eea8c26b732708f17856af124fac19fe504df7c2d2"
    )
    assert manifest.unit_set_digest == external_supervisor_unit_set_digest(manifest.unit_sha256)
    assert DEFAULT_PREDECESSOR_MANIFEST.read_bytes() == manifest.to_bytes()
    assert {
        name: hashlib.sha256(payload.encode()).hexdigest()
        for name, payload in manifest.unit_payloads.items()
    } == dict(manifest.unit_sha256)

    repository = Path(__file__).resolve().parents[3]
    for source_path, expected_digest in manifest.source_file_sha256.items():
        source = subprocess.run(
            ("git", "show", f"{manifest.source_commit}:{source_path}"),
            cwd=repository,
            check=True,
            capture_output=True,
        ).stdout
        assert hashlib.sha256(source).hexdigest() == expected_digest


def test_checked_in_pr1197_oldlab_predecessor_is_source_reproducible() -> None:
    manifest = load_predecessor_manifest(execution_host="TRT-EAI-OLDLAB-1")

    assert hashlib.sha256(OLDLAB_PREDECESSOR_MANIFEST.read_bytes()).hexdigest() == (
        PR1197_PREDECESSOR_BYTES_SHA256
    )
    assert manifest.authority_id == "merged-pr-1197"
    assert manifest.source_commit == "78fbcbacb6dcdebb577692c1257f6e2226b73de6"
    assert manifest.source_tree == "bfabdac8be9c7e4b187addf3e6fa099ced0a7122"
    assert set(manifest.unit_sha256) == {
        "loom-autoscaler-oldlab-staging.service",
        "loom-autoscaler-oldlab-staging.timer",
    }
    assert OLDLAB_PREDECESSOR_MANIFEST.read_bytes() == manifest.to_bytes()

    repository = Path(__file__).resolve().parents[3]
    for source_path, expected_digest in manifest.source_file_sha256.items():
        source = subprocess.run(
            ("git", "show", f"{manifest.source_commit}:{source_path}"),
            cwd=repository,
            check=True,
            capture_output=True,
        ).stdout
        assert hashlib.sha256(source).hexdigest() == expected_digest


def test_predecessor_loader_rejects_self_consistent_redefinition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = json.loads(DEFAULT_PREDECESSOR_MANIFEST.read_bytes())
    raw["source_commit"] = "1" * 40
    payload = {key: value for key, value in raw.items() if key != "manifest_digest"}
    raw["manifest_digest"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    redefined = tmp_path / "predecessor.json"
    redefined.write_bytes(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    monkeypatch.setattr(
        "loom_cli.rollout.external_supervisor_predecessor.DEFAULT_PREDECESSOR_MANIFEST",
        redefined,
    )

    with pytest.raises(ValueError, match="byte identity drifted"):
        load_predecessor_manifest()


def test_predecessor_manifest_rejects_duplicate_and_noncanonical_json() -> None:
    payload = DEFAULT_PREDECESSOR_MANIFEST.read_bytes()
    duplicate = payload.replace(b'{"authority_id":', b'{"authority_id":"shadow","authority_id":', 1)
    with pytest.raises(ValueError, match="duplicate"):
        type(load_predecessor_manifest()).from_bytes(duplicate)
    with pytest.raises(ValueError, match="canonical"):
        type(load_predecessor_manifest()).from_bytes(payload.replace(b'":', b'": ', 1))


@pytest.mark.parametrize("bad_kind", [{}, [], 1, None])
def test_typed_identity_parsers_reject_non_string_kinds_as_value_error(
    bad_kind: object,
) -> None:
    manifest = load_predecessor_manifest()
    canonical = ExternalSupervisorCanonicalIdentity.from_manifest(manifest)
    raw = canonical.to_bytes().replace(b'"legacy-snapshot"', b"{}", 1)
    with pytest.raises(ValueError):
        ExternalSupervisorCanonicalIdentity.from_bytes(raw)
    with pytest.raises(ValueError):
        ExternalSupervisorPredecessorAuthority(
            kind=bad_kind,  # type: ignore[arg-type]
            authority_digest=manifest.manifest_digest,
            unit_sha256=manifest.unit_sha256,
        )


def test_unit_set_digest_rejects_empty_unpaired_or_untyped_input() -> None:
    with pytest.raises(ValueError):
        external_supervisor_unit_set_digest({})
    with pytest.raises(ValueError):
        external_supervisor_unit_set_digest({"loom-autoscaler-gb10-staging.service": "a" * 64})
    with pytest.raises(ValueError):
        external_supervisor_unit_set_digest(  # type: ignore[arg-type]
            {"loom-autoscaler-gb10-staging.service": {}, "x.timer": "a" * 64}
        )


def test_pool_identity_binds_legacy_only_through_schema_0066() -> None:
    _pool_identity("0065", legacy_count=1, target_count=0).require_predecessor_kind(
        "legacy-manifest"
    )
    _pool_identity("0066", legacy_count=1, target_count=0).require_predecessor_kind(
        "legacy-manifest"
    )

    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        _pool_identity("0067", legacy_count=0, target_count=1).require_predecessor_kind(
            "legacy-manifest"
        )


@pytest.mark.parametrize(
    ("legacy_count", "target_count"),
    [(1, 0), (1, 1)],
    ids=["old-only", "dual-identity"],
)
def test_pool_identity_rejects_old_or_dual_identity_after_schema_0067(
    legacy_count: int,
    target_count: int,
) -> None:
    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        _pool_identity(
            "0067",
            legacy_count=legacy_count,
            target_count=target_count,
        ).require_predecessor_kind("canonical")


def test_pool_identity_digest_and_shape_are_fail_closed() -> None:
    identity = _pool_identity("0067", legacy_count=0, target_count=1)
    assert len(identity.evidence_digest) == 64
    identity.require_predecessor_kind("canonical")

    with pytest.raises(ValueError, match="database pool identity is invalid"):
        ExternalSupervisorPoolIdentity.build(
            schema_revision=67,  # type: ignore[arg-type]
            legacy_rows={name: 0 for name in _POOL_IDENTITY_TABLES},
            target_rows={name: 1 for name in _POOL_IDENTITY_TABLES},
        )
    with pytest.raises(ValueError, match="database pool identity is invalid"):
        ExternalSupervisorPoolIdentity.build(
            schema_revision="0067",
            legacy_rows={name: 0 for name in _POOL_IDENTITY_TABLES[:-1]},
            target_rows={name: 1 for name in _POOL_IDENTITY_TABLES},
        )
