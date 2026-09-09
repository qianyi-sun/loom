"""Run rendered credential init under Kubernetes-like emptyDir ownership."""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from uuid import UUID

import pytest
import yaml

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    PersonalDevManifestBinding,
    personal_dev_preparation_manifest_documents,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config

pytestmark = pytest.mark.docker
_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("profile", ["staging", "personal"])
def test_rendered_runtime_init_preserves_root_owned_volume(tmp_path: Path, profile: str) -> None:
    if profile == "staging":
        documents = list(
            yaml.safe_load_all(
                render_manifests(
                    load_cluster_config(_ROOT / "deploy/environments/staging.cluster.toml")
                )
            )
        )
    else:
        config = DevInstanceManifestConfig(
            image_tag="",
            candidate_sha="b" * 64,
            deployment_generation=8,
            container_registry="",
            minio_endpoint="https://minio.example",
            image_references={
                component: f"registry.example/loom-{component}@sha256:{index:064x}"
                for index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1)
            },
            lifecycle_binding=PersonalDevManifestBinding(
                subject_id=UUID(int=1),
                subject_incarnation=UUID(int=2),
                operation_id=UUID(int=3),
                attempt_id=UUID(int=4),
                operation_epoch=5,
            ),
        )
        documents = personal_dev_preparation_manifest_documents(derive_identity("alice"), config)
    deployment = next(
        doc
        for doc in documents
        if doc
        and doc["kind"] == "Deployment"
        and doc["metadata"]["name"].startswith("loom-control-plane")
    )
    pod = deployment["spec"]["template"]["spec"]
    init = next(c for c in pod["initContainers"] if c["name"] == "protected-worker-runtime-init")
    mounts = {m["name"]: m for m in init["volumeMounts"]}
    volume_root = mounts["protected-worker-runtime"]["mountPath"]
    consumer = next(
        m for m in pod["containers"][0]["volumeMounts"] if m["name"] == "protected-worker-runtime"
    )
    payloads = {"database-url": "postgresql://fixture.invalid/runtime"}
    if profile == "staging":
        payloads["ca.crt"] = "fixture-ca-not-a-real-certificate"
    source = tmp_path / "projected"
    generation = source / "..generation"
    generation.mkdir(parents=True)
    source.chmod(0o755)
    generation.chmod(0o755)
    (source / "..data").symlink_to("..generation")
    for name, value in payloads.items():
        (generation / name).write_text(value)
        (generation / name).chmod(0o444)
        (source / name).symlink_to(f"..data/{name}")

    # The production command must succeed before any layout assertion: old
    # manifests reproduce EPERM on the root-owned mount, not a string mismatch.
    verification = f"""
import os, stat
from pathlib import Path
root = Path({volume_root!r})
assert root.stat().st_uid == 0
assert stat.S_IMODE(root.stat().st_mode) == 0o2770
private = root / {consumer.get("subPath", "")!r}
assert private != root
assert private.stat().st_uid == 65532
assert stat.S_IMODE(private.stat().st_mode) == 0o700
files = private / 'files'
assert files.stat().st_uid == 65532
assert stat.S_IMODE(files.stat().st_mode) == 0o700
expected = {payloads!r}
assert set(p.name for p in files.iterdir()) == set(expected)
for name, value in expected.items():
    path = files / name
    assert path.read_text() == value
    assert path.stat().st_uid == 65532
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
print('private-runtime-copy-ok')
"""
    command = shlex.join(init["command"])
    # Repeat in the same volume to exercise an init-container restart as well.
    script = f"{command}; {command}; python -c {shlex.quote(verification)}"
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--read-only",
            "--user",
            f"{init['securityContext']['runAsUser']}:{pod['securityContext']['fsGroup']}",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--tmpfs",
            f"{volume_root}:rw,noexec,nosuid,uid=0,gid=65532,mode=2770",
            "--mount",
            f"type=bind,src={_ROOT / 'src'},dst=/opt/loom/src,readonly",
            "--mount",
            f"type=bind,src={source},dst={mounts['protected-worker-runtime-projected']['mountPath']},readonly",
            "--env",
            "PYTHONPATH=/opt/loom/src",
            "--env",
            "PYTHONDONTWRITEBYTECODE=1",
            "python:3.11-slim",
            "/bin/sh",
            "-euc",
            script,
        ],
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "private-runtime-copy-ok"
    assert consumer["mountPath"] == "/run/loom/protected-worker-runtime"
    assert consumer["readOnly"] is True
    assert init["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert init["securityContext"]["allowPrivilegeEscalation"] is False
    assert init["securityContext"]["readOnlyRootFilesystem"] is True
    assert pod["securityContext"]["runAsNonRoot"] is True
