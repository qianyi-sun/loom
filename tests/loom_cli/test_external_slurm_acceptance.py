from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_cli.external_slurm_acceptance import (
    ExternalSlurmAcceptanceError,
    ExternalSlurmAuthorityConfig,
    canonical_json_bytes,
    load_authority_config,
    run_fixed_activation_verifier,
    validate_authority_payload,
    verify_authority,
)

SHA = "a" * 40
TREE = "b" * 40
PROFILE_SHA = "c" * 64
GENERATION_ID = "f" * 64
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16) if index != 7)
CONFIG = Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml")


def _config(tmp_path: Path) -> ExternalSlurmAuthorityConfig:
    return ExternalSlurmAuthorityConfig(
        environment="staging",
        pool="gb10",
        source_host="trt-eai-oldlab-1",
        submit_host="trt-gb10-1",
        controller="trt-gb10-1",
        cluster="trt-gb10",
        partition="gb10",
        producer_user="loom-rollout",
        producer_group="loom-rollout",
        producer_uid=995,
        producer_gid=982,
        producer_home=Path("/var/lib/loom-staging-rollout"),
        producer_shell=Path("/bin/sh"),
        batch_user="loom-staging-worker",
        batch_group="loom-staging-worker",
        batch_uid=31024,
        batch_gid=31024,
        batch_home=Path("/nonexistent"),
        batch_shell=Path("/usr/sbin/nologin"),
        batch_supplementary_groups=("docker",),
        slurm_account="loom-staging",
        qos="loom-staging",
        artifact_root=tmp_path / "authority",
        public_key=tmp_path / "authority.pub",
        private_key=tmp_path / "authority.key",
        supervisor_service="loom-autoscaler-gb10-staging.service",
        supervisor_timer="loom-autoscaler-gb10-staging.timer",
        max_age_seconds=900,
        shared_mount_source="192.168.20.12:/shared_work2/loom/staging",
        shared_mount_target=Path("/srv/loom/staging-shared"),
        shared_mount_filesystem_type="nfs4",
        shared_mount_unit=r"srv-loom-staging\x2dshared.mount",
        repository_root=Path("/srv/loom/staging-shared/candidates"),
        worker_env_root=Path("/srv/loom/staging-shared/generated"),
        result_root=Path("/srv/loom/staging-shared/results"),
        broker_transport=Path("/usr/local/libexec/loom-developer-sandbox-node-transport"),
        broker_node="trt-gb10-1",
        broker_domain="gb10",
        broker_sandbox="staging",
        broker_submit_action="staging-allocation-submit",
        broker_cancel_action="staging-allocation-cancel",
        allowed_nodes=NODES,
        host_aliases={node: f"host-{index}" for index, node in enumerate(NODES)},
        repository_template=("/srv/loom/staging-shared/candidates/loom-remote-worker-{image_tag}"),
        worker_env_template=(
            "/srv/loom/staging-shared/generated/staging-gb10-worker-{image_tag}.env"
        ),
        producer_repository_template=(
            "/var/lib/loom-staging-rollout/prepared/candidates/loom-remote-worker-{image_tag}"
        ),
        producer_worker_env_template=(
            "/var/lib/loom-staging-rollout/prepared/generated/staging-gb10-worker-{image_tag}.env"
        ),
        environment_state_profile=tmp_path / "staging.toml",
        probe_action="staging-allocation-probe",
        probe_result_root=Path("/srv/loom/staging-shared/results"),
        probe_job_timeout_seconds=240,
        probe_heartbeat_interval_seconds=5,
    )


def _stamp(offset: int) -> str:
    return (NOW + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def _payload(config: ExternalSlurmAuthorityConfig) -> dict:
    repository = "/srv/loom/staging-shared/candidates/loom-remote-worker-staging-aaaaaaa"
    worker_env = "/srv/loom/staging-shared/generated/staging-gb10-worker-staging-aaaaaaa.env"
    rows = []
    for index, node in enumerate(NODES):
        rows.append(
            {
                "node": node,
                "job_id": str(1000 + index),
                "job_name": f"loom-staging-{index}",
                "account": "loom-staging",
                "qos": "loom-staging",
                "user": "loom-staging-worker",
                "uid": 31024,
                "gid": 31024,
                "sbatch_verified": True,
                "srun_verified": True,
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "repository": repository,
                "repository_device": 100,
                "repository_inode": 1000 + index,
                "worker_env": worker_env,
                "worker_env_device": 100,
                "worker_env_inode": 2000 + index,
                "worker_env_sha256": "d" * 64,
                "compose_project": f"loom-staging-{index}",
                "compose_config_sha256": "e" * 64,
                "docker_server_version": "27.5.1",
                "worker_id": f"worker-{index}",
                "registered_at": _stamp(-50),
                "first_heartbeat_at": _stamp(-40),
                "last_heartbeat_at": _stamp(-30),
                "heartbeat_count": 2,
                "cancel_requested_at": _stamp(-25),
                "stopped_at": _stamp(-20),
                "job_terminal_at": _stamp(-10),
                "job_state": "COMPLETED",
                "orphan_containers": 0,
                "orphan_networks": 0,
                "orphan_volumes": 0,
                "cleanup_verified": True,
            }
        )
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_acceptance",
        "generation": 1,
        "generation_id": GENERATION_ID,
        "environment": "staging",
        "pool": "gb10",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "profile_sha256": PROFILE_SHA,
        "source_host": "trt-eai-oldlab-1",
        "created_at": _stamp(0),
        "expires_at": _stamp(900),
        "service_identity": {
            "username": "loom-staging-worker",
            "group": "loom-staging-worker",
            "uid": 31024,
            "gid": 31024,
            "home": "/nonexistent",
            "shell": "/usr/sbin/nologin",
            "supplementary_groups": ["docker"],
        },
        "cluster": "trt-gb10",
        "controller": "trt-gb10-1",
        "submit_host": "trt-gb10-1",
        "partition": "gb10",
        "slurm_account": "loom-staging",
        "qos": "loom-staging",
        "allowed_nodes": list(config.allowed_nodes),
        "repository": repository,
        "worker_env": worker_env,
        "supervisor": {
            "service": "loom-autoscaler-gb10-staging.service",
            "timer": "loom-autoscaler-gb10-staging.timer",
            "enabled": False,
            "active": False,
        },
        "nodes": rows,
        "result": "pass",
    }


def _write_signed(
    config: ExternalSlurmAuthorityConfig,
    payload: dict,
) -> None:
    private_key = Ed25519PrivateKey.generate()
    public = private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    config.public_key.write_bytes(public)
    artifact = canonical_json_bytes(payload)
    signature = private_key.sign(artifact)
    root = config.artifact_root / "authorities" / SHA / "generations" / payload["generation_id"]
    root.mkdir(parents=True)
    (root / "acceptance.json").write_bytes(artifact)
    (root / "acceptance.sig").write_bytes(base64.b64encode(signature) + b"\n")
    current = {
        "schema_version": 1,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "generation": payload["generation"],
        "generation_id": payload["generation_id"],
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "signature_sha256": hashlib.sha256(signature).hexdigest(),
        "key_id": hashlib.sha256(public).hexdigest(),
        "created_at": payload["created_at"],
        "expires_at": payload["expires_at"],
    }
    (config.artifact_root / "current.json").write_bytes(canonical_json_bytes(current))


def test_verify_authority_accepts_signed_exact_fourteen_node_closed_receipt(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _write_signed(config, _payload(config))

    verified = verify_authority(
        config=config,
        candidate_sha=SHA,
        candidate_tree=TREE,
        profile_sha256=PROFILE_SHA,
        now=NOW + timedelta(seconds=1),
        enforce_root_security=False,
    )

    assert verified.payload["result"] == "pass"
    assert len(verified.payload["nodes"]) == 14
    assert len(verified.key_id) == 64


def test_verify_authority_follows_atomic_current_to_new_immutable_generation(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    first = _payload(config)
    _write_signed(config, first)
    second = _payload(config)
    second.update(
        {
            "generation": 2,
            "generation_id": "9" * 64,
            "created_at": _stamp(1),
            "expires_at": _stamp(901),
        }
    )
    _write_signed(config, second)

    verified = verify_authority(
        config=config,
        candidate_sha=SHA,
        candidate_tree=TREE,
        profile_sha256=PROFILE_SHA,
        now=NOW + timedelta(seconds=2),
        enforce_root_security=False,
    )

    assert verified.payload["generation"] == 2
    assert verified.artifact_path.endswith(f"/{'9' * 64}/acceptance.json")
    assert (
        config.artifact_root
        / "authorities"
        / SHA
        / "generations"
        / GENERATION_ID
        / "acceptance.json"
    ).exists()


def test_verify_authority_rejects_current_pointer_digest_drift(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_signed(config, _payload(config))
    current_path = config.artifact_root / "current.json"
    current = json.loads(current_path.read_bytes())
    current["artifact_sha256"] = "0" * 64
    current_path.write_bytes(canonical_json_bytes(current))

    with pytest.raises(ExternalSlurmAcceptanceError, match="current pointer"):
        verify_authority(
            config=config,
            candidate_sha=SHA,
            now=NOW + timedelta(seconds=1),
            enforce_root_security=False,
        )


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda payload: payload["nodes"].pop(), "cover 14 nodes"),
        (
            lambda payload: payload["nodes"][0].update(cleanup_verified=False),
            "cleanup_verified mismatch",
        ),
        (
            lambda payload: payload.update(candidate_sha="d" * 40),
            "candidate_sha mismatch",
        ),
    ],
)
def test_validate_authority_rejects_partial_or_mismatched_receipts(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    config = _config(tmp_path)
    payload = _payload(config)
    mutation(payload)

    with pytest.raises(ExternalSlurmAcceptanceError, match=match):
        validate_authority_payload(
            payload,
            config=config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            profile_sha256=PROFILE_SHA,
            now=NOW + timedelta(seconds=1),
        )


def test_verify_authority_rejects_artifact_tamper_after_signature(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    payload = _payload(config)
    _write_signed(config, payload)
    artifact = (
        config.artifact_root
        / "authorities"
        / SHA
        / "generations"
        / GENERATION_ID
        / "acceptance.json"
    )
    tampered = json.loads(artifact.read_bytes())
    tampered["nodes"][0]["worker_id"] = "tampered"
    artifact.write_bytes(canonical_json_bytes(tampered))

    with pytest.raises(ExternalSlurmAcceptanceError, match="signature verification"):
        verify_authority(
            config=config,
            candidate_sha=SHA,
            now=NOW + timedelta(seconds=1),
            enforce_root_security=False,
        )


def test_apply_verifier_uses_only_fixed_program_and_candidate_sha() -> None:
    profile = SimpleNamespace(
        environment="staging",
        autoscaler_policies=[
            {
                "pool_name": "gb10",
                "actuator": "slurm",
                "enabled": True,
                "actuator_config": {
                    "external_runner": True,
                    "candidate_sha": SHA,
                },
            }
        ],
        external_slurm_runner_prerequisites={
            "materialize": True,
            "require_external_allocation_authority": True,
        },
        external_slurm_autoscaler_supervisors=[
            {"pool_name": "gb10", "enabled": True, "active": True}
        ],
        gb10_desired_states=[
            {
                "environment": "staging",
                "pool_name": "gb10",
                "target_slots": 0,
                "host_intents": {f"trt-gb10-{index}": "stopped" for index in range(1, 16)},
            }
        ],
    )

    seen: list[list[str]] = []

    def runner(argv):
        seen.append(list(argv))
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps({"result": "pass", "candidate_sha": SHA}),
            stderr="",
        )

    result = run_fixed_activation_verifier(profile, runner=runner)

    assert result is not None
    assert seen == [
        [
            "sudo",
            "-n",
            "/usr/local/libexec/loom-staging-external-slurm-authority",
            "activate",
            "--candidate-sha",
            SHA,
        ]
    ]


@pytest.mark.parametrize(
    ("old", "new", "match"),
    [
        (
            'source_root = "/opt/loom-developer-sandbox-node-authority/source"',
            'source_root = "/tmp/source"',
            "fixed system installation",
        ),
        (
            'receipt_root = "/var/lib/loom-developer-sandbox-node-authority/'
            'staging-infrastructure"',
            'receipt_root = "/tmp/receipts"',
            "fixed convergence authority",
        ),
    ],
)
def test_load_authority_config_rejects_fixed_system_table_drift(
    tmp_path: Path,
    old: str,
    new: str,
    match: str,
) -> None:
    text = CONFIG.read_text()
    assert old in text
    path = tmp_path / "authority.toml"
    path.write_text(text.replace(old, new, 1))

    with pytest.raises(ExternalSlurmAcceptanceError, match=match):
        load_authority_config(path)


def test_load_authority_config_rejects_unknown_top_level_fields(tmp_path: Path) -> None:
    path = tmp_path / "authority.toml"
    text = CONFIG.read_text()
    path.write_text(text.replace("[installation]", "unknown_contract = true\n\n[installation]", 1))

    with pytest.raises(ExternalSlurmAcceptanceError, match="closed set"):
        load_authority_config(path)
