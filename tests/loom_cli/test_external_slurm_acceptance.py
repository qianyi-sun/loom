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
    parse_staging_worker_env,
    run_fixed_activation_verifier,
    validate_authority_payload,
    verify_authority,
    verify_candidate_repository,
)

SHA = "a" * 40
TREE = "b" * 40
PROFILE_SHA = "c" * 64
GENERATION_ID = "f" * 64
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
INFRASTRUCTURE_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
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
        infrastructure_nodes=INFRASTRUCTURE_NODES,
        allowed_nodes=NODES,
        excluded_nodes=(),
        host_aliases={node: f"host-{index}" for index, node in enumerate(INFRASTRUCTURE_NODES)},
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
        "excluded_nodes": [],
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
        "allowed_nodes": list(config.allowed_nodes),
        "excluded_nodes": [],
    }
    (config.artifact_root / "current.json").write_bytes(canonical_json_bytes(current))


def test_verify_authority_accepts_signed_exact_fifteen_node_closed_receipt(
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
    assert len(verified.payload["nodes"]) == 15
    assert verified.payload["excluded_nodes"] == []
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
        (lambda payload: payload["nodes"].pop(), "cover 15 nodes"),
        (
            lambda payload: payload["nodes"][0].update(cleanup_verified=False),
            "cleanup_verified mismatch",
        ),
        (
            lambda payload: payload.update(candidate_sha="d" * 40),
            "candidate_sha mismatch",
        ),
        (
            lambda payload: payload.update(excluded_nodes=["trt-gb10-7"]),
            "excluded_nodes mismatch",
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


def test_verify_authority_rejects_current_pointer_node_scope_drift(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _write_signed(config, _payload(config))
    current_path = config.artifact_root / "current.json"
    current = json.loads(current_path.read_bytes())
    current["excluded_nodes"] = ["trt-gb10-7"]
    current_path.write_bytes(canonical_json_bytes(current))

    with pytest.raises(ExternalSlurmAcceptanceError, match="current pointer"):
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


def test_checked_in_config_uses_full_infrastructure_for_allocation() -> None:
    config = load_authority_config(CONFIG)

    assert config.infrastructure_nodes == INFRASTRUCTURE_NODES
    assert config.allowed_nodes == NODES
    assert config.excluded_nodes == ()
    assert "trt-gb10-7" in config.infrastructure_nodes
    assert "trt-gb10-7" in config.allowed_nodes
    assert set(config.host_aliases) == set(config.infrastructure_nodes)
    assert config.host_aliases["trt-gb10-7"] == "gx10-0faf"


def test_load_authority_config_rejects_any_excluded_node(tmp_path: Path) -> None:
    path = tmp_path / "authority.toml"
    text = CONFIG.read_text(encoding="utf-8")
    path.write_text(
        text.replace(
            "excluded_nodes = []",
            'excluded_nodes = ["trt-gb10-7"]',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ExternalSlurmAcceptanceError,
        match="excluded_nodes",
    ):
        load_authority_config(path)


def test_load_authority_config_rejects_missing_infrastructure_inventory(
    tmp_path: Path,
) -> None:
    lines = CONFIG.read_text(encoding="utf-8").splitlines(keepends=True)
    legacy: list[str] = []
    skipping_infrastructure_nodes = False
    for line in lines:
        if line == "infrastructure_nodes = [\n":
            skipping_infrastructure_nodes = True
            continue
        if skipping_infrastructure_nodes:
            if line == "]\n":
                skipping_infrastructure_nodes = False
            continue
        if line.startswith("trt-gb10-7 = "):
            continue
        legacy.append(line)
    path = tmp_path / "legacy-authority.toml"
    path.write_text("".join(legacy), encoding="utf-8")

    with pytest.raises(
        ExternalSlurmAcceptanceError,
        match="exact closed set",
    ):
        load_authority_config(path)


def test_load_authority_config_rejects_incomplete_infrastructure_aliases(
    tmp_path: Path,
) -> None:
    text = CONFIG.read_text(encoding="utf-8")
    assert 'trt-gb10-7 = "gx10-0faf"\n' in text
    path = tmp_path / "authority.toml"
    path.write_text(
        text.replace('trt-gb10-7 = "gx10-0faf"\n', "", 1),
        encoding="utf-8",
    )

    with pytest.raises(ExternalSlurmAcceptanceError, match="host_aliases"):
        load_authority_config(path)


def _staging_worker_env(**updates: str) -> bytes:
    values = {
        "IMAGE_TAG": "staging-aaaaaaa",
        "ENV_CONFIG_VERSION": "staging-aaaaaaa",
        "LOOM_IMAGE_TAG": "staging-aaaaaaa",
        "LOOM_WORKER_ENV_CONFIG_VERSION": "staging-aaaaaaa",
        "LOOM_WORKER_IMAGE_ID": "sha256:" + "d" * 64,
        "LOOM_WORKER_CANDIDATE_SHA": SHA,
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://control.example:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://control.example:9100",
        "LOOM_WORKER_TOKEN": "secret-token",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://control.example:9000",
        "LOOM_WORKER_MINIO_ACCESS_KEY": "access",
        "LOOM_WORKER_MINIO_SECRET_KEY": "secret",
        "LOOM_WORKER_POOL_NAME": "gb10",
        "LOOM_WORKER_MAX_CONCURRENT": "10",
    }
    values.update(updates)
    return "".join(f"{key}={value}\n" for key, value in values.items()).encode()


def test_staging_worker_env_parser_accepts_exact_closed_candidate_binding() -> None:
    values = parse_staging_worker_env(
        _staging_worker_env(),
        candidate_sha=SHA,
        pool="gb10",
        concurrency=10,
    )

    assert values["LOOM_WORKER_CANDIDATE_SHA"] == SHA
    assert values["LOOM_WORKER_POOL_NAME"] == "gb10"


@pytest.mark.parametrize(
    ("suffix", "updates", "match"),
    [
        ("LOOM_WORKER_POOL_NAME=duplicate\n", {}, "invalid or duplicate"),
        ("export BAD=value\n", {}, "invalid or duplicate"),
        ("UNDECLARED=value\n", {}, "incomplete or unknown"),
        ("LOOM_WORKER_SUBPROCESS_GATEWAY_URL=$FOO\n", {}, "invalid or duplicate"),
        ("LOOM_WORKER_SUBPROCESS_GATEWAY_URL=${FOO}\n", {}, "invalid or duplicate"),
        ("", {"LOOM_WORKER_CANDIDATE_SHA": "e" * 40}, "source binding is stale"),
        ("", {"LOOM_IMAGE_TAG": "staging-stale"}, "source binding is stale"),
    ],
)
def test_staging_worker_env_parser_rejects_ambiguous_unknown_or_stale_input(
    suffix: str,
    updates: dict[str, str],
    match: str,
) -> None:
    with pytest.raises(ExternalSlurmAcceptanceError, match=match):
        parse_staging_worker_env(
            _staging_worker_env(**updates) + suffix.encode(),
            candidate_sha=SHA,
            pool="gb10",
            concurrency=10,
        )


@pytest.mark.parametrize("control", (b"\x00", b"\r"))
def test_staging_worker_env_parser_rejects_unsafe_control_characters(
    control: bytes,
) -> None:
    with pytest.raises(ExternalSlurmAcceptanceError, match="control character"):
        parse_staging_worker_env(
            _staging_worker_env() + b"# unsafe" + control + b"value\n",
            candidate_sha=SHA,
            pool="gb10",
            concurrency=10,
        )


def _candidate_repository(tmp_path: Path) -> tuple[Path, str, str]:
    repository = tmp_path / "candidate"
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.name", "Test"],
        check=True,
    )
    (repository / "tracked.txt").write_text("candidate\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repository), "add", "tracked.txt"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-q", "-m", "candidate"],
        check=True,
    )
    sha = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
        text=True,
    ).strip()
    return repository, sha, tree


def test_candidate_repository_verifier_rejects_hidden_index_and_raw_byte_drift(
    tmp_path: Path,
) -> None:
    repository, sha, tree = _candidate_repository(tmp_path)
    assert (
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )["tracked_files"]
        == 1
    )

    subprocess.run(
        ["git", "-C", str(repository), "update-index", "--skip-worktree", "tracked.txt"],
        check=True,
    )
    with pytest.raises(ExternalSlurmAcceptanceError, match="skip-worktree"):
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )
    subprocess.run(
        ["git", "-C", str(repository), "update-index", "--no-skip-worktree", "tracked.txt"],
        check=True,
    )
    (repository / "tracked.txt").write_text("drifted\n", encoding="utf-8")
    with pytest.raises(ExternalSlurmAcceptanceError, match="raw tracked bytes"):
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )


def test_candidate_repository_verifier_rejects_clean_filter_interference(
    tmp_path: Path,
) -> None:
    repository, _sha, _tree = _candidate_repository(tmp_path)
    (repository / ".gitattributes").write_text(
        "tracked.txt filter=unsafe-clean\n",
        encoding="utf-8",
    )
    subprocess.run(
        ["git", "-C", str(repository), "add", ".gitattributes"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-q", "-m", "attributes"],
        check=True,
    )
    sha = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
        text=True,
    ).strip()

    with pytest.raises(ExternalSlurmAcceptanceError, match="interfering Git filter"):
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )


@pytest.mark.parametrize(
    "relative",
    (
        "objects/info/alternates",
        "objects/info/http-alternates",
        "config.worktree",
    ),
)
def test_candidate_repository_verifier_rejects_external_git_storage_files(
    tmp_path: Path,
    relative: str,
) -> None:
    repository, sha, tree = _candidate_repository(tmp_path)
    path = repository / ".git" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("../external-object-store\n", encoding="utf-8")

    with pytest.raises(ExternalSlurmAcceptanceError, match="not self-contained"):
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("core.worktree", "../foreign-worktree"),
        ("core.bare", "true"),
        ("extensions.worktreeConfig", "true"),
        ("include.path", "../foreign-config"),
        ("remote.origin.promisor", "true"),
        ("remote.origin.partialCloneFilter", "blob:none"),
    ),
)
def test_candidate_repository_verifier_rejects_external_git_config_resolution(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    repository, sha, tree = _candidate_repository(tmp_path)
    subprocess.run(
        ["git", "-C", str(repository), "config", "--local", key, value],
        check=True,
    )

    with pytest.raises(ExternalSlurmAcceptanceError, match="not self-contained"):
        verify_candidate_repository(
            repository,
            candidate_sha=sha,
            candidate_tree=tree,
        )
