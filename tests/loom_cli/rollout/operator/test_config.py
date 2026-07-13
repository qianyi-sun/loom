from __future__ import annotations

import hashlib
import os
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from loom_cli.rollout.operator.config import (
    APPROVED_REMOTE_URL,
    ConfigError,
    OperatorConfig,
)
from loom_cli.rollout.operator.model import (
    ActivePointer,
    AttemptIdentity,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    RequestEvent,
    RolloutRequest,
)

VALID_CONFIG = f"""\
schema_version = 1
service_user = "loom-rollout"
operator_group = "loom-staging-operators"
remote_url = "{APPROVED_REMOTE_URL}"
target_ref = "refs/heads/dev"
runner_repo = "/opt/loom-staging-runner/repo"
state_root = "/var/lib/loom-staging-rollout"
runtime_root = "/run/loom-staging-rollout"
rollout_root = "/data/loom-staging"
kubeconfig_path = "/var/lib/loom-staging-rollout/kubeconfig"
cluster_config_path = "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
admin_token_source = "file:/var/lib/loom-staging-rollout/credentials/admin-token"
worker_token_source = "file:/var/lib/loom-staging-rollout/credentials/worker-token"
service_token_source = "file:/var/lib/loom-staging-rollout/credentials/service-token"
expect_admin_token_fingerprint = "sha256:abc123def456 len=64"
cluster_name = "loom-staging"
namespace = "loom-staging"
environment = "staging"
cp_url = "http://127.0.0.1:18081"
smoke_on_behalf_username = "devansh"
smoke_on_behalf_team_id = "11111111-1111-4111-8111-111111111111"
scope = "current-gb10"
gb10_prep_concurrency = 8
"""


def _write_config(path: Path, contents: str = VALID_CONFIG) -> Path:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o600)
    return path


def _replace_config_value(source: str, key: str, replacement: str) -> str:
    lines = source.splitlines()
    prefix = f"{key} = "
    return "\n".join(replacement if line.startswith(prefix) else line for line in lines) + "\n"


def make_driver_envelope(**overrides: object) -> DriverEnvelope:
    values: dict[str, object] = {
        "schema_version": 1,
        "request_id": "stg-20260713-abcdef12",
        "rollout_id": "staging-abcdef1",
        "initiating_operator": "hongjian",
        "initiating_uid": 2002,
        "attempt_number": 1,
        "attempt_operator": "hongjian",
        "attempt_uid": 2002,
        "remote_url": APPROVED_REMOTE_URL,
        "target_ref": "origin/dev",
        "resolved_sha": "abcdef1234567890abcdef1234567890abcdef12",
        "image_tag": "staging-abcdef1",
        "fetched_at": "2026-07-13T20:00:00Z",
        "backup_manifest_path": (
            "/data/loom-staging/backups/20260713T200000Z-stg-20260713-abcdef12/backup-manifest.json"
        ),
        "backup_manifest_sha256": "1" * 64,
        "runner_config_sha256": "2" * 64,
        "cluster_name": "loom-staging",
        "namespace": "loom-staging",
        "environment": "staging",
        "cp_url": "http://127.0.0.1:18081",
        "cluster_config_path": (
            "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
        ),
        "rollout_root": "/data/loom-staging",
        "admin_token_source": ("file:/var/lib/loom-staging-rollout/credentials/admin-token"),
        "worker_token_source": ("file:/var/lib/loom-staging-rollout/credentials/worker-token"),
        "service_token_source": ("file:/var/lib/loom-staging-rollout/credentials/service-token"),
        "expect_admin_token_fingerprint": "sha256:abc123def456 len=64",
        "smoke_on_behalf_username": "devansh",
        "smoke_on_behalf_team_id": "11111111-1111-4111-8111-111111111111",
        "scope": "current-gb10",
        "gb10_prep_concurrency": 8,
        "resume": False,
    }
    values.update(overrides)
    return DriverEnvelope(**values)  # type: ignore[arg-type]


def make_request(**overrides: object) -> RolloutRequest:
    candidate = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="abcdef1234567890abcdef1234567890abcdef12",
        image_tag="staging-abcdef1",
        fetched_at="2026-07-13T20:00:00Z",
    )
    values: dict[str, object] = {
        "request_id": "stg-20260713-abcdef12",
        "rollout_id": "staging-abcdef1",
        "caller": CallerIdentity(username="hongjian", uid=2002),
        "candidate": candidate,
        "requested_at": "2026-07-13T20:00:01Z",
        "runner_config_sha256": "2" * 64,
        "command": "start",
        "status": "pending",
    }
    values.update(overrides)
    return RolloutRequest(**values)  # type: ignore[arg-type]


def test_config_loads_exact_schema_and_records_digest(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "staging-rollout.toml")

    config = OperatorConfig.load(path, expected_owner_uid=os.getuid())

    assert config.schema_version == 1
    assert config.service_user == "loom-rollout"
    assert config.remote_url == APPROVED_REMOTE_URL
    assert config.target_ref == "refs/heads/dev"
    assert config.runner_repo == Path("/opt/loom-staging-runner/repo")
    assert config.state_root == Path("/var/lib/loom-staging-rollout")
    assert config.runtime_root == Path("/run/loom-staging-rollout")
    assert config.cluster_config_path.is_absolute()
    assert config.config_path == path
    assert config.config_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_config_rejects_non_root_owned_or_writable_file(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "staging-rollout.toml")
    path.chmod(0o666)
    with pytest.raises(ConfigError, match="group/world writable"):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())

    path.chmod(0o600)
    with pytest.raises(ConfigError, match="owner UID"):
        OperatorConfig.load(path, expected_owner_uid=os.getuid() + 1)


def test_config_rejects_symlink_and_non_regular_file(tmp_path: Path) -> None:
    target = _write_config(tmp_path / "target.toml")
    link = tmp_path / "staging-rollout.toml"
    link.symlink_to(target)

    with pytest.raises(ConfigError, match="regular file"):
        OperatorConfig.load(link, expected_owner_uid=os.getuid())
    with pytest.raises(ConfigError, match="regular file"):
        OperatorConfig.load(tmp_path, expected_owner_uid=os.getuid())


def test_config_rejects_unknown_and_missing_keys(tmp_path: Path) -> None:
    unknown = _write_config(
        tmp_path / "unknown.toml",
        VALID_CONFIG + 'admin_token = "raw-secret"\n',
    )
    with pytest.raises(ConfigError, match=r"unknown config keys.*admin_token"):
        OperatorConfig.load(unknown, expected_owner_uid=os.getuid())

    missing_text = "\n".join(
        line for line in VALID_CONFIG.splitlines() if not line.startswith("runtime_root = ")
    )
    missing = _write_config(tmp_path / "missing.toml", missing_text + "\n")
    with pytest.raises(ConfigError, match=r"missing config keys.*runtime_root"):
        OperatorConfig.load(missing, expected_owner_uid=os.getuid())


@pytest.mark.parametrize(
    ("key", "line", "message"),
    [
        ("schema_version", "schema_version = 2", "schema_version must be 1"),
        ("service_user", 'service_user = "root"', "service_user must be loom-rollout"),
        (
            "operator_group",
            'operator_group = "docker"',
            "operator_group must be loom-staging-operators",
        ),
        (
            "remote_url",
            'remote_url = "https://github.com/fork/loom.git"',
            "remote_url is not approved",
        ),
        ("target_ref", 'target_ref = "refs/heads/main"', "target_ref must be refs/heads/dev"),
        ("cluster_name", 'cluster_name = "loom-prod"', "cluster_name must be loom-staging"),
        ("namespace", 'namespace = "loom-prod"', "namespace must be loom-staging"),
        ("environment", 'environment = "prod"', "environment must be staging"),
        ("scope", 'scope = "full-cluster"', "scope must be current-gb10"),
    ],
)
def test_config_rejects_wrong_literal_values(
    tmp_path: Path,
    key: str,
    line: str,
    message: str,
) -> None:
    path = _write_config(
        tmp_path / f"wrong-{key}.toml",
        _replace_config_value(VALID_CONFIG, key, line),
    )
    with pytest.raises(ConfigError, match=message):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())


@pytest.mark.parametrize(
    ("key", "line"),
    [
        ("runner_repo", 'runner_repo = "relative/repo"'),
        ("state_root", 'state_root = "state"'),
        ("runtime_root", 'runtime_root = "runtime"'),
        ("rollout_root", 'rollout_root = "rollouts"'),
        ("kubeconfig_path", 'kubeconfig_path = "kubeconfig"'),
        ("cluster_config_path", 'cluster_config_path = "staging.cluster.toml"'),
        ("admin_token_source", 'admin_token_source = "file:relative/admin-token"'),
        ("worker_token_source", 'worker_token_source = "literal-secret"'),
        ("service_token_source", 'service_token_source = "env:SERVICE_TOKEN"'),
    ],
)
def test_config_rejects_relative_or_non_file_protected_paths(
    tmp_path: Path,
    key: str,
    line: str,
) -> None:
    path = _write_config(
        tmp_path / f"relative-{key}.toml",
        _replace_config_value(VALID_CONFIG, key, line),
    )
    with pytest.raises(ConfigError, match=key):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())


@pytest.mark.parametrize("value", [0, 16, True, "8"])
def test_config_rejects_unbounded_or_wrong_type_concurrency(
    tmp_path: Path,
    value: object,
) -> None:
    rendered = "true" if value is True else f'"{value}"' if isinstance(value, str) else str(value)
    path = _write_config(
        tmp_path / "concurrency.toml",
        _replace_config_value(
            VALID_CONFIG,
            "gb10_prep_concurrency",
            f"gb10_prep_concurrency = {rendered}",
        ),
    )
    with pytest.raises(ConfigError, match="gb10_prep_concurrency"):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())


def test_config_rejects_non_redacted_expected_fingerprint(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "fingerprint.toml",
        _replace_config_value(
            VALID_CONFIG,
            "expect_admin_token_fingerprint",
            'expect_admin_token_fingerprint = "actual-token-value"',
        ),
    )
    with pytest.raises(ConfigError, match="expect_admin_token_fingerprint"):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())


def test_driver_envelope_keeps_attempt_actor_out_of_immutable_inputs() -> None:
    envelope = make_driver_envelope(
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    immutable = envelope.rollout_inputs()

    assert immutable == {
        "request_id": envelope.request_id,
        "rollout_id": envelope.rollout_id,
        "initiating_operator": "hongjian",
        "initiating_uid": 2002,
        "remote_url": APPROVED_REMOTE_URL,
        "target_ref": "origin/dev",
        "resolved_sha": envelope.resolved_sha,
        "image_tag": envelope.image_tag,
        "backup_manifest_path": envelope.backup_manifest_path,
        "backup_manifest_sha256": envelope.backup_manifest_sha256,
        "runner_config_sha256": envelope.runner_config_sha256,
    }
    assert "attempt_operator" not in immutable
    assert "attempt_number" not in immutable
    assert "attempt_uid" not in immutable
    assert "resume" not in immutable


def test_pre_backup_request_is_strict_and_has_no_backup_binding() -> None:
    request = make_request()

    payload = request.to_dict()

    assert payload["schema_version"] == 1
    assert payload["caller"]["schema_version"] == 1
    assert payload["candidate"]["schema_version"] == 1
    assert payload["command"] == "start"
    assert payload["status"] == "pending"
    assert "backup_manifest_path" not in payload
    assert "backup_manifest_sha256" not in payload
    assert RolloutRequest.from_dict(payload) == request


def test_model_records_are_frozen_and_slotted() -> None:
    records = [
        CallerIdentity("hongjian", 2002),
        CandidateBinding(
            APPROVED_REMOTE_URL,
            "origin/dev",
            "abcdef1234567890abcdef1234567890abcdef12",
            "staging-abcdef1",
            "2026-07-13T20:00:00Z",
        ),
        make_request(),
        AttemptIdentity(1, "hongjian", 2002, False),
        ActivePointer("stg-20260713-abcdef12", 1, "unit-first", "pending"),
        make_driver_envelope(),
        RequestEvent(
            request_id="stg-20260713-abcdef12",
            event="requested",
            occurred_at="2026-07-13T20:00:01Z",
            operator="hongjian",
            operator_uid=2002,
            status="pending",
        ),
    ]

    for record in records:
        assert not hasattr(record, "__dict__")
        with pytest.raises(FrozenInstanceError):
            record.schema_version = 2


def test_model_read_rejects_unknown_keys_wrong_schema_and_unknown_literals() -> None:
    request_payload = make_request().to_dict()
    request_payload["unexpected"] = "value"
    with pytest.raises(ValueError, match=r"unknown keys.*unexpected"):
        RolloutRequest.from_dict(request_payload)

    request_payload = make_request().to_dict()
    request_payload["schema_version"] = "1"
    with pytest.raises(ValueError, match="schema_version must be 1"):
        RolloutRequest.from_dict(request_payload)

    request_payload = make_request().to_dict()
    request_payload["status"] = "queued"
    with pytest.raises(ValueError, match="unknown request status"):
        RolloutRequest.from_dict(request_payload)

    envelope_payload = make_driver_envelope().to_dict()
    envelope_payload["resume"] = "false"
    with pytest.raises(ValueError, match="resume must be a boolean"):
        DriverEnvelope.from_dict(envelope_payload)

    pointer_payload = ActivePointer("stg-20260713-abcdef12", 1, "unit-first", "pending").to_dict()
    pointer_payload["status"] = "done"
    with pytest.raises(ValueError, match="unknown active status"):
        ActivePointer.from_dict(pointer_payload)


@pytest.mark.parametrize(
    "request_id",
    ["short", "UPPERCASE-ID", "../escape-id", "-leading-id", "trailing-id-" + "x" * 70],
)
def test_request_ids_must_match_safe_grammar(request_id: str) -> None:
    with pytest.raises(ValueError, match="request_id"):
        make_request(request_id=request_id)

    with pytest.raises(ValueError, match="request_id"):
        ActivePointer(request_id, 1, "unit-first", "pending")
