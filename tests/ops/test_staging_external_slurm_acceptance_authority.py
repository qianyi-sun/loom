from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
import tomllib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import developer_sandbox_node_authority as node_authority
from scripts.ops import staging_external_slurm_acceptance_authority as authority

from loom_cli.external_slurm_acceptance import (
    ExternalSlurmAuthorityConfig,
    load_authority_config,
)

SHA = "a" * 40
TREE = "b" * 40
CONFIG = Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml")


def _key_config(tmp_path: Path) -> ExternalSlurmAuthorityConfig:
    key_root = tmp_path / "keys"
    key_root.mkdir(mode=0o700)
    return replace(
        load_authority_config(CONFIG),
        private_key=key_root / "authority-private.pem",
        public_key=key_root / "authority-public.pem",
    )


def _enable_user_owned_key_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    read_key_leaf = authority._read_key_leaf

    def read_test_leaf(
        path: Path,
        *,
        label: str,
        mode: int,
        uid: int = 0,
        gid: int = 0,
    ) -> bytes:
        del uid, gid
        return read_key_leaf(
            path,
            uid=os.getuid(),
            gid=os.getgid(),
            label=label,
            mode=mode,
        )

    def rename_noreplace(source: Path, target: Path) -> None:
        os.link(source, target, follow_symlinks=False)
        source.unlink()

    monkeypatch.setattr(authority, "_read_key_leaf", read_test_leaf)
    monkeypatch.setattr(authority, "_verify_root_parent_chain", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)
    monkeypatch.setattr(authority, "_rename_noreplace", rename_noreplace)


def _private_pem(private: Ed25519PrivateKey) -> bytes:
    return private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _public_pem(private: Ed25519PrivateKey) -> bytes:
    return private.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def test_keypair_clean_install_is_persistent_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)

    _private, key_id = authority._converge_signing_keypair(config)
    private_before = config.private_key.read_bytes()
    public_before = config.public_key.read_bytes()
    metadata_before = (
        config.private_key.stat().st_ino,
        config.private_key.stat().st_mtime_ns,
        config.public_key.stat().st_ino,
        config.public_key.stat().st_mtime_ns,
    )

    private = serialization.load_pem_private_key(private_before, password=None)
    public = serialization.load_pem_public_key(public_before)
    assert isinstance(private, Ed25519PrivateKey)
    challenge = b"independent-readback"
    public.verify(private.sign(challenge), challenge)
    assert key_id == hashlib.sha256(public_before).hexdigest()
    assert stat.S_IMODE(config.private_key.stat().st_mode) == 0o600
    assert stat.S_IMODE(config.public_key.stat().st_mode) == 0o644

    authority._converge_signing_keypair(config)

    assert config.private_key.read_bytes() == private_before
    assert config.public_key.read_bytes() == public_before
    assert (
        config.private_key.stat().st_ino,
        config.private_key.stat().st_mtime_ns,
        config.public_key.stat().st_ino,
        config.public_key.stat().st_mtime_ns,
    ) == metadata_before
    assert not list(config.private_key.parent.glob(".*.tmp-*"))


def test_keypair_crash_after_private_publish_rolls_forward_without_rotation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    install = authority._install_key_file_no_replace
    calls = 0

    def crash_after_first_publish(path: Path, payload: bytes, *, mode: int) -> bool:
        nonlocal calls
        calls += 1
        installed = install(path, payload, mode=mode)
        if calls == 1:
            raise RuntimeError("injected crash")
        return installed

    monkeypatch.setattr(
        authority,
        "_install_key_file_no_replace",
        crash_after_first_publish,
    )
    with pytest.raises(RuntimeError, match="injected crash"):
        authority._converge_signing_keypair(config)
    private_before = config.private_key.read_bytes()
    assert not config.public_key.exists()

    monkeypatch.setattr(authority, "_install_key_file_no_replace", install)
    authority._converge_signing_keypair(config)

    assert config.private_key.read_bytes() == private_before
    private = serialization.load_pem_private_key(private_before, password=None)
    assert isinstance(private, Ed25519PrivateKey)
    assert config.public_key.read_bytes() == _public_pem(private)


def test_keypair_private_only_derives_public_after_validating_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    private = Ed25519PrivateKey.generate()
    private_bytes = _private_pem(private)
    config.private_key.write_bytes(private_bytes)
    config.private_key.chmod(0o600)

    authority._converge_signing_keypair(config)

    assert config.private_key.read_bytes() == private_bytes
    assert config.public_key.read_bytes() == _public_pem(private)


def test_keypair_public_only_fails_closed_without_creating_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    public_bytes = _public_pem(Ed25519PrivateKey.generate())
    config.public_key.write_bytes(public_bytes)
    config.public_key.chmod(0o644)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="public key exists without its private key",
    ):
        authority._converge_signing_keypair(config)

    assert not config.private_key.exists()
    assert config.public_key.read_bytes() == public_bytes


def test_key_file_publication_is_no_replace_and_preserves_existing_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    existing = b"existing-public-material\n"
    config.public_key.write_bytes(existing)
    config.public_key.chmod(0o644)
    inode = config.public_key.stat().st_ino

    installed = authority._install_key_file_no_replace(
        config.public_key,
        b"replacement-must-not-win\n",
        mode=0o644,
    )

    assert installed is False
    assert config.public_key.read_bytes() == existing
    assert config.public_key.stat().st_ino == inode
    assert not list(config.public_key.parent.glob(".*.tmp-*"))


@pytest.mark.parametrize("unsafe_kind", ("symlink", "hardlink", "wrong-mode"))
def test_keypair_rejects_unsafe_private_leaf_without_publishing_public(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_kind: str,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    payload = _private_pem(Ed25519PrivateKey.generate())
    if unsafe_kind == "symlink":
        target = config.private_key.parent / "foreign-private.pem"
        target.write_bytes(payload)
        target.chmod(0o600)
        config.private_key.symlink_to(target)
    else:
        config.private_key.write_bytes(payload)
        config.private_key.chmod(0o644 if unsafe_kind == "wrong-mode" else 0o600)
        if unsafe_kind == "hardlink":
            os.link(config.private_key, config.private_key.parent / "private-alias.pem")

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="single-link service-owned regular file",
    ):
        authority._converge_signing_keypair(config)

    assert not config.public_key.exists()


def test_keypair_rejects_wrong_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    config.private_key.write_bytes(_private_pem(Ed25519PrivateKey.generate()))
    config.private_key.chmod(0o600)
    monkeypatch.setattr(authority, "_verify_root_parent_chain", lambda *_args, **_kwargs: None)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="single-link service-owned regular file",
    ):
        authority._converge_signing_keypair(config)

    assert not config.public_key.exists()


@pytest.mark.parametrize("unsafe_kind", ("symlink", "hardlink", "wrong-mode"))
def test_keypair_rejects_unsafe_public_leaf(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_kind: str,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    private = Ed25519PrivateKey.generate()
    config.private_key.write_bytes(_private_pem(private))
    config.private_key.chmod(0o600)
    public_payload = _public_pem(private)
    if unsafe_kind == "symlink":
        target = config.public_key.parent / "foreign-public.pem"
        target.write_bytes(public_payload)
        target.chmod(0o644)
        config.public_key.symlink_to(target)
    else:
        config.public_key.write_bytes(public_payload)
        config.public_key.chmod(0o600 if unsafe_kind == "wrong-mode" else 0o644)
        if unsafe_kind == "hardlink":
            os.link(config.public_key, config.public_key.parent / "public-alias.pem")

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="single-link service-owned regular file",
    ):
        authority._converge_signing_keypair(config)


def test_keypair_rejects_mismatched_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    config.private_key.write_bytes(_private_pem(Ed25519PrivateKey.generate()))
    config.private_key.chmod(0o600)
    config.public_key.write_bytes(_public_pem(Ed25519PrivateKey.generate()))
    config.public_key.chmod(0o644)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="key pair does not match",
    ):
        authority._converge_signing_keypair(config)


def test_invalid_private_key_fails_without_leaking_or_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    _enable_user_owned_key_fixture(monkeypatch)
    sentinel = b"PRIVATE-SENTINEL-MUST-NOT-LEAK\n"
    config.private_key.write_bytes(sentinel)
    config.private_key.chmod(0o600)

    with pytest.raises(authority.ExternalSlurmAcceptanceError) as raised:
        authority._converge_signing_keypair(config)

    assert "PRIVATE-SENTINEL" not in str(raised.value)
    assert not config.public_key.exists()


def test_bootstrap_converges_keypair_before_other_local_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _key_config(tmp_path)
    reached: list[ExternalSlurmAuthorityConfig] = []
    monkeypatch.setattr(authority, "_require_root", lambda: None)
    monkeypatch.setattr(authority, "_require_source_host", lambda _config: None)
    monkeypatch.setattr(
        authority,
        "_load_infrastructure_receipt",
        lambda *_args, **_kwargs: ({"result": "pass"}, "d" * 64),
    )
    monkeypatch.setattr(authority, "_verify_producer_identity", lambda _config: {})
    monkeypatch.setattr(authority, "_verify_root_parent_chain", lambda *_args, **_kwargs: None)

    def stop_after_keypair(candidate: ExternalSlurmAuthorityConfig) -> None:
        reached.append(candidate)
        raise RuntimeError("stop after keypair")

    monkeypatch.setattr(authority, "_converge_signing_keypair", stop_after_keypair)

    with pytest.raises(RuntimeError, match="stop after keypair"):
        authority.bootstrap(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
        )

    assert reached == [config]


def test_checked_in_authority_config_is_fixed_to_staging_and_all_gb10_nodes() -> None:
    config = load_authority_config(CONFIG)

    assert config.environment == "staging"
    assert config.pool == "gb10"
    assert config.producer_user == "loom-rollout"
    assert config.batch_user == "loom-staging-worker"
    assert (config.batch_uid, config.batch_gid) == (31024, 31024)
    assert config.shared_mount_target == Path("/srv/loom/staging-shared")
    assert config.shared_mount_source == "192.168.20.12:/shared_work2/loom/staging"
    assert config.source_host == "trt-eai-oldlab-1"
    assert config.submit_host == "trt-gb10-1"
    assert config.controller == "trt-gb10-1"
    assert config.infrastructure_nodes == tuple(f"trt-gb10-{index}" for index in range(1, 16))
    assert len(config.allowed_nodes) == 15
    assert config.allowed_nodes == config.infrastructure_nodes
    assert config.probe_action == "staging-allocation-probe"


def test_probe_transport_and_envelope_are_closed_to_exact_candidate() -> None:
    config = load_authority_config(CONFIG)
    prepared = {
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "repository": "/fixed/repository",
        "worker_env": "/fixed/worker.env",
    }

    argv = authority._probe_argv(config)
    envelope = json.loads(
        authority._probe_envelope(
            config,
            prepared,
            probe_id="1" * 64,
        )
    )
    inner = json.loads(base64.b64decode(envelope["payload_base64"], validate=True))

    assert argv == [
        "/usr/local/libexec/loom-developer-sandbox-node-transport",
        "invoke",
        "--node",
        "trt-gb10-1",
        "--verb",
        "transact",
    ]
    assert envelope["sandbox"] == "staging"
    assert envelope["payload_kind"] == "staging-allocation-probe-request"
    assert inner == {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_probe_request",
        "request_id": "1" * 64,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
    }
    rendered = json.dumps(envelope)
    for forbidden in (
        "repository",
        "worker_env",
        "loom-staging-worker",
        "allowed_nodes",
        "slurm_account",
        '"qos"',
        '"config"',
    ):
        assert forbidden not in rendered
    parsed = node_authority._parse_request(
        authority.canonical_json_bytes(envelope),
        verb="transact",
        policy=node_authority.AuthorityPolicy(
            source_sha=SHA,
            source_tree=TREE,
            node="trt-gb10-1",
            asset_sha256={str(path): "c" * 64 for path in node_authority.SOURCE_ASSETS},
        ),
    )
    assert parsed.action == "staging-allocation-probe"
    assert json.loads(parsed.payload_bytes) == inner


def _infrastructure_transport_receipt(
    config: ExternalSlurmAuthorityConfig,
    *,
    action: str,
    node: str,
    completed_at: datetime,
    convergence_id: str,
    requested_at: str,
) -> dict[str, object]:
    envelope, inner_request_id = authority._expected_infrastructure_request(
        config,
        action=action,
        node=node,
        candidate_sha=SHA,
        candidate_tree=TREE,
        convergence_id=convergence_id,
        requested_at=requested_at,
    )
    request = json.loads(envelope)
    return {
        "schema_version": 1,
        "request_id": request["request_id"],
        "action": action,
        "node": node,
        "domain": config.broker_domain,
        "sandbox": config.broker_sandbox,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "payload_sha256": request["payload_sha256"],
        "result_sha256": hashlib.sha256(f"{action}@{node}".encode()).hexdigest(),
        "inner_receipt": (
            f"staging-accounting/v1/{inner_request_id}"
            if action == "staging-slurm-accounting-converge"
            else (
                f"staging-shared-source-bootstrap/v1/{'a' * 64}"
                if action == "staging-shared-source-bootstrap"
                else (
                    "staging-allocation-bootstrap/v1/"
                    f"{int(node.rsplit('-', 1)[1]):08x}-0000-4000-8000-"
                    f"{int(node.rsplit('-', 1)[1]):012x}/"
                    f"{hashlib.sha256(f'mount:{node}'.encode()).hexdigest()}"
                )
            )
        ),
        "completed_at": authority._timestamp(completed_at),
        "status": "succeeded",
    }


def _infrastructure_payload(
    config: ExternalSlurmAuthorityConfig,
    *,
    now: datetime,
) -> dict[str, object]:
    source_completed = now - timedelta(seconds=180)
    accounting_completed = now - timedelta(seconds=170)
    node_started = now - timedelta(seconds=150)
    convergence_id = "d" * 64
    requested_at = authority._timestamp(now - timedelta(seconds=200))
    converge_request = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
    }
    return {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-receipt",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "generation": 1,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
        "request_sha256": hashlib.sha256(
            authority.canonical_json_bytes(converge_request)
        ).hexdigest(),
        "source_controller": "oldlab-2",
        "source_controller_host": "trt-eai-oldlab-2",
        "created_at": authority._timestamp(now - timedelta(seconds=100)),
        "expires_at": authority._timestamp(now + timedelta(seconds=300)),
        "source_bootstrap": _infrastructure_transport_receipt(
            config,
            action="staging-shared-source-bootstrap",
            node="trt-gb10-2",
            completed_at=source_completed,
            convergence_id=convergence_id,
            requested_at=requested_at,
        ),
        "accounting": _infrastructure_transport_receipt(
            config,
            action="staging-slurm-accounting-converge",
            node=config.controller,
            completed_at=accounting_completed,
            convergence_id=convergence_id,
            requested_at=requested_at,
        ),
        "node_bootstraps": [
            _infrastructure_transport_receipt(
                config,
                action="staging-allocation-bootstrap",
                node=node,
                completed_at=node_started + timedelta(seconds=index),
                convergence_id=convergence_id,
                requested_at=requested_at,
            )
            for index, node in enumerate(config.infrastructure_nodes)
        ],
        "mount_contract": authority._expected_infrastructure_mount_contract(config),
        "result": "pass",
    }


def _write_infrastructure_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
) -> Path:
    receipt_root = tmp_path / "staging-infrastructure"
    receipt_root.mkdir()
    monkeypatch.setattr(authority, "_INFRASTRUCTURE_RECEIPT_ROOT", receipt_root)
    path = receipt_root / f"{SHA}.json"
    path.write_bytes(authority.canonical_json_bytes(payload))
    path.chmod(0o600)
    return path


def test_converge_infrastructure_retries_active_id_then_refreshes_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(load_authority_config(CONFIG), artifact_root=tmp_path / "artifacts")
    state = config.artifact_root / "state"
    state.mkdir(parents=True)
    lock = state / "authority.lock"
    lock.write_bytes(b"")
    envelopes: list[dict[str, object]] = []
    bootstrap_attempts = 0
    monkeypatch.setattr(authority, "_require_root", lambda: None)
    monkeypatch.setattr(authority, "_require_source_host", lambda _config: None)
    monkeypatch.setattr(
        authority,
        "_publisher_lock",
        lambda _config: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(
        authority,
        "_write_root_file",
        lambda path, payload, **_kwargs: path.write_bytes(payload),
    )
    monkeypatch.setattr(
        authority,
        "_commit_infrastructure_convergence_journal",
        lambda path, _journal: path.unlink(),
    )

    def run(
        _argv: object,
        *,
        input_text: str,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        envelope = json.loads(input_text)
        envelopes.append(envelope)
        inner = json.loads(base64.b64decode(envelope["payload_base64"], validate=True))
        receipt = {
            "schema_version": 1,
            "request_id": envelope["request_id"],
            "action": envelope["action"],
            "node": envelope["node"],
            "domain": envelope["domain"],
            "sandbox": envelope["sandbox"],
            "candidate_sha": envelope["candidate_sha"],
            "candidate_tree": envelope["candidate_tree"],
            "payload_sha256": envelope["payload_sha256"],
            "result_sha256": "e" * 64,
            "inner_receipt": f"staging-infrastructure/v1/{inner['convergence_id']}",
            "completed_at": authority._timestamp(),
            "status": "succeeded",
        }
        return subprocess.CompletedProcess([], 0, json.dumps(receipt), "")

    def bootstrap_once(_config: object, **_kwargs: object) -> dict[str, object]:
        nonlocal bootstrap_attempts
        bootstrap_attempts += 1
        if bootstrap_attempts == 1:
            raise authority.ExternalSlurmAcceptanceError("injected crash")
        return {"result": "pass"}

    monkeypatch.setattr(authority, "_run", run)
    monkeypatch.setattr(authority, "bootstrap", bootstrap_once)
    monkeypatch.setattr(
        authority,
        "verify_infrastructure",
        lambda *_args, **_kwargs: {
            "generation": len(envelopes),
            "receipt_path": f"/receipt/{len(envelopes)}",
            "payload_sha256": "f" * 64,
            "node_count": 15,
            "source_controller": "oldlab-2",
            "source_controller_host": "trt-eai-oldlab-2",
        },
    )

    with pytest.raises(authority.ExternalSlurmAcceptanceError, match="injected crash"):
        authority.converge_infrastructure(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
        )
    authority.converge_infrastructure(config, candidate_sha=SHA, candidate_tree=TREE)
    authority.converge_infrastructure(config, candidate_sha=SHA, candidate_tree=TREE)

    inner_ids = [
        json.loads(base64.b64decode(item["payload_base64"], validate=True))["convergence_id"]
        for item in envelopes
    ]
    assert inner_ids[0] == inner_ids[1]
    assert inner_ids[2] != inner_ids[1]


def test_infrastructure_receipt_accepts_exact_closed_ordered_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    path = _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    loaded, digest = authority._load_infrastructure_receipt(
        config,
        candidate_sha=SHA,
        candidate_tree=TREE,
        now=now,
        enforce_root_security=False,
    )
    summary = authority._infrastructure_summary(loaded, payload_sha256=digest)

    assert loaded == payload
    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert set(summary) == {
        "schema_version",
        "kind",
        "candidate_sha",
        "candidate_tree",
        "generation",
        "convergence_id",
        "requested_at",
        "request_sha256",
        "receipt_path",
        "payload_sha256",
        "source_controller",
        "source_controller_host",
        "created_at",
        "expires_at",
        "source_bootstrap",
        "accounting",
        "infrastructure_nodes",
        "node_bootstraps",
        "mount_contract",
        "mount_digests",
        "mount_digest",
        "source_digest",
        "boot_ids",
        "node_count",
        "result",
    }
    assert summary["node_count"] == 15
    assert summary["infrastructure_nodes"] == list(config.infrastructure_nodes)
    assert [row["node"] for row in summary["node_bootstraps"]] == list(config.infrastructure_nodes)
    assert set(summary["boot_ids"]) == set(config.infrastructure_nodes)
    assert set(summary["mount_digests"]) == set(config.infrastructure_nodes)
    assert summary["source_digest"] == "a" * 64
    assert summary["receipt_path"] == str(path)


def test_infrastructure_receipt_rejects_reordered_node_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    node_bootstraps = payload["node_bootstraps"]
    assert isinstance(node_bootstraps, list)
    node_bootstraps[0], node_bootstraps[1] = node_bootstraps[1], node_bootstraps[0]
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="binding mismatch",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candidate_sha", "c" * 40),
        ("candidate_tree", "c" * 40),
        ("source_controller", "oldlab-1"),
        ("source_controller_host", "trt-eai-oldlab-1"),
    ],
)
def test_infrastructure_receipt_rejects_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    payload[field] = value
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="candidate or controller binding mismatch",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


def test_infrastructure_receipt_rejects_stale_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    payload["expires_at"] = authority._timestamp(now)
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="stale or has an invalid lifetime",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


def test_infrastructure_receipt_rejects_missing_candidate_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        authority,
        "_INFRASTRUCTURE_RECEIPT_ROOT",
        tmp_path / "staging-infrastructure",
    )

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="unavailable",
    ):
        authority._load_infrastructure_receipt(
            load_authority_config(CONFIG),
            candidate_sha=SHA,
            candidate_tree=TREE,
            enforce_root_security=False,
        )


def test_infrastructure_receipt_rejects_tampered_transport_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    accounting = payload["accounting"]
    assert isinstance(accounting, dict)
    accounting["payload_sha256"] = "f" * 64
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="binding mismatch",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


def test_infrastructure_receipt_rejects_path_valued_inner_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    source_bootstrap = payload["source_bootstrap"]
    assert isinstance(source_bootstrap, dict)
    source_bootstrap["inner_receipt"] = "/tmp/forged.json"
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="binding mismatch",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


def test_infrastructure_receipt_rejects_mount_contract_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_authority_config(CONFIG)
    now = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    payload = _infrastructure_payload(config, now=now)
    mount_contract = payload["mount_contract"]
    assert isinstance(mount_contract, dict)
    mount_contract["result_root_mode"] = "0o0770"
    _write_infrastructure_receipt(tmp_path, monkeypatch, payload)

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="mount contract mismatch",
    ):
        authority._load_infrastructure_receipt(
            config,
            candidate_sha=SHA,
            candidate_tree=TREE,
            now=now,
            enforce_root_security=False,
        )


def test_global_config_override_is_rejected(capsys) -> None:
    with pytest.raises(SystemExit) as raised:
        authority.main(
            [
                "--config",
                str(CONFIG),
                "prepare",
                "--candidate-sha",
                SHA,
                "--candidate-tree",
                TREE,
                "--image-tag",
                "staging-aaaaaaa",
            ]
        )

    assert raised.value.code == 2
    assert "error:" in capsys.readouterr().err


def test_deploy_assets_install_only_fixed_authority_program() -> None:
    sudoers = Path(
        "deploy/developer-sandboxes/loom-staging-external-slurm-authority.sudoers"
    ).read_text(encoding="utf-8")
    service = Path(
        "deploy/developer-sandboxes/loom-staging-external-slurm-authority.service"
    ).read_text(encoding="utf-8")

    assert "loom-rollout ALL=(root) NOPASSWD:NOSETENV:" in sudoers
    assert "/usr/local/libexec/loom-staging-external-slurm-authority" in sudoers
    assert "verify-infrastructure --candidate-sha * --candidate-tree *" in sudoers
    assert "developer_sandbox_staging_promotion" not in sudoers
    assert "User=root" in service
    assert "ProtectSystem=strict" in service
    assert "verify-current" in service
    mount_unit = Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount").read_text(
        encoding="utf-8"
    )
    assert "What=192.168.20.12:/shared_work2/loom/staging" in mount_unit
    assert "Where=/srv/loom/staging-shared" in mount_unit
    assert "Type=nfs4" in mount_unit
    program = Path(authority.__file__).read_text(encoding="utf-8")
    assert program.startswith("#!/bin/false\n")
    assert "/usr/local/lib/loom-staging-external-slurm-authority" in program
    assert set(authority.REQUIRED_INSTALLATION_ASSETS) == {
        "scripts/ops/staging_external_slurm_acceptance_authority.py",
        "src/loom_cli/external_slurm_acceptance.py",
        "deploy/developer-sandboxes/staging-external-slurm-authority.toml",
        "deploy/developer-sandboxes/loom-staging-external-slurm-authority.service",
        "deploy/developer-sandboxes/loom-staging-external-slurm-authority.sudoers",
        "deploy/developer-sandboxes/loom-staging-external-slurm-authority.wrapper",
        r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount",
        "deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf",
    }
    raw_config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    assert raw_config["infrastructure_nodes"] == [f"trt-gb10-{index}" for index in range(1, 16)]
    assert raw_config["installation"] == {
        "source_root": "/opt/loom-developer-sandbox-node-authority/source",
        "candidate_runtime_template": (
            "/opt/loom-staging-runner/candidates/{candidate_sha}/venv/bin/python"
        ),
        "wrapper": "/usr/local/libexec/loom-staging-external-slurm-authority",
        "isolated_python": True,
        "required_modules": ["loom_cli", "cryptography"],
    }
    assert raw_config["infrastructure"] == {
        "receipt_root": ("/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure"),
        "source_controller": "oldlab-2",
        "source_controller_host": "trt-eai-oldlab-2",
        "max_age_seconds": 3600,
    }


def _publication_fixture(
    tmp_path: Path,
) -> tuple[ExternalSlurmAuthorityConfig, str, str, str, Path, Path]:
    repository_root = tmp_path / "prepared" / "candidates"
    env_root = tmp_path / "prepared" / "generated"
    target_repository_root = tmp_path / "shared" / "candidates"
    target_env_root = tmp_path / "shared" / "generated"
    for root in (
        repository_root,
        env_root,
        target_repository_root,
        target_env_root,
    ):
        root.mkdir(parents=True, mode=0o700)
    seed = tmp_path / "seed"
    seed.mkdir()
    subprocess.run(["git", "init", "-q", str(seed)], check=True)
    subprocess.run(
        ["git", "-C", str(seed), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(seed), "config", "user.name", "Test"],
        check=True,
    )
    (seed / "worker.txt").write_text("candidate\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(seed), "add", "worker.txt"], check=True)
    subprocess.run(["git", "-C", str(seed), "commit", "-q", "-m", "candidate"], check=True)
    sha = subprocess.check_output(
        ["git", "-C", str(seed), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    tree = subprocess.check_output(
        ["git", "-C", str(seed), "rev-parse", "HEAD^{tree}"],
        text=True,
    ).strip()
    image_tag = f"staging-{sha[:7]}"
    source_repository = repository_root / f"loom-remote-worker-{image_tag}"
    subprocess.run(
        ["git", "clone", "-q", "--no-hardlinks", str(seed), str(source_repository)],
        check=True,
    )
    source_env = env_root / f"staging-gb10-worker-{image_tag}.env"
    source_env.write_text(
        f"LOOM_WORKER_CANDIDATE_SHA={sha}\n",
        encoding="utf-8",
    )
    source_env.chmod(0o600)
    checked_in = load_authority_config(CONFIG)
    config = replace(
        checked_in,
        producer_uid=os.getuid(),
        producer_gid=os.getgid(),
        batch_uid=os.getuid(),
        batch_gid=os.getgid(),
        artifact_root=tmp_path / "authority",
        repository_root=target_repository_root,
        worker_env_root=target_env_root,
        producer_repository_template=str(repository_root / "loom-remote-worker-{image_tag}"),
        producer_worker_env_template=str(env_root / "staging-gb10-worker-{image_tag}.env"),
        repository_template=str(target_repository_root / "loom-remote-worker-{image_tag}"),
        worker_env_template=str(target_env_root / "staging-gb10-worker-{image_tag}.env"),
    )
    return config, sha, tree, image_tag, source_repository, source_env


def test_producer_tree_validation_is_read_only(tmp_path: Path) -> None:
    config, _sha, _tree, _image_tag, source_repository, source_env = _publication_fixture(tmp_path)
    before = {
        path.relative_to(source_repository): (
            path.lstat().st_mode,
            path.lstat().st_uid,
            path.lstat().st_gid,
        )
        for path in (source_repository, *source_repository.rglob("*"))
    }
    descriptor = os.open(source_repository, os.O_RDONLY | os.O_DIRECTORY)
    try:
        authority._validate_producer_tree(
            descriptor,
            uid=config.producer_uid,
            gid=config.producer_gid,
        )
    finally:
        os.close(descriptor)
    after = {
        path.relative_to(source_repository): (
            path.lstat().st_mode,
            path.lstat().st_uid,
            path.lstat().st_gid,
        )
        for path in (source_repository, *source_repository.rglob("*"))
    }
    assert after == before
    assert source_env.stat().st_mode & 0o777 == 0o600


def test_generation_publication_orders_immutable_files_high_water_then_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(load_authority_config(CONFIG), artifact_root=tmp_path / "authority")
    generation_id = "1" * 64
    events: list[Path] = []

    def fake_private_dir(path: Path) -> None:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)

    def fake_write(path: Path, payload: bytes, *, no_replace: bool = False) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if no_replace:
            path.touch(exist_ok=False)
        path.write_bytes(payload)
        events.append(path)

    monkeypatch.setattr(authority, "_safe_private_dir", fake_private_dir)
    monkeypatch.setattr(authority, "_write_root_file", fake_write)
    monkeypatch.setattr(authority, "_rename_noreplace", os.rename)
    pointer = {
        "schema_version": 1,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "generation": 1,
        "generation_id": generation_id,
        "artifact_sha256": "2" * 64,
        "signature_sha256": "3" * 64,
        "key_id": "4" * 64,
        "created_at": "2026-07-29T00:00:00Z",
        "expires_at": "2026-07-29T00:15:00Z",
    }
    authority._publish_generation(
        config,
        candidate_sha=SHA,
        generation_id=generation_id,
        artifact=b"{}\n",
        signature=b"c2ln\n",
        pointer=pointer,
    )

    generation = config.artifact_root / "authorities" / SHA / "generations" / generation_id
    assert (generation / "acceptance.json").read_bytes() == b"{}\n"
    assert (generation / "acceptance.sig").read_bytes() == b"c2ln\n"
    assert events[-2:] == [
        config.artifact_root / "state" / "generation-high-water.json",
        config.artifact_root / "current.json",
    ]


def test_publication_rejects_foreign_stage_without_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, sha, tree, image_tag, source_repository, source_env = _publication_fixture(tmp_path)
    stage = config.repository_root / f".publish-{sha}"
    stage.mkdir()
    marker = stage / "foreign"
    marker.write_text("preserve\n", encoding="utf-8")
    monkeypatch.setattr(
        authority,
        "_publisher_lock",
        lambda _config: os.open("/dev/null", os.O_RDONLY),
    )

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="foreign candidate publication stage",
    ):
        authority._publish_candidate(
            config,
            candidate_sha=sha,
            candidate_tree=tree,
            image_tag=image_tag,
        )

    assert marker.read_text(encoding="utf-8") == "preserve\n"
    assert source_repository.exists()
    assert source_env.exists()


def test_publication_rejects_mismatched_recovery_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, sha, tree, image_tag, _source_repository, _source_env = _publication_fixture(tmp_path)
    journal = authority._publisher_journal_path(config)
    journal.parent.mkdir(parents=True)
    journal.write_bytes(
        authority.canonical_json_bytes(
            {
                "schema_version": 1,
                "kind": "staging_external_slurm_candidate_publication",
                "candidate_sha": "f" * 40,
                "candidate_tree": tree,
                "image_tag": image_tag,
                "phase": "prepared",
            }
        )
    )
    monkeypatch.setattr(
        authority,
        "_publisher_lock",
        lambda _config: os.open("/dev/null", os.O_RDONLY),
    )

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="another candidate publication transaction",
    ):
        authority._publish_candidate(
            config,
            candidate_sha=sha,
            candidate_tree=tree,
            image_tag=image_tag,
        )

    assert journal.exists()


def test_publication_recovery_rejects_non_root_stage_even_with_matching_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, sha, tree, image_tag, source_repository, source_env = _publication_fixture(tmp_path)
    stage = config.repository_root / f".publish-{sha}"
    stage.mkdir()
    (stage / "partial").write_text("partial\n", encoding="utf-8")
    foreign = config.repository_root / ".foreign"
    foreign.mkdir()
    marker = foreign / "preserve"
    marker.write_text("preserve\n", encoding="utf-8")
    target_repository = config.repository_root / f"loom-remote-worker-{image_tag}"
    target_env = config.worker_env_root / f"staging-gb10-worker-{image_tag}.env"
    journal_payload = {
        "schema_version": 1,
        "kind": "staging_external_slurm_candidate_publication",
        "candidate_sha": sha,
        "candidate_tree": tree,
        "image_tag": image_tag,
        "source_repository": str(source_repository),
        "source_worker_env": str(source_env),
        "target_repository": str(target_repository),
        "target_worker_env": str(target_env),
        "stage_repository": str(stage),
        "stage_worker_env": str(config.worker_env_root / f".publish-{sha}.env"),
        "phase": "prepared",
    }
    journal = authority._publisher_journal_path(config)
    journal.parent.mkdir(parents=True)
    journal.write_bytes(authority.canonical_json_bytes(journal_payload))
    monkeypatch.setattr(
        authority,
        "_publisher_lock",
        lambda _config: os.open("/dev/null", os.O_RDONLY),
    )

    with pytest.raises(
        authority.ExternalSlurmAcceptanceError,
        match="publication stage is foreign",
    ):
        authority._publish_candidate(
            config,
            candidate_sha=sha,
            candidate_tree=tree,
            image_tag=image_tag,
        )

    assert stage.exists()
    assert marker.read_text(encoding="utf-8") == "preserve\n"
