from __future__ import annotations

import json
import os
import stat
import subprocess
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from scripts.ops import staging_pressure_reclaim_authority as authority

CONFIG_PATH = Path(
    "deploy/developer-sandboxes/staging-pressure-reclaim-authority.toml",
)
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
SESSION_ID = "00000000-0000-0000-0000-000000000001"
TEAM_ID = "00000000-0000-0000-0000-000000000002"
CLAIM_TRIAL_ID = "00000000-0000-0000-0000-000000000003"
CLAIM_WORKER_ID = "00000000-0000-0000-0000-000000000004"
INTERRUPTED_TRIAL_ID = "00000000-0000-0000-0000-000000000005"
REGISTRY_ID = "00000000-0000-0000-0000-000000000006"
DRAINED_WORKER_ID = "00000000-0000-0000-0000-000000000007"
SHA = "a" * 40
TREE = "b" * 40
ACCEPTANCE_SESSION_ID = "1" * 32


def _session() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": authority.KIND_SESSION,
        "session_id": SESSION_ID,
        "acceptance_session_id": ACCEPTANCE_SESSION_ID,
        "environment": "staging",
        "pool": "gb10",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "created_at": authority._iso(NOW),
        "expires_at": authority._iso(NOW + timedelta(hours=1)),
        "loom_jobs": [
            {
                "registry_id": REGISTRY_ID,
                "job_id": "12345",
                "compose_project": "loom-pressure-acceptance",
                "worker_id": DRAINED_WORKER_ID,
                "sandbox_identity": "qianyi",
                "slurm_user": "loom-staging-worker",
                "slurm_account": "loom-staging",
                "slurm_qos": "loom-staging",
                "job_name": "loom-pressure",
            },
        ],
        "interrupted_trial": {
            "trial_id": INTERRUPTED_TRIAL_ID,
            "team_id": TEAM_ID,
            "task_id": "pressure-interrupted",
            "worker_id": DRAINED_WORKER_ID,
        },
        "claim_probe": {
            "trial_id": CLAIM_TRIAL_ID,
            "team_id": TEAM_ID,
            "task_id": "pressure-claim-probe",
            "worker_id": CLAIM_WORKER_ID,
            "caps": [
                {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "nvidia",
                    "network_policies": ["public"],
                    "mounted_fs": True,
                    "resource_modes": ["auto"],
                },
            ],
        },
    }


def _config(tmp_path: Path) -> authority.Config:
    return authority.Config(
        environment="staging",
        pool="gb10",
        source_host="trt-eai-oldlab-1",
        submit_host="trt-gb10-1",
        partition="gb10",
        control_plane_url="http://127.0.0.1:8080",
        admin_secret_file=tmp_path / "admin-token",
        worker_secret_file=tmp_path / "worker-token",
        node_transport=Path("/usr/local/libexec/loom-developer-sandbox-node-transport"),
        published_root=tmp_path / "published",
        public_key=tmp_path / "public.pem",
        private_key=tmp_path / "private.pem",
        max_session_age_seconds=7200,
        poll_interval_seconds=0.01,
        terminal_timeout_seconds=1,
        retry_timeout_seconds=1,
        http_timeout_seconds=1,
    )


def _private_pem(
    private: ed25519.Ed25519PrivateKey | rsa.RSAPrivateKey,
) -> bytes:
    return private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _public_pem(
    private: ed25519.Ed25519PrivateKey | rsa.RSAPrivateKey,
) -> bytes:
    return private.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


class _CryptoRun:
    def __call__(
        self,
        argv: tuple[str, ...],
        *,
        input: bytes | None = None,
        **_kwargs: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        try:
            if argv[1:4] == ("genpkey", "-algorithm", "Ed25519"):
                stdout = _private_pem(ed25519.Ed25519PrivateKey.generate())
            elif argv[1] == "pkey":
                source = Path(argv[argv.index("-in") + 1]).read_bytes()
                if "-pubin" in argv:
                    public = serialization.load_pem_public_key(source)
                else:
                    private = serialization.load_pem_private_key(source, password=None)
                    public = private.public_key()
                encoding = (
                    serialization.Encoding.DER
                    if argv[-2:] == ("-outform", "DER")
                    else serialization.Encoding.PEM
                )
                stdout = public.public_bytes(
                    encoding,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            elif argv[1:4] == ("pkeyutl", "-sign", "-rawin"):
                private = serialization.load_pem_private_key(
                    Path(argv[argv.index("-inkey") + 1]).read_bytes(),
                    password=None,
                )
                assert input is not None
                stdout = private.sign(input)
            elif argv[1:4] == ("pkeyutl", "-verify", "-rawin"):
                public = serialization.load_pem_public_key(
                    Path(argv[argv.index("-inkey") + 1]).read_bytes(),
                )
                signature = Path(argv[argv.index("-sigfile") + 1]).read_bytes()
                assert input is not None
                public.verify(signature, input)
                stdout = b"Signature Verified Successfully\n"
            else:
                raise AssertionError(argv)
        except (TypeError, ValueError, InvalidSignature):
            return subprocess.CompletedProcess(argv, 1, stdout=b"", stderr=b"")
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr=b"")


def _enable_user_owned_security_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    secure_read = authority._read_secure_bytes
    load_secret = authority._load_secret

    def user_owned_read(path: Path, **kwargs: Any) -> bytes:
        kwargs.pop("uid", None)
        kwargs.pop("gid", None)
        return secure_read(path, uid=os.getuid(), gid=os.getgid(), **kwargs)

    def rename_noreplace(source: Path, target: Path) -> None:
        os.link(source, target, follow_symlinks=False)
        source.unlink()

    def user_owned_secret(path: Path) -> str:
        return load_secret(path, uid=os.getuid(), gid=os.getgid())

    monkeypatch.setattr(authority, "_read_secure_bytes", user_owned_read)
    monkeypatch.setattr(authority, "_load_secret", user_owned_secret)
    monkeypatch.setattr(authority, "_verify_trusted_parent_chain", lambda *_a, **_k: None)
    monkeypatch.setattr(authority.os, "fchown", lambda *_args: None)
    monkeypatch.setattr(authority, "_rename_noreplace", rename_noreplace)
    monkeypatch.setattr(authority, "_require_root", lambda: None)
    monkeypatch.setattr(authority, "_require_source_host", lambda _config: None)


def _write_secret(path: Path, value: str) -> None:
    path.write_text(value + "\n", encoding="utf-8")
    path.chmod(0o600)


def _active_row() -> dict[str, Any]:
    return {
        "id": REGISTRY_ID,
        "environment": "staging",
        "pool_name": "gb10",
        "state": "running",
        "job_id": "12345",
        "worker_id": DRAINED_WORKER_ID,
        "compose_project": "loom-pressure-acceptance",
        "sandbox_identity": "qianyi",
        "candidate_sha": SHA,
        "redacted_env": {
            authority.SESSION_MARKER_KEY: SESSION_ID,
            authority.OWNERSHIP_MARKER_KEY: authority.OWNERSHIP_MARKER_VALUE,
        },
    }


def _observation(*, include_owned: bool, phase: str = "before") -> dict[str, Any]:
    jobs = [
        {
            "job_id": "99999",
            "user": "researcher",
            "account": "research",
            "qos": "normal",
            "state": "RUNNING",
            "nodes": "trt-gb10-2",
            "name": "foreign-peer",
        },
    ]
    if include_owned:
        jobs.append(
            {
                "job_id": "12345",
                "user": "loom-staging-worker",
                "account": "loom-staging",
                "qos": "loom-staging",
                "state": "RUNNING",
                "nodes": "trt-gb10-1",
                "name": "loom-pressure",
            },
        )
    result = {
        "schema_version": 1,
        "kind": authority.KIND_OBSERVE_RESULT,
        "submit_host": "trt-gb10-1",
        "environment": "staging",
        "pool": "gb10",
        "partition": "gb10",
        "account": "loom-staging",
        "qos": "loom-staging",
        "phase": phase,
        "session_id": SESSION_ID,
        "acceptance_session_id": ACCEPTANCE_SESSION_ID,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "observed_at": authority._iso(NOW),
        "jobs": sorted(jobs, key=lambda row: row["job_id"]),
    }
    result["snapshot_sha256"] = authority._digest(result)
    return result


def test_checked_in_config_is_staging_only_and_closed() -> None:
    raw = tomllib.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    assert set(raw) == authority.CONFIG_FIELDS
    assert raw["environment"] == "staging"
    assert raw["pool"] == "gb10"
    assert raw["source_host"] == "trt-eai-oldlab-1"
    assert raw["submit_host"] == "trt-gb10-1"
    assert raw["control_plane_url"].startswith("http://127.0.0.1:")
    assert not any("prod" in str(value).lower() for value in raw.values())
    assert raw["control_plane_url"] == authority.CONTROL_PLANE_URL
    assert raw["admin_secret_file"] == str(authority.ADMIN_SECRET_FILE)
    assert raw["worker_secret_file"] == str(authority.WORKER_SECRET_FILE)
    assert raw["node_transport"] == str(authority.NODE_TRANSPORT)
    assert raw["published_root"] == str(authority.PUBLISHED_ROOT)
    assert raw["public_key"] == str(authority.PUBLIC_KEY)
    assert raw["private_key"] == str(authority.PRIVATE_KEY)


@pytest.mark.parametrize(
    "field",
    (
        "control_plane_url",
        "admin_secret_file",
        "worker_secret_file",
        "node_transport",
        "published_root",
        "public_key",
        "private_key",
    ),
)
def test_config_rejects_any_security_sensitive_path_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    raw = tomllib.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    raw[field] = "/foreign"
    path = tmp_path / "authority.toml"
    lines = [
        f"{key} = {json.dumps(value)}"
        for key, value in raw.items()
        if isinstance(value, (str, int, float))
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        authority,
        "_read_secure_bytes",
        lambda candidate, **_kwargs: candidate.read_bytes(),
    )

    with pytest.raises(authority.AuthorityError, match="fixed staging identity or path"):
        authority._load_config(path)


def test_bootstrap_generates_persistent_keypair_and_check_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _write_secret(config.admin_secret_file, "admin-secret")
    _write_secret(config.worker_secret_file, "worker-secret")
    _enable_user_owned_security_fixture(monkeypatch)
    monkeypatch.setattr(authority, "_prepare_state", lambda: None)
    monkeypatch.setattr(
        authority,
        "_prepare_private_directory",
        lambda path: path.mkdir(mode=0o700, parents=True, exist_ok=True),
    )
    crypto = _CryptoRun()

    result = authority.bootstrap(config=config, execute=True, run=crypto)
    private_before = config.private_key.read_bytes()
    public_before = config.public_key.read_bytes()
    metadata_before = (
        config.private_key.stat().st_ino,
        config.private_key.stat().st_mtime_ns,
        config.public_key.stat().st_ino,
        config.public_key.stat().st_mtime_ns,
    )
    checked = authority.check_authority(config=config, run=crypto)
    repeated = authority.bootstrap(config=config, execute=True, run=crypto)

    private = serialization.load_pem_private_key(private_before, password=None)
    public = serialization.load_pem_public_key(public_before)
    challenge = b"independent-pressure-key-readback"
    public.verify(private.sign(challenge), challenge)
    assert result == {
        "status": "bootstrapped",
        "keypair": "ed25519",
        "external_secret_prerequisites": "verified",
    }
    assert checked == {
        "status": "verified",
        "keypair": "ed25519",
        "external_secret_prerequisites": "verified",
    }
    assert repeated["status"] == "existing-keypair-verified"
    assert config.private_key.read_bytes() == private_before
    assert config.public_key.read_bytes() == public_before
    assert stat.S_IMODE(config.private_key.stat().st_mode) == 0o600
    assert stat.S_IMODE(config.public_key.stat().st_mode) == 0o600
    assert (
        config.private_key.stat().st_ino,
        config.private_key.stat().st_mtime_ns,
        config.public_key.stat().st_ino,
        config.public_key.stat().st_mtime_ns,
    ) == metadata_before
    rendered = json.dumps([result, checked, repeated])
    for forbidden in (
        "admin-secret",
        "worker-secret",
        "Authorization",
        "Bearer",
        "sha256",
        "digest",
    ):
        assert forbidden not in rendered
    assert not list(tmp_path.glob(".*.tmp-*"))
    assert not list(tmp_path.glob(".authority-key-readback-*.sig"))


def test_private_only_crash_recovery_rolls_forward_without_rotation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _enable_user_owned_security_fixture(monkeypatch)
    crypto = _CryptoRun()
    install = authority._install_key_no_replace
    calls = 0

    def crash_after_private(path: Path, payload: bytes, *, mode: int) -> bool:
        nonlocal calls
        calls += 1
        installed = install(path, payload, mode=mode)
        if calls == 1:
            raise RuntimeError("injected crash")
        return installed

    monkeypatch.setattr(authority, "_install_key_no_replace", crash_after_private)
    with pytest.raises(RuntimeError, match="injected crash"):
        authority._converge_keypair(config, run=crypto)
    private_before = config.private_key.read_bytes()
    assert not config.public_key.exists()

    monkeypatch.setattr(authority, "_install_key_no_replace", install)
    assert authority._converge_keypair(config, run=crypto) == "private-key-roll-forward"
    assert config.private_key.read_bytes() == private_before
    private = serialization.load_pem_private_key(private_before, password=None)
    assert config.public_key.read_bytes() == _public_pem(private)


def test_public_only_is_preserved_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _enable_user_owned_security_fixture(monkeypatch)
    public = _public_pem(ed25519.Ed25519PrivateKey.generate())
    config.public_key.write_bytes(public)
    config.public_key.chmod(0o600)

    with pytest.raises(authority.AuthorityError, match="exists without its private key"):
        authority._converge_keypair(config, run=_CryptoRun())

    assert not config.private_key.exists()
    assert config.public_key.read_bytes() == public


@pytest.mark.parametrize(
    ("leaf", "unsafe_kind"),
    (
        ("private", "symlink"),
        ("private", "hardlink"),
        ("private", "wrong-mode"),
        ("public", "symlink"),
        ("public", "hardlink"),
        ("public", "wrong-mode"),
    ),
)
def test_unsafe_key_leaf_fails_closed_without_deleting_foreign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    leaf: str,
    unsafe_kind: str,
) -> None:
    config = _config(tmp_path)
    _enable_user_owned_security_fixture(monkeypatch)
    private = ed25519.Ed25519PrivateKey.generate()
    config.private_key.write_bytes(_private_pem(private))
    config.private_key.chmod(0o600)
    config.public_key.write_bytes(_public_pem(private))
    config.public_key.chmod(0o600)
    path = config.private_key if leaf == "private" else config.public_key
    payload = path.read_bytes()
    path.unlink()
    foreign = tmp_path / f"foreign-{leaf}"
    if unsafe_kind == "symlink":
        foreign.write_bytes(payload)
        foreign.chmod(0o600)
        path.symlink_to(foreign)
    else:
        path.write_bytes(payload)
        path.chmod(0o644 if unsafe_kind == "wrong-mode" else 0o600)
        if unsafe_kind == "hardlink":
            os.link(path, foreign)

    with pytest.raises(authority.AuthorityError):
        authority._converge_keypair(config, run=_CryptoRun())

    assert path.exists() or path.is_symlink()
    if unsafe_kind == "symlink":
        assert path.is_symlink()
        assert foreign.read_bytes() == payload
    elif unsafe_kind == "hardlink":
        assert foreign.read_bytes() == payload
        assert path.stat().st_nlink == 2
    else:
        assert path.read_bytes() == payload
        assert stat.S_IMODE(path.stat().st_mode) == 0o644


def test_wrong_owner_key_leaf_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    private = ed25519.Ed25519PrivateKey.generate()
    config.private_key.write_bytes(_private_pem(private))
    config.private_key.chmod(0o600)
    config.public_key.write_bytes(_public_pem(private))
    config.public_key.chmod(0o600)
    monkeypatch.setattr(authority, "_verify_trusted_parent_chain", lambda *_a, **_k: None)

    with pytest.raises(authority.AuthorityError, match="secure file metadata"):
        authority._converge_keypair(config, run=_CryptoRun())

    assert config.private_key.exists()
    assert config.public_key.exists()


def test_rsa_and_mismatched_pairs_are_preserved_and_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _enable_user_owned_security_fixture(monkeypatch)
    rsa_private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    config.private_key.write_bytes(_private_pem(rsa_private))
    config.private_key.chmod(0o600)

    with pytest.raises(authority.AuthorityError, match="must be Ed25519"):
        authority._converge_keypair(config, run=_CryptoRun())
    rsa_bytes = config.private_key.read_bytes()
    assert not config.public_key.exists()

    config.private_key.write_bytes(_private_pem(ed25519.Ed25519PrivateKey.generate()))
    config.public_key.write_bytes(_public_pem(ed25519.Ed25519PrivateKey.generate()))
    config.public_key.chmod(0o600)
    with pytest.raises(authority.AuthorityError, match="does not match"):
        authority._converge_keypair(config, run=_CryptoRun())
    assert config.public_key.exists()
    assert rsa_bytes.startswith(b"-----BEGIN PRIVATE KEY-----")


@pytest.mark.parametrize("unsafe_kind", ("symlink", "hardlink", "wrong-mode"))
def test_external_token_prerequisite_rejects_unsafe_leaf_without_deleting_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_kind: str,
) -> None:
    config = _config(tmp_path)
    _enable_user_owned_security_fixture(monkeypatch)
    path = config.admin_secret_file
    foreign = tmp_path / "foreign-token"
    if unsafe_kind == "symlink":
        _write_secret(foreign, "foreign-secret")
        path.symlink_to(foreign)
    else:
        _write_secret(path, "foreign-secret")
        if unsafe_kind == "hardlink":
            os.link(path, foreign)
        else:
            path.chmod(0o640)

    with pytest.raises(authority.AuthorityError) as raised:
        authority._load_secret(path)

    assert "foreign-secret" not in str(raised.value)
    assert path.exists() or path.is_symlink()
    if foreign.exists():
        assert foreign.read_text(encoding="utf-8") == "foreign-secret\n"


def test_external_token_wrong_owner_and_missing_token_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _write_secret(config.admin_secret_file, "owner-secret")
    monkeypatch.setattr(authority, "_verify_trusted_parent_chain", lambda *_a, **_k: None)

    with pytest.raises(authority.AuthorityError) as owner_error:
        authority._load_secret(config.admin_secret_file)
    assert "owner-secret" not in str(owner_error.value)

    with pytest.raises(authority.AuthorityError) as missing_error:
        authority._load_secret(config.worker_secret_file)
    assert "owner-secret" not in str(missing_error.value)


def test_live_cli_rejects_config_override() -> None:
    with pytest.raises(SystemExit):
        authority._parser().parse_args(
            ["--config", "/tmp/foreign.toml", "run", "--session-id", SESSION_ID],
        )


def test_session_binds_interrupted_trial_to_owned_worker() -> None:
    valid = authority._validate_session(
        _session(),
        now=NOW,
        max_age_seconds=7200,
    )
    assert valid["interrupted_trial"]["worker_id"] == DRAINED_WORKER_ID

    invalid = _session()
    invalid["interrupted_trial"]["worker_id"] = CLAIM_WORKER_ID
    with pytest.raises(authority.AuthorityError, match="not owned"):
        authority._validate_session(invalid, now=NOW, max_age_seconds=7200)


def test_closed_world_registry_rejects_foreign_loom_job() -> None:
    session = _session()
    assert authority._active_pool_jobs({"jobs": [_active_row()]}, session)[0]["job_id"] == "12345"

    foreign = dict(_active_row())
    foreign["id"] = "00000000-0000-0000-0000-000000000009"
    foreign["job_id"] = "54321"
    with pytest.raises(authority.AuthorityError, match="foreign or missing"):
        authority._active_pool_jobs({"jobs": [_active_row(), foreign]}, session)


def test_closed_world_registry_requires_acceptance_markers() -> None:
    row = _active_row()
    row["redacted_env"] = {}
    with pytest.raises(authority.AuthorityError, match="ownership markers"):
        authority._active_pool_jobs({"jobs": [row]}, _session())


def test_peer_snapshot_excludes_only_exact_owned_job_ids() -> None:
    observation = authority._validate_observation(
        _observation(include_owned=True),
        session=_session(),
    )
    peers = authority._peer_snapshot(
        observation,
        session=_session(),
        require_owned=True,
    )
    assert peers == [
        {
            "job_id": "99999",
            "user": "researcher",
            "account": "research",
            "qos": "normal",
            "state": "RUNNING",
            "nodes": "trt-gb10-2",
            "name": "foreign-peer",
        },
    ]


def test_observe_slurm_uses_fixed_partition_and_bounded_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authority, "_host", lambda: "trt-gb10-1")
    captured: dict[str, Any] = {}

    def fake_run(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=(
                b"12345|loom-staging-worker|loom-staging|loom-staging|"
                b"RUNNING|trt-gb10-1|loom-pressure\n"
            ),
            stderr=b"",
        )

    request = {
        "schema_version": 1,
        "kind": authority.KIND_OBSERVE_REQUEST,
        "source_host": "trt-eai-oldlab-1",
        "submit_host": "trt-gb10-1",
        "environment": "staging",
        "pool": "gb10",
        "partition": "gb10",
        "account": "loom-staging",
        "qos": "loom-staging",
        "phase": "before",
        "session_id": SESSION_ID,
        "acceptance_session_id": ACCEPTANCE_SESSION_ID,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "owned_jobs": [
            {
                "job_id": "12345",
                "user": "loom-staging-worker",
                "account": "loom-staging",
                "qos": "loom-staging",
                "name": "loom-pressure",
            },
        ],
    }
    result = authority.observe_slurm(request, run=fake_run)

    assert captured["argv"] == (
        "/usr/bin/squeue",
        "--noheader",
        "--partition",
        "gb10",
        "--states",
        ",".join(sorted(authority.ACTIVE_SLURM_STATES)),
        "--format",
        "%i|%u|%a|%q|%T|%N|%j",
    )
    assert result["jobs"][0]["job_id"] == "12345"
    assert captured["kwargs"]["env"]["PATH"] == "/usr/sbin:/usr/bin:/sbin:/bin"


def test_complete_transaction_proves_fence_retry_recovery_and_peer_zero_impact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_root = tmp_path / "state"
    monkeypatch.setattr(authority, "STATE_ROOT", state_root)
    monkeypatch.setattr(authority, "SESSION_ROOT", state_root / "sessions")
    monkeypatch.setattr(authority, "TRANSACTION_ROOT", state_root / "transactions")
    monkeypatch.setattr(authority, "RECEIPT_ROOT", state_root / "receipts")
    monkeypatch.setattr(authority, "HIGH_WATER_ROOT", state_root / "high-water")
    monkeypatch.setattr(authority, "LOCK_PATH", state_root / "authority.lock")
    monkeypatch.setattr(authority, "CURRENT_PATH", state_root / "current.json")
    monkeypatch.setattr(authority, "_require_root", lambda: None)
    monkeypatch.setattr(authority, "_require_source_host", lambda _config: None)
    monkeypatch.setattr(authority, "_load_secret", lambda _path: "injected-token")
    monkeypatch.setattr(authority, "_read_secure_bytes", lambda path, **_kwargs: path.read_bytes())
    monkeypatch.setattr(
        authority,
        "_prepare_private_directory",
        lambda path: path.mkdir(mode=0o700, parents=True, exist_ok=True),
    )

    def prepare_state() -> None:
        for path in (
            authority.STATE_ROOT,
            authority.SESSION_ROOT,
            authority.TRANSACTION_ROOT,
            authority.RECEIPT_ROOT,
            authority.HIGH_WATER_ROOT,
        ):
            path.mkdir(parents=True, exist_ok=True)
        authority.LOCK_PATH.touch(mode=0o600, exist_ok=True)

    monkeypatch.setattr(authority, "_prepare_state", prepare_state)
    prepare_state()
    session = _session()
    (authority.SESSION_ROOT / f"{SESSION_ID}.json").write_bytes(
        authority._canonical(session),
    )
    config = _config(tmp_path)
    config.public_key.write_bytes(b"test-public-key\n")
    remote = {
        "pressure_active": False,
        "pressure_seen": False,
        "claim_state": "queued",
    }

    def trial_body(trial_id: str) -> dict[str, Any]:
        if trial_id == INTERRUPTED_TRIAL_ID:
            state = "queued" if remote["pressure_seen"] else "running"
            return {
                "id": INTERRUPTED_TRIAL_ID,
                "team_id": TEAM_ID,
                "task_id": "pressure-interrupted",
                "state": state,
                "failure_reason": ("prod_capacity_pressure" if remote["pressure_seen"] else None),
            }
        return {
            "id": CLAIM_TRIAL_ID,
            "team_id": TEAM_ID,
            "task_id": "pressure-claim-probe",
            "state": remote["claim_state"],
            "failure_reason": None,
        }

    def http_call(
        *,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> authority.HttpResponse:
        if method == "GET" and path == "/admin/slurm-worker-jobs/status":
            if remote["pressure_seen"]:
                terminal = {
                    **_active_row(),
                    "state": "cancelled",
                    "pending_reason": "cancelled by prod-pressure reclaim",
                }
                return authority.HttpResponse(200, {"jobs": [terminal]})
            return authority.HttpResponse(200, {"jobs": [_active_row()]})
        if method == "GET" and path.startswith("/trials/"):
            return authority.HttpResponse(200, trial_body(path.rsplit("/", 1)[-1]))
        if method == "POST" and path.endswith("/prod-pressure"):
            assert body is not None
            remote["pressure_active"] = bool(body["prod_capacity_shortfall"])
            remote["pressure_seen"] = remote["pressure_seen"] or remote["pressure_active"]
            return authority.HttpResponse(
                200,
                {
                    "action": "draining" if remote["pressure_active"] else "recovered",
                    "actuator": "slurm",
                    "environment": "staging",
                    "pool_name": "gb10",
                    "new_staging_claims_allowed": not remote["pressure_active"],
                    "drain_intent_active": remote["pressure_active"],
                    "grace": {
                        "action": ("cancel_retryable" if remote["pressure_active"] else "none"),
                    },
                    "prod_pressure": {"has_pressure": remote["pressure_active"]},
                },
            )
        if method == "POST" and path == "/trials/claim":
            if remote["pressure_active"]:
                return authority.HttpResponse(204, None)
            remote["claim_state"] = "claimed"
            return authority.HttpResponse(
                200,
                {"trial_id": CLAIM_TRIAL_ID, "state": "claimed"},
            )
        if method == "POST" and path == f"/trials/{CLAIM_TRIAL_ID}/retry":
            remote["claim_state"] = "queued"
            return authority.HttpResponse(
                200,
                {"trial_id": CLAIM_TRIAL_ID, "state": "queued"},
            )
        raise AssertionError((method, path, body))

    observe_count = 0

    def observe_call(*, phase: str, **_kwargs: Any) -> dict[str, Any]:
        nonlocal observe_count
        observe_count += 1
        return _observation(include_owned=observe_count == 1, phase=phase)

    def sign_run(
        argv: tuple[str, ...],
        **_kwargs: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(argv, 0, stdout=b"s" * 64, stderr=b"")

    result = authority.run_session(
        SESSION_ID,
        config=config,
        http_call=http_call,
        observe_call=observe_call,
        clock=lambda: NOW,
        sleep=lambda _seconds: None,
        sign_run=sign_run,
    )

    receipt = json.loads(Path(result["receipt_path"]).read_text(encoding="utf-8"))
    assert result["status"] == "committed"
    assert receipt["evidence"]["claim_fence"]["status"] == 204
    assert receipt["evidence"]["claim_recovered"]["trial_id"] == CLAIM_TRIAL_ID
    assert receipt["evidence"]["foreign_peer_zero_impact"] is True
    assert receipt["evidence"]["interrupted_trial_retryable"]["failure_reason"] == (
        "prod_capacity_pressure"
    )
    assert remote == {
        "pressure_active": False,
        "pressure_seen": True,
        "claim_state": "queued",
    }
