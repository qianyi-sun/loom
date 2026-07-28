from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_crossover_probe as probe

ADMIN_A = "loom_admin_" + ("A" * 43)
ADMIN_B = "loom_admin_" + ("B" * 43)
ADMIN_C = "loom_admin_" + ("C" * 43)
WORKER_A = "loom_w_" + ("a" * 40)
WORKER_B = "loom_w_" + ("b" * 40)
WORKER_C = "loom_w_" + ("c" * 40)
SHA = "a" * 40
TREE = "b" * 40


def _write_admin(path: Path, token: str) -> None:
    path.write_text(
        f'[admin]\ntoken = "{token}"\ncreated_at = "2026-07-28T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


def _write_worker_env(path: Path, token: str) -> None:
    path.write_text(f"LOOM_WORKER_TOKEN={token}\n", encoding="utf-8")
    path.chmod(0o600)


def _write_secret_line(path: Path, value: str) -> None:
    path.write_text(f"{value}\n", encoding="utf-8")
    path.chmod(0o600)


def _write_state(path: Path, *, sandbox: str, sha: str = SHA) -> None:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sandbox": sandbox,
                "compose_project": f"loom-sandbox-{sandbox}",
                "candidate_sha": sha,
                "candidate_tree": TREE,
                "source_repo": f"/shared_work/loom/candidates/sandboxes/{sandbox}/{sha}",
                "updated_at": "2026-07-28T00:00:00Z",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _canonical_without_digest(payload: dict[str, object]) -> bytes:
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    return json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def _write_runtime_receipts(root: Path, *, sha: str = SHA, tree: str = TREE) -> None:
    import hashlib

    now = datetime.now(UTC).replace(microsecond=0)
    for sandbox in probe.ALLOWED_SANDBOXES:
        fleet_expires = now + timedelta(minutes=15)
        collector_expires = now + timedelta(minutes=10)
        payload: dict[str, object] = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-combined-activation",
            "sandbox": sandbox,
            "candidate_sha": sha,
            "candidate_tree": tree,
            "collector": {
                "hostname": "trt-eai-oldlab-2",
                "collected_at": now.isoformat(),
                "expires_at": collector_expires.isoformat(),
            },
            "fleet_attestation": {
                "path": (
                    "/var/lib/loom-developer-sandbox-links/attestations/"
                    f"{sandbox}/{sha}/fleet.json"
                ),
                "payload_sha256": "sha256:" + ("c" * 64),
                "generated_at": now.isoformat().replace("+00:00", "Z"),
                "expires_at": fleet_expires.isoformat().replace("+00:00", "Z"),
            },
            "domains": {
                name: {
                    "manifest_path": f"/attestations/{sandbox}/{sha}/{name}.json",
                    "signature_path": f"/attestations/{sandbox}/{sha}/{name}.sig",
                    "payload_sha256": "d" * 64,
                    "signature_sha256": "e" * 64,
                    "key_id": f"{name}-key",
                    "generation": 1,
                    "published_at": now.isoformat(),
                    "expires_at": fleet_expires.isoformat(),
                }
                for name in ("oldlab", "gb10")
            },
        }
        payload["payload_sha256"] = hashlib.sha256(
            _canonical_without_digest(payload),
        ).hexdigest()
        destination = root / sandbox / sha / "combined.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        destination.chmod(0o600)


def _execute_argv(
    tmp_path: Path,
    profiles_dir: Path,
    runtime_root: Path,
) -> list[str]:
    argv = [
        "--execute",
        "--profiles-dir",
        str(profiles_dir),
        "--candidate-sha",
        SHA,
        "--runtime-attestation-root",
        str(runtime_root),
    ]
    for sandbox, worker, admin in (
        ("qianyi", WORKER_A, ADMIN_A),
        ("hongjian", WORKER_B, ADMIN_B),
        ("devansh", WORKER_C, ADMIN_C),
    ):
        worker_path = tmp_path / f"{sandbox}-worker.env"
        admin_path = tmp_path / f"{sandbox}-admin.toml"
        access_path = tmp_path / f"{sandbox}-access"
        secret_path = tmp_path / f"{sandbox}-secret"
        _write_worker_env(worker_path, worker)
        _write_admin(admin_path, admin)
        _write_secret_line(access_path, f"{sandbox}-access")
        _write_secret_line(secret_path, f"{sandbox}-secret")
        argv.extend(
            [
                f"--{sandbox}-worker-token-file",
                str(worker_path),
                f"--{sandbox}-admin-secret-file",
                str(admin_path),
                f"--{sandbox}-minio-access-key-file",
                str(access_path),
                f"--{sandbox}-minio-secret-key-file",
                str(secret_path),
            ],
        )
    return argv


def _stub_candidate_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe,
        "_verify_candidate_repository",
        lambda profile, **_: profile.candidate_root / SHA,
    )


def _write_profiles(tmp_path: Path) -> Path:
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    for index, sandbox in enumerate(probe.ALLOWED_SANDBOXES):
        port_base = 20_000 + index * 1_000
        state_root = tmp_path / "state" / sandbox
        (profiles_dir / f"{sandbox}.toml").write_text(
            f"""schema_version = 1
sandbox = "{sandbox}"
ssh_target = "oldlab-2"
canonical_hostname = "trt-eai-oldlab-2"
compose_project = "loom-sandbox-{sandbox}"
bind_address = "127.0.0.1"
provider_connection_namespace = "sandbox-{sandbox}"
candidate_root = "/shared_work/loom/candidates/sandboxes/{sandbox}"
state_root = "{state_root}"
cache_root = "{state_root / "cache"}"
evidence_root = "{state_root / "evidence"}"
runtime_root = "{state_root / "runtime"}"

[ports]
postgres = {port_base + 1}
minio = {port_base + 2}
minio_console = {port_base + 3}
control_plane = {port_base + 4}
loom_service = {port_base + 5}
llm_gateway = {port_base + 6}
egress_xds = {port_base + 7}
egress_proxy = {port_base + 8}
egress_admin = {port_base + 9}
web = {port_base + 10}

[database]
name = "loom_sandbox_{sandbox}"

[object_store]
task_bucket = "loom-sandbox-{sandbox}-tasks"
trajectories_bucket = "loom-sandbox-{sandbox}-trajectories"
artifacts_bucket = "loom-sandbox-{sandbox}-artifacts"
""",
            encoding="utf-8",
        )
        _write_state(state_root / "sandbox-state.json", sandbox=sandbox)
    return profiles_dir


def test_dry_run_matrix_is_secret_free(tmp_path: Path) -> None:
    admin_a = tmp_path / "a-admin.toml"
    admin_b = tmp_path / "b-admin.toml"
    worker_a = tmp_path / "a-worker.env"
    worker_b = tmp_path / "b-worker.env"
    _write_admin(admin_a, ADMIN_A)
    _write_admin(admin_b, ADMIN_B)
    _write_worker_env(worker_a, WORKER_A)
    _write_worker_env(worker_b, WORKER_B)

    evidence_path = tmp_path / "evidence.json"
    code = probe.main(
        [
            "--qianyi-cp-url",
            "http://127.0.0.1:20080",
            "--qianyi-worker-token-file",
            str(worker_a),
            "--qianyi-admin-secret-file",
            str(admin_a),
            "--hongjian-cp-url",
            "http://127.0.0.1:21080",
            "--hongjian-worker-token-file",
            str(worker_b),
            "--hongjian-admin-secret-file",
            str(admin_b),
            "--write-evidence",
            str(evidence_path),
            "--json",
        ],
    )
    assert code == 0
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "dry-run"
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["total"] >= 4
    assert any("not live A3" in note for note in payload["notes"])
    assert any("#896" in note for note in payload["notes"])
    rendered = json.dumps(payload)
    assert ADMIN_A not in rendered
    assert WORKER_A not in rendered
    assert "loom_w_" not in rendered
    assert "loom_admin_" not in rendered
    fps = {
        row["source_worker_fingerprint"]
        for row in payload["results"]
        if row.get("source_worker_fingerprint")
    }
    assert any(fp and fp.startswith("sha256:") for fp in fps)


def test_directed_pairs_cover_six_edges() -> None:
    pairs = probe.directed_pairs(["qianyi", "hongjian", "devansh"])
    assert len(pairs) == 6
    assert ("qianyi", "hongjian") in pairs
    assert ("hongjian", "qianyi") in pairs
    assert ("qianyi", "qianyi") not in pairs


def test_cli_rejects_literal_token_argv(capsys) -> None:  # type: ignore[no-untyped-def]
    import sys

    old = sys.argv
    try:
        sys.argv = [
            "developer_sandbox_crossover_probe.py",
            "--qianyi-cp-url",
            "http://127.0.0.1:20080",
            WORKER_A,
        ]
        code = probe.main(
            ["--qianyi-cp-url", "http://127.0.0.1:20080", WORKER_A],
        )
    finally:
        sys.argv = old
    assert code == 2
    err = capsys.readouterr().err
    assert "refusing literal token" in err


def test_secure_secret_file_rejects_world_readable(tmp_path: Path) -> None:
    path = tmp_path / "open.env"
    path.write_text("LOOM_WORKER_TOKEN=x\n", encoding="utf-8")
    path.chmod(0o644)
    with pytest.raises(ValueError, match="mode 0600"):
        probe.secure_secret_file(path, label="worker-token secret file")


def test_secure_secret_file_rejects_hard_link(tmp_path: Path) -> None:
    path = tmp_path / "worker.env"
    path.write_text("LOOM_WORKER_TOKEN=x\n", encoding="utf-8")
    path.chmod(0o600)
    (tmp_path / "worker-copy.env").hardlink_to(path)
    with pytest.raises(ValueError, match="exactly one hard link"):
        probe.secure_secret_file(path, label="worker-token secret file")


def test_secure_secret_file_rejects_symlink(tmp_path: Path) -> None:
    path = tmp_path / "worker.env"
    path.write_text("LOOM_WORKER_TOKEN=x\n", encoding="utf-8")
    path.chmod(0o600)
    link = tmp_path / "worker-link.env"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="non-symlink"):
        probe.secure_secret_file(link, label="worker-token secret file")


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:20080",
        "http://user:pass@127.0.0.1:20080",
        "http://127.0.0.1:20080/path",
        "http://127.0.0.1:20080/?query=yes",
        "http://127.0.0.1:20080/#fragment",
        "http://127.0.0.1",
    ],
)
def test_endpoint_rejects_non_exact_url_shapes(url: str) -> None:
    with pytest.raises(ValueError, match="http://host:port"):
        probe._normalize_http_url(url)


def test_evidence_write_is_atomic_private_and_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "evidence.json"
    probe._write_evidence(target, {"schema_version": 1, "result": "pass"})
    assert target.stat().st_mode & 0o777 == 0o600
    assert json.loads(target.read_text(encoding="utf-8"))["result"] == "pass"

    target.unlink()
    outside = tmp_path / "outside.json"
    outside.write_text("preserve\n", encoding="utf-8")
    target.symlink_to(outside)
    with pytest.raises(ValueError, match="evidence target is unsafe"):
        probe._write_evidence(target, {"result": "pass"})
    assert outside.read_text(encoding="utf-8") == "preserve\n"


def test_candidate_repository_readback_requires_exact_clean_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = tmp_path / "candidates" / SHA
    candidate.mkdir(parents=True)
    profile = probe.SandboxProfileView(
        sandbox="qianyi",
        bind_address="127.0.0.1",
        compose_project="loom-sandbox-qianyi",
        candidate_root=tmp_path / "candidates",
        state_root=tmp_path / "state",
        control_plane_port=20080,
        minio_port=20900,
        artifacts_bucket="loom-sandbox-qianyi-artifacts",
        trajectories_bucket="loom-sandbox-qianyi-trajectories",
        task_bucket="loom-sandbox-qianyi-tasks",
    )

    def fake_git(_repository: Path, *args: str) -> str:
        if args[:2] == ("status", "--porcelain=v1"):
            return ""
        if args[-1] == "HEAD^{tree}":
            return TREE
        return SHA

    monkeypatch.setattr(probe, "_run_git_readback", fake_git)
    resolved = probe._verify_candidate_repository(
        profile,
        expected_sha=SHA,
        expected_tree=TREE,
        state_source_repo=str(candidate),
    )
    assert resolved == candidate.resolve()

    monkeypatch.setattr(
        probe,
        "_run_git_readback",
        lambda _repository, *args: "dirty" if args[0] == "status" else SHA,
    )
    with pytest.raises(ValueError, match=r"tree readback drifted|not clean"):
        probe._verify_candidate_repository(
            profile,
            expected_sha=SHA,
            expected_tree=TREE,
            state_source_repo=str(candidate),
        )


def test_runtime_activation_rejects_stale_or_tampered_receipt(tmp_path: Path) -> None:
    root = tmp_path / "runtime-attestations"
    _write_runtime_receipts(root)
    candidate = probe.CandidateIdentity(
        sandbox="qianyi",
        candidate_sha=SHA,
        candidate_tree=TREE,
        compose_project="loom-sandbox-qianyi",
        source_repo="/candidate",
        state_path="/state",
        state_payload_sha256="f" * 64,
        updated_at="2026-07-28T00:00:00Z",
    )
    receipt = root / "qianyi" / SHA / "combined.json"
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["candidate_tree"] = "0" * 40
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)
    with pytest.raises(ValueError, match="binding is invalid"):
        probe.load_runtime_activation(root, candidate)

    _write_runtime_receipts(root)
    with pytest.raises(ValueError, match="stale or untrusted"):
        probe.load_runtime_activation(
            root,
            candidate,
            now=datetime.now(UTC) + timedelta(hours=1),
        )


def test_execute_requires_minio_and_candidate_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profiles_dir = _write_profiles(tmp_path)
    _stub_candidate_readback(monkeypatch)
    runtime_root = tmp_path / "runtime-attestations"
    _write_runtime_receipts(runtime_root)
    secrets: dict[str, Path] = {}
    for sandbox, worker, admin in (
        ("qianyi", WORKER_A, ADMIN_A),
        ("hongjian", WORKER_B, ADMIN_B),
        ("devansh", WORKER_C, ADMIN_C),
    ):
        worker_path = tmp_path / f"{sandbox}-worker.env"
        admin_path = tmp_path / f"{sandbox}-admin.toml"
        _write_worker_env(worker_path, worker)
        _write_admin(admin_path, admin)
        secrets[f"{sandbox}_worker"] = worker_path
        secrets[f"{sandbox}_admin"] = admin_path

    argv = [
        "--execute",
        "--profiles-dir",
        str(profiles_dir),
        "--candidate-sha",
        SHA,
        "--runtime-attestation-root",
        str(runtime_root),
        "--qianyi-worker-token-file",
        str(secrets["qianyi_worker"]),
        "--qianyi-admin-secret-file",
        str(secrets["qianyi_admin"]),
        "--hongjian-worker-token-file",
        str(secrets["hongjian_worker"]),
        "--hongjian-admin-secret-file",
        str(secrets["hongjian_admin"]),
        "--devansh-worker-token-file",
        str(secrets["devansh_worker"]),
        "--devansh-admin-secret-file",
        str(secrets["devansh_admin"]),
    ]
    code = probe.main(argv)
    assert code == 1


def test_execute_rejects_mismatched_cp_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profiles_dir = _write_profiles(tmp_path)
    _stub_candidate_readback(monkeypatch)
    runtime_root = tmp_path / "runtime-attestations"
    _write_runtime_receipts(runtime_root)
    argv = _execute_argv(tmp_path, profiles_dir, runtime_root)
    argv.extend(["--qianyi-cp-url", "http://127.0.0.1:1"])
    code = probe.main(argv)
    assert code == 1


def test_execute_rejects_stale_candidate_sha(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profiles_dir = _write_profiles(tmp_path)
    _stub_candidate_readback(monkeypatch)
    stale = tmp_path / "state" / "hongjian" / "sandbox-state.json"
    _write_state(stale, sandbox="hongjian", sha="c" * 40)

    argv = ["--execute", "--profiles-dir", str(profiles_dir), "--candidate-sha", SHA]
    for sandbox, worker, admin in (
        ("qianyi", WORKER_A, ADMIN_A),
        ("hongjian", WORKER_B, ADMIN_B),
        ("devansh", WORKER_C, ADMIN_C),
    ):
        worker_path = tmp_path / f"{sandbox}-worker.env"
        admin_path = tmp_path / f"{sandbox}-admin.toml"
        access_path = tmp_path / f"{sandbox}-access"
        secret_path = tmp_path / f"{sandbox}-secret"
        _write_worker_env(worker_path, worker)
        _write_admin(admin_path, admin)
        _write_secret_line(access_path, f"{sandbox}-access")
        _write_secret_line(secret_path, f"{sandbox}-secret")
        argv.extend(
            [
                f"--{sandbox}-worker-token-file",
                str(worker_path),
                f"--{sandbox}-admin-secret-file",
                str(admin_path),
                f"--{sandbox}-minio-access-key-file",
                str(access_path),
                f"--{sandbox}-minio-secret-key-file",
                str(secret_path),
            ],
        )
    code = probe.main(argv)
    assert code == 1


def test_same_sandbox_status_classes_are_specific() -> None:
    assert probe.WORKER_CLAIM_SAME_STATUSES == frozenset({200, 204})
    assert 401 not in probe.WORKER_CLAIM_SAME_STATUSES
    assert probe.ADMIN_MINT_SAME_STATUSES == frozenset({201})
    assert probe.WORKER_CLAIM_FOREIGN_STATUSES == frozenset({401})
    assert probe.ADMIN_MINT_FOREIGN_STATUSES == frozenset({403})


def test_build_targets_execute_binds_reviewed_endpoints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profiles_dir = _write_profiles(tmp_path)
    _stub_candidate_readback(monkeypatch)
    runtime_root = tmp_path / "runtime-attestations"
    _write_runtime_receipts(runtime_root)
    ns_args = _execute_argv(tmp_path, profiles_dir, runtime_root)
    args = probe.build_parser().parse_args(ns_args)
    targets, candidates, candidate_sha = probe.build_targets(args, execute=True)
    assert candidate_sha == SHA
    assert len(candidates) == 3
    assert targets["qianyi"].control_plane_url == "http://127.0.0.1:20004"
    assert targets["qianyi"].minio_endpoint == "http://127.0.0.1:20002"
    assert targets["qianyi"].own_bucket == "loom-sandbox-qianyi-artifacts"
    assert targets["qianyi"].foreign_bucket == "loom-sandbox-hongjian-artifacts"
    assert all(row.candidate_sha == SHA for row in candidates)
    assert targets["qianyi"].runtime_activation is not None
    assert (
        targets["qianyi"].runtime_activation.fleet_payload_sha256
        == "sha256:" + ("c" * 64)
    )
