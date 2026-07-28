from __future__ import annotations

import json
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


def test_execute_requires_minio_and_candidate_binding(tmp_path: Path) -> None:
    profiles_dir = _write_profiles(tmp_path)
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


def test_execute_rejects_mismatched_cp_url(tmp_path: Path) -> None:
    profiles_dir = _write_profiles(tmp_path)
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
    argv.extend(["--qianyi-cp-url", "http://127.0.0.1:1"])
    code = probe.main(argv)
    assert code == 1


def test_execute_rejects_stale_candidate_sha(tmp_path: Path) -> None:
    profiles_dir = _write_profiles(tmp_path)
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
    assert probe.ADMIN_MINT_SAME_STATUSES == frozenset({200, 201})
    assert probe.WORKER_CLAIM_FOREIGN_STATUSES == frozenset({401})


def test_build_targets_execute_binds_reviewed_endpoints(tmp_path: Path) -> None:
    profiles_dir = _write_profiles(tmp_path)
    ns_args = [
        "--execute",
        "--profiles-dir",
        str(profiles_dir),
        "--candidate-sha",
        SHA,
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
        ns_args.extend(
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
    args = probe.build_parser().parse_args(ns_args)
    targets, candidates, candidate_sha = probe.build_targets(args, execute=True)
    assert candidate_sha == SHA
    assert len(candidates) == 3
    assert targets["qianyi"].control_plane_url == "http://127.0.0.1:20004"
    assert targets["qianyi"].minio_endpoint == "http://127.0.0.1:20002"
    assert targets["qianyi"].own_bucket == "loom-sandbox-qianyi-artifacts"
    assert targets["qianyi"].foreign_bucket == "loom-sandbox-hongjian-artifacts"
    assert all(row.candidate_sha == SHA for row in candidates)
