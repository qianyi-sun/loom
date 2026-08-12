from __future__ import annotations

import base64
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import (
    _artifact,
)

from loom_cli.rollout.operator.protected_gb10_external_supervisor_transport import (
    _encode_helper_request,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BROKER_PATH = REPO_ROOT / "scripts/ops/gb10_external_supervisor_broker.py"
SPEC = importlib.util.spec_from_file_location("gb10_external_supervisor_broker", BROKER_PATH)
assert SPEC is not None and SPEC.loader is not None
broker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = broker
SPEC.loader.exec_module(broker)


def _public_key(seed: int = 7, comment: str = "test") -> bytes:
    algorithm = b"ssh-ed25519"
    raw = bytes([seed]) * 32
    blob = len(algorithm).to_bytes(4, "big") + algorithm + len(raw).to_bytes(4, "big") + raw
    return b"ssh-ed25519 " + base64.b64encode(blob) + b" " + comment.encode() + b"\n"


def _run(*argv: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env={
            "HOME": str(cwd or REPO_ROOT),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
        },
    )
    return result.stdout.strip()


def _source_repo(tmp_path: Path) -> tuple[Path, str, str]:
    source = tmp_path / "source"
    source.mkdir()
    _run("git", "init", "-q", cwd=source)
    _run("git", "config", "user.email", "test@example.invalid", cwd=source)
    _run("git", "config", "user.name", "Test", cwd=source)
    (source / "payload.txt").write_text("candidate\n", encoding="utf-8")
    _run("git", "add", "payload.txt", cwd=source)
    _run("git", "commit", "-qm", "candidate", cwd=source)
    sha = _run("git", "rev-parse", "HEAD", cwd=source)
    tree = _run("git", "rev-parse", "HEAD^{tree}", cwd=source)
    return source, sha, tree


def _existing_candidate(
    tmp_path: Path,
) -> tuple[Path, str, str, Path, Path]:
    source, sha, tree = _source_repo(tmp_path)
    root = tmp_path / "candidates"
    candidate = root / sha
    candidate.mkdir(parents=True)
    _run("git", "clone", "-q", str(source), str(candidate / "repo"), cwd=tmp_path)
    _run("git", "checkout", "-q", "--detach", sha, cwd=candidate / "repo")
    venv_bin = candidate / "venv/bin"
    venv_bin.mkdir(parents=True)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (venv_bin / "python").symlink_to(system_python)
    root.chmod(0o755)
    broker._harden_tree(candidate, owner_uid=os.geteuid(), owner_gid=os.getegid())
    return root, sha, tree, source, system_python


def test_broker_parses_only_canonical_typed_protocol_identity(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    request = _encode_helper_request(
        operation="observe",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        artifact=artifact,
    )

    assert broker.parse_request_identity(request.encode()) == (
        artifact.candidate_sha,
        artifact.candidate_tree,
    )

    changed = json.loads(request)
    changed["command"] = "/bin/sh"
    with pytest.raises(broker.BrokerError, match="fields"):
        broker.parse_request_identity(
            (json.dumps(changed, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )


def test_forced_key_render_is_idempotent_and_preserves_unrelated_authority() -> None:
    original = b"# preserve\n" + _public_key(9, "unrelated")

    first = broker.render_authorized_keys(original, _public_key())
    second = broker.render_authorized_keys(first, _public_key())

    assert first == second
    assert first.startswith(original)
    assert first.count(b" loom-gb10-external-supervisor\n") == 1
    assert (
        b'restrict,command="/usr/bin/sudo -n -- '
        b'/usr/local/libexec/loom-gb10-external-supervisor-broker" ssh-ed25519 ' in first
    )


def test_forced_key_render_rejects_same_key_with_unrestricted_authority() -> None:
    public_key = _public_key()

    with pytest.raises(broker.BrokerError, match="already present"):
        broker.render_authorized_keys(public_key, public_key)


def test_existing_candidate_requires_exact_clean_hardened_root_owned_runtime(
    tmp_path: Path,
) -> None:
    root, sha, tree, source, system_python = _existing_candidate(tmp_path)

    assert broker.candidate_ready(
        root,
        sha,
        tree,
        remote_url=str(source),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        system_python=system_python,
    )

    (root / sha / "repo/payload.txt").chmod(0o666)
    assert not broker.candidate_ready(
        root,
        sha,
        tree,
        remote_url=str(source),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        system_python=system_python,
    )


def test_absent_candidate_is_published_atomically_and_verified(tmp_path: Path) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir()
    candidates.chmod(0o755)
    fake_uv = tmp_path / "uv"
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    fake_uv.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        f'ln -s {system_python} "$UV_PROJECT_ENVIRONMENT/bin/python"\n'
        'touch "$UV_PROJECT_ENVIRONMENT/.lock"\n',
        encoding="utf-8",
    )
    fake_uv.chmod(0o700)

    published = broker.ensure_candidate(
        candidates,
        sha,
        tree,
        remote_url=str(source),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        build_uid=os.geteuid(),
        build_gid=os.getegid(),
        system_python=system_python,
        uv_binary=fake_uv,
    )

    assert published == candidates / sha
    assert broker.candidate_ready(
        candidates,
        sha,
        tree,
        remote_url=str(source),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        system_python=system_python,
    )
    assert not tuple(candidates.glob(f".{sha}.*"))
    candidate_mode = (candidates / sha).stat().st_mode
    assert candidate_mode & 0o055 == 0o055
    assert (candidates / sha / "repo/payload.txt").stat().st_mode & 0o044 == 0o044


def test_candidate_materialization_commands_run_as_unprivileged_service_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    fake_uv = tmp_path / "uv"
    fake_uv.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    fake_uv.chmod(0o755)
    calls: list[tuple[tuple[str, ...], tuple[int, int] | None]] = []
    original_run = broker._run

    def spy_run(argv, **kwargs):
        calls.append((tuple(argv), kwargs.get("run_as")))
        return original_run(argv, **kwargs)

    monkeypatch.setattr(broker, "_run", spy_run)

    with pytest.raises(broker.BrokerError):
        broker.ensure_candidate(
            candidates,
            sha,
            tree,
            remote_url=str(source),
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
            build_uid=os.geteuid(),
            build_gid=os.getegid(),
            system_python=system_python,
            uv_binary=fake_uv,
        )

    materialization = [
        run_as
        for argv, run_as in calls
        if argv and (argv[0] == "/usr/bin/git" or argv[0] == str(fake_uv))
    ]
    assert materialization
    assert all(identity == (os.geteuid(), os.getegid()) for identity in materialization)


def test_failed_candidate_publication_never_exposes_an_unverified_final_path(
    tmp_path: Path,
) -> None:
    source, sha, _tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    candidates.chmod(0o755)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    fake_uv = tmp_path / "uv"
    fake_uv.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        f'ln -s {system_python} "$UV_PROJECT_ENVIRONMENT/bin/python"\n',
        encoding="utf-8",
    )
    fake_uv.chmod(0o700)

    with pytest.raises(broker.BrokerError, match="tree identity"):
        broker.ensure_candidate(
            candidates,
            sha,
            "f" * 40,
            remote_url=str(source),
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
            build_uid=os.geteuid(),
            build_gid=os.getegid(),
            system_python=system_python,
            uv_binary=fake_uv,
        )

    assert not os.path.lexists(candidates / sha)
    assert not tuple(candidates.glob(f".{sha}.*"))


def test_helper_exec_spec_is_fixed_to_candidate_module_and_service_identity(
    tmp_path: Path,
) -> None:
    root, sha, _tree, _source, _system_python = _existing_candidate(tmp_path)

    spec = broker.helper_exec_spec(root / sha, service_uid=995, service_gid=2007)

    assert spec.cwd == root / sha / "repo"
    assert spec.argv == (
        str(root / sha / "venv/bin/python"),
        "-I",
        "-B",
        "-m",
        "loom_cli.rollout.operator.protected_gb10_external_supervisor_transport",
    )
    assert spec.environment["HOME"] == "/var/lib/loom-rollout"
    assert spec.environment["XDG_RUNTIME_DIR"] == "/run/user/995"
    assert spec.environment["DBUS_SESSION_BUS_ADDRESS"] == "unix:path=/run/user/995/bus"
