from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import (
    _artifact,
)

from loom_cli.rollout.gb10_readiness import FULL_GB10_HOSTS
from loom_cli.rollout.operator.protected_gb10_external_supervisor_transport import (
    _encode_helper_request,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BROKER_PATH = REPO_ROOT / "scripts/ops/gb10_external_supervisor_broker.py"
AUTHORITY_PATH = REPO_ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py"
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


def _candidate_toolchain(tmp_path: Path, uv_script: str) -> tuple[Path, Path]:
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    fake_uv = tmp_path / "uv"
    fake_uv.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        f'ln -s {system_python} "$UV_PROJECT_ENVIRONMENT/bin/python"\n'
        f"{uv_script}",
        encoding="utf-8",
    )
    fake_uv.chmod(0o700)
    return system_python, fake_uv


def _existing_candidate(
    tmp_path: Path,
) -> tuple[Path, str, str, Path, Path]:
    source, sha, tree = _source_repo(tmp_path)
    root = tmp_path / "candidates"
    candidate = root / sha
    candidate.mkdir(parents=True)
    _run(
        "git",
        "clone",
        "-q",
        "--no-hardlinks",
        str(source),
        str(candidate / "repo"),
        cwd=tmp_path,
    )
    _run("git", "checkout", "-q", "--detach", sha, cwd=candidate / "repo")
    venv_bin = candidate / "venv/bin"
    venv_bin.mkdir(parents=True)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (venv_bin / "python").symlink_to(system_python)
    root.chmod(0o755)
    untrusted = candidate.with_name(f".{sha}.untrusted")
    candidate.rename(untrusted)
    candidate.mkdir()
    broker._copy_hardened_tree(
        untrusted,
        candidate,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
    )
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

    capacity_request = _encode_helper_request(
        operation="accept_capacity",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        profile_sha256="c" * 64,
        nodes=FULL_GB10_HOSTS,
    )
    assert broker.parse_request_identity(capacity_request.encode()) == (
        artifact.candidate_sha,
        artifact.candidate_tree,
    )
    non_integer_schema = json.loads(capacity_request)
    non_integer_schema["schema_version"] = True
    with pytest.raises(broker.BrokerError, match="fields"):
        broker.parse_request_identity(
            (json.dumps(non_integer_schema, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )


def test_broker_capacity_operation_invokes_only_fixed_installed_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    generated_at = datetime.now(UTC)
    acceptance = {
        "schema_version": 1,
        "kind": "loom_gb10_slurm_acceptance",
        "result": "pass",
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "profile_sha256": "c" * 64,
        "cluster_name": "trt-gb10",
        "controller_host": "gx10-01c7",
        "service_identity": {
            "user": "loom-rollout",
            "uid": 995,
            "gid": 2007,
            "account": "loom-staging",
            "qos": "loom-staging",
        },
        "nodes": list(FULL_GB10_HOSTS),
        "node_count": 15,
        "probed_nodes": list(FULL_GB10_HOSTS),
        "probed_node_count": 15,
        "deferred_busy_nodes": [],
        "trial_cache_registry": {
            "ca_sha256": "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3",
            "canary_digest": "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e",
            "repository": "192.168.50.103:5443/loom-trial-cache",
        },
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=15)).isoformat(),
    }
    request = _encode_helper_request(
        operation="accept_capacity",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        profile_sha256="c" * 64,
        nodes=FULL_GB10_HOSTS,
    ).encode()
    checked_paths: list[Path] = []
    calls: list[tuple[list[str], int]] = []

    def safe_executable(path: Path, **_kwargs: object) -> None:
        checked_paths.append(path)

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, int(kwargs["timeout"])))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(acceptance, sort_keys=True, separators=(",", ":")) + "\n",
            "",
        )

    monkeypatch.setattr(broker, "_safe_executable", safe_executable)
    monkeypatch.setattr(broker, "_run", run)

    response = json.loads(broker.accept_capacity(request))

    assert checked_paths == [broker.ACCEPTANCE_AUTHORITY]
    assert calls == [
        (
            [
                str(broker.ACCEPTANCE_AUTHORITY),
                "--candidate-sha",
                artifact.candidate_sha,
                "--image-tag",
                "staging-aaaaaaa",
            ],
            1200,
        )
    ]
    assert response == {
        "acceptance": acceptance,
        "operation": "accept_capacity",
        "schema_version": 1,
        "status": "ok",
    }


def test_broker_sanitized_environment_resolves_authority_system_executables() -> None:
    script = f"""
import importlib.util
import json

path = {str(REPO_ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py")!r}
spec = importlib.util.spec_from_file_location("gb10_acceptance_composition", path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
runuser = authority._run(authority._service_command("/usr/bin/true"), check=False)
srun = authority._run([authority.SRUN, "--version"])
print(json.dumps({{"runuser": runuser.returncode, "srun": srun.returncode}}))
"""

    result = broker._run([sys.executable, "-c", script], timeout=30)

    assert json.loads(result.stdout) == {"runuser": 1, "srun": 0}


def _detached_authority_command(
    *,
    authority_started: Path,
    detached_started: Path,
    escaped_marker: Path,
    cleanup_evidence: Path,
) -> list[str]:
    detached = (
        "import pathlib,sys,time; "
        "pathlib.Path(sys.argv[1]).write_text('started'); "
        "time.sleep(1.2); pathlib.Path(sys.argv[2]).write_text('escaped')"
    )
    authority = f"""
import importlib.util
import pathlib
import subprocess
import sys
import time

authority_path = pathlib.Path(sys.argv[1])
authority_started, detached_started, escaped_marker, cleanup_evidence = map(
    pathlib.Path, sys.argv[2:]
)
spec = importlib.util.spec_from_file_location("nested_acceptance_authority", authority_path)
if spec is None or spec.loader is None:
    raise SystemExit(4)
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
child = subprocess.Popen(
    [sys.executable, "-c", {detached!r}, str(detached_started), str(escaped_marker)],
    start_new_session=True,
)
authority._ACTIVE_PROCESS = child
authority._install_signal_handlers()
deadline = time.monotonic() + 2
while not detached_started.exists() and time.monotonic() < deadline:
    time.sleep(0.01)
if not detached_started.exists():
    raise SystemExit(3)
authority_started.write_text("started")
try:
    time.sleep(30)
except authority.AuthorityInterruptedError:
    child.wait(timeout=1)
finally:
    authority._ACTIVE_PROCESS = None
    cleanup_evidence.write_text("scancel_job_id=123\\nsqueue_readback=\\n")
"""
    return [
        sys.executable,
        "-c",
        authority,
        str(AUTHORITY_PATH),
        str(authority_started),
        str(detached_started),
        str(escaped_marker),
        str(cleanup_evidence),
    ]


def test_broker_timeout_allows_detached_authority_cleanup_before_reaping(
    tmp_path: Path,
) -> None:
    authority_started = tmp_path / "authority-started"
    detached_started = tmp_path / "detached-started"
    escaped_marker = tmp_path / "escaped-detached-child"
    cleanup_evidence = tmp_path / "cleanup-evidence"

    with pytest.raises(broker.BrokerError, match="failed safely"):
        broker._run(
            _detached_authority_command(
                authority_started=authority_started,
                detached_started=detached_started,
                escaped_marker=escaped_marker,
                cleanup_evidence=cleanup_evidence,
            ),
            timeout=1,
        )

    time.sleep(0.5)
    assert authority_started.exists()
    assert cleanup_evidence.read_text(encoding="utf-8") == (
        "scancel_job_id=123\nsqueue_readback=\n"
    )
    assert not escaped_marker.exists()


def test_broker_timeout_kills_and_reaps_the_authority_process_group(tmp_path: Path) -> None:
    orphan_marker = tmp_path / "escaped-authority-child"
    child = (
        "import pathlib,sys,time; time.sleep(1.2); pathlib.Path(sys.argv[1]).write_text('escaped')"
    )
    parent = (
        "import signal,subprocess,sys,time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"subprocess.Popen([sys.executable, '-c', {child!r}, sys.argv[1]]); "
        "time.sleep(5)"
    )

    started = time.monotonic()
    with pytest.raises(broker.BrokerError, match="failed safely"):
        broker._run(
            [sys.executable, "-c", parent, str(orphan_marker)],
            timeout=1,
        )
    assert time.monotonic() - started < 2
    time.sleep(0.4)
    assert not orphan_marker.exists()


@pytest.mark.parametrize(
    "broker_signal",
    (signal.SIGTERM, signal.SIGINT),
    ids=("sigterm", "sigint"),
)
def test_broker_termination_kills_and_reaps_the_authority_process_group(
    tmp_path: Path,
    broker_signal: signal.Signals,
) -> None:
    authority_started = tmp_path / "authority-started"
    detached_started = tmp_path / "detached-started"
    escaped_marker = tmp_path / "escaped-detached-child"
    cleanup_evidence = tmp_path / "cleanup-evidence"
    authority_command = _detached_authority_command(
        authority_started=authority_started,
        detached_started=detached_started,
        escaped_marker=escaped_marker,
        cleanup_evidence=cleanup_evidence,
    )
    script = f"""
import importlib.util
import sys

broker_path = sys.argv[1]
spec = importlib.util.spec_from_file_location("signal_broker", broker_path)
assert spec is not None and spec.loader is not None
broker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = broker
spec.loader.exec_module(broker)
broker._install_signal_handlers()
try:
    broker._run({authority_command!r}, timeout=30)
except broker.BrokerError as exc:
    if "interrupted safely" not in str(exc):
        raise
    raise SystemExit(42)
raise SystemExit(3)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(BROKER_PATH),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while (
            not authority_started.exists()
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert authority_started.exists(), process.communicate(timeout=1)
        process.send_signal(broker_signal)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

    assert process.returncode == 42, (stdout, stderr)
    time.sleep(0.5)
    assert cleanup_evidence.read_text(encoding="utf-8") == (
        "scancel_job_id=123\nsqueue_readback=\n"
    )
    assert not escaped_marker.exists()


def test_broker_main_dispatches_capacity_without_candidate_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    request = _encode_helper_request(
        operation="accept_capacity",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        profile_sha256="c" * 64,
        nodes=FULL_GB10_HOSTS,
    ).encode()
    response = b'{"operation":"accept_capacity","schema_version":1,"status":"ok"}\n'
    output = SimpleNamespace(buffer=io.BytesIO())
    calls: list[bytes] = []
    candidate_runtime_calls: list[str] = []
    monkeypatch.setattr(broker.os, "geteuid", lambda: 0)
    monkeypatch.setattr(broker.os, "getegid", lambda: 0)
    monkeypatch.setattr(broker.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(request)))
    monkeypatch.setattr(broker.sys, "stdout", output)
    monkeypatch.setattr(broker, "_require_host_authority", lambda: None)
    monkeypatch.setattr(
        broker,
        "_safe_executable",
        lambda *_args, **_kwargs: candidate_runtime_calls.append("validate toolchain"),
    )
    monkeypatch.setattr(
        broker,
        "ensure_candidate",
        lambda *_args, **_kwargs: (
            candidate_runtime_calls.append("ensure candidate"),
            Path("/opt/loom-staging-runner/candidates") / artifact.candidate_sha,
        )[1],
    )
    monkeypatch.setattr(
        broker,
        "accept_capacity",
        lambda payload: (calls.append(payload), response)[1],
    )
    monkeypatch.setattr(
        broker,
        "_exec_helper",
        lambda *_args: pytest.fail("capacity operation reached candidate helper"),
    )

    assert broker.main([]) == 0
    assert calls == [request]
    assert candidate_runtime_calls == []
    assert output.buffer.getvalue() == response


def test_broker_main_dispatches_non_capacity_through_candidate_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    request = _encode_helper_request(
        operation="observe",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        artifact=artifact,
    ).encode()
    candidate = Path("/opt/loom-staging-runner/candidates") / artifact.candidate_sha
    checked_paths: list[Path] = []
    ensured: list[tuple[Path, str, str]] = []
    executed: list[tuple[Path, bytes]] = []

    monkeypatch.setattr(broker.os, "geteuid", lambda: 0)
    monkeypatch.setattr(broker.os, "getegid", lambda: 0)
    monkeypatch.setattr(broker.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(request)))
    monkeypatch.setattr(broker, "_require_host_authority", lambda: None)
    monkeypatch.setattr(
        broker,
        "_safe_executable",
        lambda path, **_kwargs: checked_paths.append(path),
    )
    monkeypatch.setattr(
        broker,
        "ensure_candidate",
        lambda root, sha, tree: (ensured.append((root, sha, tree)), candidate)[1],
    )

    class HelperExecReachedError(RuntimeError):
        pass

    def exec_helper(candidate_path: Path, payload: bytes) -> None:
        executed.append((candidate_path, payload))
        raise HelperExecReachedError

    monkeypatch.setattr(broker, "_exec_helper", exec_helper)
    monkeypatch.setattr(
        broker,
        "accept_capacity",
        lambda _payload: pytest.fail("non-capacity operation reached acceptance authority"),
    )

    with pytest.raises(HelperExecReachedError):
        broker.main([])

    assert checked_paths == [broker.UV_BINARY, broker.SYSTEM_PYTHON]
    assert ensured == [(broker.CANDIDATES_ROOT, artifact.candidate_sha, artifact.candidate_tree)]
    assert executed == [(candidate, request)]


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
        inspection_uid=os.geteuid(),
        inspection_gid=os.getegid(),
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
        inspection_uid=os.geteuid(),
        inspection_gid=os.getegid(),
        system_python=system_python,
    )


def test_candidate_tree_accepts_standard_venv_interpreter_symlink_chain(
    tmp_path: Path,
) -> None:
    root = tmp_path / "candidate"
    venv_bin = root / "venv/bin"
    venv_bin.mkdir(parents=True)
    root.chmod(0o755)
    (root / "venv").chmod(0o755)
    venv_bin.chmod(0o755)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (venv_bin / "python").symlink_to(system_python)
    (venv_bin / "python3").symlink_to("python")
    (venv_bin / "python3.12").symlink_to("python")

    broker._safe_tree(
        root,
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        system_python=system_python,
    )


def test_candidate_tree_rejects_direct_relative_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    link_dir = root / "bin"
    link_dir.mkdir(parents=True)
    root.chmod(0o755)
    link_dir.chmod(0o755)
    outside = tmp_path / "outside"
    outside.write_text("outside\n", encoding="utf-8")
    (link_dir / "escaped").symlink_to("../../outside")

    with pytest.raises(broker.BrokerError, match="escapes authority"):
        broker._safe_tree(
            root,
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
            system_python=outside,
        )


def test_candidate_tree_rejects_relative_symlink_chain_escape(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    link_dir = root / "bin"
    link_dir.mkdir(parents=True)
    root.chmod(0o755)
    link_dir.chmod(0o755)
    outside = tmp_path / "outside"
    outside.write_text("outside\n", encoding="utf-8")
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (link_dir / "a").symlink_to("..")
    (link_dir / "escaped").symlink_to("a/../outside")

    with pytest.raises(broker.BrokerError, match="escapes authority"):
        broker._safe_tree(
            root,
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
            system_python=system_python,
        )


def test_candidate_tree_rejects_relative_symlink_escape_that_reenters_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "candidate"
    link_dir = root / "bin"
    link_dir.mkdir(parents=True)
    root.chmod(0o755)
    link_dir.chmod(0o755)
    safe = root / "safe"
    safe.write_text("safe\n", encoding="utf-8")
    safe.chmod(0o644)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (link_dir / "outside").symlink_to("../..")
    (link_dir / "reentered").symlink_to("outside/candidate/safe")

    with pytest.raises(broker.BrokerError, match="escapes authority"):
        broker._safe_tree(
            root,
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
            system_python=system_python,
        )


def test_candidate_inspection_disables_candidate_configured_executable_git_hooks(
    tmp_path: Path,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    root = tmp_path / "candidates"
    candidate = root / sha
    candidate.mkdir(parents=True)
    repo = candidate / "repo"
    _run("git", "clone", "-q", "--no-hardlinks", str(source), str(repo), cwd=tmp_path)
    _run("git", "checkout", "-q", "--detach", sha, cwd=repo)
    marker = tmp_path / "candidate-git-hook-ran"
    fsmonitor = candidate / "candidate-fsmonitor"
    fsmonitor.write_text(
        f"#!/bin/sh\ntouch {marker}\nprintf '\\n'\n",
        encoding="utf-8",
    )
    fsmonitor.chmod(0o755)
    _run("git", "config", "core.fsmonitor", str(fsmonitor), cwd=repo)
    venv_bin = candidate / "venv/bin"
    venv_bin.mkdir(parents=True)
    system_python = tmp_path / "system-python"
    system_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    system_python.chmod(0o755)
    (venv_bin / "python").symlink_to(system_python)
    root.chmod(0o755)
    untrusted = candidate.with_name("candidate-untrusted")
    candidate.rename(untrusted)
    candidate.mkdir()
    broker._copy_hardened_tree(
        untrusted,
        candidate,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
    )

    ready = broker.candidate_ready(
        root,
        sha,
        tree,
        remote_url=str(source),
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
        inspection_uid=os.geteuid(),
        inspection_gid=os.getegid(),
        system_python=system_python,
    )

    assert ready is True
    assert not marker.exists()


def test_candidate_hardening_rejects_external_hardlinks_before_mutation(
    tmp_path: Path,
) -> None:
    external = tmp_path / "service-owned-state"
    external.write_text("preserve\n", encoding="utf-8")
    external.chmod(0o600)
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    os.link(external, candidate / "linked-state")
    published = tmp_path / "published"
    published.mkdir()
    before = external.stat()

    with pytest.raises(broker.BrokerError, match="hardlink"):
        broker._copy_hardened_tree(
            candidate,
            published,
            source_uid=os.geteuid(),
            source_gid=os.getegid(),
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
        )

    after = external.stat()
    assert after.st_mode == before.st_mode
    assert after.st_uid == before.st_uid
    assert after.st_gid == before.st_gid
    assert external.read_text(encoding="utf-8") == "preserve\n"


def test_candidate_hardening_rejects_external_hardlinked_symlink_before_mutation(
    tmp_path: Path,
) -> None:
    external = tmp_path / "service-owned-link"
    external.symlink_to("preserve-target")
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    os.link(external, candidate / "linked-symlink", follow_symlinks=False)
    published = tmp_path / "published"
    published.mkdir()
    before = os.lstat(external)

    with pytest.raises(broker.BrokerError, match="hardlink"):
        broker._copy_hardened_tree(
            candidate,
            published,
            source_uid=os.geteuid(),
            source_gid=os.getegid(),
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
        )

    after = os.lstat(external)
    assert after.st_mode == before.st_mode
    assert after.st_uid == before.st_uid
    assert after.st_gid == before.st_gid
    assert after.st_nlink == before.st_nlink
    assert os.readlink(external) == "preserve-target"


def test_absent_candidate_is_published_atomically_and_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    uv_timeouts: list[int] = []
    original_run = broker._run

    def recording_run(argv: list[str], **kwargs):
        if argv[0] == str(fake_uv):
            uv_timeouts.append(kwargs.get("timeout", 900))
        return original_run(argv, **kwargs)

    monkeypatch.setattr(broker, "_run", recording_run)

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
        inspection_uid=os.geteuid(),
        inspection_gid=os.getegid(),
        system_python=system_python,
    )
    assert not tuple(candidates.glob(f".{sha}.*"))
    candidate_mode = (candidates / sha).stat().st_mode
    assert candidate_mode & 0o055 == 0o055
    assert (candidates / sha / "repo/payload.txt").stat().st_mode & 0o044 == 0o044
    assert uv_timeouts == [1200]


def test_candidate_publication_never_mutates_a_raced_external_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    external = tmp_path / "service-owned-state"
    external.write_text("external state\n", encoding="utf-8")
    external.chmod(0o600)
    before = external.stat()
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
    original_open = broker.os.open
    attacked = False

    def swap_source_before_descriptor_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        if not attacked and path == "payload.txt" and dir_fd is not None:
            attacked = True
            os.unlink(path, dir_fd=dir_fd)
            os.link(external, path, dst_dir_fd=dir_fd)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(broker.os, "open", swap_source_before_descriptor_open)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    after = external.stat()
    assert attacked
    assert not (candidates / sha).exists()
    assert after.st_mode == before.st_mode
    assert after.st_uid == before.st_uid
    assert after.st_gid == before.st_gid
    assert external.read_text(encoding="utf-8") == "external state\n"


def test_candidate_publication_rejects_same_owner_file_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    module_name = "protected_gb10_external_supervisor_transport.py"
    system_python, fake_uv = _candidate_toolchain(
        tmp_path,
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/loom_cli/rollout"\n'
        f'printf "trusted\\n" > "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/'
        f'loom_cli/rollout/{module_name}"\n',
    )
    replacement = tmp_path / "replacement-module.py"
    replacement.write_text("malicious\n", encoding="utf-8")
    original_open = broker.os.open
    attacked = False

    def replace_file_before_descriptor_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        if not attacked and path == module_name and dir_fd is not None:
            attacked = True
            os.replace(replacement, path, dst_dir_fd=dir_fd)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(broker.os, "open", replace_file_before_descriptor_open)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    assert attacked
    assert not (candidates / sha).exists()


def test_candidate_publication_rejects_same_owner_directory_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    system_python, fake_uv = _candidate_toolchain(
        tmp_path,
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/loom_cli/rollout"\n'
        'printf "trusted\\n" > "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/'
        'loom_cli/rollout/helper.py"\n',
    )
    replacement = tmp_path / "replacement-rollout"
    replacement.mkdir()
    (replacement / "helper.py").write_text("malicious\n", encoding="utf-8")
    original_open = broker.os.open
    attacked = False

    def replace_directory_before_descriptor_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        if (
            not attacked
            and path == "rollout"
            and dir_fd is not None
            and flags & getattr(os, "O_DIRECTORY", 0)
        ):
            attacked = True
            os.rename(path, "trusted-rollout", src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            os.rename(replacement, path, dst_dir_fd=dir_fd)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(broker.os, "open", replace_directory_before_descriptor_open)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    assert attacked
    assert not (candidates / sha).exists()


def test_candidate_publication_rejects_same_owner_symlink_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    system_python, fake_uv = _candidate_toolchain(
        tmp_path,
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/lib"\n'
        'ln -s trusted-target "$UV_PROJECT_ENVIRONMENT/lib/selected"\n',
    )
    replacement = tmp_path / "replacement-link"
    replacement.symlink_to("malicious-target")
    original_open = broker.os.open
    attacked = False

    def replace_symlink_before_descriptor_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        if not attacked and path == "selected" and dir_fd is not None:
            attacked = True
            os.replace(replacement, path, dst_dir_fd=dir_fd)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(broker.os, "open", replace_symlink_before_descriptor_open)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    assert attacked
    assert not (candidates / sha).exists()


def test_candidate_publication_rejects_regular_file_mutation_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    file_name = "large-runtime.bin"
    system_python, fake_uv = _candidate_toolchain(
        tmp_path,
        f'head -c 2097152 /dev/zero > "$UV_PROJECT_ENVIRONMENT/{file_name}"\n',
    )
    original_read = broker.os.read
    original_readlink = broker.os.readlink
    attacked = False

    def mutate_source_after_first_chunk(descriptor: int, size: int) -> bytes:
        nonlocal attacked
        chunk = original_read(descriptor, size)
        if not attacked and chunk:
            source_path = Path(original_readlink(f"/proc/self/fd/{descriptor}"))
            if source_path.name == file_name:
                attacked = True
                with source_path.open("r+b") as stream:
                    stream.seek(1024 * 1024)
                    stream.write(b"malicious")
                    stream.flush()
                    os.fsync(stream.fileno())
        return chunk

    monkeypatch.setattr(broker.os, "read", mutate_source_after_first_chunk)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    assert attacked
    assert not (candidates / sha).exists()


def test_candidate_publication_rejects_directory_mutation_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, sha, tree = _source_repo(tmp_path)
    candidates = tmp_path / "candidates"
    candidates.mkdir(mode=0o755)
    system_python, fake_uv = _candidate_toolchain(
        tmp_path,
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/loom_cli/rollout"\n'
        'printf "trusted\\n" > "$UV_PROJECT_ENVIRONMENT/lib/python3.11/site-packages/'
        'loom_cli/rollout/helper.py"\n',
    )
    original_listdir = broker.os.listdir
    original_readlink = broker.os.readlink
    attacked = False

    def mutate_directory_after_listing(path: str | bytes | Path | int) -> list[str]:
        nonlocal attacked
        names = original_listdir(path)
        if not attacked and isinstance(path, int):
            source_path = Path(original_readlink(f"/proc/self/fd/{path}"))
            if source_path.name == "rollout":
                attacked = True
                (source_path / "late-module.py").write_text("malicious\n", encoding="utf-8")
        return names

    monkeypatch.setattr(broker.os, "listdir", mutate_directory_after_listing)

    with pytest.raises(broker.BrokerError, match="changed during publication"):
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

    assert attacked
    assert not (candidates / sha).exists()


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
