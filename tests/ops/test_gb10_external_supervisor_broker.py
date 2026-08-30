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
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import (
    _artifact,
)

from loom_cli.rollout.operator.protected_gb10_external_supervisor_transport import (
    _encode_helper_request,
)

NORMAL_GB10_WORKER_HOSTS = (
    "trt-gb10-1",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BROKER_PATH = REPO_ROOT / "scripts/ops/gb10_external_supervisor_broker.py"
AUTHORITY_PATH = REPO_ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py"
SPEC = importlib.util.spec_from_file_location("gb10_external_supervisor_broker", BROKER_PATH)
assert SPEC is not None and SPEC.loader is not None
broker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = broker
SPEC.loader.exec_module(broker)
_TEST_REQUEST_NONCE = "0123456789abcdef01234567"
_TEST_UNIT_NAME = f"loom-gb10-capacity-{_TEST_REQUEST_NONCE}.service"
_TEST_JOB_NAME = f"loom-accept-abcdef0-1-{_TEST_REQUEST_NONCE}"


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
        nodes=NORMAL_GB10_WORKER_HOSTS,
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
        "nodes": list(NORMAL_GB10_WORKER_HOSTS),
        "node_count": 14,
        "probed_nodes": list(NORMAL_GB10_WORKER_HOSTS),
        "probed_node_count": 14,
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
        nodes=NORMAL_GB10_WORKER_HOSTS,
    ).encode()
    checked_paths: list[Path] = []
    calls: list[dict[str, object]] = []

    def safe_executable(path: Path, **_kwargs: object) -> None:
        checked_paths.append(path)

    def run_contained(**kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(kwargs)
        return subprocess.CompletedProcess(
            ["contained-authority"],
            0,
            json.dumps(acceptance, sort_keys=True, separators=(",", ":")) + "\n",
            "",
        )

    monkeypatch.setattr(broker, "_safe_executable", safe_executable)
    monkeypatch.setattr(broker, "_acceptance_lock", lambda: nullcontext())
    monkeypatch.setattr(broker, "_reconcile_stale_job_states", lambda **_kwargs: None)
    monkeypatch.setattr(broker.secrets, "token_hex", lambda _size: "1" * 24)
    monkeypatch.setattr(broker, "_run_contained_authority", run_contained)

    response = json.loads(broker.accept_capacity(request))

    assert checked_paths == [broker.ACCEPTANCE_AUTHORITY, broker.SYSTEM_PYTHON]
    assert calls == [
        {
            "candidate_sha": artifact.candidate_sha,
            "unit_name": "loom-gb10-capacity-111111111111111111111111.service",
            "job_state_path": (
                broker.ACCEPTANCE_JOB_STATE_ROOT
                / "loom-gb10-capacity-111111111111111111111111.service.json"
            ),
            "cgroup_path": (
                broker.CGROUP_ROOT
                / "system.slice/loom-gb10-capacity-111111111111111111111111.service"
            ),
            "timeout": 1200,
        }
    ]
    assert response == {
        "acceptance": acceptance,
        "operation": "accept_capacity",
        "schema_version": 1,
        "status": "ok",
    }


def test_broker_sanitized_environment_composes_fixed_authority_executables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_systemd_run = tmp_path / "systemd-run"
    fake_systemd_run.write_text(
        '#!/bin/sh\nset -eu\nwhile [ "$1" != -- ]; do shift; done\nshift\nexec "$@"\n',
        encoding="utf-8",
    )
    fake_systemd_run.chmod(0o755)
    wrapper = tmp_path / "authority-wrapper.py"
    script = f"""
import importlib.util
import json
import sys

path = {str(REPO_ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py")!r}
spec = importlib.util.spec_from_file_location("gb10_acceptance_composition", path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
runuser = authority._run(authority._service_command("/usr/bin/true"), check=False)
srun = authority._service_command(authority.SRUN, "--version")
print(json.dumps({{
    "docker_fixed": '["/usr/bin/docker", "info"]' in authority._NODE_PROBE,
    "runuser": runuser.returncode,
    "srun": srun,
    "system_python": authority.SYSTEM_PYTHON,
}}))
"""
    wrapper.write_text(script, encoding="utf-8")
    wrapper.chmod(0o755)
    job_state = tmp_path / "active-job.json"
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    intercepted = tmp_path / "intercepted"
    for name in ("python3", "docker", "srun", "runuser"):
        executable = attacker / name
        executable.write_text(
            f"#!/bin/sh\nprintf '%s' {name} > {intercepted}\nexit 77\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
    monkeypatch.setattr(broker, "SYSTEMD_RUN", fake_systemd_run)
    monkeypatch.setattr(broker, "ACCEPTANCE_AUTHORITY", wrapper)

    argv = broker._contained_authority_argv(
        unit_name=_TEST_UNIT_NAME,
        candidate_sha="a" * 40,
        job_state_path=job_state,
    )
    result = broker._run(
        argv,
        timeout=30,
        environment={
            "HOME": str(tmp_path),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": str(attacker),
        },
    )

    assert json.loads(result.stdout) == {
        "docker_fixed": True,
        "runuser": 1,
        "srun": ["/usr/sbin/runuser", "-u", "loom-rollout", "--", "/usr/bin/srun", "--version"],
        "system_python": "/usr/bin/python3",
    }
    assert not intercepted.exists()


def test_capacity_authority_uses_fixed_systemd_service_boundary(tmp_path: Path) -> None:
    job_state = tmp_path / "loom-gb10-capacity-test.job.json"

    argv = broker._contained_authority_argv(
        unit_name=_TEST_UNIT_NAME,
        candidate_sha="a" * 40,
        job_state_path=job_state,
    )

    assert argv[0] == "/usr/bin/systemd-run"
    assert f"--unit={_TEST_UNIT_NAME}" in argv
    assert "--property=KillMode=control-group" in argv
    assert "--property=Delegate=no" in argv
    assert "--property=TimeoutStopSec=45s" in argv
    assert "--property=RuntimeMaxSec=1140s" in argv
    command_index = argv.index("--") + 1
    assert argv[command_index : command_index + 2] == [
        "/usr/bin/python3",
        str(broker.ACCEPTANCE_AUTHORITY),
    ]
    assert argv[-2:] == ["--job-state-path", str(job_state)]


def _contained_process_tree(
    tmp_path: Path, *, ignore_term: bool
) -> tuple[subprocess.Popen[str], int]:
    child_pid_path = tmp_path / "child.pid"
    child = (
        "import signal,time; "
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
        + "time.sleep(30)"
    )
    parent = (
        "import pathlib,signal,subprocess,sys,time; "
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
        + f"child=subprocess.Popen([sys.executable, '-c', {child!r}], start_new_session=True); "
        + "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(30)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", parent, str(child_pid_path)],
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 5
    while not child_pid_path.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert child_pid_path.exists(), process.communicate(timeout=1)
    return process, int(child_pid_path.read_text(encoding="utf-8"))


def _pid_is_live(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        if Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()[2] == "Z":
            return False
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return False
    return True


def test_pid_liveness_treats_procfs_esrch_as_process_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def process_disappeared(*_args: object, **_kwargs: object) -> str:
        raise ProcessLookupError(3, "No such process")

    monkeypatch.setattr(Path, "read_text", process_disappeared)

    assert not _pid_is_live(os.getpid())


def test_cgroup_population_proof_includes_descendant_cgroups(tmp_path: Path) -> None:
    cgroup = tmp_path / "system.slice" / _TEST_UNIT_NAME
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.procs").write_text("", encoding="ascii")
    events = cgroup / "cgroup.events"
    events.write_text("populated 1\nfrozen 0\n", encoding="ascii")

    assert not broker._cgroup_is_empty(cgroup)

    events.write_text("populated 0\nfrozen 0\n", encoding="ascii")
    assert broker._cgroup_is_empty(cgroup)


@pytest.mark.parametrize(
    "encoded",
    (
        "",
        "frozen 0\n",
        "populated 0\npopulated 0\n",
        "populated 2\n",
        "populated\n",
    ),
)
def test_cgroup_population_proof_rejects_missing_duplicate_or_malformed_evidence(
    tmp_path: Path,
    encoded: str,
) -> None:
    cgroup = tmp_path / "system.slice" / _TEST_UNIT_NAME
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.events").write_text(encoded, encoding="ascii")

    with pytest.raises(broker.BrokerError, match="containment evidence is unsafe"):
        broker._cgroup_is_empty(cgroup)


def test_containment_grace_expiry_force_kills_every_cgroup_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process, child_pid = _contained_process_tree(tmp_path, ignore_term=True)
    cgroup = tmp_path / "system.slice" / _TEST_UNIT_NAME
    cgroup.mkdir(parents=True)
    procs = cgroup / "cgroup.procs"
    procs.write_text(f"{process.pid}\n{child_pid}\n", encoding="ascii")
    events = cgroup / "cgroup.events"
    events.write_text("populated 1\n", encoding="ascii")
    commands: list[tuple[str, ...]] = []

    def systemctl(*arguments: str, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(arguments)
        requested_signal = next(
            (item.removeprefix("--signal=") for item in arguments if item.startswith("--signal=")),
            None,
        )
        if requested_signal == "SIGTERM":
            for pid in (process.pid, child_pid):
                os.kill(pid, signal.SIGTERM)
        elif requested_signal == "SIGKILL":
            for pid in (process.pid, child_pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 1.0
            while any(_pid_is_live(pid) for pid in (process.pid, child_pid)):
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.001)
            if not any(_pid_is_live(pid) for pid in (process.pid, child_pid)):
                procs.write_text("", encoding="ascii")
                events.write_text("populated 0\n", encoding="ascii")
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(broker, "_systemctl", systemctl)
    try:
        broker._terminate_and_verify_containment(
            unit_name=_TEST_UNIT_NAME,
            cgroup_path=cgroup,
            job_state_path=tmp_path / "missing.job.json",
            graceful_timeout=0.05,
            forced_timeout=1.0,
        )
        process.wait(timeout=2)
        assert not _pid_is_live(child_pid)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if _pid_is_live(child_pid):
            os.kill(child_pid, signal.SIGKILL)

    assert any("--signal=SIGTERM" in command for command in commands)
    assert any("--signal=SIGKILL" in command for command in commands)


def test_containment_kills_escaped_setsid_descendant_before_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process, child_pid = _contained_process_tree(tmp_path, ignore_term=False)
    cgroup = tmp_path / "system.slice" / _TEST_UNIT_NAME
    cgroup.mkdir(parents=True)
    procs = cgroup / "cgroup.procs"
    procs.write_text(f"{process.pid}\n{child_pid}\n", encoding="ascii")
    events = cgroup / "cgroup.events"
    events.write_text("populated 1\n", encoding="ascii")

    def systemctl(*arguments: str, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "--signal=SIGTERM" in arguments:
            for pid in (process.pid, child_pid):
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 1
            while (
                _pid_is_live(process.pid) or _pid_is_live(child_pid)
            ) and time.monotonic() < deadline:
                time.sleep(0.01)
            procs.write_text("", encoding="ascii")
            events.write_text("populated 0\n", encoding="ascii")
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(broker, "_systemctl", systemctl)
    try:
        broker._terminate_and_verify_containment(
            unit_name=_TEST_UNIT_NAME,
            cgroup_path=cgroup,
            job_state_path=tmp_path / "missing.job.json",
            graceful_timeout=1.0,
            forced_timeout=1.0,
        )
        process.wait(timeout=2)
        assert not _pid_is_live(child_pid)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if _pid_is_live(child_pid):
            os.kill(child_pid, signal.SIGKILL)


def test_fake_systemd_boundary_reaps_setsid_descendant_host_hermetically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unit_name = _TEST_UNIT_NAME
    cgroup_root = tmp_path / "cgroup"
    cgroup = cgroup_root / "system.slice" / unit_name
    cgroup.mkdir(parents=True)
    child_pid_path = tmp_path / "child.pid"
    escaped_marker = tmp_path / "escaped"
    systemctl_log = tmp_path / "systemctl.log"
    fake_authority = tmp_path / "authority.py"
    fake_authority.write_text(
        "import pathlib,signal,subprocess,sys,time\n"
        f"cgroup=pathlib.Path({str(cgroup)!r})\n"
        f"pid_path=pathlib.Path({str(child_pid_path)!r})\n"
        f"marker=pathlib.Path({str(escaped_marker)!r})\n"
        'descendant=("import pathlib,signal,sys,time; "\n'
        '            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "\n'
        "            \"time.sleep(1.2); pathlib.Path(sys.argv[1]).write_text('escaped')\")\n"
        "child=subprocess.Popen([sys.executable,'-c',descendant,str(marker)],start_new_session=True)\n"
        "pid_path.write_text(str(child.pid))\n"
        "(cgroup/'cgroup.procs').write_text(f'{__import__(\"os\").getpid()}\\n{child.pid}\\n')\n"
        "(cgroup/'cgroup.events').write_text('populated 1\\n')\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    fake_authority.chmod(0o755)
    fake_systemd_run = tmp_path / "systemd-run"
    fake_systemd_run.write_text(
        "#!/usr/bin/python3\n"
        "import subprocess,sys\n"
        "arguments=sys.argv[1:]\n"
        "command=arguments[arguments.index('--')+1:]\n"
        "raise SystemExit(subprocess.call(command))\n",
        encoding="utf-8",
    )
    fake_systemd_run.chmod(0o755)
    fake_systemctl = tmp_path / "systemctl"
    fake_systemctl.write_text(
        "#!/usr/bin/python3\n"
        "import os,pathlib,signal,sys\n"
        f"root=pathlib.Path({str(cgroup_root)!r})\n"
        f"log=pathlib.Path({str(systemctl_log)!r})\n"
        "arguments=sys.argv[1:]\n"
        "with log.open('a') as stream: stream.write(' '.join(arguments)+'\\n')\n"
        "if arguments and arguments[0]=='kill':\n"
        " unit=arguments[-1]\n"
        " requested=next(item.split('=',1)[1] for item in arguments if item.startswith('--signal='))\n"
        " signum=getattr(signal,requested)\n"
        " procs=root/'system.slice'/unit/'cgroup.procs'\n"
        " events=root/'system.slice'/unit/'cgroup.events'\n"
        " for raw in procs.read_text().splitlines():\n"
        "  try: os.kill(int(raw),signum)\n"
        "  except ProcessLookupError: pass\n"
        " if signum==signal.SIGKILL:\n"
        "  procs.write_text('')\n"
        "  events.write_text('populated 0\\n')\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    (cgroup / "cgroup.procs").write_text("", encoding="ascii")
    (cgroup / "cgroup.events").write_text("populated 0\n", encoding="ascii")
    monkeypatch.setattr(broker, "SYSTEMD_RUN", fake_systemd_run)
    monkeypatch.setattr(broker, "SYSTEMCTL", fake_systemctl)
    monkeypatch.setattr(broker, "ACCEPTANCE_AUTHORITY", fake_authority)
    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    real_run = broker._run

    def run_with_short_fixture_timeout(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return real_run(argv, timeout=1.0)

    monkeypatch.setattr(broker, "_run", run_with_short_fixture_timeout)

    try:
        with pytest.raises(broker.BrokerError, match="failed safely"):
            broker._run_contained_authority(
                candidate_sha="a" * 40,
                unit_name=unit_name,
                job_state_path=tmp_path / "missing.job.json",
                cgroup_path=cgroup,
                timeout=120,
                graceful_timeout=0.05,
                forced_timeout=1.0,
            )
        time.sleep(1.3)
        assert not escaped_marker.exists()
        assert child_pid_path.exists()
        assert not _pid_is_live(int(child_pid_path.read_text(encoding="ascii")))
    finally:
        if child_pid_path.exists():
            child_pid = int(child_pid_path.read_text(encoding="ascii"))
            if _pid_is_live(child_pid):
                os.kill(child_pid, signal.SIGKILL)

    commands = systemctl_log.read_text(encoding="utf-8").splitlines()
    assert any("--signal=SIGTERM" in command for command in commands)
    assert any("--signal=SIGKILL" in command for command in commands)


def _write_job_state(path: Path, *, job_name: str, job_id: str | None = None) -> None:
    payload = {"job_name": job_name, "schema_version": 1}
    if job_id is not None:
        payload["job_id"] = job_id
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_broker_timeout_cleans_exact_persisted_job_and_reads_back_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    cgroup = tmp_path / "system.slice" / _TEST_UNIT_NAME
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.procs").write_text("", encoding="ascii")
    (cgroup / "cgroup.events").write_text("populated 0\n", encoding="ascii")
    cleanup_commands: list[list[str]] = []

    def fail_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert json.loads(state_path.read_text(encoding="utf-8")) == {
            "schema_version": 1,
            "unit_name": _TEST_UNIT_NAME,
        }
        _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
        raise broker.BrokerError("GB10 external supervisor command failed safely")

    def cleanup_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        cleanup_commands.append(argv)
        if "/usr/bin/scancel" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run", fail_run)
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup_run)
    monkeypatch.setattr(
        broker,
        "_systemctl",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    with pytest.raises(broker.BrokerError, match="failed safely"):
        broker._run_contained_authority(
            candidate_sha="a" * 40,
            unit_name=_TEST_UNIT_NAME,
            job_state_path=state_path,
            cgroup_path=cgroup,
            timeout=1,
        )

    assert [command[4:] for command in cleanup_commands] == [
        ["/usr/bin/scancel", "123"],
        [
            "/usr/bin/squeue",
            "--noheader",
            "--user=loom-rollout",
            f"--name={_TEST_JOB_NAME}",
            "--format=%A|%j",
        ],
        [
            "/usr/bin/squeue",
            "--noheader",
            "--user=loom-rollout",
            f"--name={_TEST_JOB_NAME}",
            "--format=%A|%j",
        ],
    ]
    assert not state_path.exists()


def test_contained_authority_reserves_cleanup_inside_absolute_hard_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    launch_timeouts: list[float] = []
    unit_name = _TEST_UNIT_NAME
    cgroup = tmp_path / "system.slice" / unit_name
    cgroup.mkdir(parents=True)
    procs = cgroup / "cgroup.procs"
    procs.write_text("999999\n", encoding="ascii")
    events = cgroup / "cgroup.events"
    events.write_text("populated 1\n", encoding="ascii")
    state_path = tmp_path / "active-job.json"

    def run(*_args: object, timeout: float, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        launch_timeouts.append(timeout)
        clock[0] += timeout
        _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
        raise broker.BrokerError("GB10 external supervisor command failed safely")

    def systemctl(
        *arguments: str, timeout: float, **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        clock[0] += timeout
        if "--signal=SIGKILL" in arguments:
            procs.write_text("", encoding="ascii")
            events.write_text("populated 0\n", encoding="ascii")
        return subprocess.CompletedProcess(arguments, 0, "", "")

    wait_calls = 0

    def wait_for_empty(_path: Path, *, timeout: float) -> bool:
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            clock[0] += timeout
            return False
        return True

    def cleanup(
        argv: list[str], *, timeout: float, deadline: float, **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del timeout
        clock[0] += min(0.01, deadline - clock[0])
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(broker, "_run", run)
    monkeypatch.setattr(broker, "_systemctl", systemctl)
    monkeypatch.setattr(broker, "_wait_for_empty_cgroup", wait_for_empty)
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)

    with pytest.raises(broker.BrokerError, match="failed safely"):
        broker._run_contained_authority(
            candidate_sha="a" * 40,
            unit_name=unit_name,
            job_state_path=state_path,
            cgroup_path=cgroup,
            timeout=1200,
        )

    assert launch_timeouts == [1110.0]
    assert clock[0] <= 1200.0
    assert not state_path.exists()


def test_systemctl_failure_does_not_suppress_exact_scheduler_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unit_name = _TEST_UNIT_NAME
    cgroup = tmp_path / "system.slice" / unit_name
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.procs").write_text("999999\n", encoding="ascii")
    (cgroup / "cgroup.events").write_text("populated 1\n", encoding="ascii")
    state_path = tmp_path / "active-job.json"
    _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
    scheduler_commands: list[list[str]] = []

    def cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        scheduler_commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(
        broker,
        "_systemctl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            broker.BrokerError("systemctl failed safely")
        ),
    )
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)

    with pytest.raises(broker.BrokerError, match="systemctl failed safely"):
        broker._terminate_and_verify_containment(
            unit_name=unit_name,
            cgroup_path=cgroup,
            job_state_path=state_path,
            graceful_timeout=0.0,
            forced_timeout=0.0,
        )

    assert [command[4] for command in scheduler_commands] == [
        "/usr/bin/scancel",
        "/usr/bin/squeue",
        "/usr/bin/squeue",
    ]
    assert not state_path.exists()


def test_broker_scancel_timeout_still_requires_empty_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
    events: list[str] = []

    def cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "/usr/bin/scancel" in argv:
            events.append("scancel-timeout")
            raise broker.BrokerError("GB10 cleanup command timed out safely")
        if "/usr/bin/squeue" in argv:
            events.append("squeue-empty")
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)

    broker._cleanup_persisted_probe_job(state_path, deadline=None)

    assert events == ["scancel-timeout", "squeue-empty", "squeue-empty"]
    assert not state_path.exists()


def test_broker_exact_cleanup_polls_until_two_consecutive_empty_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    _write_job_state(state_path, job_name=job_name, job_id="123")
    observations = iter((f"123|{job_name}\n", "", ""))
    events: list[str] = []

    def cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "/usr/bin/scancel" in argv:
            events.append("scancel-123")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            events.append("squeue")
            return subprocess.CompletedProcess(argv, 0, next(observations), "")
        raise AssertionError(argv)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)

    broker._cleanup_persisted_probe_job(
        state_path,
        deadline=time.monotonic() + 1.0,
    )

    assert events == ["scancel-123", "squeue", "squeue", "squeue"]
    assert not state_path.exists()


def test_broker_exact_cleanup_fails_when_job_persists_until_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    _write_job_state(state_path, job_name=job_name, job_id="123")
    clock = [0.0]
    name_queries = 0

    def cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal name_queries
        if "/usr/bin/scancel" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            name_queries += 1
            return subprocess.CompletedProcess(argv, 0, f"123|{job_name}\n", "")
        raise AssertionError(argv)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)
    monkeypatch.setattr(broker.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        broker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )

    with pytest.raises(broker.BrokerError, match="did not converge"):
        broker._cleanup_persisted_probe_job(state_path, deadline=0.11)

    assert name_queries >= 2
    assert clock[0] <= 0.11
    assert state_path.exists()


def test_broker_cleanup_defers_signal_through_exact_empty_readback(tmp_path: Path) -> None:
    state_path = tmp_path / "active-job.json"
    cleanup_log = tmp_path / "cleanup.log"
    script = r"""
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys

broker_path, state_path, log_path = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("cleanup_signal_broker", broker_path)
assert spec is not None and spec.loader is not None
broker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = broker
spec.loader.exec_module(broker)
broker.ROOT_UID = os.geteuid()
broker.ROOT_GID = os.getegid()
events = []


def run(argv, **kwargs):
    del kwargs
    if "/usr/bin/scancel" in argv:
        events.append("scancel:123")
        os.kill(os.getpid(), signal.SIGTERM)
        return subprocess.CompletedProcess(argv, 0, "", "")
    if "/usr/bin/squeue" in argv:
        events.append("squeue-empty")
        return subprocess.CompletedProcess(argv, 0, "", "")
    raise AssertionError(argv)


broker._run_cleanup_command = run
broker._install_signal_handlers()
try:
    broker._cleanup_persisted_probe_job(state_path, deadline=None)
except broker.BrokerInterruptedError:
    log_path.write_text("\n".join(events) + "\n")
    raise SystemExit(42)
raise SystemExit(3)
"""
    _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
    process = subprocess.run(
        [sys.executable, "-c", script, str(BROKER_PATH), str(state_path), str(cleanup_log)],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert process.returncode == 42, (process.stdout, process.stderr)
    assert cleanup_log.read_text(encoding="utf-8").splitlines() == [
        "scancel:123",
        "squeue-empty",
        "squeue-empty",
    ]
    assert not state_path.exists()


def test_broker_rejects_job_state_not_bound_to_expected_unit_nonce(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    expected_nonce = "0123456789abcdef01234567"
    _write_job_state(
        state_path,
        job_name="loom-accept-abcdef0-1-fedcba9876543210fedcba98",
        job_id="123",
    )

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(
        broker,
        "_run_cleanup_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mismatched request attempted scheduler cleanup")
        ),
    )

    with pytest.raises(broker.BrokerError, match="request binding mismatched"):
        broker._cleanup_persisted_probe_job(
            state_path,
            deadline=None,
            expected_unit_name=f"loom-gb10-capacity-{expected_nonce}.service",
        )

    assert state_path.exists()


def test_broker_pre_id_fallback_rejects_ambiguous_jobs_without_cancelling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    _write_job_state(state_path, job_name=_TEST_JOB_NAME)
    cleanup_commands: list[list[str]] = []

    def cleanup_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        cleanup_commands.append(argv)
        if "/usr/bin/squeue" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                f"123|{_TEST_JOB_NAME}\n456|{_TEST_JOB_NAME}\n",
                "",
            )
        raise AssertionError("ambiguous fallback attempted cancellation")

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup_run)

    with pytest.raises(broker.BrokerError, match="ambiguous"):
        broker._cleanup_persisted_probe_job(state_path, deadline=None)

    assert len(cleanup_commands) == 1
    assert "/usr/bin/squeue" in cleanup_commands[0]
    assert all("/usr/bin/scancel" not in command for command in cleanup_commands)
    assert state_path.exists()


def test_broker_pre_id_cleanup_waits_for_quiescent_empty_before_cancelling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "active-job.json"
    _write_job_state(state_path, job_name=_TEST_JOB_NAME)
    events: list[str] = []
    name_queries = 0

    def cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal name_queries
        if "/usr/bin/scancel" in argv:
            events.append("scancel-123")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv and any(item.startswith("--name=") for item in argv):
            name_queries += 1
            events.append(f"name-query-{name_queries}")
            if name_queries == 1:
                stdout = ""
            elif name_queries == 2:
                stdout = f"123|{_TEST_JOB_NAME}\n"
            else:
                stdout = ""
            return subprocess.CompletedProcess(argv, 0, stdout, "")
        raise AssertionError(argv)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "_run_cleanup_command", cleanup)

    broker._cleanup_persisted_probe_job(state_path, deadline=None)

    assert events == [
        "name-query-1",
        "name-query-2",
        "scancel-123",
        "name-query-3",
        "name-query-4",
    ]
    assert not state_path.exists()


def test_broker_recovery_terminates_stale_unit_before_new_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    state_root = runtime_root / "jobs"
    cgroup_root = tmp_path / "cgroup"
    state_root.mkdir(parents=True, mode=0o700)
    runtime_root.chmod(0o700)
    unit_name = _TEST_UNIT_NAME
    state_path = state_root / f"{unit_name}.json"
    _write_job_state(state_path, job_name=_TEST_JOB_NAME, job_id="123")
    calls: list[tuple[str, Path, Path]] = []

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "ACCEPTANCE_JOB_STATE_ROOT", state_root)
    monkeypatch.setattr(broker, "CGROUP_ROOT", cgroup_root)
    monkeypatch.setattr(
        broker,
        "_terminate_and_verify_containment",
        lambda *, unit_name, cgroup_path, job_state_path, **_kwargs: calls.append(
            (unit_name, cgroup_path, job_state_path)
        ),
    )

    broker._reconcile_stale_job_states(deadline=time.monotonic() + 60)

    assert calls == [
        (
            unit_name,
            cgroup_root / "system.slice" / unit_name,
            state_path,
        )
    ]


def test_broker_recovery_removes_verified_stale_atomic_temp_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_root = tmp_path / "jobs"
    state_root.mkdir(mode=0o700)
    stale_paths = [
        state_root / ".active-job.12345678",
        state_root / ".broker-job.abcdefgh",
    ]
    for path in stale_paths:
        path.write_text("partial", encoding="ascii")
        path.chmod(0o600)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "ACCEPTANCE_JOB_STATE_ROOT", state_root)

    broker._reconcile_stale_job_states(deadline=time.monotonic() + 1.0)

    assert all(not path.exists() for path in stale_paths)


@pytest.mark.parametrize("mutation", ("mode", "symlink", "hardlink"))
def test_broker_recovery_rejects_unsafe_stale_atomic_temp_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    state_root = tmp_path / "jobs"
    state_root.mkdir(mode=0o700)
    stale_path = state_root / ".active-job.12345678"
    if mutation == "symlink":
        target = tmp_path / "target"
        target.write_text("partial", encoding="ascii")
        stale_path.symlink_to(target)
    else:
        target = tmp_path / "target"
        target.write_text("partial", encoding="ascii")
        target.chmod(0o600)
        if mutation == "hardlink":
            os.link(target, stale_path)
        else:
            stale_path.write_text("partial", encoding="ascii")
            stale_path.chmod(0o640)

    monkeypatch.setattr(broker, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(broker, "ROOT_GID", os.getegid())
    monkeypatch.setattr(broker, "ACCEPTANCE_JOB_STATE_ROOT", state_root)

    with pytest.raises(broker.BrokerError, match="temporary metadata is unsafe"):
        broker._reconcile_stale_job_states(deadline=time.monotonic() + 1.0)

    assert os.path.lexists(stale_path)


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
        nodes=NORMAL_GB10_WORKER_HOSTS,
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
