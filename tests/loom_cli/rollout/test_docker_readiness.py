from __future__ import annotations

import subprocess

from loom_cli.rollout.docker_readiness import DockerRuntimeReadiness, probe_docker_runtime


def test_docker_runtime_probe_collects_both_independent_results() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[0] == "sysctl":
            return subprocess.CompletedProcess(argv, 0, "1024\n", "")
        return subprocess.CompletedProcess(argv, 1 if argv[-1] == "info" else 0, "secret", "")

    readiness = probe_docker_runtime(run)

    assert readiness == DockerRuntimeReadiness(
        daemon_ready=False,
        buildx_ready=True,
        inotify_capacity_ready=True,
    )
    assert calls == [
        ("docker", "info"),
        ("docker", "buildx", "version"),
        ("sysctl", "-n", "fs.inotify.max_user_instances"),
    ]
    assert len(readiness.evidence_digest) == 64
    assert "secret" not in repr(readiness)


def test_docker_runtime_probe_fails_closed_per_command_exception() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[-1] == "info":
            raise OSError("private daemon diagnostic")
        stdout = "1023\n" if argv[0] == "sysctl" else ""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    readiness = probe_docker_runtime(run)

    assert readiness.daemon_ready is False
    assert readiness.buildx_ready is True
    assert readiness.inotify_capacity_ready is False
    assert len(calls) == 3
