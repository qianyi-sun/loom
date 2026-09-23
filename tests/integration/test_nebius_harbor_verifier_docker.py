"""Run the generated Harbor wrapper with real shell exit/reward semantics."""

from __future__ import annotations

import io
import json
import subprocess
import tarfile
from pathlib import Path
from uuid import uuid4

import pytest

from loom.models.verifier import VerifierResult
from loom.nebius_terminus_ingest import offline_verifier_run_sh_bytes
from loom.nebius_terminus_image import prepare_nebius_terminus_image

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]


@pytest.fixture
def run_wrapper(tmp_path: Path):
    import docker

    client = docker.from_env()
    tmp_path.chmod(0o755)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/proof").write_text("private test input\n")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/run.sh").write_bytes(offline_verifier_run_sh_bytes())

    def run(script: str) -> tuple[int, dict | None]:
        (tmp_path / "verifier/harbor-offline.sh").write_text(script)
        container = client.containers.run(
            "python:3.11-slim", ["/source/verifier/run.sh"], entrypoint="/bin/sh",
            detach=True, network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            environment={"LOOM_TASK_DIR": "/source", "LOOM_VERIFIER_OUTPUT": "/result.json"},
            volumes={str(tmp_path): {"bind": "/source", "mode": "ro"}},
        )
        try:
            status = container.wait(timeout=30)["StatusCode"]
            print(container.logs().decode(errors="replace"))
            try:
                chunks, _ = container.get_archive("/result.json")
            except docker.errors.NotFound:
                return status, None
            with tarfile.open(fileobj=io.BytesIO(b"".join(chunks))) as archive:
                member = archive.extractfile("result.json")
                assert member is not None
                result = json.load(member)
            VerifierResult.model_validate(result)
            return status, result
        finally:
            container.remove(force=True)

    try:
        yield run
    finally:
        client.close()


@pytest.mark.parametrize("exit_code", [1, 2, 137])
def test_failed_harbor_verifier_retains_reward_and_original_failure(run_wrapper, exit_code):
    # Harbor's EXIT trap records zero even when set -e stops at pytest or setup.
    status, result = run_wrapper(
        "set -eu\n"
        "trap 'echo 0 > /logs/verifier/reward.txt' EXIT\n"
        "test -f /tests/proof\n"
        f"exit {exit_code}\n"
    )
    assert status == exit_code
    assert result is not None
    assert result["rewards"] == {"resolved": 0, "passed": 0}
    assert result["structured"] == {"exit_code": exit_code, "reward": 0}
    assert result["checks"][0]["passed"] is False


@pytest.mark.parametrize("reward", [0, 1])
def test_successful_harbor_verifier_preserves_zero_and_positive_rewards(run_wrapper, reward):
    status, result = run_wrapper(f"echo {reward} > /logs/verifier/reward.txt\n")
    assert status == 0
    assert result is not None
    assert result["rewards"] == {"resolved": reward, "passed": reward}


@pytest.mark.parametrize("script", ["exit 0\n", "echo invalid > /logs/verifier/reward.txt\n"])
def test_missing_or_invalid_harbor_reward_still_fails_without_result(run_wrapper, script):
    status, result = run_wrapper(script)
    assert status != 0
    assert result is None


@pytest.mark.timeout(600)
def test_prepared_image_preserves_task_shell_and_python_alias(tmp_path: Path):
    import docker

    shell_setup = (
        "printf '#!/bin/sh\\necho called >> /authored-shell.log\\nexec /bin/sh \"$@\"\\n' "
        "> /task-shell && chmod +x /task-shell && "
        "ln -s /usr/local/bin/python /usr/bin/python"
    )
    original = (
        "FROM python:3.11-slim\n"
        "RUN " + json.dumps(["/bin/sh", "-c", shell_setup]) + "\n"
        'SHELL ["/task-shell", "-c"]\n'
        "RUN touch /authored-run\nWORKDIR /app\n"
    )
    (tmp_path / "Dockerfile").write_text(original)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(
        "pip install pytest==8.4.1\npytest /tests/test_example.py\n"
    )
    environment = {"dockerfile": "Dockerfile", "docker_build_context": ".", "workdir": "/app"}
    prepare_nebius_terminus_image(tmp_path, environment)
    tag = "loom-preparation-shell-test:" + uuid4().hex
    client = docker.from_env()
    container = None
    try:
        result = subprocess.run(
            ["docker", "build", "--tag", tag, "--file", str(tmp_path / environment["dockerfile"]), str(tmp_path)],
            capture_output=True, text=True, timeout=480,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        built = client.images.get(tag)
        assert built.attrs["Config"]["Shell"] == ["/task-shell", "-c"]
        container = client.containers.run(
            tag, entrypoint="/bin/sh", command=["-ec", (
                'test "$(id -u)" = 65532; test -f /authored-run; '
                'test "$(cat /authored-shell.log)" = called; '
                'test "$(readlink /usr/bin/python)" = /usr/local/bin/python; '
                'test "$(python -c \'import sys; print(sys.version_info[:2])\')" = "(3, 11)"; '
                '/opt/verifier/bin/python -m pytest --version'
            )],
            network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"], detach=True,
        )
        status = container.wait(timeout=30)["StatusCode"]
        assert status == 0, container.logs().decode(errors="replace")
        assert b"pytest 8.4.1" in container.logs()
        assert (tmp_path / "Dockerfile").read_text() == original
    finally:
        if container is not None:
            container.remove(force=True)
        try:
            client.images.remove(tag, force=True)
        except docker.errors.ImageNotFound:
            pass
        client.close()
