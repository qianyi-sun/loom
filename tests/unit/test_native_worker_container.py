"""Fixed native worker Docker composition rejects ambient and mutable authority."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from tests.unit.test_worker_native_entrypoint import _configured_bootstrap


def _image():
    return {
        "Id": "sha256:" + "a" * 64,
        "RepoDigests": ["ghcr.io/qianyi-sun/loom-worker@sha256:" + "b" * 64],
        "Os": "linux", "Architecture": "amd64",
        "Config": {"Env": ["PATH=/evil", "LD_PRELOAD=/evil.so", "PYTHONPATH=/evil"],
                   "Volumes": None, "OnBuild": None},
    }


def _prepared():
    from loom_capacity_executor.native_worker_container import prepare_native_image

    return prepare_native_image(_image(), image_digest=_image()["RepoDigests"][0], platform="linux/amd64")


@pytest.mark.parametrize("tamper", ["digest", "id", "platform", "volume", "onbuild", "env", "duplicate"])
def test_native_image_preflight_refuses_unverified_image_composition(tamper):
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        prepare_native_image,
    )

    image = _image()
    if tamper == "digest":
        image["RepoDigests"] = []
    elif tamper == "id":
        image["Id"] = "mutable-tag"
    elif tamper == "platform":
        image["Architecture"] = "arm64"
    elif tamper == "volume":
        image["Config"]["Volumes"] = {"/usr/local": {}}
    elif tamper == "onbuild":
        image["Config"]["OnBuild"] = ["RUN evil"]
    elif tamper == "env":
        image["Config"]["Env"] = ["INVALID-KEY=evil"]
    else:
        image["Config"]["Env"] = ["PATH=first", "PATH=second"]
    with pytest.raises(NativeContainerError):
        prepare_native_image(image, image_digest=_image()["RepoDigests"][0], platform="linux/amd64")


def _allocation():
    from loom_capacity_executor.native_worker_container import NativeWorkerAllocation

    return NativeWorkerAllocation(intent_id="11111111-1111-4111-8111-111111111111", job_id="101",
        cgroup_parent="/system.slice/slurmstepd.scope/job_101", cpu_millicores=1000, memory_bytes=1024**3,
        pids_max=128, concurrency_slots=1, scratch_directory="/var/lib/loom/native-workers/test",
        docker_socket_gid=998, runtime_uid=65532, runtime_gid=65532, pool_id="oldlab",
        hostname="oldlab-5", candidate_sha="a" * 64)


@pytest.mark.parametrize("parent", [
    "loom-job-101.slice", "/loom.slice/loom-job.slice/loom-job-101.slice", "/",
    "/system.slice/slurmstepd.scope/job_202", "/system.slice/slurmstepd.scope/job_101/step_batch",
    "/system.slice/slurmstepd.scope/job_101/../job_202", "/system.slice/foreign.scope/job_101",
])
def test_native_allocation_requires_exact_slurm_job_ancestor(parent):
    from loom_capacity_executor.native_worker_container import NativeContainerError

    with pytest.raises(NativeContainerError, match="allocation"):
        replace(_allocation(), cgroup_parent=parent)


def test_fixed_create_removes_loader_environment_and_has_no_secret_or_command_override():
    from loom_capacity_executor.native_worker_container import native_create_argv

    bootstrap = _configured_bootstrap()
    argv = native_create_argv(_prepared(), _allocation(), name="loom-native-test", ownership="c" * 64)
    assert argv[-4:] == ("sha256:" + "a" * 64, "-I", "-m", "loom_worker.native_main",)
    assert "--entrypoint=/usr/local/bin/python" in argv
    for flag in ("--read-only", "--restart=no", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                 "--cgroup-parent=/system.slice/slurmstepd.scope/job_101", "--memory=1073741824", "--pids-limit=128"):
        assert flag in argv
    assert "--env=LD_PRELOAD" in argv
    assert "--env=PYTHONPATH" in argv
    assert "--env=PATH=/usr/local/bin:/usr/bin:/bin" in argv
    assert not any("/app" in arg or "env-file" in arg for arg in argv)
    assert bootstrap.worker_credential not in repr(argv)


def test_runtime_settings_bind_allocation_and_refuse_ungated_runtime_modes():
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        bind_native_settings,
    )

    original = _configured_bootstrap()
    result = bind_native_settings(original, _allocation())
    settings = result.worker_settings()
    assert settings["require_cgroup_parent"] is True
    assert settings["cgroup_parent"] == "/system.slice/slurmstepd.scope/job_101"
    assert settings["slurm_job_id"] == "101"
    assert settings["slurm_allocated_gpus"] == 0
    assert settings["max_concurrent"] == 1
    assert settings["pool_name"] == "oldlab"
    assert settings["hostname"] == "oldlab-5"
    assert settings["candidate_sha"] == "a" * 64
    assert result.worker_credential == original.worker_credential
    for overrides in ({"enable_worker_vllm": True}, {"sandbox_isolation": True}, {"pool_name": "task-image-builder"},
                      {"cgroup_parent": "/foreign"}, {"docker_socket": "/foreign.sock"}):
        changed = replace(original, canonical_worker_settings=json.dumps(
            original.worker_settings() | overrides, sort_keys=True, separators=(",", ":")))
        with pytest.raises(NativeContainerError):
            bind_native_settings(changed, _allocation())


def test_cli_never_inherits_host_configuration_or_tokens(monkeypatch, tmp_path):
    from loom_capacity_executor.native_worker_container import FixedDockerCLI

    monkeypatch.setenv("DOCKER_HOST", "tcp://foreign:2375")
    monkeypatch.setenv("LOOM_EXECUTOR_WORKER_CREDENTIAL", "secret")
    observed = []

    def run(argv, **kwargs):
        observed.append((argv, kwargs))
        raise OSError("test boundary")

    monkeypatch.setattr("loom_capacity_executor.native_worker_container.subprocess.Popen", run)
    cli = FixedDockerCLI(executable="/proc/self/fd/9", descriptor=9, config_directory="/etc/loom/empty-docker")
    from loom_capacity_executor.native_worker_container import NativeContainerError

    with pytest.raises(NativeContainerError):
        cli.json("image", "inspect", "sha256:" + "a" * 64)
    argv, kwargs = observed[0]
    assert argv[:3] == ("/proc/self/fd/9", "--config=/etc/loom/empty-docker", "--host=unix:///var/run/docker.sock")
    assert kwargs["env"] == {}
    assert kwargs["pass_fds"] == (9,)


def test_cleanup_requires_successful_remove_and_positive_daemon_readback(monkeypatch):
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        remove_native_container,
    )

    class CLI:
        def __init__(self):
            self.present = True
            self.remove_failed = False

        def call(self, *args, **kwargs):
            if self.remove_failed:
                raise NativeContainerError("daemon unavailable")
            return b"a" * 64 if self.present else b""

    cli = CLI()
    with pytest.raises(NativeContainerError, match="cleanup"):
        remove_native_container(cli, "a" * 64)
    cli.present = False
    cli.remove_failed = True
    with pytest.raises(NativeContainerError):
        remove_native_container(cli, "a" * 64)
    cli.remove_failed = False
    remove_native_container(cli, "a" * 64)


@pytest.mark.parametrize("override", [
    {"candidate_argv": ("/usr/bin/docker", "run", "foreign")},
    {"candidate_argv": ("/usr/bin/docker", "--config=/foreign")},
])
def test_native_trusted_config_forbids_candidate_suffix(override):
    from loom_capacity_executor.trusted_launcher import TrustedLauncherConfigV2

    value = {
        "handoff_directory": "/var/lib/loom/handoff", "admission_directory": "/etc/loom/admission",
        "admission_directory_sha256": "a" * 64,
        "candidate_executable": {"path": "/usr/bin/docker", "sha256": "b" * 64, "owner_uid": 0, "mode": 493},
        "candidate_image_digest": _image()["RepoDigests"][0], "candidate_argv": ("/usr/bin/docker",),
        "native_worker": {
            "native_execution": _configured_bootstrap().native_execution.model_dump(mode="json"),
            "canonical_worker_settings": _configured_bootstrap().canonical_worker_settings,
            "docker_config_directory": "/etc/loom/empty-docker", "pids_max": 128,
        },
    }
    assert TrustedLauncherConfigV2.model_validate(value).native_worker is not None
    with pytest.raises(ValueError, match=r"native.*suffix"):
        TrustedLauncherConfigV2.model_validate(value | override)


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_control_command_output_flood_is_bounded(stream):
    import subprocess
    import sys

    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        capture_native_control_output,
    )

    script = f"import sys; sys.{stream}.buffer.write(b'x' * (2 * 1024 * 1024)); sys.{stream}.flush()"
    with subprocess.Popen([sys.executable, "-I", "-c", script], stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        with pytest.raises(NativeContainerError, match="output"):
            capture_native_control_output(process, timeout=10)
        assert process.poll() is not None


def test_control_command_timeout_reaps_cli_without_claiming_container_cleanup():
    import subprocess
    import sys

    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        capture_native_control_output,
    )

    with subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(60)"],
                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        with pytest.raises(NativeContainerError, match="uncertain"):
            capture_native_control_output(process, timeout=0.05)
        assert process.poll() is not None


def test_image_prefetch_does_not_consume_handoff_on_platform_failure(monkeypatch, tmp_path):
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        prefetch_native_image,
    )

    class CLI:
        def call(self, *args, **kwargs):
            assert args[:2] == ("image", "pull")
            return b""

        def json(self, *args, **kwargs):
            return [_image() | {"Architecture": "arm64"}]

    with pytest.raises(NativeContainerError):
        prefetch_native_image(CLI(), image_digest=_image()["RepoDigests"][0], platform="linux/amd64")
