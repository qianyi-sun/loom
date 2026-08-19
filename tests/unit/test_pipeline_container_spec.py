from __future__ import annotations

import os
import stat
from pathlib import Path, PurePosixPath
from uuid import UUID

import pytest

from loom_worker.pipeline_container_runner import (
    GATEWAY_NETWORK_NAME,
    ContainerLimits,
    MountSpec,
    PipelineContainerContractError,
    PipelineContainerSpec,
    build_pipeline_container_spec,
)
from loom_worker.pipeline_runtime_secret import (
    RuntimeSecretError,
    RuntimeSecretMount,
)

IMAGE = "registry.example/loom/behavior@sha256:" + "a" * 64


def _directories(tmp_path: Path) -> dict[str, Path]:
    tmp_path.mkdir(exist_ok=True)
    paths = {
        "input_dir": tmp_path / "inputs",
        "outputs_dir": tmp_path / "outputs",
        "scratch_dir": tmp_path / "scratch",
        "runtime_secret_dir": tmp_path / "runtime-secret",
    }
    for path in paths.values():
        path.mkdir()
    return paths


def _build(tmp_path: Path, **overrides: object) -> PipelineContainerSpec:
    values: dict[str, object] = {
        "image": IMAGE,
        "argv": ["/opt/behavior/run", "--request", "/inputs/stage-request.json"],
        "workdir": "/workspace",
        "uid": 10001,
        "gid": 10001,
        **_directories(tmp_path),
        "network_profile": "gateway",
        "cpus": 2.0,
        "memory_bytes": 4 * 1024**3,
        "pids": 256,
        "scratch_bytes": 8 * 1024**3,
    }
    values.update(overrides)
    return build_pipeline_container_spec(**values)  # type: ignore[arg-type]


def test_gateway_spec_is_closed_and_secure(tmp_path: Path) -> None:
    spec = _build(tmp_path)

    assert spec.network_mode == GATEWAY_NETWORK_NAME
    assert [str(mount.target) for mount in spec.mounts] == [
        "/inputs",
        "/outputs",
        "/scratch",
        "/run/loom",
    ]
    assert [mount.read_only for mount in spec.mounts] == [True, False, False, True]
    assert [mount.quota_group for mount in spec.mounts] == [
        None,
        "attempt-scratch",
        "attempt-scratch",
        None,
    ]
    assert spec.mounts[1].quota_bytes == spec.mounts[2].quota_bytes == 8 * 1024**3
    secret = spec.mounts[-1]
    assert (
        secret.recursive_read_only,
        secret.nosuid,
        secret.nodev,
        secret.noexec,
        secret.container_mode,
    ) == (True, True, True, True, 0o500)
    assert spec.cap_drop == ("ALL",)
    assert spec.security_opt == ("no-new-privileges:true",)
    assert spec.read_only_rootfs is True
    assert spec.seccomp_profile == "default"
    assert spec.privileged is spec.host_pid is spec.host_ipc is False
    assert spec.devices == ()

    docker = spec.docker_create_kwargs()
    assert docker["image"] == IMAGE
    assert docker["command"] == list(spec.argv)
    assert docker["user"] == "10001:10001"
    assert docker["read_only"] is True
    assert docker["cap_drop"] == ["ALL"]
    assert docker["security_opt"] == ["no-new-privileges:true"]
    assert docker["privileged"] is False
    assert docker["pid_mode"] is None
    assert docker["ipc_mode"] is None
    assert docker["devices"] == []
    assert "/var/run/docker.sock" not in repr(docker)


def test_none_spec_has_no_runtime_secret_or_network(tmp_path: Path) -> None:
    directories = _directories(tmp_path)
    spec = build_pipeline_container_spec(
        image=IMAGE,
        argv=["/app/run"],
        workdir=PurePosixPath("/workspace"),
        uid=1234,
        gid=1234,
        input_dir=directories["input_dir"],
        outputs_dir=directories["outputs_dir"],
        scratch_dir=directories["scratch_dir"],
        network_profile="none",
        cpus=1,
        memory_bytes=1024,
        pids=16,
        scratch_bytes=4096,
    )

    assert spec.network_mode == "none"
    assert [mount.target for mount in spec.mounts] == [
        PurePosixPath("/inputs"),
        PurePosixPath("/outputs"),
        PurePosixPath("/scratch"),
    ]


def test_resume_checkpoint_adds_only_reserved_environment(tmp_path: Path) -> None:
    artifact_id = UUID(int=9)
    spec = _build(tmp_path, resume_checkpoint_artifact_id=artifact_id)
    assert dict(spec.environment) == {
        "LOOM_RESUME_CHECKPOINT": "/inputs/loom_checkpoint",
        "LOOM_RESUME_CHECKPOINT_ARTIFACT_ID": str(artifact_id),
    }
    assert spec.docker_create_kwargs()["environment"] == dict(spec.environment)


@pytest.mark.parametrize(
    "image",
    [
        "registry.example/loom/behavior:latest",
        "registry.example/loom/behavior@sha256:" + "A" * 64,
        "behavior@sha256:" + "a" * 64,
        "registry.example/loom/behavior@sha256:abc",
    ],
)
def test_image_must_be_repository_lowercase_digest(tmp_path: Path, image: str) -> None:
    with pytest.raises(PipelineContainerContractError, match="repository@sha256"):
        _build(tmp_path, image=image)


@pytest.mark.parametrize(
    "argv",
    [
        "echo hello",
        [],
        ["sh", "-c", "echo hello"],
        ["/bin/bash", "-c", "true"],
        ["/app/run", "--token=raw-secret-value"],
        ["/app/run", "Bearer abcdefghijklmnop"],
    ],
)
def test_argv_is_an_array_and_never_a_shell(tmp_path: Path, argv: object) -> None:
    with pytest.raises(PipelineContainerContractError, match=r"argv|shell|secret"):
        _build(tmp_path, argv=argv)


@pytest.mark.parametrize("uid,gid", [(0, 1), (1, 0), (-1, 1), (True, 1)])
def test_container_identity_is_non_root(tmp_path: Path, uid: object, gid: object) -> None:
    with pytest.raises(PipelineContainerContractError, match="non-root"):
        _build(tmp_path, uid=uid, gid=gid)


def test_network_and_secret_mount_must_agree(tmp_path: Path) -> None:
    with pytest.raises(PipelineContainerContractError, match="must not have"):
        _build(tmp_path / "none", network_profile="none")
    with pytest.raises(PipelineContainerContractError, match="requires"):
        _build(tmp_path / "gateway", runtime_secret_dir=None)
    with pytest.raises(PipelineContainerContractError, match="none or gateway"):
        _build(tmp_path / "public", network_profile="public")


def test_manual_spec_cannot_add_arbitrary_mount(tmp_path: Path) -> None:
    paths = _directories(tmp_path)
    mounts = (
        MountSpec(paths["input_dir"], PurePosixPath("/inputs"), True),
        MountSpec(
            paths["outputs_dir"],
            PurePosixPath("/outputs"),
            False,
            quota_group="attempt-scratch",
            quota_bytes=1,
        ),
        MountSpec(
            paths["scratch_dir"],
            PurePosixPath("/scratch"),
            False,
            quota_group="attempt-scratch",
            quota_bytes=1,
        ),
        MountSpec(tmp_path, PurePosixPath("/host"), False),
    )
    with pytest.raises(PipelineContainerContractError, match="mount set"):
        PipelineContainerSpec(
            image=IMAGE,
            argv=("/app/run",),
            workdir=PurePosixPath("/workspace"),
            uid=1,
            gid=1,
            network_profile="none",
            network_mode="none",
            mounts=mounts,
            limits=ContainerLimits(cpus=1, memory_bytes=1, pids=1, scratch_bytes=1),
        )


def test_secret_rotation_is_atomic_private_and_tears_down(tmp_path: Path) -> None:
    uid = os.getuid() or 65_534
    gid = os.getgid() or 65_534
    mount = RuntimeSecretMount(tmp_path / "loom", container_uid=uid, container_gid=gid)
    mount.initialize()
    assert stat.S_IMODE(mount.root.stat().st_mode) == 0o500

    first = mount.rotate(b"loom_step_first")
    assert mount.read_verified() == b"loom_step_first"
    assert first.mode == 0o400
    assert first.uid == uid
    assert first.gid == gid
    assert mount.root.stat().st_uid == uid
    assert mount.root.stat().st_gid == gid
    second = mount.rotate(b"loom_step_second")
    assert mount.read_verified() == b"loom_step_second"
    assert second.inode != first.inode
    assert [entry.name for entry in mount.root.iterdir()] == ["step-jwt"]
    assert stat.S_IMODE(mount.root.stat().st_mode) == 0o500

    mount.teardown()
    assert not mount.root.exists()
    mount.teardown()


def test_secret_mount_rejects_symlink_and_unknown_teardown_entry(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    uid = os.getuid() or 65_534
    gid = os.getgid() or 65_534
    with pytest.raises(RuntimeSecretError, match="real directory"):
        RuntimeSecretMount(alias, container_uid=uid, container_gid=gid).initialize()
    with pytest.raises(RuntimeSecretError, match="real directory"):
        RuntimeSecretMount(alias, container_uid=uid, container_gid=gid).teardown()

    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "missing", target_is_directory=True)
    with pytest.raises(RuntimeSecretError, match="real directory"):
        RuntimeSecretMount(dangling, container_uid=uid, container_gid=gid).teardown()

    mount = RuntimeSecretMount(tmp_path / "owned", container_uid=uid, container_gid=gid)
    mount.initialize()
    os.chmod(mount.root, 0o700)
    (mount.root / "unexpected").write_text("do not delete", encoding="utf-8")
    with pytest.raises(RuntimeSecretError, match="unexpected"):
        mount.teardown()
    assert (mount.root / "unexpected").exists()
