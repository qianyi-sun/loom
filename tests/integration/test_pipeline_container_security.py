from __future__ import annotations

from pathlib import Path

from loom_worker.pipeline_container_runner import build_pipeline_container_spec

IMAGE = "registry.example.com/loom/pipeline@sha256:" + "a" * 64


def test_pipeline_docker_projection_is_closed_and_non_root(tmp_path: Path) -> None:
    inputs = tmp_path / "inputs"
    outputs = tmp_path / "outputs"
    scratch = tmp_path / "scratch"
    for path in (inputs, outputs, scratch):
        path.mkdir()
    spec = build_pipeline_container_spec(
        image=IMAGE,
        argv=["python", "-m", "approved.stage"],
        workdir="/workspace",
        uid=65532,
        gid=65532,
        input_dir=inputs,
        outputs_dir=outputs,
        scratch_dir=scratch,
        network_profile="none",
        cpus=2,
        memory_bytes=1_073_741_824,
        pids=256,
        scratch_bytes=2_147_483_648,
    )
    create = dict(spec.docker_create_kwargs())
    assert create["network_mode"] == "none"
    assert create["user"] == "65532:65532"
    assert create["cap_drop"] == ["ALL"]
    assert create["security_opt"] == ["no-new-privileges:true"]
    assert create["read_only"] is True
    assert create["privileged"] is False
    assert create["pid_mode"] is None and create["ipc_mode"] is None
    assert create["devices"] == []
    assert "/var/run/docker.sock" not in create["volumes"]
    assert {item["bind"] for item in create["volumes"].values()} == {
        "/inputs",
        "/outputs",
        "/scratch",
    }
