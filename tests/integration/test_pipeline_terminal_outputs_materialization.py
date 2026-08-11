from __future__ import annotations

from pathlib import Path, PurePosixPath

from loom_worker.pipeline_container_runner import build_pipeline_container_spec


def test_input_cache_is_never_container_visible_or_writable(tmp_path: Path) -> None:
    inputs, outputs, scratch = (tmp_path / name for name in ("inputs", "outputs", "scratch"))
    for path in (inputs, outputs, scratch):
        path.mkdir()
    spec = build_pipeline_container_spec(
        image="registry.example/loom/behavior@sha256:" + "a" * 64,
        argv=["/app/run"],
        workdir="/workspace",
        uid=1000,
        gid=1000,
        input_dir=inputs,
        outputs_dir=outputs,
        scratch_dir=scratch,
        network_profile="none",
        cpus=1,
        memory_bytes=1024,
        pids=32,
        scratch_bytes=4096,
    )

    assert spec.mounts[0].target == PurePosixPath("/inputs")
    assert spec.mounts[0].read_only is True
    assert all(mount.target != PurePosixPath("/cache") for mount in spec.mounts)
    assert [mount.read_only for mount in spec.mounts[1:]] == [False, False]
