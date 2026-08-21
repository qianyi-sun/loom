from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps import s04_publish_images
from loom_cli.rollout.steps.s04_publish_images import PublishImagesStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def test_publishes_exact_images_to_configured_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_id = "sha256:" + "a" * 64
    manifest_digest = "sha256:" + "c" * 64
    images = (("loom-control-plane", "Dockerfile.control-plane", "."),)
    ctx = SimpleNamespace(
        cluster_name="loom-staging",
        cluster_config_path=tmp_path / "cluster.toml",
        image_tag="staging-abcdef0",
        resolved_sha="b" * 40,
    )
    ctx.cluster_config_path.write_text(
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n',
        encoding="utf-8",
    )
    step_dir = StepDir(4, "publish-images", tmp_path / "04-publish-images")
    step_dir.path.mkdir()
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(s04_publish_images, "rollout_images_from_candidate", lambda _ctx: images)
    monkeypatch.setattr(
        s04_publish_images,
        "rollout_image_bindings",
        lambda _ctx, _step_dir: (images, {"loom-control-plane": image_id}),
    )

    def run(argv, **_kwargs):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        stdout = ""
        if command[:3] == ("docker", "manifest", "inspect"):
            stdout = json.dumps(
                {
                    "Descriptor": {"digest": manifest_digest},
                    "SchemaV2Manifest": {"config": {"digest": image_id}},
                }
            )
        return SubprocessResult(list(argv), 0, stdout, "")

    monkeypatch.setattr(s04_publish_images, "run_captured", run)

    result = PublishImagesStep().run(ctx, step_dir)  # type: ignore[arg-type]

    assert result.exit_code == 0
    assert (
        "docker",
        "tag",
        "loom-control-plane:staging-abcdef0",
        "localhost:5000/loom-control-plane:staging-abcdef0",
    ) in calls
    assert (
        "docker",
        "push",
        "localhost:5000/loom-control-plane:staging-abcdef0",
    ) in calls
    assert s04_publish_images.registry_image_digests(ctx, step_dir) == {
        "loom-control-plane": manifest_digest
    }
