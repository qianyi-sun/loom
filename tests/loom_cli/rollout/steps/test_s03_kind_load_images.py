from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps import s03_kind_load_images
from loom_cli.rollout.steps.s03_kind_load_images import KindLoadImagesStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def test_registry_profile_publishes_exact_images_without_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_id = "sha256:" + "a" * 64
    images = (("loom-control-plane", "Dockerfile.control-plane", "."),)
    ctx = SimpleNamespace(
        cluster_name="loom-staging",
        cluster_config_path=tmp_path / "cluster.toml",
        image_tag="staging-abcdef0",
        resolved_sha="b" * 40,
    )
    step_dir = StepDir(4, "kind-load-images", tmp_path / "04-kind-load-images")
    step_dir.path.mkdir()
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        s03_kind_load_images,
        "_registry_publication",
        lambda _ctx: ("192.168.50.13:5000", "localhost:5000"),
    )
    monkeypatch.setattr(s03_kind_load_images, "rollout_images_from_candidate", lambda _ctx: images)
    monkeypatch.setattr(
        s03_kind_load_images,
        "rollout_image_bindings",
        lambda _ctx, _step_dir: (images, {"loom-control-plane": image_id}),
    )
    monkeypatch.setattr(s03_kind_load_images, "candidate_loom_cwd", lambda _step_dir: tmp_path)
    monkeypatch.setattr(s03_kind_load_images, "candidate_loom_env", lambda _step_dir: {})

    def run(argv, **_kwargs):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        stdout = ""
        if command[:3] == ("docker", "manifest", "inspect"):
            stdout = json.dumps(
                {
                    "Descriptor": {"digest": "sha256:" + "c" * 64},
                    "SchemaV2Manifest": {"config": {"digest": image_id}},
                }
            )
        return SubprocessResult(list(argv), 0, stdout, "")

    monkeypatch.setattr(s03_kind_load_images, "run_captured", run)

    result = KindLoadImagesStep().run(ctx, step_dir)  # type: ignore[arg-type]

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
    assert s03_kind_load_images.registry_image_digests(ctx, step_dir) == {
        "loom-control-plane": "sha256:" + "c" * 64
    }
    assert not any(command and command[0] in {"kind", "loom"} for command in calls)
