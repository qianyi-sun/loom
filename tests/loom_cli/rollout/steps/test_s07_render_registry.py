from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ROLLOUT_IMAGES
from loom_cli.rollout.steps import s07_render
from loom_cli.rollout.steps.s07_render import RenderStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def test_registry_render_pins_every_standing_image_to_step04_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_tag = "staging-abcdef0"
    registry = "192.168.50.13:5000"
    registry_digests = {
        name: f"sha256:{hashlib.sha256((name + '-manifest').encode()).hexdigest()}"
        for name, _path in ROLLOUT_IMAGES
    }
    local_ids = {
        name: f"sha256:{hashlib.sha256((name + '-config').encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }
    containers = "\n".join(
        f"        - name: {name}\n          image: {registry}/{name}:{image_tag}"
        for name, _path in ROLLOUT_IMAGES
    )
    rendered = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: exact-candidate
  namespace: loom-staging
spec:
  template:
    spec:
      containers:
{containers}
"""
    ctx = SimpleNamespace(image_tag=image_tag, namespace="loom-staging")
    step_dir = StepDir(7, "render", tmp_path / "07-render")
    step_dir.path.mkdir()
    monkeypatch.setattr(s07_render, "candidate_loom_cwd", lambda _step_dir: tmp_path)
    monkeypatch.setattr(s07_render, "candidate_loom_env", lambda _step_dir: {})
    monkeypatch.setattr(s07_render, "rollout_cluster_config", lambda _ctx, _step_dir: tmp_path / "cluster.toml")
    monkeypatch.setattr(s07_render, "_registry_publication", lambda _ctx: (registry, "localhost:5000"))
    monkeypatch.setattr(s07_render, "registry_image_digests", lambda _ctx, _step_dir: registry_digests)
    monkeypatch.setattr(
        s07_render,
        "rollout_all_image_bindings",
        lambda _ctx, _step_dir: ((), local_ids),
    )
    monkeypatch.setattr(
        s07_render,
        "run_captured",
        lambda *args, **kwargs: SubprocessResult([], 0, rendered, ""),
    )

    result = RenderStep()._run_impl(ctx, step_dir)  # type: ignore[arg-type]

    assert result.exit_code == 0
    pinned = step_dir.artifact_path("rendered.yaml").read_text(encoding="utf-8")
    assert f":{image_tag}" not in pinned
    for name, _path in ROLLOUT_IMAGES:
        assert f"{registry}/{name}@{registry_digests[name]}" in pinned
