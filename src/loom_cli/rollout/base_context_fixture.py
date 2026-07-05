"""Shared test fixture builder for RolloutContext (#340).

Kept alongside the module (not in tests/) so both foundation tests and
concrete-step tests can import it without a per-test-file conftest.
"""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.context import RolloutContext


def make_ctx(
    tmp_path: Path,
    *,
    image_tag: str = "staging-abc123",
    target_ref: str = "origin/dev",
    resolved_sha: str = "a" * 40,
    cluster_name: str = "loom-staging",
    namespace: str = "loom-staging",
    environment: str = "staging",
    cp_url: str = "http://localhost:8080",
    cluster_config_sha256: str = "b" * 64,
    backup_manifest_path: Path | None = None,
    scope: str = "current-gb10",
    exclude_oldlab: bool = False,
    resume: bool = False,
) -> RolloutContext:
    """Return a minimally-populated RolloutContext for tests."""
    cfg = tmp_path / "cluster-config.toml"
    cfg.write_text("image_tag = 'x'\n")
    if backup_manifest_path is None:
        backup_manifest_path = tmp_path / "backup-manifest.json"
    return RolloutContext(
        image_tag=image_tag,
        target_ref=target_ref,
        resolved_sha=resolved_sha,
        cluster_name=cluster_name,
        namespace=namespace,
        environment=environment,
        cp_url=cp_url,
        cluster_config_path=cfg,
        cluster_config_sha256=cluster_config_sha256,
        rollout_root=tmp_path,
        backup_manifest_path=backup_manifest_path,
        scope=scope,
        exclude_oldlab=exclude_oldlab,
        resume=resume,
        metadata={"rollout_id": "test-rid"},
    )
