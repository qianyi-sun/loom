"""Unit tests for the publish-local cpu_arch promotion (#342)."""

from __future__ import annotations

from pathlib import Path

from loom_cli.local_benchmark_publish import (
    _promote_cpu_arch_if_runtime_fallback,
)


def _make_bundle(tmp_path: Path, dockerfile_body: str) -> Path:
    """Return a bundle dir with a task.toml pointing at Dockerfile."""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "Dockerfile").write_text(dockerfile_body)
    return bundle


class TestPromoteCpuArch:
    def test_promotes_to_any_when_dockerfile_uses_terminus_2_base(
        self, tmp_path: Path,
    ) -> None:
        bundle = _make_bundle(
            tmp_path,
            "FROM mictern2/terminus2-full:latest\nRUN echo ready\n",
        )
        raw_cfg: dict[str, object] = {
            "environment": {"os": "linux", "dockerfile": "Dockerfile"},
        }
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, bundle)
        assert raw_cfg["environment"] == {  # type: ignore[comparison-overlap]
            "os": "linux",
            "dockerfile": "Dockerfile",
            "cpu_arch": "any",
        }

    def test_leaves_non_fallback_base_alone(self, tmp_path: Path) -> None:
        bundle = _make_bundle(tmp_path, "FROM python:3.11-slim\n")
        raw_cfg: dict[str, object] = {
            "environment": {"os": "linux", "dockerfile": "Dockerfile"},
        }
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, bundle)
        assert "cpu_arch" not in raw_cfg["environment"]  # type: ignore[operator]

    def test_respects_explicit_cpu_arch(self, tmp_path: Path) -> None:
        """User pinned cpu_arch=x86_64 explicitly — don't override even
        though the base is runtime-fallback-eligible."""
        bundle = _make_bundle(
            tmp_path,
            "FROM mictern2/terminus2-full:latest\n",
        )
        raw_cfg: dict[str, object] = {
            "environment": {
                "os": "linux",
                "dockerfile": "Dockerfile",
                "cpu_arch": "x86_64",
            },
        }
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, bundle)
        assert raw_cfg["environment"]["cpu_arch"] == "x86_64"  # type: ignore[index]

    def test_noop_when_environment_missing(self, tmp_path: Path) -> None:
        raw_cfg: dict[str, object] = {"task": {"id": "t"}}
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, tmp_path)
        assert raw_cfg == {"task": {"id": "t"}}

    def test_noop_when_dockerfile_not_set(self, tmp_path: Path) -> None:
        raw_cfg: dict[str, object] = {
            "environment": {"os": "linux", "docker_image": "alpine"},
        }
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, tmp_path)
        assert "cpu_arch" not in raw_cfg["environment"]  # type: ignore[operator]

    def test_noop_when_dockerfile_missing_from_disk(
        self, tmp_path: Path,
    ) -> None:
        raw_cfg: dict[str, object] = {
            "environment": {"os": "linux", "dockerfile": "nope/Dockerfile"},
        }
        _promote_cpu_arch_if_runtime_fallback(raw_cfg, tmp_path)
        assert "cpu_arch" not in raw_cfg["environment"]  # type: ignore[operator]
