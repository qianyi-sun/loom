"""Unit tests for the publish-local environment/ flattener (#369)."""

from __future__ import annotations

from pathlib import Path

from loom_cli.local_benchmark_publish import _flatten_environment_subdir


class TestFlattenEnvironmentSubdir:
    def test_flattens_top_level_files(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        (bundle / "environment").mkdir(parents=True)
        (bundle / "environment" / "inventory.csv").write_text("a,b\n")
        (bundle / "environment" / "setup_repo.sh").write_text("echo ok\n")

        flattened = _flatten_environment_subdir(bundle)

        assert sorted(flattened) == ["inventory.csv", "setup_repo.sh"]
        assert (bundle / "inventory.csv").read_text() == "a,b\n"
        assert (bundle / "setup_repo.sh").read_text() == "echo ok\n"
        # Original tree preserved.
        assert (bundle / "environment" / "inventory.csv").is_file()
        assert (bundle / "environment" / "setup_repo.sh").is_file()

    def test_flattens_nested_files(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        (bundle / "environment" / "tests").mkdir(parents=True)
        (bundle / "environment" / "tests" / "t.py").write_text("pass\n")

        flattened = _flatten_environment_subdir(bundle)

        assert flattened == ["tests/t.py"]
        assert (bundle / "tests" / "t.py").read_text() == "pass\n"

    def test_top_level_wins_on_name_collision(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        (bundle / "environment").mkdir(parents=True)
        (bundle / "environment" / "conflict.txt").write_text("env-copy\n")
        (bundle / "conflict.txt").write_text("root-copy\n")

        flattened = _flatten_environment_subdir(bundle)

        assert flattened == []
        assert (bundle / "conflict.txt").read_text() == "root-copy\n"

    def test_no_op_when_environment_missing(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "Dockerfile").write_text("FROM alpine\n")

        flattened = _flatten_environment_subdir(bundle)

        assert flattened == []
        assert list(bundle.iterdir()) == [bundle / "Dockerfile"]

    def test_preserves_executable_bit(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        (bundle / "environment").mkdir(parents=True)
        script = bundle / "environment" / "setup.sh"
        script.write_text("#!/bin/sh\necho ok\n")
        script.chmod(0o755)

        _flatten_environment_subdir(bundle)

        flat = bundle / "setup.sh"
        assert flat.is_file()
        assert (flat.stat().st_mode & 0o755) == 0o755

    def test_skips_symlinks_inside_environment(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        (bundle / "environment").mkdir(parents=True)
        (bundle / "environment" / "real.txt").write_text("real\n")
        (bundle / "environment" / "link.txt").symlink_to(
            bundle / "environment" / "real.txt",
        )

        flattened = _flatten_environment_subdir(bundle)

        assert flattened == ["real.txt"]
        assert (bundle / "real.txt").is_file()
        assert not (bundle / "link.txt").exists()

    def test_deep_environment_tree_creates_parent_dirs(
        self, tmp_path: Path,
    ) -> None:
        bundle = tmp_path / "bundle"
        deep = bundle / "environment" / "a" / "b" / "c"
        deep.mkdir(parents=True)
        (deep / "leaf.txt").write_text("leaf\n")

        _flatten_environment_subdir(bundle)

        assert (bundle / "a" / "b" / "c" / "leaf.txt").read_text() == "leaf\n"
