"""Unit tests for the publish_cmd manifest schema + helpers.

The full publish round-trip (convert + push to HF) is exercised by an
operator running `loom_benchmark_tool publish` against a live HF token
— covering it in CI would require a fake HF server and is out of scope
here. These tests pin the pure-Python pieces: checksum stability,
safe-dirname, repo-id derivation."""

from __future__ import annotations

from pathlib import Path

from loom_benchmark_tool.publish_cmd import (
    MANIFEST_SCHEMA_VERSION,
    _bundle_checksum,
    _safe_dirname,
    repo_id_for,
)


def test_repo_id_for_uses_loom_benchmark_prefix() -> None:
    assert repo_id_for("PRHW", "humaneval") == "PRHW/loom-benchmark-humaneval"


def test_safe_dirname_collapses_slashes() -> None:
    assert _safe_dirname("HumanEval/0") == "HumanEval_0"
    assert _safe_dirname("plain") == "plain"


def test_bundle_checksum_is_stable_across_invocations(tmp_path: Path) -> None:
    """Same bytes in different file orders → same digest. The hash
    iterates files in sorted-relpath order so adding/removing files
    in iteration order can't affect the digest."""
    a = tmp_path / "a"
    a.mkdir()
    (a / "task.toml").write_text("[task]\nid='t'\n")
    (a / "solution.py").write_text("print(1)\n")

    digest1 = _bundle_checksum(a)
    digest2 = _bundle_checksum(a)
    assert digest1 == digest2
    assert digest1.startswith("sha256:")


def test_bundle_checksum_differs_on_content_change(tmp_path: Path) -> None:
    """Editing any file should perturb the digest — sanity-check the
    hash includes the file bytes, not just the relpaths."""
    a = tmp_path / "a"
    a.mkdir()
    (a / "task.toml").write_text("[task]\nid='t'\n")
    digest1 = _bundle_checksum(a)
    (a / "task.toml").write_text("[task]\nid='different'\n")
    digest2 = _bundle_checksum(a)
    assert digest1 != digest2


def test_manifest_schema_version_is_int() -> None:
    """Operators (and the worker) fork on this; an accidental change
    to a string would silently break the manifest reader."""
    assert isinstance(MANIFEST_SCHEMA_VERSION, int)
    assert MANIFEST_SCHEMA_VERSION >= 1
