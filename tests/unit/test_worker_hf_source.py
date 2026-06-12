"""Worker hf:// source URL parser.

The materialize path itself requires HF Hub roundtrip + filesystem
operations; tested operator-side via a register + claim. Here we pin
the pure-Python parser so URL-shape changes break loudly in CI."""

from __future__ import annotations

import pytest

from loom_worker.main_loop import _parse_hf_source


def test_parse_with_explicit_revision() -> None:
    repo, rev, path = _parse_hf_source(
        "hf://PRHW/loom-benchmark-humaneval@main/HumanEval_0/",
    )
    assert repo == "PRHW/loom-benchmark-humaneval"
    assert rev == "main"
    assert path == "HumanEval_0/"


def test_parse_without_revision_defaults_to_main() -> None:
    repo, rev, path = _parse_hf_source(
        "hf://PRHW/loom-benchmark-aime/HumanEval_0/",
    )
    assert repo == "PRHW/loom-benchmark-aime"
    assert rev == "main"
    assert path == "HumanEval_0/"


def test_parse_with_sha_revision() -> None:
    _, rev, path = _parse_hf_source(
        "hf://PRHW/loom-benchmark-aime@3d20ed5e6f/0/",
    )
    assert rev == "3d20ed5e6f"
    assert path == "0/"


def test_parse_with_multi_segment_path() -> None:
    repo, _, path = _parse_hf_source(
        "hf://PRHW/loom-benchmark-swe-bench@main/django__django/0001/",
    )
    assert repo == "PRHW/loom-benchmark-swe-bench"
    assert path == "django__django/0001/"


def test_parse_rejects_non_hf_scheme() -> None:
    with pytest.raises(ValueError, match="not an hf://"):
        _parse_hf_source("s3://bucket/key/")


def test_parse_rejects_missing_repo() -> None:
    with pytest.raises(ValueError, match="missing repo"):
        _parse_hf_source("hf://PRHW")


def test_parse_rejects_empty_org() -> None:
    with pytest.raises(ValueError, match="missing org or repo"):
        _parse_hf_source("hf:///loom-benchmark-x/0/")
