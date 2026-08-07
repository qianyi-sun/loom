from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.ci_image_candidate import (
    CandidateError,
    aggregate_records,
    parse_matrix,
    validate_index,
    validate_record,
)

HEAD_SHA = "1" * 40
BASE_SHA = "2" * 40
TREE_SHA = "3" * 40
REPOSITORY = "qianyi-sun/loom"


def _matrix() -> list[dict[str, str]]:
    return [
        {
            "image": "service",
            "image_name": "loom-service",
            "dockerfile": "deploy/Dockerfile.service",
            "context": ".",
            "architecture": architecture,
            "platform": f"linux/{architecture}",
        }
        for architecture in ("amd64", "arm64")
    ]


def _record(architecture: str) -> dict[str, object]:
    row = next(row for row in _matrix() if row["architecture"] == architecture)
    digest_character = "a" if architecture == "amd64" else "b"
    return {
        "schema": 1,
        "repository": REPOSITORY,
        "pull_request": 1199,
        "head_sha": HEAD_SHA,
        "base_sha": BASE_SHA,
        "tree_sha": TREE_SHA,
        "run_id": 12345,
        "run_attempt": 2,
        **row,
        "artifact_name": f"image-candidate-archive-service-{architecture}-attempt-2",
        "archive_sha256": digest_character * 64,
        "archive_size": 1024,
    }


def _record_files(tmp_path: Path) -> list[Path]:
    paths: list[Path] = []
    for architecture in ("amd64", "arm64"):
        path = tmp_path / f"service-{architecture}.json"
        path.write_text(json.dumps(_record(architecture)), encoding="utf-8")
        paths.append(path)
    return paths


def test_parse_matrix_requires_native_architecture_platform_pairs() -> None:
    matrix = _matrix()
    matrix[1]["platform"] = "linux/amd64"

    with pytest.raises(CandidateError, match="inconsistent architecture/platform"):
        parse_matrix(json.dumps(matrix))


def test_parse_matrix_rejects_unexpected_fields() -> None:
    matrix = _matrix()
    matrix[0]["candidate_digest"] = f"sha256:{'a' * 64}"

    with pytest.raises(CandidateError, match="differs from schema"):
        parse_matrix(json.dumps(matrix))


def test_record_rejects_artifact_from_another_image() -> None:
    record = _record("amd64")
    record["artifact_name"] = "image-candidate-archive-worker-amd64-attempt-2"

    with pytest.raises(CandidateError, match="does not match its image"):
        validate_record(record)


def test_aggregate_records_preserves_exact_matrix_order_and_archive_identity(
    tmp_path: Path,
) -> None:
    paths = _record_files(tmp_path)

    index = aggregate_records(paths, _matrix())

    assert index["head_sha"] == HEAD_SHA
    assert index["base_sha"] == BASE_SHA
    assert [build["architecture"] for build in index["builds"]] == [
        "amd64",
        "arm64",
    ]
    assert [build["candidate_artifact"] for build in index["builds"]] == [
        "image-candidate-archive-service-amd64-attempt-2",
        "image-candidate-archive-service-arm64-attempt-2",
    ]
    assert [build["archive_sha256"] for build in index["builds"]] == [
        "a" * 64,
        "b" * 64,
    ]


def test_aggregate_records_fails_closed_on_missing_architecture(tmp_path: Path) -> None:
    paths = _record_files(tmp_path)

    with pytest.raises(CandidateError, match="differ from expected matrix"):
        aggregate_records(paths[:1], _matrix())


def test_aggregate_records_rejects_mixed_run_attempts(tmp_path: Path) -> None:
    paths = _record_files(tmp_path)
    changed = _record("arm64")
    changed["run_attempt"] = 3
    changed["artifact_name"] = "image-candidate-archive-service-arm64-attempt-3"
    paths[1].write_text(json.dumps(changed), encoding="utf-8")

    with pytest.raises(CandidateError, match="one source identity"):
        aggregate_records(paths, _matrix())


def test_record_rejects_artifact_from_another_attempt() -> None:
    record = _record("amd64")
    record["artifact_name"] = "image-candidate-archive-service-amd64-attempt-1"

    with pytest.raises(CandidateError, match="does not match its image"):
        validate_record(record)


def test_verify_index_binds_merge_to_exact_head_base_tree_run_and_attempt(
    tmp_path: Path,
) -> None:
    index = aggregate_records(_record_files(tmp_path), _matrix())

    verified = validate_index(
        index,
        expected_matrix=_matrix(),
        repository=REPOSITORY,
        pull_request=1199,
        head_sha=HEAD_SHA,
        base_sha=BASE_SHA,
        tree_sha=TREE_SHA,
        run_id=12345,
        run_attempt=2,
    )

    assert verified == index


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("head_sha", "4" * 40),
        ("base_sha", "5" * 40),
        ("tree_sha", "6" * 40),
        ("run_id", 54321),
        ("run_attempt", 3),
    ],
)
def test_verify_index_invalidates_changed_source_identity(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    index = aggregate_records(_record_files(tmp_path), _matrix())
    expected: dict[str, object] = {
        "repository": REPOSITORY,
        "pull_request": 1199,
        "head_sha": HEAD_SHA,
        "base_sha": BASE_SHA,
        "tree_sha": TREE_SHA,
        "run_id": 12345,
        "run_attempt": 2,
    }
    expected[field] = value

    with pytest.raises(CandidateError, match="does not match the merge"):
        validate_index(index, expected_matrix=_matrix(), **expected)
