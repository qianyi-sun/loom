from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from loom_cli import cluster_cmd
from loom_cli.__main__ import main
from loom_cli.cluster_rollout_evidence import (
    build_docker_image_evidence,
    normalize_cluster_status_format,
)


def test_docker_image_evidence_reads_repo_tags_without_repository_field() -> None:
    evidence = build_docker_image_evidence(
        [
            {
                "Id": "sha256:" + ("a" * 64),
                "RepoTags": [
                    "ghcr.io/qianyi-sun/loom-service:staging-cb6af75",
                ],
            },
        ],
        expected_repo_tags=[
            "ghcr.io/qianyi-sun/loom-service:staging-cb6af75",
        ],
    )

    assert evidence["ok"] is True
    assert evidence["diagnostics"] == []
    assert evidence["images"] == [
        {
            "id": "sha256:" + ("a" * 64),
            "repo_tags": [
                "ghcr.io/qianyi-sun/loom-service:staging-cb6af75",
            ],
            "repo_digests": [],
        },
    ]


def test_docker_image_evidence_reports_structured_missing_tag() -> None:
    evidence = build_docker_image_evidence(
        [
            {
                "Id": "sha256:" + ("b" * 64),
                "RepoTags": [
                    "ghcr.io/qianyi-sun/loom-worker:staging-cb6af75",
                ],
            },
        ],
        expected_repo_tags=[
            "ghcr.io/qianyi-sun/loom-service:staging-cb6af75",
        ],
    )

    assert evidence["ok"] is False
    assert evidence["diagnostics"] == [
        {
            "code": "docker_expected_repo_tag_missing",
            "message": (
                "Expected Docker repo tag was not present in image inspect "
                "RepoTags."
            ),
            "expected_repo_tag": (
                "ghcr.io/qianyi-sun/loom-service:staging-cb6af75"
            ),
        },
    ]


def test_docker_image_evidence_omits_config_env_secrets() -> None:
    evidence = build_docker_image_evidence(
        [
            {
                "Id": "sha256:" + ("c" * 64),
                "RepoTags": ["loom-service:staging-cb6af75"],
                "Config": {
                    "Env": [
                        "LOOM_ADMIN_TOKEN=should_not_print",
                        "MODEL_PROVIDER_API_KEY=also_should_not_print",
                    ],
                },
            },
        ],
        expected_repo_tags=["loom-service:staging-cb6af75"],
    )

    rendered = json.dumps(evidence)
    assert "should_not_print" not in rendered
    assert "also_should_not_print" not in rendered
    assert "Config" not in rendered


def test_cluster_status_format_accepts_legacy_text_alias() -> None:
    normalized, diagnostics = normalize_cluster_status_format("text")

    assert normalized == "table"
    assert diagnostics == [
        {
            "code": "cluster_status_format_alias",
            "message": (
                "`loom cluster status --format text` is a legacy spelling; "
                "using `table`."
            ),
            "requested_format": "text",
            "normalized_format": "table",
        },
    ]


def test_rollout_evidence_docker_images_cli_returns_structured_failure(
    tmp_path: Path,
    capsys,
) -> None:
    inspect_json = tmp_path / "inspect.json"
    inspect_json.write_text(
        json.dumps([
            {
                "Id": "sha256:" + ("d" * 64),
                "RepoTags": ["loom-worker:staging-cb6af75"],
                "Config": {"Env": ["LOOM_WORKER_TOKEN=should_not_print"]},
            },
        ]),
        encoding="utf-8",
    )

    rc = main([
        "cluster",
        "rollout-evidence",
        "docker-images",
        "--inspect-json",
        str(inspect_json),
        "--expect-repo-tag",
        "loom-service:staging-cb6af75",
    ])

    assert rc == 1
    out = capsys.readouterr().out
    body = json.loads(out)
    assert body["ok"] is False
    assert body["diagnostics"][0]["code"] == "docker_expected_repo_tag_missing"
    assert "should_not_print" not in out


def test_rollout_evidence_cluster_status_reports_structured_collection_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    def fail_load_clients(context: str | None):  # type: ignore[no-untyped-def]
        raise RuntimeError("kubernetes client unavailable")

    monkeypatch.setattr(cluster_cmd, "_load_clients", fail_load_clients)

    rc = cluster_cmd._rollout_evidence_cluster_status(
        Namespace(context=None, namespace="loom", status_format="json")
    )

    assert rc == 2
    body = json.loads(capsys.readouterr().out)
    assert body == {
        "diagnostics": [
            {
                "code": "cluster_status_evidence_unavailable",
                "message": "kubernetes client unavailable",
            },
        ],
        "ok": False,
        "schema_version": 1,
    }
