from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator import candidate_source_publication as publication
from loom_cli.rollout.operator.model import CandidateBinding

from .test_candidate import FETCH_URL, make_config

SHA = "a" * 40
TREE = "b" * 40
BASE = "c" * 40


def _binding() -> CandidateBinding:
    return CandidateBinding(
        remote_url=FETCH_URL,
        target_ref="origin/dev",
        resolved_sha=SHA,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree=TREE,
        approved_base_sha=BASE,
    )


def _config(tmp_path: Path):  # type: ignore[no-untyped-def]
    del tmp_path
    return replace(
        make_config(Path(f"/opt/loom-staging-runner/candidates/{SHA}/repo")),
        source_mode="sealed-cumulative",
        source_commit_sha=SHA,
        source_tree_sha=TREE,
        source_base_sha=BASE,
    )


def _record(action: str) -> dict[str, object]:
    return {
        "repo_action": action,
        "repo_dir": (
            "/shared_work2/qianyi/.loom-staging-rollout/worker-repos/"
            "loom-remote-worker-staging-aaaaaaa"
        ),
        "repo_group_id": 2007,
        "repo_head": SHA,
        "repo_mode": "0750",
        "repo_status": "clean",
    }


def test_prepare_publishes_exact_candidate_and_emits_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())
    calls: list[dict[str, object]] = []

    def materialize(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return _record("created")

    result = publication.publish_installed_candidate_source(
        config=_config(tmp_path),
        candidate=_binding(),
        operation="prepare",
        materialize=materialize,
    )

    assert calls == [
        {
            "expected_ref": "staging-aaaaaaa",
            "repo_dir": publication._SHARED_REPOSITORY_ROOT / "loom-remote-worker-staging-aaaaaaa",
            "resolved_sha": SHA,
            "source_repo": Path(f"/opt/loom-staging-runner/candidates/{SHA}/repo"),
        }
    ]
    digest = str(result.pop("evidence_sha256"))
    assert (
        digest
        == hashlib.sha256(
            json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert result == {
        "action": "created",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "image_tag": "staging-aaaaaaa",
        "service_uid": os.getuid(),
        "status": "clean",
    }


def test_check_is_read_only_and_requires_matched_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())
    calls: list[dict[str, object]] = []

    def verify(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return _record("matched")

    result = publication.publish_installed_candidate_source(
        config=_config(tmp_path),
        candidate=_binding(),
        operation="check",
        verify=verify,
    )

    assert len(calls) == 1
    assert "source_repo" not in calls[0]
    assert result["action"] == "matched"


@pytest.mark.parametrize(
    "record",
    [
        {**_record("matched"), "repo_head": "d" * 40},
        {**_record("matched"), "repo_mode": "0770"},
        {**_record("matched"), "repo_status": "dirty"},
        {**_record("matched"), "repo_group_id": 0},
        {**_record("matched"), "repo_group_id": True},
        {**_record("matched"), "unexpected": "field"},
        _record("created"),
        {**_record("unexpected")},
    ],
)
def test_publication_rejects_unbound_materializer_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record: dict[str, object],
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())

    with pytest.raises(
        publication.CandidateSourcePublicationError,
        match="evidence is invalid",
    ):
        publication.publish_installed_candidate_source(
            config=_config(tmp_path),
            candidate=_binding(),
            operation="check",
            verify=lambda **_kwargs: record,
        )


def test_publication_rejects_non_staging_or_source_mode_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())
    config = replace(_config(tmp_path), namespace="other")

    with pytest.raises(
        publication.CandidateSourcePublicationError,
        match="binding is invalid",
    ):
        publication.publish_installed_candidate_source(
            config=config,
            candidate=_binding(),
            operation="prepare",
        )


def test_publication_rejects_cross_candidate_runtime_or_tree_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())
    cross_candidate = replace(
        _config(tmp_path),
        runner_repo=Path(f"/opt/loom-staging-runner/candidates/{'d' * 40}/repo"),
    )
    with pytest.raises(
        publication.CandidateSourcePublicationError,
        match="binding is invalid",
    ):
        publication.publish_installed_candidate_source(
            config=cross_candidate,
            candidate=_binding(),
            operation="check",
        )

    with pytest.raises(
        publication.CandidateSourcePublicationError,
        match="binding is invalid",
    ):
        publication.publish_installed_candidate_source(
            config=_config(tmp_path),
            candidate=_binding(),
            candidate_tree="d" * 40,
            operation="check",
        )


def test_publication_rejects_non_mapping_materializer_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())

    with pytest.raises(
        publication.CandidateSourcePublicationError,
        match="failed safely",
    ):
        publication.publish_installed_candidate_source(
            config=_config(tmp_path),
            candidate=_binding(),
            operation="check",
            verify=lambda **_kwargs: None,  # type: ignore[return-value]
        )


def test_merged_candidate_accepts_separately_verified_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_service_uid", lambda _config: os.getuid())
    candidate = replace(
        _binding(),
        source_mode="merged-dev",
        resolved_tree=None,
        approved_base_sha=None,
    )

    result = publication.publish_installed_candidate_source(
        config=make_config(Path(f"/opt/loom-staging-runner/candidates/{SHA}/repo")),
        candidate=candidate,
        candidate_tree=TREE,
        operation="check",
        verify=lambda **_kwargs: _record("matched"),
    )

    assert result["candidate_tree"] == TREE
