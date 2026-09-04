from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ops.trusted_image_release_controller import (
    GitHubReleaseClient,
    ReconcileError,
    ReconcileRequest,
    reconcile_release,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
HEAD = "f" * 40
PARENT = "e" * 40
OLDER = "d" * 40


@dataclass
class FakeClient:
    head: str = HEAD
    runs: list[dict[str, Any]] = field(default_factory=list)
    release_artifact_run_ids: set[int] = field(default_factory=set)
    artifact_checks: list[int] = field(default_factory=list)
    dispatches: list[tuple[str, str]] = field(default_factory=list)

    def get_branch_head(self, branch: str) -> str:
        assert branch == "dev"
        return self.head

    def list_image_runs(self, branch: str) -> list[dict[str, Any]]:
        assert branch == "dev"
        return self.runs

    def has_trusted_release_artifact(self, run: dict[str, Any]) -> bool:
        run_id = run["id"]
        self.artifact_checks.append(run_id)
        return run_id in self.release_artifact_run_ids

    def dispatch_images(self, *, branch: str, base_sha: str) -> None:
        self.dispatches.append((branch, base_sha))


@dataclass
class FakeHistory:
    distances: dict[str, int] = field(default_factory=lambda: {PARENT: 1, OLDER: 4})

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        assert descendant == HEAD
        return ancestor in self.distances

    def distance(self, ancestor: str, descendant: str) -> int:
        assert descendant == HEAD
        return self.distances[ancestor]


def _request(**overrides: Any) -> ReconcileRequest:
    values = {
        "repository": "qianyi-sun/loom",
        "branch": "dev",
        "checkout_head": HEAD,
    }
    values.update(overrides)
    return ReconcileRequest(**values)


def _run(
    *,
    sha: str,
    event: str = "push",
    status: str = "completed",
    conclusion: str | None = "success",
    title: str = "gate=full / head=release",
    run_id: int = 1,
    actor: str = "github-actions[bot]",
) -> dict[str, Any]:
    return {
        "id": run_id,
        "run_attempt": 1,
        "head_sha": sha,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "display_title": title,
        "actor": {"login": actor},
    }


def test_github_client_binds_exact_nonexpired_release_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubReleaseClient(
        repository="qianyi-sun/loom",
        token="test-token",
        api_url="https://api.github.test",
    )
    observed_paths: list[str] = []

    def artifact_response(path: str) -> dict[str, Any]:
        observed_paths.append(path)
        return {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-11-attempt-1",
                    "expired": False,
                    "size_in_bytes": 3745,
                    "workflow_run": {"id": 11, "head_sha": HEAD},
                }
            ],
        }

    monkeypatch.setattr(client, "_json", artifact_response)

    assert client.has_trusted_release_artifact(_run(sha=HEAD, run_id=11)) is True
    assert observed_paths == [
        "/repos/qianyi-sun/loom/actions/runs/11/artifacts?"
        "name=personal-dev-trusted-release-run-11-attempt-1&per_page=100"
    ]


def test_github_client_paginates_all_image_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubReleaseClient(
        repository="qianyi-sun/loom",
        token="test-token",
        api_url="https://api.github.test",
    )
    first_page = [
        {"id": run_id, "head_branch": "dev", "event": "push"}
        for run_id in range(1, 101)
    ]
    matching_dispatch = {
        "id": 101,
        "head_branch": "dev",
        "event": "workflow_dispatch",
    }
    observed_paths: list[str] = []

    def runs_response(path: str) -> dict[str, Any]:
        observed_paths.append(path)
        if "page=2" in path:
            return {
                "total_count": 103,
                "workflow_runs": [
                    matching_dispatch,
                    {"id": 102, "head_branch": "feature", "event": "push"},
                    {"id": 103, "head_branch": "dev", "event": "pull_request"},
                ],
            }
        return {"total_count": 103, "workflow_runs": first_page}

    monkeypatch.setattr(client, "_json", runs_response)

    assert client.list_image_runs("dev") == [*first_page, matching_dispatch]
    assert observed_paths == [
        "/repos/qianyi-sun/loom/actions/workflows/302898384/runs?"
        "per_page=100&page=1",
        "/repos/qianyi-sun/loom/actions/workflows/302898384/runs?"
        "per_page=100&page=2",
    ]


def test_github_client_reports_missing_release_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubReleaseClient(
        repository="qianyi-sun/loom",
        token="test-token",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(
        client,
        "_json",
        lambda _path: {"total_count": 0, "artifacts": []},
    )

    assert client.has_trusted_release_artifact(_run(sha=HEAD, run_id=11)) is False


def test_github_client_rejects_boolean_artifact_workflow_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubReleaseClient(
        repository="qianyi-sun/loom",
        token="test-token",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(
        client,
        "_json",
        lambda _path: {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-1-attempt-1",
                    "expired": False,
                    "size_in_bytes": 3745,
                    "workflow_run": {"id": True, "head_sha": HEAD},
                }
            ],
        },
    )

    with pytest.raises(ReconcileError, match="trusted release artifact"):
        client.has_trusted_release_artifact(_run(sha=HEAD, run_id=1))


@pytest.mark.parametrize(
    "payload",
    [
        {"total_count": 2, "artifacts": []},
        {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-11-attempt-1",
                    "expired": True,
                    "size_in_bytes": 3745,
                    "workflow_run": {"id": 11, "head_sha": HEAD},
                }
            ],
        },
        {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-12-attempt-1",
                    "expired": False,
                    "size_in_bytes": 3745,
                    "workflow_run": {"id": 11, "head_sha": HEAD},
                }
            ],
        },
        {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-11-attempt-1",
                    "expired": False,
                    "size_in_bytes": 0,
                    "workflow_run": {"id": 11, "head_sha": HEAD},
                }
            ],
        },
        {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 99,
                    "name": "personal-dev-trusted-release-run-11-attempt-1",
                    "expired": False,
                    "size_in_bytes": 3745,
                    "workflow_run": {"id": 12, "head_sha": HEAD},
                }
            ],
        },
    ],
)
def test_github_client_rejects_ambiguous_or_unbound_release_artifact(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
) -> None:
    client = GitHubReleaseClient(
        repository="qianyi-sun/loom",
        token="test-token",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(client, "_json", lambda _path: payload)

    with pytest.raises(ReconcileError, match="trusted release artifact"):
        client.has_trusted_release_artifact(_run(sha=HEAD, run_id=11))


def test_dispatches_from_nearest_successful_trusted_ancestor() -> None:
    client = FakeClient(
        runs=[_run(sha=OLDER, run_id=3), _run(sha=PARENT, run_id=7)],
        release_artifact_run_ids={3, 7},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record == {
        "schema": "loom-trusted-image-release-reconcile-v1",
        "decision": "dispatch_requested",
        "branch": "dev",
        "head_sha": HEAD,
        "base_sha": PARENT,
        "base_run_id": 7,
        "commit_distance": 1,
    }
    assert client.dispatches == [("dev", PARENT)]


def test_completed_success_without_release_artifact_reconciles_from_published_ancestor() -> None:
    client = FakeClient(
        runs=[_run(sha=HEAD, run_id=11), _run(sha=PARENT, run_id=7)],
        release_artifact_run_ids={7},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record == {
        "schema": "loom-trusted-image-release-reconcile-v1",
        "decision": "dispatch_requested",
        "branch": "dev",
        "head_sha": HEAD,
        "base_sha": PARENT,
        "base_run_id": 7,
        "commit_distance": 1,
    }
    assert client.dispatches == [("dev", PARENT)]


def test_skips_unpublished_nearer_ancestor_for_published_older_ancestor() -> None:
    client = FakeClient(
        runs=[_run(sha=PARENT, run_id=7), _run(sha=OLDER, run_id=3)],
        release_artifact_run_ids={3},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record["base_sha"] == OLDER
    assert record["base_run_id"] == 3
    assert record["commit_distance"] == 4
    assert client.dispatches == [("dev", OLDER)]


def test_checks_ancestor_artifacts_in_distance_order_and_stops_at_nearest() -> None:
    client = FakeClient(
        runs=[_run(sha=OLDER, run_id=3), _run(sha=PARENT, run_id=7)],
        release_artifact_run_ids={3, 7},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record["base_run_id"] == 7
    assert client.artifact_checks == [7]


@pytest.mark.parametrize(
    ("run", "decision"),
    [
        (_run(sha=HEAD, status="in_progress", conclusion=None), "already_active"),
        (_run(sha=HEAD), "already_published"),
        (_run(sha=HEAD, conclusion="failure"), "blocked_failed_release"),
        (
            _run(
                sha=HEAD,
                event="workflow_dispatch",
                title="gate=trusted-publish / head=exact / base=prior",
            ),
            "already_published",
        ),
    ],
)
def test_exact_head_active_or_successful_release_is_idempotent(
    run: dict[str, Any], decision: str
) -> None:
    client = FakeClient(
        runs=[run],
        release_artifact_run_ids={run["id"]} if decision == "already_published" else set(),
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record["decision"] == decision
    assert client.dispatches == []


def test_ordinary_manual_dispatch_is_not_trusted_release_evidence() -> None:
    client = FakeClient(
        runs=[
            _run(
                sha=HEAD,
                event="workflow_dispatch",
                title="gate=manual / head=exact",
            ),
            _run(sha=PARENT),
        ],
        release_artifact_run_ids={1},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record["decision"] == "dispatch_requested"
    assert client.dispatches == [("dev", PARENT)]


def test_human_cannot_block_reconciliation_with_trusted_run_name() -> None:
    client = FakeClient(
        runs=[
            _run(
                sha=HEAD,
                event="workflow_dispatch",
                status="completed",
                conclusion="failure",
                title="gate=trusted-publish / head=exact / base=prior",
                actor="qianyi-sun",
            ),
            _run(sha=PARENT),
        ],
        release_artifact_run_ids={1},
    )

    record = reconcile_release(_request(), client, FakeHistory())

    assert record["decision"] == "dispatch_requested"
    assert client.dispatches == [("dev", PARENT)]


@pytest.mark.parametrize(
    ("reconcile_request", "client", "message"),
    [
        (_request(branch="feature"), FakeClient(), "dev or main"),
        (_request(checkout_head="A" * 40), FakeClient(), "lowercase"),
        (_request(), FakeClient(head=PARENT), "current protected branch head"),
        (_request(), FakeClient(runs=[]), "no successful trusted ancestor"),
        (
            _request(),
            FakeClient(runs=[_run(sha=PARENT, status="completed", conclusion="failure")]),
            "no successful trusted ancestor",
        ),
    ],
)
def test_untrusted_or_unanchored_reconciliation_fails_closed(
    reconcile_request: ReconcileRequest, client: FakeClient, message: str
) -> None:
    with pytest.raises(ReconcileError, match=message):
        reconcile_release(reconcile_request, client, FakeHistory())
    assert client.dispatches == []


def test_controller_workflow_is_default_branch_only_and_least_privilege() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/trusted-image-release-controller.yml").read_text(
            encoding="utf-8"
        )
    )
    on_config = workflow.get("on", workflow.get(True))

    assert set(on_config) == {"schedule", "workflow_dispatch"}
    assert on_config["schedule"] == [{"cron": "17,47 * * * *"}]
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["reconcile"]
    assert job["permissions"] == {"actions": "write", "contents": "read"}
    checkout = job["steps"][0]
    assert checkout["with"] == {"fetch-depth": 0, "persist-credentials": False}
    script = job["steps"][1]
    assert script["env"]["GITHUB_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert "--branch dev" in script["run"]
    assert '--checkout-head "$GITHUB_SHA"' in script["run"]
