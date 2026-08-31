from __future__ import annotations

import io
import json
import urllib.error
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from loom_control_plane import ci_runner_lease_broker as leases
from loom_control_plane import ci_runner_route_controller as routes

HEAD_SHA = "a" * 40
RUNTIME_SHA = "c" * 40
RUNTIME_TREE = "b" * 40
WORKFLOW_BLOB_SHA = "d" * 40
MERGE_SHA = "e" * 40
MERGE_TREE = "f" * 40
PR_HEAD_SHA = "1" * 40
CHANGED_WORKFLOW_BLOB_SHA = "2" * 40
SECOND_MERGE_SHA = "3" * 40
SECOND_MERGE_TREE = "4" * 40
SECOND_PR_HEAD_SHA = "5" * 40
SECOND_WORKFLOW_BLOB_SHA = "6" * 40
PUBLISHER_APP_ID = 424_242
NOW = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)


def _config() -> leases.LeaseBrokerConfig:
    return leases.LeaseBrokerConfig(
        repository="qianyi-sun/loom",
        oldlab_labels=(
            "self-hosted",
            "linux",
            "x64",
            "loom-ci",
            "oldlab-5",
            "ephemeral-kvm",
        ),
        capacities={"normal": 5, "image": 4, "smoke": 2},
    )


def _request(
    *, workflow_name: str = "CI", run_id: int = 30_000, job_count: int = 7
) -> leases.RouteRequest:
    workflow_id, _, allowed_jobs, _ = leases.WORKFLOW_CLASS_CONTRACTS[workflow_name]
    return leases.RouteRequest(
        repository="qianyi-sun/loom",
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        workflow_run_id=run_id,
        run_attempt=1,
        head_sha=HEAD_SHA,
        job_keys=tuple(allowed_jobs[:job_count]),
    )


def _zip_request(
    request: leases.RouteRequest,
    *,
    filename: str = routes.ROUTE_REQUEST_FILENAME,
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, json.dumps(request.public_dict()))
    return buffer.getvalue()


class FakeRouteAPI:
    app_id = PUBLISHER_APP_ID

    def __init__(self, request: leases.RouteRequest) -> None:
        self.request = request
        self.artifacts: list[dict[str, object]] = [
            {
                "id": 71,
                "name": (
                    f"{routes.ARTIFACT_PREFIX}{request.workflow_id}-"
                    f"{request.workflow_run_id}-{request.run_attempt}"
                ),
                "expired": False,
                "created_at": NOW.isoformat().replace("+00:00", "Z"),
                "workflow_run": {
                    "id": request.workflow_run_id,
                    "head_sha": request.head_sha,
                },
            }
        ]
        self.archives = {71: _zip_request(request)}
        self.runs: dict[int, dict[str, object]] = {
            request.workflow_run_id: {
                "id": request.workflow_run_id,
                "run_attempt": request.run_attempt,
                "workflow_id": request.workflow_id,
                "head_sha": request.head_sha,
                "name": request.workflow_name,
                "event": "pull_request",
                "status": "in_progress",
                "repository": {"full_name": request.repository},
            }
        }
        workflow_path = routes.WORKFLOW_PATHS[request.workflow_name]
        self.blobs = {
            (workflow_path, request.head_sha): WORKFLOW_BLOB_SHA,
            **{(path, RUNTIME_SHA): WORKFLOW_BLOB_SHA for path in routes.WORKFLOW_PATHS.values()},
        }
        self.dev_head = RUNTIME_SHA
        self.commits: dict[str, dict[str, object]] = {
            RUNTIME_SHA: {
                "sha": RUNTIME_SHA,
                "commit": {"tree": {"sha": RUNTIME_TREE}},
                "parents": [],
            }
        }
        self.compares: dict[tuple[str, str], dict[str, object]] = {}
        self.pulls: dict[str, list[dict[str, object]]] = {}
        self.checks: dict[tuple[str, str], list[dict[str, object]]] = {}
        self.created_checks: list[dict[str, object]] = []
        self.dispatches: list[dict[str, object]] = []
        self.publisher_error = False
        self.jobs: dict[tuple[int, int], list[dict[str, object]]] = {
            (request.workflow_run_id, request.run_attempt): []
        }

    def branch_head(self, branch: str) -> str:
        assert branch == routes.TRUSTED_BRANCH
        return self.dev_head

    def commit(self, ref: str) -> dict[str, object]:
        return self.commits[ref]

    def compare_commits(self, base: str, head: str) -> dict[str, object]:
        return self.compares[(base, head)]

    def associated_pull_requests(self, commit_sha: str) -> list[dict[str, object]]:
        return self.pulls.get(commit_sha, [])

    def active_workflow_runs(self, workflow_id: int) -> list[dict[str, object]]:
        return [
            run
            for run in self.runs.values()
            if run["workflow_id"] == workflow_id and run["status"] == "in_progress"
        ]

    def route_artifact(
        self, *, workflow_id: int, workflow_run_id: int, run_attempt: int
    ) -> dict[str, object] | None:
        expected = f"{routes.ARTIFACT_PREFIX}{workflow_id}-{workflow_run_id}-{run_attempt}"
        matches = [
            artifact
            for artifact in self.artifacts
            if artifact["name"] == expected and artifact["expired"] is False
        ]
        if len(matches) > 1:
            raise routes.RouteControllerError("GitHub route artifact identity is ambiguous")
        return matches[0] if matches else None

    def download_artifact(self, artifact_id: int) -> bytes:
        return self.archives[artifact_id]

    def workflow_run(self, run_id: int) -> dict[str, object]:
        return self.runs[run_id]

    def content_blob_sha(self, path: str, ref: str) -> str:
        return self.blobs[(path, ref)]

    def check_runs(self, head_sha: str, name: str) -> list[dict[str, object]]:
        return self.checks.get((head_sha, name), [])

    def publish(self, payload: dict[str, object]) -> dict[str, object]:
        self.dispatches.append(dict(payload))
        if self.publisher_error:
            raise routes.RouteControllerError("direct CheckRun publisher unavailable")
        created = {**payload, "app": {"id": self.app_id}}
        self.created_checks.append(created)
        self.checks.setdefault((created["head_sha"], created["name"]), []).append(created)
        return created

    def workflow_jobs(self, run_id: int, attempt: int) -> list[dict[str, object]]:
        return self.jobs[(run_id, attempt)]


def _configure_protected_merge(
    api: FakeRouteAPI,
    *,
    merge_sha: str = MERGE_SHA,
    merge_tree: str = MERGE_TREE,
    parent_sha: str = RUNTIME_SHA,
    pull_head_sha: str = PR_HEAD_SHA,
    changed_workflow: str = "images",
    changed_blob_sha: str = CHANGED_WORKFLOW_BLOB_SHA,
    dev_head: str | None = None,
) -> None:
    api.dev_head = dev_head or merge_sha
    api.commits[merge_sha] = {
        "sha": merge_sha,
        "commit": {"tree": {"sha": merge_tree}},
        "parents": [{"sha": parent_sha}],
    }
    api.compares[(parent_sha, api.dev_head)] = {
        "status": "ahead",
        "ahead_by": 1,
        "behind_by": 0,
        "total_commits": 1,
        "commits": [{"sha": merge_sha, "parents": [{"sha": parent_sha}]}],
    }
    api.pulls[merge_sha] = [
        {
            "number": 1630,
            "state": "closed",
            "merged_at": NOW.isoformat().replace("+00:00", "Z"),
            "merge_commit_sha": merge_sha,
            "head": {
                "sha": pull_head_sha,
                "repo": {"full_name": "qianyi-sun/loom"},
            },
            "base": {
                "ref": routes.TRUSTED_BRANCH,
                "repo": {"full_name": "qianyi-sun/loom"},
            },
        }
    ]
    for index, check_name in enumerate(routes.REQUIRED_SOURCE_CHECKS, start=1):
        api.checks[(pull_head_sha, check_name)] = [
            {
                "id": 10_000 + index,
                "name": check_name,
                "head_sha": pull_head_sha,
                "status": "completed",
                "conclusion": "success",
                "started_at": (NOW + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
                "completed_at": (NOW + timedelta(seconds=index, milliseconds=500))
                .isoformat()
                .replace("+00:00", "Z"),
                "app": {"id": routes.GITHUB_ACTIONS_APP_ID},
                "details_url": (
                    f"https://github.com/qianyi-sun/loom/actions/runs/{20_000 + index}"
                    f"/job/{30_000 + index}"
                ),
            }
        ]
    for workflow_name, path in routes.WORKFLOW_PATHS.items():
        api.blobs[(path, merge_sha)] = (
            changed_blob_sha if workflow_name == changed_workflow else WORKFLOW_BLOB_SHA
        )


def _controller(
    tmp_path: Path, request: leases.RouteRequest
) -> tuple[routes.CiRunnerRouteController, FakeRouteAPI, leases.CiRunnerLeaseBroker]:
    api = FakeRouteAPI(request)
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())
    controller = routes.CiRunnerRouteController(
        api=api,
        broker=broker,
        runtime_sha=RUNTIME_SHA,
        publisher=api,
        now=lambda: NOW,
    )
    return controller, api, broker


def test_controller_publishes_exact_oldlab_first_route(tmp_path: Path) -> None:
    request = _request(job_count=7)
    controller, api, broker = _controller(tmp_path, request)

    result = controller.reconcile()

    assert result.public_dict() == {
        "requests_seen": 1,
        "routes_published": 1,
        "routes_replayed": 0,
        "routes_pending": 0,
        "routes_abandoned": 0,
        "assignments_released": 0,
        "decisions_pruned": 0,
        "runtime_sha": RUNTIME_SHA,
        "trusted_workflow_sha": RUNTIME_SHA,
        "trusted_workflow_digest": broker.current_trusted_workflow_generation().generation_digest,
        "observed_dev_sha": RUNTIME_SHA,
        "generation_lag_commits": 0,
        "workflow_blob_drift": {
            "CI": False,
            "cluster-smoke": False,
            "images": False,
            "staging-smoke": False,
        },
        "generation_promoted": False,
        "generation_blocker": None,
    }
    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["request_sha256"] in api.created_checks[0]["external_id"]
    assert summary["oldlab_eligible"] is True
    assert [item["target"] for item in summary["assignments"]].count("oldlab") == 5
    assert [item["target"] for item in summary["assignments"]].count("github_hosted") == 2
    assert broker.status()["classes"]["normal"]["oldlab_assigned"] == 5
    assert broker.route_decisions()[0].publisher_app_id == PUBLISHER_APP_ID
    status = broker.status(now=NOW)
    assert status["route_generation_healthy"] is True
    assert status["trusted_workflow_observation"]["publisher_app_id"] == PUBLISHER_APP_ID
    assert status["metrics"]["route_decisions_by_eligibility_reason"]["trusted_workflow_match"] == 1
    assert len(api.dispatches) == 1


def test_dynamic_run_name_does_not_replace_workflow_identity(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.runs[request.workflow_run_id]["name"] = (
        "gate=full / head=abc / action=ready_for_review / pull=1245"
    )

    result = controller.reconcile()

    assert result.routes_published == 1


def test_changed_workflow_blob_forces_every_job_to_hosted(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    api.blobs[(routes.WORKFLOW_PATHS["CI"], HEAD_SHA)] = "e" * 40

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert {item["target"] for item in summary["assignments"]} == {"github_hosted"}
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_protected_merge_advances_workflow_generation_without_runtime_rollout(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=2)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    api.blobs[(routes.WORKFLOW_PATHS["images"], request.head_sha)] = CHANGED_WORKFLOW_BLOB_SHA

    result = controller.reconcile()

    assert result.generation_promoted is True
    assert result.runtime_sha == RUNTIME_SHA
    assert result.trusted_workflow_sha == MERGE_SHA
    assert api.app_id == PUBLISHER_APP_ID
    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is True
    assert {item["target"] for item in summary["assignments"]} == {"oldlab"}
    generations = broker.trusted_workflow_generations()
    assert [item.candidate_sha for item in generations] == [RUNTIME_SHA, MERGE_SHA]
    assert generations[1].predecessor_generation_id == generations[0].generation_id
    assert generations[1].evidence()["kind"] == "protected_merge"
    decision = broker.route_decisions()[0]
    assert decision.trust_generation_id == generations[1].generation_id
    assert decision.eligibility_reason == "trusted_workflow_match"


def test_wrong_app_source_check_preserves_generation_and_forces_hosted(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    api.checks[(PR_HEAD_SHA, "images-gate")][0]["app"] = {"id": 999}
    api.blobs[(routes.WORKFLOW_PATHS["images"], request.head_sha)] = CHANGED_WORKFLOW_BLOB_SHA

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.trusted_workflow_sha == RUNTIME_SHA
    assert result.observed_dev_sha == MERGE_SHA
    assert result.generation_lag_commits == 1
    assert result.workflow_blob_drift == {
        "CI": False,
        "cluster-smoke": False,
        "images": True,
        "staging-smoke": False,
    }
    assert result.generation_blocker == (
        "protected source check images-gate is missing or ambiguous"
    )
    assert [item.candidate_sha for item in broker.trusted_workflow_generations()] == [RUNTIME_SHA]
    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert summary["assignments"][0]["target"] == "github_hosted"
    status = broker.status(now=NOW)
    assert status["trusted_workflow_observation"]["promotion_result"] == "blocked"
    assert status["metrics"] == {
        "generation_lag_commits": 1,
        "promotion_blocked": 1,
        "workflow_blob_drift": {
            "CI": 0,
            "cluster-smoke": 0,
            "images": 1,
            "staging-smoke": 0,
        },
        "route_decisions_by_eligibility_reason": {
            "future_request": 0,
            "legacy_schema2_frozen": 0,
            "stale_request": 0,
            "trusted_workflow_match": 0,
            "workflow_blob_drift": 1,
        },
    }
    assert status["route_generation_healthy"] is False


def test_unassociated_dev_commit_preserves_last_trusted_generation(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    api.pulls[MERGE_SHA] = []
    api.blobs[(routes.WORKFLOW_PATHS["images"], request.head_sha)] = CHANGED_WORKFLOW_BLOB_SHA

    result = controller.reconcile()

    assert result.generation_blocker == ("trusted dev commit has ambiguous merge ownership")
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA
    decision = broker.route_decisions()[0]
    assert decision.oldlab_eligible is False
    assert decision.eligibility_reason == "workflow_blob_drift"


def test_same_name_wrong_app_duplicate_cannot_hide_beside_authoritative_check(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    duplicate = dict(api.checks[(PR_HEAD_SHA, "images-gate")][0])
    duplicate["id"] = 99_999
    duplicate["app"] = {"id": 999}
    api.checks[(PR_HEAD_SHA, "images-gate")].append(duplicate)

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.generation_blocker == (
        "protected source check images-gate is missing or ambiguous"
    )
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA


def test_same_app_non_source_duplicate_cannot_hide_beside_authoritative_check(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    duplicate = dict(api.checks[(PR_HEAD_SHA, "images-gate")][0])
    duplicate.update(
        {
            "id": 99_999,
            "details_url": "https://example.invalid/not-a-source-job",
            "started_at": (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        }
    )
    api.checks[(PR_HEAD_SHA, "images-gate")].append(duplicate)

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.generation_blocker == (
        "protected source check images-gate is missing or ambiguous"
    )
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA


def test_same_app_retry_uses_unique_newest_successful_source_check(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    original = api.checks[(PR_HEAD_SHA, "repository-checks")][0]
    original["conclusion"] = "failure"
    retry = dict(original)
    retry.update(
        {
            "id": 99_999,
            "conclusion": "success",
            "started_at": (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            "completed_at": (NOW + timedelta(minutes=1, seconds=1))
            .isoformat()
            .replace("+00:00", "Z"),
        }
    )
    api.checks[(PR_HEAD_SHA, "repository-checks")].append(retry)

    result = controller.reconcile()

    assert result.generation_promoted is True
    assert result.generation_blocker is None
    generation = broker.current_trusted_workflow_generation()
    assert generation.candidate_sha == MERGE_SHA
    assert generation.evidence()["checks"]["repository-checks"]["id"] == 99_999


def test_same_app_retry_with_newest_failure_preserves_generation(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    retry = dict(api.checks[(PR_HEAD_SHA, "repository-checks")][0])
    retry.update(
        {
            "id": 99_999,
            "conclusion": "failure",
            "started_at": (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            "completed_at": (NOW + timedelta(minutes=1, seconds=1))
            .isoformat()
            .replace("+00:00", "Z"),
        }
    )
    api.checks[(PR_HEAD_SHA, "repository-checks")].append(retry)

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.generation_blocker == (
        "protected source check repository-checks is missing or ambiguous"
    )
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA


def test_same_app_retry_with_newest_incomplete_run_preserves_generation(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    retry = dict(api.checks[(PR_HEAD_SHA, "repository-checks")][0])
    retry.update(
        {
            "id": 99_999,
            "status": "in_progress",
            "conclusion": None,
            "started_at": (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            "completed_at": None,
        }
    )
    api.checks[(PR_HEAD_SHA, "repository-checks")].append(retry)

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.generation_blocker == (
        "protected source check repository-checks is missing or ambiguous"
    )
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA


def test_same_app_retry_with_tied_start_time_is_ambiguous(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    _configure_protected_merge(api)
    retry = dict(api.checks[(PR_HEAD_SHA, "repository-checks")][0])
    retry["id"] = 99_999
    api.checks[(PR_HEAD_SHA, "repository-checks")].append(retry)

    result = controller.reconcile()

    assert result.generation_promoted is False
    assert result.generation_blocker == (
        "protected source check repository-checks is missing or ambiguous"
    )
    assert broker.current_trusted_workflow_generation().candidate_sha == RUNTIME_SHA


def test_multiple_fast_protected_merges_converge_one_generation_per_reconcile(
    tmp_path: Path,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts = []
    api.runs = {}
    _configure_protected_merge(api, dev_head=SECOND_MERGE_SHA)
    api.compares[(RUNTIME_SHA, SECOND_MERGE_SHA)]["ahead_by"] = 2
    api.compares[(RUNTIME_SHA, SECOND_MERGE_SHA)]["total_commits"] = 2
    _configure_protected_merge(
        api,
        merge_sha=SECOND_MERGE_SHA,
        merge_tree=SECOND_MERGE_TREE,
        parent_sha=MERGE_SHA,
        pull_head_sha=SECOND_PR_HEAD_SHA,
        changed_blob_sha=SECOND_WORKFLOW_BLOB_SHA,
    )

    first = controller.reconcile()
    second = controller.reconcile()

    assert first.trusted_workflow_sha == MERGE_SHA
    assert first.generation_lag_commits == 1
    assert first.workflow_blob_drift["images"] is True
    assert second.trusted_workflow_sha == SECOND_MERGE_SHA
    assert second.generation_lag_commits == 0
    assert second.workflow_blob_drift == {
        "CI": False,
        "cluster-smoke": False,
        "images": False,
        "staging-smoke": False,
    }
    assert [item.candidate_sha for item in broker.trusted_workflow_generations()] == [
        RUNTIME_SHA,
        MERGE_SHA,
        SECOND_MERGE_SHA,
    ]


def test_promoted_generation_survives_restart_and_routes_with_pinned_runtime(
    tmp_path: Path,
) -> None:
    initial_request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, initial_request)
    api.artifacts = []
    api.runs = {}
    _configure_protected_merge(api)

    promoted = controller.reconcile()

    next_request = replace(initial_request, workflow_run_id=initial_request.workflow_run_id + 1)
    restarted_api = FakeRouteAPI(next_request)
    restarted_api.dev_head = MERGE_SHA
    restarted_api.commits[MERGE_SHA] = api.commits[MERGE_SHA]
    for path in routes.WORKFLOW_PATHS.values():
        restarted_api.blobs[(path, MERGE_SHA)] = api.blobs[(path, MERGE_SHA)]
    restarted_api.blobs[(routes.WORKFLOW_PATHS["images"], next_request.head_sha)] = (
        CHANGED_WORKFLOW_BLOB_SHA
    )
    restarted = routes.CiRunnerRouteController(
        api=restarted_api,
        broker=leases.CiRunnerLeaseBroker(broker.state_db, _config()),
        runtime_sha=RUNTIME_SHA,
        publisher=restarted_api,
        now=lambda: NOW + timedelta(seconds=10),
    )

    result = restarted.reconcile()

    assert promoted.generation_promoted is True
    assert result.generation_promoted is False
    assert result.trusted_workflow_sha == MERGE_SHA
    assert restarted_api.app_id == PUBLISHER_APP_ID
    summary = json.loads(restarted_api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is True


def test_github_interruption_preserves_generation_then_recovers_automatically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(workflow_name="images", job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    artifacts = list(api.artifacts)
    runs = dict(api.runs)
    api.artifacts = []
    api.runs = {}
    _configure_protected_merge(api)
    original_compare = api.compare_commits

    def unavailable(_base: str, _head: str) -> dict[str, object]:
        raise routes.RouteControllerError("GitHub compare temporarily unavailable")

    monkeypatch.setattr(api, "compare_commits", unavailable)
    interrupted = controller.reconcile()
    monkeypatch.setattr(api, "compare_commits", original_compare)
    api.artifacts = artifacts
    api.runs = runs
    api.blobs[(routes.WORKFLOW_PATHS["images"], request.head_sha)] = CHANGED_WORKFLOW_BLOB_SHA

    recovered = controller.reconcile()

    assert interrupted.trusted_workflow_sha == RUNTIME_SHA
    assert interrupted.observed_dev_sha == MERGE_SHA
    assert interrupted.generation_lag_commits is None
    assert interrupted.generation_blocker == "GitHub compare temporarily unavailable"
    assert recovered.generation_promoted is True
    assert recovered.generation_lag_commits == 0
    assert broker.status(now=NOW)["trusted_workflow_observation"]["promotion_result"] == "promoted"
    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is True


def test_stale_request_forces_hosted_without_consuming_oldlab(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = (
        (NOW - timedelta(seconds=routes.OLDLAB_REQUEST_MAX_AGE_SECONDS + 1))
        .isoformat()
        .replace("+00:00", "Z")
    )

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert {item["target"] for item in summary["assignments"]} == {"github_hosted"}
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_future_request_fails_safe_to_hosted(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = (NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert summary["assignments"][0]["target"] == "github_hosted"
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_invalid_artifact_time_fails_before_persistent_capacity_mutation(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = "not-a-timestamp"

    with pytest.raises(routes.RouteControllerError, match=r"artifact\.created_at"):
        controller.reconcile()

    assert broker.active_assignments() == ()
    assert broker.route_decisions() == ()


def test_restart_replays_the_exact_persisted_route_decision(
    tmp_path: Path,
) -> None:
    request = _request(job_count=2)
    controller, api, broker = _controller(tmp_path, request)
    first = controller.reconcile()
    restarted = routes.CiRunnerRouteController(
        api=api,
        broker=leases.CiRunnerLeaseBroker(broker.state_db, _config()),
        runtime_sha=RUNTIME_SHA,
        publisher=api,
        now=lambda: NOW + timedelta(minutes=1),
    )
    replay = restarted.reconcile()

    assert first.routes_published == 1
    assert replay.routes_published == 0
    assert replay.routes_replayed == 1
    assert len(api.created_checks) == 1


def test_legacy_actions_app_check_replays_after_direct_publisher_upgrade(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    artifacts = api.artifacts
    runs = api.runs
    api.artifacts = []
    api.runs = {}
    controller.reconcile()
    api.artifacts = artifacts
    api.runs = runs
    decision = broker.decide_route(request, now=NOW)
    payload = {
        **controller._route_payload(decision),
        "app": {"id": routes.GITHUB_ACTIONS_APP_ID},
    }
    api.checks[(request.head_sha, payload["name"])] = [payload]

    result = controller.reconcile()

    assert decision.publisher_app_id == routes.GITHUB_ACTIONS_APP_ID
    assert result.routes_replayed == 1
    assert api.dispatches == []


def test_direct_publisher_retry_replays_frozen_oldlab_decision_without_wedging(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.publisher_error = True

    with pytest.raises(routes.RouteControllerError, match="publisher unavailable"):
        controller.reconcile()
    frozen = broker.route_decisions(states=(leases.RouteDecisionState.PENDING,))[0]
    api.publisher_error = False
    controller.now = lambda: NOW + timedelta(seconds=16)

    replay = controller.reconcile()
    published = broker.route_decisions(states=(leases.RouteDecisionState.PUBLISHED,))[0]

    assert frozen.document().oldlab_eligible is True
    assert replay.routes_published == 1
    assert published.response_json == frozen.response_json
    assert len(api.dispatches) == 2
    assert api.dispatches[0] == api.dispatches[1]


def test_unrelated_repository_artifact_burst_cannot_block_fresh_route(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.artifacts.extend(
        {
            "id": 1_000 + index,
            "name": f"unrelated-{index}",
            "expired": False,
            "created_at": NOW.isoformat().replace("+00:00", "Z"),
            "workflow_run": {"id": 99_000 + index, "head_sha": HEAD_SHA},
        }
        for index in range(1_000)
    )

    result = controller.reconcile()

    assert result.requests_seen == 1
    assert result.routes_published == 1
    assert len(api.dispatches) == 1


def test_github_discovery_is_bounded_to_active_runs_and_exact_artifact_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = routes.GitHubRouteAPI(repository="qianyi-sun/loom", token="opaque")
    workflow_id = leases.WORKFLOW_CLASS_CONTRACTS["CI"][0]
    requested_paths: list[str] = []

    def request(
        method: str,
        path: str,
        *,
        payload: object | None = None,
    ) -> dict[str, object]:
        assert method == "GET"
        assert payload is None
        requested_paths.append(path)
        if path.startswith(f"/actions/workflows/{workflow_id}/runs?"):
            return {"total_count": 1, "workflow_runs": [{"id": 30_000}]}
        return {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 71,
                    "name": (f"{routes.ARTIFACT_PREFIX}{workflow_id}-30000-1"),
                    "expired": False,
                }
            ],
        }

    monkeypatch.setattr(api, "_request", request)

    assert api.active_workflow_runs(workflow_id) == [{"id": 30_000}]
    assert api.route_artifact(
        workflow_id=workflow_id,
        workflow_run_id=30_000,
        run_attempt=1,
    ) == {
        "id": 71,
        "name": f"{routes.ARTIFACT_PREFIX}{workflow_id}-30000-1",
        "expired": False,
    }
    assert "status=in_progress" in requested_paths[0]
    assert requested_paths[1].startswith(
        f"/actions/artifacts?name={routes.ARTIFACT_PREFIX}{workflow_id}-30000-1&"
    )


def test_github_active_run_inventory_overflow_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = routes.GitHubRouteAPI(repository="qianyi-sun/loom", token="opaque")
    monkeypatch.setattr(
        api,
        "_request",
        lambda *_args, **_kwargs: {
            "total_count": routes.MAX_ACTIVE_RUNS_PER_WORKFLOW + 1,
            "workflow_runs": [],
        },
    )

    with pytest.raises(routes.RouteControllerError, match="exceeds the scan bound"):
        api.active_workflow_runs(leases.WORKFLOW_CLASS_CONTRACTS["CI"][0])


def test_github_active_run_inventory_recovers_from_transient_count_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = routes.GitHubRouteAPI(repository="qianyi-sun/loom", token="opaque")
    payloads = iter(
        [
            {"total_count": 2, "workflow_runs": [{"id": 30_000}]},
            {"total_count": 1, "workflow_runs": [{"id": 30_000}]},
        ]
    )
    requests = 0

    def request(*_args: object, **_kwargs: object) -> object:
        nonlocal requests
        requests += 1
        return next(payloads)

    monkeypatch.setattr(api, "_request", request)

    assert api.active_workflow_runs(
        leases.WORKFLOW_CLASS_CONTRACTS["CI"][0]
    ) == [{"id": 30_000}]
    assert requests == 2


def test_github_active_run_inventory_persistent_malformed_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = routes.GitHubRouteAPI(repository="qianyi-sun/loom", token="opaque")
    requests = 0

    def request(*_args: object, **_kwargs: object) -> object:
        nonlocal requests
        requests += 1
        return {"total_count": 1, "workflow_runs": []}

    monkeypatch.setattr(api, "_request", request)

    with pytest.raises(routes.RouteControllerError, match="bounded retries"):
        api.active_workflow_runs(leases.WORKFLOW_CLASS_CONTRACTS["CI"][0])
    assert requests == routes.ACTIVE_WORKFLOW_INVENTORY_ATTEMPTS


def test_terminal_jobs_release_exact_leases(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["status"] = "completed"
    api.jobs[(request.workflow_run_id, 1)] = [
        {
            "name": routes.JOB_NAMES["CI"][job_key],
            "status": "completed",
            "conclusion": "success",
        }
        for job_key in request.job_keys
    ]

    result = controller.reconcile()

    assert result.assignments_released == 3
    assert broker.active_assignments() == ()


def test_new_workflow_attempt_releases_superseded_assignments(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["run_attempt"] = 2
    api.runs[request.workflow_run_id]["status"] = "queued"

    result = controller.reconcile()

    assert result.assignments_released == 3
    assert broker.active_assignments() == ()


def test_workflow_attempt_regression_fails_closed(tmp_path: Path) -> None:
    request = replace(_request(job_count=1), run_attempt=2)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["run_attempt"] = 1

    with pytest.raises(routes.RouteControllerError, match="attempt does not match"):
        controller.reconcile()

    assert len(broker.active_assignments()) == 1


def test_terminal_hosted_fallback_abandons_outbox_and_releases_lease(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.publisher_error = True
    with pytest.raises(routes.RouteControllerError, match="publisher unavailable"):
        controller.reconcile()
    api.jobs[(request.workflow_run_id, 1)] = [
        {
            "name": routes.ROUTE_JOB_NAMES[request.workflow_name],
            "status": "completed",
            "conclusion": "success",
        }
    ]

    result = controller.reconcile()

    assert result.routes_abandoned == 1
    assert result.assignments_released == 1
    assert broker.active_assignments() == ()
    assert broker.route_decisions()[0].state is leases.RouteDecisionState.ABANDONED


def test_route_artifact_identity_and_shape_fail_closed(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.artifacts[0]["workflow_run"] = {
        "id": 999,
        "head_sha": request.head_sha,
    }

    with pytest.raises(routes.RouteControllerError, match="workflow run does not match"):
        controller.reconcile()

    api.artifacts[0]["workflow_run"] = {
        "id": request.workflow_run_id,
        "head_sha": request.head_sha,
    }
    api.archives[71] = _zip_request(request, filename="wrong.json")
    with pytest.raises(
        routes.RouteControllerError,
        match=r"only loom-ci-route-request\.json",
    ):
        controller.reconcile()


def test_controller_filename_matches_the_pinned_route_action() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    action = (repo_root / ".github/actions/ci-runner-route/action.yml").read_text(encoding="utf-8")

    expected_path = "${{ runner.temp }}/loom-ci-route-request.json"
    assert routes.ROUTE_REQUEST_FILENAME == "loom-ci-route-request.json"
    assert f"ROUTE_REQUEST_PATH: {expected_path}" in action
    assert f"path: {expected_path}" in action


def test_artifact_redirect_never_forwards_github_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _zip_request(_request(job_count=1))
    redirected_to = "https://example.blob.core.windows.net/result/archive.zip?sig=fake"

    class RedirectOpener:
        @staticmethod
        def open(request: object, timeout: int) -> None:
            assert timeout == 20
            assert request.get_header("Authorization") == "Bearer top-secret"
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": redirected_to},
                None,
            )

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        @staticmethod
        def read(_: int) -> bytes:
            return payload

    def urlopen(request: object, timeout: int) -> Response:
        assert timeout == 20
        assert request.full_url == redirected_to
        assert request.get_header("Authorization") is None
        return Response()

    monkeypatch.setattr(routes.urllib.request, "build_opener", lambda *_: RedirectOpener())
    monkeypatch.setattr(routes.urllib.request, "urlopen", urlopen)

    api = routes.GitHubRouteAPI(
        repository="qianyi-sun/loom", token_provider=lambda: "top-secret"
    )
    assert api.download_artifact(71) == payload


def test_github_app_publisher_mints_least_privilege_token_and_creates_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = rsa.generate_private_key(public_exponent=65_537, key_size=2_048)
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    payload: dict[str, object] = {
        "name": "loom-ci-route-v1/CI/30000/1",
        "head_sha": HEAD_SHA,
        "external_id": "route-external-id",
        "status": "completed",
        "conclusion": "success",
        "output": {"title": "oldlab-first route assignment", "summary": "{}"},
    }
    calls: list[object] = []

    class Response:
        def __init__(self, value: dict[str, object]) -> None:
            self.raw = json.dumps(value).encode()

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self.raw

    def urlopen(request: object, timeout: int) -> Response:
        assert timeout == 20
        calls.append(request)
        body = json.loads(request.data)
        authorization = request.get_header("Authorization")
        if request.full_url.endswith("/app/installations/777/access_tokens"):
            assert body == {
                "repositories": ["loom"],
                "permissions": {
                    "actions": "read",
                    "checks": "write",
                    "contents": "read",
                    "pull_requests": "read",
                },
            }
            assert authorization.startswith("Bearer ")
            claims = jwt.decode(
                authorization.removeprefix("Bearer "),
                options={"verify_signature": False},
            )
            assert claims["iss"] == str(PUBLISHER_APP_ID)
            return Response(
                {
                    "token": "ghs_test-installation-token",
                    "expires_at": "2026-08-20T19:00:00Z",
                    "permissions": {
                        "actions": "read",
                        "checks": "write",
                        "contents": "read",
                        "metadata": "read",
                        "pull_requests": "read",
                    },
                }
            )
        assert request.full_url.endswith("/repos/qianyi-sun/loom/check-runs")
        assert authorization == "Bearer ghs_test-installation-token"
        assert body == payload
        return Response({**payload, "app": {"id": PUBLISHER_APP_ID}})

    monkeypatch.setattr(routes.urllib.request, "urlopen", urlopen)
    publisher = routes.GitHubAppRouteCheckPublisher(
        repository="qianyi-sun/loom",
        app_id=PUBLISHER_APP_ID,
        installation_id=777,
        private_key_pem=private_key_pem,
        now=lambda: NOW,
    )

    assert publisher.publish(payload)["app"] == {"id": PUBLISHER_APP_ID}
    assert publisher.publish(payload)["app"] == {"id": PUBLISHER_APP_ID}
    assert len(calls) == 3


def test_root_owned_credential_files_fail_closed(tmp_path: Path) -> None:
    publisher_key = tmp_path / "route-publisher-app-private-key.pem"
    publisher_key.write_bytes(b"test-private-key")
    publisher_key.chmod(0o600)
    assert routes._read_app_private_key(publisher_key) == b"test-private-key"

    publisher_key.chmod(0o640)
    with pytest.raises(routes.RouteControllerError, match="group or other"):
        routes._read_app_private_key(publisher_key)


def test_route_controller_has_an_independent_high_frequency_systemd_timer() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    unit_root = repo_root / "deploy/ci-runners"
    service = (unit_root / "loom-ci-runner-route-controller.service").read_text(encoding="utf-8")
    timer = (unit_root / "loom-ci-runner-route-controller.timer").read_text(encoding="utf-8")
    pool_service = (unit_root / "loom-ci-runner-pool.service").read_text(encoding="utf-8")

    assert (
        "ExecStart=/usr/local/lib/loom-ci-runner-controller/.venv/bin/python "
        "-m loom_control_plane.ci_runner_route_controller"
    ) in service
    assert "LoadCredential=github-token:" not in service
    assert "--token-file" not in service
    assert "LoadCredential=route-publisher-app-private-key:" in service
    assert "--publisher-app-id ${LOOM_CI_RUNNER_ROUTE_PUBLISHER_APP_ID}" in service
    assert (
        "--publisher-installation-id ${LOOM_CI_RUNNER_ROUTE_PUBLISHER_INSTALLATION_ID}" in service
    )
    assert (
        "--publisher-app-private-key-file ${CREDENTIALS_DIRECTORY}/route-publisher-app-private-key"
    ) in service
    route_command = next(
        line
        for line in service.splitlines()
        if line.startswith("ExecStart=/usr/local/lib/loom-ci-runner-controller/.venv/bin/python ")
    )
    assert "--runtime-sha ${LOOM_CI_RUNNER_ROUTE_RUNTIME_SHA}" in route_command
    assert "LOOM_CI_RUNNER_ROUTE_CANDIDATE_SHA" not in service
    assert "LOOM_CI_RUNNER_POOL_CANDIDATE_SHA" not in route_command
    assert "--candidate-sha ${LOOM_CI_RUNNER_CANDIDATE_SHA}" not in service
    assert "Environment=GITHUB_TOKEN" not in service
    assert "OnUnitActiveSec=15s" in timer
    assert "Unit=loom-ci-runner-route-controller.service" in timer
    assert "loom-ci-runner-route-controller" not in pool_service
