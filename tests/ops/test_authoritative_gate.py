from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any

import pytest
from scripts.ops.authoritative_gate import (
    GATE_SPECS,
    RELEVANT_LABEL_ORDER,
    DuplicateCustomCheckError,
    Generation,
    PublisherError,
    parse_full_generation,
    process_event,
)

HEAD = "a" * 40
BASE = "b" * 40
REPOSITORY = "qianyi-sun/loom"


def label_mask(labels: Sequence[str]) -> str:
    selected = frozenset(labels)
    return "".join("1" if label in selected else "0" for label in RELEVANT_LABEL_ORDER)


def labels_from_mask(mask: str) -> list[str]:
    return [
        label for label, selected in zip(RELEVANT_LABEL_ORDER, mask, strict=True) if selected == "1"
    ]


def authority_event(
    event_id: int,
    name: str,
    created_at: str,
    *,
    label: str | None = None,
    commit_id: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "id": event_id,
        "event": name,
        "created_at": created_at,
    }
    if label is not None:
        event["label"] = {"name": label}
    if commit_id is not None:
        event["commit_id"] = commit_id
    return event


class FakeGitHubClient:
    repository = REPOSITORY

    def __init__(self) -> None:
        self.pull: dict[str, Any] = {
            "number": 833,
            "state": "open",
            "draft": False,
            "updated_at": "2026-07-22T10:00:00Z",
            "html_url": "https://github.com/qianyi-sun/loom/pull/999",
            "head": {
                "sha": HEAD,
                "ref": "feature",
                "repo": {"full_name": REPOSITORY},
            },
            "base": {
                "sha": BASE,
                "ref": "dev",
                "repo": {"full_name": REPOSITORY},
            },
            "labels": [],
        }
        self.publisher_active = True
        self.checks: dict[str, list[dict[str, Any]]] = {spec.context: [] for spec in GATE_SPECS}
        self.runs: dict[int, list[dict[str, Any]]] = {spec.workflow_id: [] for spec in GATE_SPECS}
        self.jobs: dict[int | tuple[int, int], list[dict[str, Any]]] = {}
        self.issue_events: list[dict[str, Any]] = [
            authority_event(1, "ready_for_review", "2026-07-22T10:00:00Z")
        ]
        self.created: list[dict[str, Any]] = []
        self.updated: list[tuple[int, dict[str, Any]]] = []
        self._next_check_id = 1000

    def get_pull_request(self, number: int) -> Mapping[str, Any]:
        assert number == 833
        return deepcopy(self.pull)

    def base_contains_publisher(self, base_sha: str) -> bool:
        assert base_sha == BASE
        return self.publisher_active

    def get_workflow(self, workflow_id: int) -> Mapping[str, Any]:
        spec = next(spec for spec in GATE_SPECS if spec.workflow_id == workflow_id)
        return {
            "id": spec.workflow_id,
            "name": spec.workflow_name,
            "path": spec.workflow_path,
        }

    def list_check_runs(self, head_sha: str, context: str) -> Sequence[Mapping[str, Any]]:
        assert head_sha == HEAD
        return deepcopy(self.checks[context])

    def create_check_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        check = deepcopy(dict(payload))
        check.update(
            {
                "id": self._next_check_id,
                "app": {"id": 15368},
            }
        )
        self._next_check_id += 1
        self.checks[str(check["name"])].append(check)
        self.created.append(deepcopy(check))
        return deepcopy(check)

    def update_check_run(self, check_run_id: int, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        for checks in self.checks.values():
            for check in checks:
                if check["id"] == check_run_id:
                    check.update(deepcopy(dict(payload)))
                    if payload.get("status") == "in_progress":
                        check.pop("conclusion", None)
                        check.pop("completed_at", None)
                    self.updated.append((check_run_id, deepcopy(dict(payload))))
                    return deepcopy(check)
        raise AssertionError(f"unknown check id {check_run_id}")

    def list_workflow_runs(
        self, workflow_id: int, head_sha: str, event: str
    ) -> Sequence[Mapping[str, Any]]:
        assert head_sha == HEAD
        return [deepcopy(run) for run in self.runs[workflow_id] if run.get("event") == event]

    def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
        return deepcopy(self.jobs.get((run_id, run_attempt), self.jobs.get(run_id, [])))

    def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]:
        assert head_sha == HEAD
        return [deepcopy(self.pull)]

    def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]:
        assert number == 833
        return deepcopy(self.issue_events)


def pull_event(
    *,
    action: str,
    updated: str = "2026-07-22T10:00:00Z",
    draft: bool = False,
    label: str | None = None,
    changes: Mapping[str, Any] | None = None,
    head_ref: str = "feature",
    base_ref: str = "dev",
    head_repository: str = REPOSITORY,
    labels: Sequence[str] | None = None,
) -> dict[str, Any]:
    effective_labels = (
        list(labels)
        if labels is not None
        else ([label] if action == "labeled" and label in RELEVANT_LABEL_ORDER else [])
    )
    event: dict[str, Any] = {
        "event_name": "pull_request_target",
        "action": action,
        "number": 833,
        "pull_request": {
            "number": 833,
            "draft": draft,
            "updated_at": updated,
            "head": {
                "sha": HEAD,
                "ref": head_ref,
                "repo": {"full_name": head_repository},
            },
            "base": {
                "sha": BASE,
                "ref": base_ref,
                "repo": {"full_name": REPOSITORY},
            },
            "labels": [{"name": name} for name in effective_labels],
        },
    }
    if label is not None:
        event["label"] = {"name": label}
    if changes is not None:
        event["changes"] = dict(changes)
    return event


def title(generation: Generation) -> str:
    fields = generation.as_dict()
    return (
        "gate=full"
        f" / head={fields['head']}"
        f" / base={fields['base']}"
        f" / pull={fields['pull']}"
        f" / updated={fields['updated']}"
        f" / action={fields['action']}"
        f" / label={fields['label']}"
        f" / labels={fields['labels']}"
    )


def workflow_run(
    *,
    spec_index: int = 0,
    run_id: int,
    generation: Generation,
    status: str,
    conclusion: str | None = None,
    run_attempt: int = 1,
    run_event: str = "pull_request",
    head_repository: str = REPOSITORY,
    include_pull: bool = True,
) -> dict[str, Any]:
    spec = GATE_SPECS[spec_index]
    return {
        "id": run_id,
        "workflow_id": spec.workflow_id,
        # GitHub replaces workflow_run.name with the dynamic run-name marker.
        "name": title(generation),
        "event": run_event,
        "head_sha": HEAD,
        "head_repository": {"full_name": head_repository},
        "repository": {"full_name": REPOSITORY},
        "display_title": title(generation),
        "status": status,
        "conclusion": conclusion,
        "run_attempt": run_attempt,
        "html_url": f"https://github.com/qianyi-sun/loom/actions/runs/{run_id}",
        "pull_requests": (
            [
                {
                    "number": 833,
                    "head": {"sha": HEAD, "ref": "feature"},
                    "base": {"sha": BASE, "ref": "dev"},
                }
            ]
            if include_pull
            else []
        ),
    }


def workflow_event(run: Mapping[str, Any]) -> dict[str, Any]:
    return {"event_name": "workflow_run", "action": run["status"], "workflow_run": run}


def generation(
    *,
    pull: str = "833",
    updated: str = "2026-07-22T10:00:00Z",
    action: str = "ready_for_review",
    label: str = "none",
    labels: Sequence[str] | None = None,
) -> Generation:
    effective_labels = (
        list(labels)
        if labels is not None
        else ([label] if action == "labeled" and label in RELEVANT_LABEL_ORDER else [])
    )
    return Generation(
        head=HEAD,
        base=BASE,
        pull=pull,
        updated=updated,
        action=action,
        label=label,
        labels=label_mask(effective_labels),
    )


def seed_invalidation(client: FakeGitHubClient, value: Generation) -> None:
    relevant_labels = labels_from_mask(value.labels)
    client.pull["labels"] = [{"name": label} for label in relevant_labels]
    client.pull["updated_at"] = value.updated
    event_name = {
        "edited": "base_ref_changed",
        "ready_for_review": "ready_for_review",
        "reopened": "reopened",
        "synchronize": "head_ref_force_pushed",
        "labeled": "labeled",
        "unlabeled": "unlabeled",
    }.get(value.action)
    event_label = value.label if value.action in {"labeled", "unlabeled"} else None
    if event_name is not None and not any(
        event.get("event") == event_name
        and event.get("created_at") == value.updated
        and (
            event_label is None
            or (
                isinstance(event.get("label"), Mapping)
                and event["label"].get("name") == event_label
            )
        )
        for event in client.issue_events
    ):
        next_event_id = (
            max(
                (
                    event.get("id", 0)
                    for event in client.issue_events
                    if isinstance(event.get("id"), int)
                ),
                default=0,
            )
            + 1
        )
        client.issue_events.append(
            authority_event(
                next_event_id,
                event_name,
                value.updated,
                label=event_label,
                commit_id=value.head if event_name == "head_ref_force_pushed" else None,
            )
        )
    event = pull_event(
        action=value.action,
        updated=value.updated,
        label=None if value.label == "none" else value.label,
        labels=relevant_labels,
    )
    result = process_event(event, client)
    assert result.outcome == "in_progress"


def test_draft_then_ready_creates_one_in_progress_check_per_context() -> None:
    client = FakeGitHubClient()
    client.pull["draft"] = True
    assert process_event(pull_event(action="opened", draft=True), client).outcome == "ignored"
    assert not client.created

    client.pull["draft"] = False
    result = process_event(pull_event(action="ready_for_review"), client)

    assert result.outcome == "in_progress"
    assert {check["name"] for check in client.created} == {spec.context for spec in GATE_SPECS}
    assert all(check["status"] == "in_progress" for check in client.created)
    assert all(
        check["external_id"] == f"loom-authoritative-gate:{REPOSITORY}:{HEAD}:{check['name']}"
        for check in client.created
    )


def test_rapid_relevant_labels_update_the_same_checks_to_latest_generation() -> None:
    client = FakeGitHubClient()
    client.pull["labels"] = [{"name": "ci:images"}]
    client.issue_events.append(
        authority_event(2, "labeled", "2026-07-22T10:00:01Z", label="ci:images")
    )
    process_event(
        pull_event(
            action="labeled",
            label="ci:images",
            updated="2026-07-22T10:00:01Z",
        ),
        client,
    )
    client.pull["labels"] = [
        {"name": "ci:images"},
        {"name": "cluster-smoke"},
    ]
    client.issue_events.append(
        authority_event(3, "labeled", "2026-07-22T10:00:02Z", label="cluster-smoke")
    )
    process_event(
        pull_event(
            action="labeled",
            label="cluster-smoke",
            labels=["ci:images", "cluster-smoke"],
            updated="2026-07-22T10:00:02Z",
        ),
        client,
    )

    assert sum(len(checks) for checks in client.checks.values()) == 4
    assert len(client.updated) == 4
    for checks in client.checks.values():
        state = json.loads(checks[0]["output"]["summary"])
        assert state["generation"]["updated"] == "2026-07-22T10:00:02Z"
        assert state["generation"]["label"] == "cluster-smoke"
        assert state["generation"]["labels"] == label_mask(["ci:images", "cluster-smoke"])


@pytest.mark.parametrize("invalid_mask", ["", "00000", "0000000", "00000x"])
def test_generation_parser_rejects_invalid_relevant_label_masks(
    invalid_mask: str,
) -> None:
    marker = title(generation())
    malformed = marker.replace("labels=000000", f"labels={invalid_mask}")

    assert parse_full_generation(malformed) is None


@pytest.mark.parametrize("invalid_pull", ["", "0", "-1", "not-a-number"])
def test_generation_parser_rejects_invalid_pull_identity(invalid_pull: str) -> None:
    marker = title(generation())
    malformed = marker.replace("pull=833", f"pull={invalid_pull}")

    assert parse_full_generation(malformed) is None


def test_completed_check_is_reset_in_place_for_a_new_same_head_generation() -> None:
    client = FakeGitHubClient()
    original_generation = generation()
    seed_invalidation(client, original_generation)
    completed = workflow_run(
        run_id=90,
        generation=original_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[90] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(completed), client).outcome == "success"

    original_check = client.checks[GATE_SPECS[0].context][0]
    original_check_id = original_check["id"]
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    result = process_event(
        pull_event(
            action="labeled",
            label="ci:images",
            labels=["ci:images"],
            updated="2026-07-22T10:00:01Z",
        ),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "in_progress"
    assert len(client.checks[GATE_SPECS[0].context]) == 1
    reset_check = client.checks[GATE_SPECS[0].context][0]
    assert reset_check["id"] == original_check_id
    assert reset_check["status"] == "in_progress"
    assert "conclusion" not in reset_check


def test_check_run_response_from_an_unexpected_app_fails_closed() -> None:
    class UnexpectedAppClient(FakeGitHubClient):
        def create_check_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
            response = dict(super().create_check_run(payload))
            response["app"] = {"id": 999}
            return response

    client = UnexpectedAppClient()

    with pytest.raises(PublisherError, match="publisher-owned repository-checks CheckRun"):
        process_event(
            pull_event(action="ready_for_review"),
            client,
            context=GATE_SPECS[0].context,
        )


@pytest.mark.parametrize(
    "event",
    [
        pull_event(action="labeled", label="triage"),
        pull_event(action="edited", changes={"body": {"from": "old"}}),
    ],
)
def test_unrelated_metadata_does_not_touch_custom_checks(event: dict[str, Any]) -> None:
    client = FakeGitHubClient()

    assert process_event(event, client).outcome == "ignored"
    assert not client.created
    assert not client.updated


def test_latest_replacement_success_is_authoritative() -> None:
    client = FakeGitHubClient()
    latest_generation = generation(
        updated="2026-07-22T10:00:02Z",
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, latest_generation)
    old_run = workflow_run(
        run_id=100, generation=generation(), status="completed", conclusion="failure"
    )
    latest_run = workflow_run(
        run_id=101,
        generation=latest_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run, latest_run]
    client.jobs[101] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(latest_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"

    repeated = process_event(workflow_event(latest_run), client)

    assert repeated.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


@pytest.mark.parametrize("job_conclusion", ["failure", "cancelled", "skipped", None])
def test_non_success_or_missing_authoritative_attempt_is_failure(
    job_conclusion: str | None,
) -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=200,
        generation=current_generation,
        status="completed",
        conclusion=job_conclusion,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    if job_conclusion is not None:
        client.jobs[200] = [
            {
                "name": GATE_SPECS[0].attempt_job,
                "conclusion": job_conclusion,
            }
        ]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "failure"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "failure"


def test_failure_completion_replay_is_idempotently_red() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=205,
        generation=current_generation,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[205] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "failure"}]

    assert process_event(workflow_event(run), client).outcome == "failure"
    assert process_event(workflow_event(run), client).outcome == "failure"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "failure"


def test_cancelled_current_run_stays_red_even_if_attempt_job_was_green() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=206,
        generation=current_generation,
        status="completed",
        conclusion="cancelled",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[206] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "failure"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "failure"


def test_stale_completion_reconciles_without_overwriting_newer_generation() -> None:
    client = FakeGitHubClient()
    latest_generation = generation(
        updated="2026-07-22T10:00:03Z",
        action="labeled",
        label="staging-smoke",
    )
    seed_invalidation(client, latest_generation)
    stale_run = workflow_run(
        run_id=300, generation=generation(), status="completed", conclusion="success"
    )
    latest_run = workflow_run(run_id=301, generation=latest_generation, status="in_progress")
    client.runs[GATE_SPECS[0].workflow_id] = [stale_run, latest_run]
    updates_before = len(client.updated)

    result = process_event(workflow_event(stale_run), client)

    assert result.outcome == "in_progress"
    assert len(client.updated) == updates_before + 1
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    state = json.loads(check["output"]["summary"])
    assert state["generation"] == latest_generation.as_dict()
    assert state["run_id"] == 301


def test_old_completion_cannot_green_after_live_relevant_label_change() -> None:
    client = FakeGitHubClient()
    old_generation = generation()
    seed_invalidation(client, old_generation)
    old_run = workflow_run(
        run_id=302,
        generation=old_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[302] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    # The label webhook has updated live PR state, but its publisher invocation and
    # replacement source run are not visible yet. The old completion must not create
    # a transient green window while the new generation waits for the concurrency lane.
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"

    result = process_event(workflow_event(old_run), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_relevant_label_aba_cannot_revalidate_an_old_generation() -> None:
    client = FakeGitHubClient()
    old_generation = generation()
    seed_invalidation(client, old_generation)
    old_run = workflow_run(
        run_id=304,
        generation=old_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[304] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    client.issue_events = [
        authority_event(
            10,
            "labeled",
            "2026-07-22T10:00:01Z",
            label="ci:images",
        ),
        authority_event(
            11,
            "unlabeled",
            "2026-07-22T10:00:02Z",
            label="ci:images",
        ),
    ]
    client.pull["labels"] = []
    client.pull["updated_at"] = "2026-07-22T10:00:02Z"

    result = process_event(workflow_event(old_run), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_final_live_read_rejects_a_relevant_change_during_completion() -> None:
    class MutatingJobsClient(FakeGitHubClient):
        def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
            jobs = super().list_run_jobs(run_id, run_attempt)
            self.pull["labels"] = [{"name": "ci:images"}]
            self.pull["updated_at"] = "2026-07-22T10:00:01Z"
            self.issue_events.append(
                authority_event(
                    12,
                    "labeled",
                    "2026-07-22T10:00:01Z",
                    label="ci:images",
                )
            )
            return jobs

    client = MutatingJobsClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=305,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[305] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_same_second_identical_label_aba_waits_for_every_source_generation() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    repeated_generation = generation(
        updated=tied_updated,
        action="labeled",
        label="ci:images",
    )
    client.issue_events = [
        authority_event(20, "labeled", tied_updated, label="ci:images"),
    ]
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = tied_updated
    seed_invalidation(client, repeated_generation)
    old_run = workflow_run(
        run_id=306,
        generation=repeated_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[306] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    client.issue_events.extend(
        [
            authority_event(21, "unlabeled", tied_updated, label="ci:images"),
            authority_event(22, "labeled", tied_updated, label="ci:images"),
        ]
    )

    assert process_event(workflow_event(old_run), client).outcome == "in_progress"
    assert client.checks[GATE_SPECS[0].context][0]["status"] == "in_progress"

    latest_run = workflow_run(
        run_id=307,
        generation=repeated_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run, latest_run]
    client.jobs[307] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(latest_run), client)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_unrelated_label_change_does_not_stale_equivalent_generation() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=303,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[303] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    client.pull["labels"] = [{"name": "triage"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    client.issue_events.append(
        authority_event(30, "labeled", "2026-07-22T10:00:01Z", label="triage")
    )

    result = process_event(workflow_event(run), client)

    assert result.outcome == "success"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_duplicate_publisher_owned_custom_checks_fail_closed() -> None:
    client = FakeGitHubClient()
    spec = GATE_SPECS[0]
    client.checks[spec.context] = [
        {
            "id": check_id,
            "name": spec.context,
            "external_id": spec.external_id(repository=REPOSITORY, head_sha=HEAD),
            "app": {"id": 15368},
        }
        for check_id in (1, 2)
    ]

    with pytest.raises(DuplicateCustomCheckError, match="duplicate custom CheckRuns"):
        process_event(pull_event(action="ready_for_review"), client)
    assert not client.created
    assert not client.updated


def test_base_without_publisher_keeps_legacy_gate_and_creates_no_custom_check() -> None:
    client = FakeGitHubClient()
    client.publisher_active = False

    result = process_event(pull_event(action="ready_for_review"), client)

    assert result.outcome == "legacy"
    assert not client.created


def test_late_pull_request_target_does_not_reopen_completed_generation() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(run_id=400, generation=current_generation, status="queued")
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    assert process_event(workflow_event(run), client).outcome == "in_progress"

    completed = {**run, "status": "completed", "conclusion": "success"}
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[400] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(completed), client).outcome == "success"
    updates_before = len(client.updated)

    result = process_event(pull_event(action="ready_for_review"), client)

    assert result.outcome == "current"
    assert GATE_SPECS[0].context not in result.contexts
    assert len(client.updated) == updates_before + 2
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_pull_request_target_as_final_invocation_reconciles_completed_source() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    completed = workflow_run(
        run_id=405,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[405] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(
        pull_event(action="ready_for_review"),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "success"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_pull_request_target_reconciliation_preserves_current_failure() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    completed = workflow_run(
        run_id=406,
        generation=current_generation,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[406] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "failure"}]

    result = process_event(
        pull_event(action="ready_for_review"),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "failure"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "failure"


def test_out_of_order_label_event_with_stale_snapshot_is_ignored() -> None:
    client = FakeGitHubClient()
    client.pull["labels"] = [
        {"name": "ci:images"},
        {"name": "cluster-smoke"},
    ]

    result = process_event(
        pull_event(
            action="labeled",
            label="ci:images",
            labels=["ci:images"],
        ),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "in_progress"
    assert len(client.created) == 1
    assert client.created[0]["status"] == "in_progress"


def test_stale_pull_request_target_cannot_replace_newer_generation() -> None:
    client = FakeGitHubClient()
    latest = generation(updated="2026-07-22T10:00:05Z")
    seed_invalidation(client, latest)
    updates_before = len(client.updated)

    result = process_event(
        pull_event(action="ready_for_review", updated="2026-07-22T10:00:01Z"),
        client,
    )

    assert result.outcome == "current"
    assert len(client.updated) == updates_before
    for checks in client.checks.values():
        state = json.loads(checks[0]["output"]["summary"])
        assert state["generation"]["updated"] == latest.updated


def test_newer_requested_run_self_heals_an_older_invalidation() -> None:
    client = FakeGitHubClient()
    seed_invalidation(client, generation())
    latest = generation(
        updated="2026-07-22T10:00:06Z",
        action="labeled",
        label="ci:images",
    )
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = latest.updated
    client.issue_events.append(authority_event(2, "labeled", latest.updated, label="ci:images"))
    run = workflow_run(run_id=410, generation=latest, status="queued")
    client.runs[GATE_SPECS[0].workflow_id] = [run]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    state = json.loads(client.checks[GATE_SPECS[0].context][0]["output"]["summary"])
    assert state["generation"] == latest.as_dict()
    assert (state["run_id"], state["run_attempt"]) == (410, 1)


@pytest.mark.parametrize("status", ["requested", "queued", "pending", "waiting", "in_progress"])
def test_all_nonterminal_workflow_states_keep_the_gate_in_progress(
    status: str,
) -> None:
    client = FakeGitHubClient()
    run = workflow_run(run_id=415, generation=generation(), status=status)
    client.runs[GATE_SPECS[0].workflow_id] = [run]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    assert client.checks[GATE_SPECS[0].context][0]["status"] == "in_progress"


def test_serialized_latest_completion_can_self_heal_without_prior_invalidation() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    run = workflow_run(
        run_id=420,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[420] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "success"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"

    repeated = process_event(workflow_event(run), client)

    assert repeated.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_same_second_relevant_labels_converge_when_only_completion_survives() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    first = generation(
        updated=tied_updated,
        action="labeled",
        label="ci:images",
    )
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = tied_updated
    seed_invalidation(client, first)
    old_run = workflow_run(
        run_id=425,
        generation=first,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[425] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(old_run), client).outcome == "success"

    latest = generation(
        updated=tied_updated,
        action="labeled",
        label="cluster-smoke",
        labels=["ci:images", "cluster-smoke"],
    )
    client.pull["labels"] = [
        {"name": "ci:images"},
        {"name": "cluster-smoke"},
    ]
    client.issue_events.append(authority_event(3, "labeled", tied_updated, label="cluster-smoke"))
    invalidated = process_event(
        pull_event(
            action="labeled",
            label="cluster-smoke",
            labels=["ci:images", "cluster-smoke"],
            updated=tied_updated,
        ),
        client,
        context=GATE_SPECS[0].context,
    )
    assert invalidated.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert state["generation"] == latest.as_dict()

    latest_run = workflow_run(
        run_id=426,
        generation=latest,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run, latest_run]
    client.jobs[426] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    result = process_event(workflow_event(latest_run), client, context=GATE_SPECS[0].context)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_dev_tip_advance_does_not_invalidate_a_loose_base_generation() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    client.pull["base"]["sha"] = "c" * 40
    run = workflow_run(
        run_id=430,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[430] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_actual_base_retarget_rejects_the_old_generation() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    client.pull["base"]["ref"] = "main"
    run = workflow_run(
        run_id=440,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[440] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    assert client.checks[GATE_SPECS[0].context][0]["status"] == "in_progress"


def test_fork_pull_request_run_can_finalize_the_base_repo_check() -> None:
    client = FakeGitHubClient()
    fork_repository = "contributor/loom"
    client.pull["head"]["repo"]["full_name"] = fork_repository
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=450,
        generation=current_generation,
        status="completed",
        conclusion="success",
        head_repository=fork_repository,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[450] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_mismatched_fork_repository_is_ignored() -> None:
    client = FakeGitHubClient()
    client.pull["head"]["repo"]["full_name"] = "contributor/loom"
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=451,
        generation=current_generation,
        status="completed",
        conclusion="success",
        head_repository="attacker/loom",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]

    assert process_event(workflow_event(run), client).outcome == "ignored"


def test_merge_group_requested_then_completed_publishes_authoritative_result() -> None:
    client = FakeGitHubClient()
    current_generation = generation(
        pull="none", updated="none", action="checks_requested", label="none"
    )
    queued = workflow_run(
        run_id=460,
        generation=current_generation,
        status="queued",
        run_event="merge_group",
        include_pull=False,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [queued]
    assert process_event(workflow_event(queued), client).outcome == "in_progress"

    completed = {**queued, "status": "completed", "conclusion": "success"}
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[460] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(completed), client).outcome == "success"


def test_merge_group_latest_completion_can_self_heal_without_requested_event() -> None:
    client = FakeGitHubClient()
    current_generation = generation(
        pull="none", updated="none", action="checks_requested", label="none"
    )
    completed = workflow_run(
        run_id=461,
        generation=current_generation,
        status="completed",
        conclusion="success",
        run_event="merge_group",
        include_pull=False,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [completed]
    client.jobs[461] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(completed), client).outcome == "success"


def test_trusted_dev_to_main_promotion_uses_publisher_before_main_has_marker() -> None:
    client = FakeGitHubClient()
    client.publisher_active = False
    client.pull["head"]["ref"] = "dev"
    client.pull["base"]["ref"] = "main"

    result = process_event(
        pull_event(
            action="ready_for_review",
            head_ref="dev",
            base_ref="main",
        ),
        client,
    )

    assert result.outcome == "in_progress"
    assert len(client.created) == len(GATE_SPECS)


def test_pull_request_lookup_falls_back_to_commit_association() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=470,
        generation=current_generation,
        status="completed",
        conclusion="success",
        include_pull=False,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[470] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_duplicate_attempt_jobs_fail_closed() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    run = workflow_run(
        run_id=480,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[480] = [
        {"name": GATE_SPECS[0].attempt_job, "conclusion": "success"},
        {"name": GATE_SPECS[0].attempt_job, "conclusion": "success"},
    ]

    assert process_event(workflow_event(run), client).outcome == "failure"


def test_filtered_workflow_run_never_creates_a_protected_check() -> None:
    client = FakeGitHubClient()
    run = workflow_run(run_id=490, generation=generation(), status="completed")
    run["display_title"] = str(run["display_title"]).replace("gate=full", "gate=filtered", 1)
    client.runs[GATE_SPECS[0].workflow_id] = [run]

    assert process_event(workflow_event(run), client).outcome == "ignored"
    assert not client.created


def test_matrix_context_limits_pull_request_invalidation_to_one_gate() -> None:
    client = FakeGitHubClient()
    context = GATE_SPECS[1].context

    result = process_event(pull_event(action="ready_for_review"), client, context=context)

    assert result.contexts == (context,)
    assert {check["name"] for check in client.created} == {context}


def test_wrong_matrix_context_cannot_publish_a_workflow_result() -> None:
    client = FakeGitHubClient()
    run = workflow_run(run_id=500, generation=generation(), status="queued")
    client.runs[GATE_SPECS[0].workflow_id] = [run]

    result = process_event(workflow_event(run), client, context=GATE_SPECS[1].context)

    assert result.outcome == "ignored"
    assert not client.created


def test_filtered_event_reconciles_the_latest_full_run_for_its_context() -> None:
    client = FakeGitHubClient()
    current_generation = generation()
    seed_invalidation(client, current_generation)
    completed = workflow_run(
        run_id=510,
        generation=current_generation,
        status="completed",
        conclusion="success",
    )
    filtered = workflow_run(
        run_id=511,
        generation=current_generation,
        status="completed",
    )
    filtered["display_title"] = str(filtered["display_title"]).replace(
        "gate=full", "gate=filtered", 1
    )
    client.runs[GATE_SPECS[0].workflow_id] = [completed, filtered]
    client.jobs[510] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(filtered), client, context=GATE_SPECS[0].context)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_current_generation_wins_over_a_delayed_old_run_with_a_larger_id() -> None:
    client = FakeGitHubClient()
    current = generation(
        updated="2026-07-22T10:00:02Z",
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, current)
    current_run = workflow_run(
        run_id=930,
        generation=current,
        status="completed",
        conclusion="success",
    )
    delayed_old_run = workflow_run(
        run_id=931,
        generation=generation(),
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run, delayed_old_run]
    client.jobs[930] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(delayed_old_run), client)

    assert result.outcome == "success"
    state = json.loads(client.checks[GATE_SPECS[0].context][0]["output"]["summary"])
    assert (state["run_id"], state["run_attempt"]) == (930, 1)


def test_exact_run_attempt_jobs_are_used() -> None:
    class RecordingClient(FakeGitHubClient):
        def __init__(self) -> None:
            super().__init__()
            self.requested_attempts: list[tuple[int, int]] = []

        def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
            self.requested_attempts.append((run_id, run_attempt))
            return super().list_run_jobs(run_id, run_attempt)

    client = RecordingClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=940,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[(940, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    client.jobs[(940, 2)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "failure"}]

    assert process_event(workflow_event(run), client).outcome == "success"
    assert client.requested_attempts == [(940, 1)]


def test_a_new_attempt_during_completion_keeps_the_gate_pending() -> None:
    class RerunDuringJobsClient(FakeGitHubClient):
        def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
            jobs = super().list_run_jobs(run_id, run_attempt)
            rerun = workflow_run(
                run_id=run_id,
                run_attempt=2,
                generation=generation(),
                status="in_progress",
            )
            self.runs[GATE_SPECS[0].workflow_id] = [rerun]
            return jobs

    client = RerunDuringJobsClient()
    current = generation()
    seed_invalidation(client, current)
    attempt_one = workflow_run(
        run_id=941,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [attempt_one]
    client.jobs[(941, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(attempt_one), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_observed_new_attempt_cannot_regress_within_one_invocation() -> None:
    class RegressingInventoryClient(FakeGitHubClient):
        inventory_reads = 0

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            self.inventory_reads += 1
            if self.inventory_reads == 3:
                return [deepcopy(second)]
            return [deepcopy(first)]

    current = generation()
    first = workflow_run(
        run_id=947,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
    )
    second = workflow_run(
        run_id=947,
        run_attempt=2,
        generation=current,
        status="in_progress",
    )
    client = RegressingInventoryClient()
    client.jobs[(947, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(first), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert (state["run_id"], state["run_attempt"]) == (947, 2)


def test_partial_issue_history_cannot_regress_the_persisted_watermark() -> None:
    client = FakeGitHubClient()
    current = generation(
        updated="2026-07-22T10:00:01Z",
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, current)
    check = client.checks[GATE_SPECS[0].context][0]
    original = json.loads(check["output"]["summary"])
    assert original["authority_history_count"] == 2

    client.issue_events = client.issue_events[:1]
    result = process_event(
        pull_event(action="ready_for_review", labels=["ci:images"]),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "in_progress"
    after = json.loads(client.checks[GATE_SPECS[0].context][0]["output"]["summary"])
    assert after["authority_epoch"] == original["authority_epoch"]
    assert after["authority_history_count"] == original["authority_history_count"]


def test_terminal_patch_is_compensated_when_authority_changes_after_write() -> None:
    class MutateAfterTerminalPatchClient(FakeGitHubClient):
        mutated = False

        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed" and not self.mutated:
                self.mutated = True
                self.pull["labels"] = [{"name": "ci:images"}]
                self.pull["updated_at"] = "2026-07-22T10:00:01Z"
                self.issue_events.append(
                    authority_event(
                        2,
                        "labeled",
                        "2026-07-22T10:00:01Z",
                        label="ci:images",
                    )
                )
            return response

    client = MutateAfterTerminalPatchClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=942,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[942] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_delayed_old_head_invocation_preserves_current_head_success() -> None:
    current_head = "c" * 40

    class MultiHeadClient(FakeGitHubClient):
        def list_check_runs(self, head_sha: str, context: str) -> Sequence[Mapping[str, Any]]:
            if head_sha != self.pull["head"]["sha"]:
                return []
            return deepcopy(self.checks[context])

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            return [
                deepcopy(run)
                for run in self.runs[workflow_id]
                if run.get("event") == event and run.get("head_sha") == head_sha
            ]

        def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]:
            return [deepcopy(self.pull)]

    client = MultiHeadClient()
    client.pull["head"]["sha"] = current_head
    current = Generation(
        head=current_head,
        base=BASE,
        pull="833",
        updated="2026-07-22T10:00:00Z",
        action="ready_for_review",
        label="none",
        labels=label_mask([]),
    )
    current_run = workflow_run(
        run_id=943,
        generation=current,
        status="completed",
        conclusion="success",
    )
    current_run["head_sha"] = current_head
    current_run["display_title"] = title(current)
    current_run["pull_requests"][0]["head"]["sha"] = current_head
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[943] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"

    old = generation()
    old_run = workflow_run(
        run_id=944,
        generation=old,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(old_run)

    result = process_event(workflow_event(old_run), client)

    assert result.outcome == "stale"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    state = json.loads(check["output"]["summary"])
    assert state["generation"]["head"] == current_head

    stale_prt = process_event(
        pull_event(action="ready_for_review"),
        client,
        context=GATE_SPECS[0].context,
    )
    assert stale_prt.outcome == "stale"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_same_second_mask_aware_ordinal_does_not_wait_for_an_unrelated_shape() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    client.issue_events = [
        authority_event(200, "labeled", tied_updated, label="ci:images"),
        authority_event(201, "unlabeled", tied_updated, label="ci:images"),
        authority_event(202, "unlabeled", tied_updated, label="cluster-smoke"),
        authority_event(203, "labeled", tied_updated, label="ci:images"),
    ]
    current = generation(
        updated=tied_updated,
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=945,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[945] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_issue_history_failure_leaves_a_known_new_event_pending() -> None:
    class FailingHistoryClient(FakeGitHubClient):
        fail_history = False

        def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]:
            if self.fail_history:
                raise PublisherError("temporary issue history failure")
            return super().list_issue_events(number)

    client = FailingHistoryClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=946,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[946] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    client.fail_history = True
    with pytest.raises(PublisherError, match="temporary issue history failure"):
        process_event(
            pull_event(
                action="labeled",
                label="ci:images",
                updated="2026-07-22T10:00:01Z",
            ),
            client,
            context=GATE_SPECS[0].context,
        )

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_workflow_attempt_webhook_is_an_inventory_floor() -> None:
    client = FakeGitHubClient()
    current = generation()
    first = workflow_run(
        run_id=950,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first]
    client.jobs[(950, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(first), client).outcome == "success"

    second = workflow_run(
        run_id=950,
        run_attempt=2,
        generation=current,
        status="in_progress",
    )
    result = process_event(workflow_event(second), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert (state["run_id"], state["run_attempt"]) == (950, 2)


def test_merge_group_new_attempt_during_completion_stays_pending() -> None:
    class MergeRerunDuringJobsClient(FakeGitHubClient):
        def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
            jobs = super().list_run_jobs(run_id, run_attempt)
            self.runs[GATE_SPECS[0].workflow_id] = [
                workflow_run(
                    run_id=run_id,
                    run_attempt=2,
                    generation=generation(
                        pull="none", updated="none", action="checks_requested", label="none"
                    ),
                    status="in_progress",
                    run_event="merge_group",
                    include_pull=False,
                )
            ]
            return jobs

    client = MergeRerunDuringJobsClient()
    current = generation(pull="none", updated="none", action="checks_requested", label="none")
    first = workflow_run(
        run_id=951,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
        run_event="merge_group",
        include_pull=False,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first]
    client.jobs[(951, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(first), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert (state["run_id"], state["run_attempt"]) == (951, 2)


def test_merge_group_observed_attempt_cannot_regress_within_one_invocation() -> None:
    class RegressingMergeInventoryClient(FakeGitHubClient):
        inventory_reads = 0

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            self.inventory_reads += 1
            if self.inventory_reads == 2:
                return [deepcopy(second)]
            return [deepcopy(first)]

    current = generation(pull="none", updated="none", action="checks_requested", label="none")
    first = workflow_run(
        run_id=966,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
        run_event="merge_group",
        include_pull=False,
    )
    second = workflow_run(
        run_id=966,
        run_attempt=2,
        generation=current,
        status="in_progress",
        run_event="merge_group",
        include_pull=False,
    )
    client = RegressingMergeInventoryClient()
    client.jobs[(966, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(first), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert (state["run_id"], state["run_attempt"]) == (966, 2)


def test_pull_read_failure_revokes_an_observed_same_head_transition() -> None:
    class FailingPullClient(FakeGitHubClient):
        fail_pull = False

        def get_pull_request(self, number: int) -> Mapping[str, Any]:
            if self.fail_pull:
                raise PublisherError("temporary pull read failure")
            return super().get_pull_request(number)

    client = FailingPullClient()
    current = generation()
    run = workflow_run(
        run_id=954,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[954] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    client.fail_pull = True
    with pytest.raises(PublisherError, match="temporary pull read failure"):
        process_event(
            pull_event(
                action="labeled",
                label="ci:images",
                updated="2026-07-22T10:00:01Z",
            ),
            client,
            context=GATE_SPECS[0].context,
        )

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_cross_second_label_aba_selects_only_the_latest_generation() -> None:
    client = FakeGitHubClient()
    client.issue_events = [
        authority_event(1, "ready_for_review", "2026-07-22T10:00:00Z"),
        authority_event(2, "labeled", "2026-07-22T10:00:01Z", label="ci:images"),
        authority_event(3, "unlabeled", "2026-07-22T10:00:02Z", label="ci:images"),
        authority_event(4, "labeled", "2026-07-22T10:00:03Z", label="ci:images"),
    ]
    current = generation(
        updated="2026-07-22T10:00:03Z",
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=955,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[955] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_issue_event_timestamp_skew_still_matches_the_source_generation() -> None:
    client = FakeGitHubClient()
    client.issue_events.append(
        authority_event(2, "labeled", "2026-07-22T10:00:02Z", label="ci:images")
    )
    current = generation(
        updated="2026-07-22T10:00:01Z",
        action="labeled",
        label="ci:images",
    )
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=956,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[956] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(run), client).outcome == "success"


def test_stale_same_head_prt_reconciles_the_completed_current_source() -> None:
    client = FakeGitHubClient()
    old = generation()
    old_run = workflow_run(
        run_id=957,
        generation=old,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[957] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(old_run), client).outcome == "success"

    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    client.issue_events.append(
        authority_event(2, "labeled", "2026-07-22T10:00:01Z", label="ci:images")
    )
    current = generation(
        updated="2026-07-22T10:00:01Z",
        action="labeled",
        label="ci:images",
    )
    current_run = workflow_run(
        run_id=958,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(current_run)
    client.jobs[958] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"

    result = process_event(
        pull_event(action="ready_for_review"),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "success"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["conclusion"] == "success"
    assert state["generation"] == current.as_dict()


def test_post_terminal_history_failure_is_compensated_to_pending() -> None:
    class FailHistoryAfterTerminalClient(FakeGitHubClient):
        fail_history = False

        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.fail_history = True
            return response

        def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]:
            if self.fail_history:
                raise PublisherError("post-write history failure")
            return super().list_issue_events(number)

    client = FailHistoryAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=959,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[959] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    with pytest.raises(PublisherError, match="post-write history failure"):
        process_event(workflow_event(run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_publisher_deactivation_reopens_an_old_custom_success() -> None:
    client = FakeGitHubClient()
    current = generation()
    run = workflow_run(
        run_id=960,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[960] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    client.publisher_active = False
    client.pull["base"]["ref"] = "main"
    result = process_event(
        pull_event(
            action="edited",
            changes={"base": {"ref": {"from": "dev"}}},
            base_ref="main",
        ),
        client,
        context=GATE_SPECS[0].context,
    )

    assert result.outcome == "legacy"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_foreign_same_head_pull_run_cannot_override_the_current_pull() -> None:
    client = FakeGitHubClient()
    current = generation()
    own_failure = workflow_run(
        run_id=961,
        generation=current,
        status="completed",
        conclusion="failure",
    )
    foreign_success = workflow_run(
        run_id=962,
        generation=current,
        status="completed",
        conclusion="success",
    )
    foreign_success["pull_requests"][0]["number"] = 999
    client.runs[GATE_SPECS[0].workflow_id] = [own_failure, foreign_success]
    client.jobs[962] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(own_failure), client)

    assert result.outcome == "failure"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["conclusion"] == "failure"
    assert state["run_id"] == 961


def test_multiple_open_pulls_for_one_head_fail_closed_before_create() -> None:
    class AmbiguousHeadClient(FakeGitHubClient):
        def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]:
            current = deepcopy(self.pull)
            other = deepcopy(self.pull)
            other["number"] = 999
            return [current, other]

    client = AmbiguousHeadClient()

    with pytest.raises(PublisherError, match="ambiguous open pull requests"):
        process_event(
            pull_event(action="ready_for_review"),
            client,
            context=GATE_SPECS[0].context,
        )

    assert not client.created


def test_missing_commit_association_fails_closed_before_create() -> None:
    class MissingAssociationClient(FakeGitHubClient):
        def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]:
            return []

    client = MissingAssociationClient()

    with pytest.raises(PublisherError, match="ambiguous open pull requests: none"):
        process_event(
            pull_event(action="ready_for_review"),
            client,
            context=GATE_SPECS[0].context,
        )

    assert not client.created


def test_workflow_registration_failure_revokes_a_new_attempt() -> None:
    class FailingWorkflowLookupClient(FakeGitHubClient):
        fail_workflow_lookup = False

        def get_workflow(self, workflow_id: int) -> Mapping[str, Any]:
            if self.fail_workflow_lookup:
                raise PublisherError("temporary workflow lookup failure")
            return super().get_workflow(workflow_id)

    client = FailingWorkflowLookupClient()
    current = generation()
    first = workflow_run(
        run_id=963,
        run_attempt=1,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first]
    client.jobs[(963, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(first), client).outcome == "success"

    client.fail_workflow_lookup = True
    second = workflow_run(
        run_id=963,
        run_attempt=2,
        generation=current,
        status="in_progress",
    )
    with pytest.raises(PublisherError, match="temporary workflow lookup failure"):
        process_event(workflow_event(second), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


@pytest.mark.parametrize("event_conclusion", ["failure", "cancelled"])
def test_terminal_webhook_overrides_stale_rest_for_the_same_attempt(
    event_conclusion: str,
) -> None:
    client = FakeGitHubClient()
    current = generation()
    stale_success = workflow_run(
        run_id=964,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [stale_success]
    client.jobs[964] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    terminal_event = {
        **stale_success,
        "conclusion": event_conclusion,
    }

    result = process_event(workflow_event(terminal_event), client)

    assert result.outcome == "failure"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "failure"


def test_invalid_terminal_patch_response_is_compensated_to_pending() -> None:
    class InvalidTerminalResponseClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                response = deepcopy(response)
                response["external_id"] = "invalid-after-write"
            return response

    client = InvalidTerminalResponseClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=965,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[965] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    with pytest.raises(PublisherError, match="did not return"):
        process_event(workflow_event(run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_observed_new_pr_authority_cannot_regress_within_one_invocation() -> None:
    class RegressingAuthorityClient(FakeGitHubClient):
        pull_reads = 0
        event_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, Any]:
            self.pull_reads += 1
            pull = deepcopy(self.pull)
            if self.pull_reads == 2:
                pull["labels"] = [{"name": "ci:images"}]
                pull["updated_at"] = "2026-07-22T10:00:01Z"
            return pull

        def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]:
            self.event_reads += 1
            if self.event_reads == 2:
                return [
                    *deepcopy(self.issue_events),
                    authority_event(
                        2,
                        "labeled",
                        "2026-07-22T10:00:01Z",
                        label="ci:images",
                    ),
                ]
            return deepcopy(self.issue_events)

    client = RegressingAuthorityClient()
    current = generation()
    run = workflow_run(
        run_id=967,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[967] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "in_progress"
    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert state["authority_epoch"] == 2
    assert state["generation"]["labels"] == label_mask(["ci:images"])


def test_delayed_older_prt_read_failure_preserves_newer_terminal_generation() -> None:
    class FailingPullAfterCurrentClient(FakeGitHubClient):
        fail_pull = False

        def get_pull_request(self, number: int) -> Mapping[str, Any]:
            if self.fail_pull:
                raise PublisherError("temporary pull read failure")
            return super().get_pull_request(number)

    client = FailingPullAfterCurrentClient()
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    client.issue_events.append(
        authority_event(2, "labeled", "2026-07-22T10:00:01Z", label="ci:images")
    )
    current = generation(
        updated="2026-07-22T10:00:01Z",
        action="labeled",
        label="ci:images",
    )
    run = workflow_run(
        run_id=968,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[968] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    client.fail_pull = True
    with pytest.raises(PublisherError, match="temporary pull read failure"):
        process_event(
            pull_event(action="ready_for_review"),
            client,
            context=GATE_SPECS[0].context,
        )

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"] == current.as_dict()


def test_delayed_older_workflow_lookup_failure_preserves_newer_terminal() -> None:
    class FailingOldWorkflowClient(FakeGitHubClient):
        fail_workflow_lookup = False

        def get_workflow(self, workflow_id: int) -> Mapping[str, Any]:
            if self.fail_workflow_lookup:
                raise PublisherError("temporary workflow lookup failure")
            return super().get_workflow(workflow_id)

    client = FailingOldWorkflowClient()
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = "2026-07-22T10:00:01Z"
    client.issue_events.append(
        authority_event(2, "labeled", "2026-07-22T10:00:01Z", label="ci:images")
    )
    current = generation(
        updated="2026-07-22T10:00:01Z",
        action="labeled",
        label="ci:images",
    )
    current_run = workflow_run(
        run_id=969,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[969] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"

    client.fail_workflow_lookup = True
    old_run = workflow_run(
        run_id=968,
        generation=generation(),
        status="completed",
        conclusion="failure",
    )
    with pytest.raises(PublisherError, match="temporary workflow lookup failure"):
        process_event(workflow_event(old_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"] == current.as_dict()


def test_closed_old_pull_event_cannot_revoke_current_same_sha_owner() -> None:
    class SequentialPullClient(FakeGitHubClient):
        def get_pull_request(self, number: int) -> Mapping[str, Any]:
            if number == 833:
                return super().get_pull_request(number)
            assert number == 832
            old_pull = deepcopy(self.pull)
            old_pull["number"] = 832
            old_pull["state"] = "closed"
            old_pull["merged"] = True
            return old_pull

    client = SequentialPullClient()
    current = generation()
    current_run = workflow_run(
        run_id=970,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[970] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"

    old_generation = generation(pull="832")
    old_run = workflow_run(
        run_id=971,
        generation=old_generation,
        status="completed",
        conclusion="failure",
    )
    old_run["pull_requests"][0]["number"] = 832
    with pytest.raises(PublisherError, match="ambiguous open pull requests: 833"):
        process_event(workflow_event(old_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"]["pull"] == "833"


def test_natural_merge_after_terminal_patch_does_not_reopen_success() -> None:
    class MergeAfterTerminalClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.pull["state"] = "closed"
                self.pull["merged"] = True
                self.pull["merged_at"] = "2026-07-22T10:05:00Z"
            return response

    client = MergeAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=972,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[972] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    assert result.outcome == "success"
    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_converted_to_draft_trusted_event_revokes_terminal_success() -> None:
    client = FakeGitHubClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=973,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[973] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    draft_updated = "2026-07-22T10:00:01Z"
    client.pull["draft"] = True
    client.pull["updated_at"] = draft_updated
    client.issue_events.append(authority_event(2, "convert_to_draft", draft_updated))

    process_event(
        pull_event(
            action="converted_to_draft",
            updated=draft_updated,
            draft=True,
        ),
        client,
        context=GATE_SPECS[0].context,
    )

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "in_progress"
    assert "conclusion" not in check
    assert state["generation"]["action"] == "converted_to_draft"


def test_same_second_cross_base_retargets_count_every_source_occurrence() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    intermediate_base = "c" * 40
    intermediate = Generation(
        head=HEAD,
        base=intermediate_base,
        pull="833",
        updated=tied_updated,
        action="edited",
        label="none",
        labels=label_mask([]),
    )
    current = generation(updated=tied_updated, action="edited")
    client.pull["updated_at"] = tied_updated
    client.issue_events.extend(
        [
            authority_event(2, "base_ref_changed", tied_updated),
            authority_event(3, "base_ref_changed", tied_updated),
        ]
    )
    intermediate_run = workflow_run(
        run_id=973,
        generation=intermediate,
        status="completed",
        conclusion="failure",
    )
    intermediate_run["pull_requests"][0]["base"] = {
        "sha": intermediate_base,
        "ref": "main",
    }
    current_run = workflow_run(
        run_id=974,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [intermediate_run, current_run]
    client.jobs[974] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(current_run), client)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_same_second_cross_head_force_push_uses_destination_commit() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    current = generation(updated=tied_updated, action="synchronize")
    client.pull["updated_at"] = tied_updated
    client.issue_events.extend(
        [
            authority_event(2, "head_ref_force_pushed", tied_updated, commit_id="c" * 40),
            authority_event(3, "head_ref_force_pushed", tied_updated, commit_id=HEAD),
        ]
    )
    current_run = workflow_run(
        run_id=975,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[975] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(current_run), client)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_ordinary_synchronize_after_same_second_force_push_uses_unique_head() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    current = generation(updated=tied_updated, action="synchronize")
    client.pull["updated_at"] = tied_updated
    client.issue_events.append(
        authority_event(2, "head_ref_force_pushed", tied_updated, commit_id="c" * 40)
    )
    current_run = workflow_run(
        run_id=976,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[976] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(current_run), client)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_same_second_force_push_aba_waits_for_final_matching_head_run() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    repeated = generation(updated=tied_updated, action="synchronize")
    client.pull["updated_at"] = tied_updated
    client.issue_events.extend(
        [
            authority_event(2, "head_ref_force_pushed", tied_updated, commit_id=HEAD),
            authority_event(3, "head_ref_force_pushed", tied_updated, commit_id="c" * 40),
            authority_event(4, "head_ref_force_pushed", tied_updated, commit_id=HEAD),
        ]
    )
    old_run = workflow_run(
        run_id=977,
        generation=repeated,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[977] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    assert process_event(workflow_event(old_run), client).outcome == "in_progress"
    assert client.checks[GATE_SPECS[0].context][0]["status"] == "in_progress"

    current_run = workflow_run(
        run_id=978,
        generation=repeated,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(current_run)
    client.jobs[978] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(current_run), client)

    assert result.outcome == "success"
    assert client.checks[GATE_SPECS[0].context][0]["conclusion"] == "success"


def test_merged_pull_does_not_preserve_success_over_a_newer_attempt() -> None:
    class MergeAndRerunAfterTerminalClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.pull["state"] = "closed"
                self.pull["merged"] = True
                self.pull["merged_at"] = "2026-07-22T10:05:00Z"
                self.runs[GATE_SPECS[0].workflow_id] = [
                    workflow_run(
                        run_id=979,
                        generation=generation(),
                        status="in_progress",
                        run_attempt=2,
                    )
                ]
            return response

    client = MergeAndRerunAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    first_attempt = workflow_run(
        run_id=979,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first_attempt]
    client.jobs[979] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    process_event(workflow_event(first_attempt), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_merged_pull_rejects_cross_base_replacement_before_event_history_catches_up() -> None:
    replacement_base = "c" * 40
    replacement = Generation(
        head=HEAD,
        base=replacement_base,
        pull="833",
        updated="2026-07-22T10:00:01Z",
        action="edited",
        label="none",
        labels=label_mask([]),
    )

    class RetargetAndMergeAfterTerminalClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.pull["state"] = "closed"
                self.pull["merged"] = True
                self.pull["merged_at"] = "2026-07-22T10:05:00Z"
                self.pull["base"] = {
                    "sha": replacement_base,
                    "ref": "main",
                    "repo": {"full_name": REPOSITORY},
                }
                replacement_run = workflow_run(
                    run_id=981,
                    generation=replacement,
                    status="in_progress",
                )
                replacement_run["pull_requests"][0]["base"] = {
                    "sha": replacement_base,
                    "ref": "main",
                }
                self.runs[GATE_SPECS[0].workflow_id] = [replacement_run]
            return response

    client = RetargetAndMergeAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    first_run = workflow_run(
        run_id=980,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first_run]
    client.jobs[980] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    process_event(workflow_event(first_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_strict_false_base_tip_advance_then_natural_merge_preserves_success() -> None:
    class BaseAdvanceAndMergeAfterTerminalClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.pull["state"] = "closed"
                self.pull["merged"] = True
                self.pull["merged_at"] = "2026-07-22T10:05:00Z"
                self.pull["base"]["sha"] = "c" * 40
            return response

    client = BaseAdvanceAndMergeAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=982,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[982] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    result = process_event(workflow_event(run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"


def test_merged_retarget_without_run_association_revokes_old_base_success() -> None:
    class RetargetAndMergeAfterTerminalClient(FakeGitHubClient):
        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed":
                self.pull["state"] = "closed"
                self.pull["merged"] = True
                self.pull["merged_at"] = "2026-07-22T10:05:00Z"
                self.pull["base"] = {
                    "sha": "c" * 40,
                    "ref": "main",
                    "repo": {"full_name": REPOSITORY},
                }
            return response

    client = RetargetAndMergeAfterTerminalClient()
    current = generation()
    seed_invalidation(client, current)
    run = workflow_run(
        run_id=983,
        generation=current,
        status="completed",
        conclusion="success",
        include_pull=False,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[983] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    process_event(workflow_event(run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_pr_handler_never_forgets_a_newer_observed_attempt() -> None:
    class RegressingRunInventoryClient(FakeGitHubClient):
        calls = 0
        newer: Mapping[str, Any]
        older: Mapping[str, Any]

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            assert workflow_id == GATE_SPECS[0].workflow_id
            assert head_sha == HEAD
            assert event == "pull_request"
            self.calls += 1
            return [deepcopy(self.newer if self.calls == 1 else self.older)]

    client = RegressingRunInventoryClient()
    current = generation()
    first_attempt = workflow_run(
        run_id=984,
        generation=current,
        status="completed",
        conclusion="success",
    )
    second_attempt = workflow_run(
        run_id=984,
        generation=current,
        status="completed",
        conclusion="failure",
        run_attempt=2,
    )
    client.older = first_attempt
    client.newer = second_attempt

    result = process_event(workflow_event(first_attempt), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "failure"
    assert check["status"] == "completed"
    assert check["conclusion"] == "failure"
    assert (state["run_id"], state["run_attempt"]) == (984, 2)


def test_prt_reconcile_never_forgets_its_selected_newer_attempt() -> None:
    class RegressingRunInventoryClient(FakeGitHubClient):
        calls = 0
        newer: Mapping[str, Any]
        older: Mapping[str, Any]

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            assert workflow_id == GATE_SPECS[0].workflow_id
            assert head_sha == HEAD
            assert event == "pull_request"
            self.calls += 1
            return [deepcopy(self.newer if self.calls == 1 else self.older)]

    client = RegressingRunInventoryClient()
    current = generation()
    client.older = workflow_run(
        run_id=985,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.newer = workflow_run(
        run_id=985,
        generation=current,
        status="completed",
        conclusion="failure",
        run_attempt=2,
    )

    result = process_event(
        pull_event(action="ready_for_review"),
        client,
        context=GATE_SPECS[0].context,
    )

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "failure"
    assert check["conclusion"] == "failure"
    assert (state["run_id"], state["run_attempt"]) == (985, 2)


def test_merge_group_handler_never_forgets_a_newer_observed_attempt() -> None:
    class RegressingMergeInventoryClient(FakeGitHubClient):
        calls = 0
        newer: Mapping[str, Any]
        older: Mapping[str, Any]

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            assert workflow_id == GATE_SPECS[0].workflow_id
            assert head_sha == HEAD
            assert event == "merge_group"
            self.calls += 1
            return [deepcopy(self.newer if self.calls == 1 else self.older)]

    client = RegressingMergeInventoryClient()
    current = generation(
        pull="none",
        updated="none",
        action="checks_requested",
    )
    first_attempt = workflow_run(
        run_id=986,
        generation=current,
        status="completed",
        conclusion="success",
        run_event="merge_group",
        include_pull=False,
    )
    second_attempt = workflow_run(
        run_id=986,
        generation=current,
        status="completed",
        conclusion="failure",
        run_attempt=2,
        run_event="merge_group",
        include_pull=False,
    )
    client.older = first_attempt
    client.newer = second_attempt

    result = process_event(workflow_event(first_attempt), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "failure"
    assert check["conclusion"] == "failure"
    assert (state["run_id"], state["run_attempt"]) == (986, 2)


def test_newer_attempt_read_failure_revokes_an_older_terminal_success() -> None:
    class FailingCurrentnessReadClient(FakeGitHubClient):
        fail_after_initial_pull = False
        pull_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, Any]:
            self.pull_reads += 1
            if self.fail_after_initial_pull and self.pull_reads > 1:
                raise PublisherError("currentness read failed")
            return super().get_pull_request(number)

    client = FailingCurrentnessReadClient()
    current = generation()
    first_attempt = workflow_run(
        run_id=987,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first_attempt]
    client.jobs[987] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(first_attempt), client).outcome == "success"

    second_attempt = workflow_run(
        run_id=987,
        generation=current,
        status="completed",
        conclusion="failure",
        run_attempt=2,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [second_attempt]
    client.pull_reads = 0
    client.fail_after_initial_pull = True

    with pytest.raises(PublisherError, match="currentness read failed"):
        process_event(
            pull_event(action="ready_for_review"),
            client,
            context=GATE_SPECS[0].context,
        )

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_partial_authority_history_cannot_hide_a_newer_attempt_failure() -> None:
    client = FakeGitHubClient()
    updated = "2026-07-22T10:00:01Z"
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = updated
    client.issue_events.append(authority_event(2, "labeled", updated, label="ci:images"))
    current = generation(
        updated=updated,
        action="labeled",
        label="ci:images",
    )
    first_attempt = workflow_run(
        run_id=988,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first_attempt]
    client.jobs[988] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(first_attempt), client).outcome == "success"

    second_attempt = workflow_run(
        run_id=988,
        generation=current,
        status="completed",
        conclusion="failure",
        run_attempt=2,
    )
    client.runs[GATE_SPECS[0].workflow_id] = [second_attempt]
    client.issue_events = client.issue_events[:1]
    filtered = workflow_run(
        run_id=989,
        generation=current,
        status="completed",
        conclusion="success",
    )
    filtered["display_title"] = str(filtered["display_title"]).replace(
        "gate=full",
        "gate=filtered",
        1,
    )

    process_event(workflow_event(filtered), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_partial_history_duplicate_preserves_exact_synchronize_terminal() -> None:
    client = FakeGitHubClient()
    labeled_updated = "2026-07-22T10:00:01Z"
    synchronize_updated = "2026-07-22T10:00:02Z"
    client.pull["labels"] = [{"name": "ci:images"}]
    client.pull["updated_at"] = synchronize_updated
    client.issue_events.append(authority_event(2, "labeled", labeled_updated, label="ci:images"))
    current = generation(
        updated=synchronize_updated,
        action="synchronize",
        labels=["ci:images"],
    )
    run = workflow_run(
        run_id=1002,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[1002] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"

    client.issue_events = client.issue_events[:1]
    result = process_event(workflow_event(run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["authority_history_count"] == 2
    assert (state["run_id"], state["run_attempt"]) == (1002, 1)


def test_partial_history_cannot_prefer_an_older_authority_run() -> None:
    client = FakeGitHubClient()
    old_generation = generation()
    old_run = workflow_run(
        run_id=990,
        generation=old_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[990] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(old_run), client).outcome == "success"

    newer_generation = generation(
        updated="2026-07-22T10:00:01Z",
        action="reopened",
    )
    client.pull["updated_at"] = newer_generation.updated
    newer_run = workflow_run(
        run_id=991,
        generation=newer_generation,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run, newer_run]
    filtered = workflow_run(
        run_id=992,
        generation=newer_generation,
        status="completed",
        conclusion="success",
    )
    filtered["display_title"] = str(filtered["display_title"]).replace(
        "gate=full",
        "gate=filtered",
        1,
    )

    process_event(workflow_event(filtered), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_stale_compensation_preserves_a_newer_authority_terminal() -> None:
    newer_updated = "2026-07-22T10:00:01Z"
    newer_generation = generation(
        updated=newer_updated,
        action="labeled",
        label="ci:images",
    )

    class NewAuthorityDuringTerminalPatchClient(FakeGitHubClient):
        triggered = False

        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if payload.get("status") == "completed" and not self.triggered:
                self.triggered = True
                self.pull["labels"] = [{"name": "ci:images"}]
                self.pull["updated_at"] = newer_updated
                self.issue_events.append(
                    authority_event(
                        2,
                        "labeled",
                        newer_updated,
                        label="ci:images",
                    )
                )
                newer_run = workflow_run(
                    run_id=994,
                    generation=newer_generation,
                    status="completed",
                    conclusion="success",
                )
                self.runs[GATE_SPECS[0].workflow_id] = [newer_run]
                self.jobs[994] = [
                    {
                        "name": GATE_SPECS[0].attempt_job,
                        "conclusion": "success",
                    }
                ]
                assert process_event(workflow_event(newer_run), self).outcome == "success"
            return response

    client = NewAuthorityDuringTerminalPatchClient()
    old_generation = generation()
    seed_invalidation(client, old_generation)
    old_run = workflow_run(
        run_id=993,
        generation=old_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[993] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]

    process_event(workflow_event(old_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"] == newer_generation.as_dict()
    assert (state["run_id"], state["run_attempt"]) == (994, 1)


def test_proven_stale_same_second_higher_run_cannot_revoke_current_success() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    client.pull["updated_at"] = tied_updated
    client.issue_events = [
        authority_event(2, "ready_for_review", tied_updated),
        authority_event(3, "reopened", tied_updated),
    ]
    current = generation(updated=tied_updated, action="reopened")
    current_run = workflow_run(
        run_id=995,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[995] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"

    stale = generation(updated=tied_updated, action="ready_for_review")
    stale_run = workflow_run(
        run_id=996,
        generation=stale,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(stale_run)
    filtered = workflow_run(
        run_id=997,
        generation=stale,
        status="completed",
        conclusion="success",
    )
    filtered["display_title"] = str(filtered["display_title"]).replace(
        "gate=full",
        "gate=filtered",
        1,
    )

    result = process_event(workflow_event(filtered), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"] == current.as_dict()


def test_post_merge_proven_stale_full_delivery_restores_preempted_success() -> None:
    client = FakeGitHubClient()
    tied_updated = "2026-07-22T10:00:01Z"
    client.pull["updated_at"] = tied_updated
    client.issue_events = [
        authority_event(2, "ready_for_review", tied_updated),
        authority_event(3, "reopened", tied_updated),
    ]
    current = generation(updated=tied_updated, action="reopened")
    current_run = workflow_run(
        run_id=999,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[999] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"

    stale = generation(updated=tied_updated, action="ready_for_review")
    stale_run = workflow_run(
        run_id=1000,
        generation=stale,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(stale_run)

    result = process_event(workflow_event(stale_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    state = json.loads(check["output"]["summary"])
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
    assert state["generation"] == current.as_dict()
    assert (state["run_id"], state["run_attempt"]) == (999, 1)


def test_post_merge_restoration_rechecks_inventory_after_terminal_patch() -> None:
    tied_updated = "2026-07-22T10:00:01Z"
    current = generation(updated=tied_updated, action="reopened")

    class RerunDuringRestorationClient(FakeGitHubClient):
        restoration_armed = False
        rerun_injected = False

        def update_check_run(
            self, check_run_id: int, payload: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            response = super().update_check_run(check_run_id, payload)
            if (
                self.restoration_armed
                and payload.get("status") == "completed"
                and not self.rerun_injected
            ):
                self.rerun_injected = True
                self.runs[GATE_SPECS[0].workflow_id].append(
                    workflow_run(
                        run_id=1020,
                        run_attempt=2,
                        generation=current,
                        status="completed",
                        conclusion="failure",
                    )
                )
            return response

    client = RerunDuringRestorationClient()
    client.pull["updated_at"] = tied_updated
    client.issue_events = [
        authority_event(2, "ready_for_review", tied_updated),
        authority_event(3, "reopened", tied_updated),
    ]
    current_run = workflow_run(
        run_id=1020,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[(1020, 1)] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"

    stale = generation(updated=tied_updated, action="ready_for_review")
    stale_run = workflow_run(
        run_id=1021,
        generation=stale,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(stale_run)
    client.restoration_armed = True

    result = process_event(workflow_event(stale_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert client.rerun_injected
    assert result.outcome == "in_progress"
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_post_merge_restoration_rejects_a_newer_live_snapshot_generation() -> None:
    tied_updated = "2026-07-22T10:00:01Z"
    client = FakeGitHubClient()
    client.pull["updated_at"] = tied_updated
    client.issue_events = [
        authority_event(2, "ready_for_review", tied_updated),
        authority_event(3, "reopened", tied_updated),
    ]
    restored_generation = generation(updated=tied_updated, action="reopened")
    restored_run = workflow_run(
        run_id=2020,
        generation=restored_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [restored_run]
    client.jobs[2020] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(restored_run), client).outcome == "success"

    # The source run for a second reopen can become visible before its issue
    # event. Its timestamp still binds to the currently visible reopen within
    # GitHub's clock tolerance, so restoration must treat it as potentially
    # current instead of considering only the exact old generation.
    newer_updated = "2026-07-22T10:00:03Z"
    client.pull["updated_at"] = newer_updated
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"
    stale_generation = generation(updated=tied_updated, action="ready_for_review")
    stale_run = workflow_run(
        run_id=2021,
        generation=stale_generation,
        status="completed",
        conclusion="failure",
    )
    newer_generation = generation(updated=newer_updated, action="reopened")
    newer_run = workflow_run(
        run_id=2022,
        generation=newer_generation,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].extend([stale_run, newer_run])

    result = process_event(workflow_event(stale_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "stale"
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_post_merge_restoration_read_failure_is_compensated_to_pending() -> None:
    tied_updated = "2026-07-22T10:00:01Z"

    class FailingPostRestorationReadClient(FakeGitHubClient):
        restoration_armed = False
        restoration_inventory_reads = 0

        def list_workflow_runs(
            self, workflow_id: int, head_sha: str, event: str
        ) -> Sequence[Mapping[str, Any]]:
            if self.restoration_armed:
                self.restoration_inventory_reads += 1
                if self.restoration_inventory_reads == 2:
                    raise PublisherError("post-restoration inventory failure")
            return super().list_workflow_runs(workflow_id, head_sha, event)

    client = FailingPostRestorationReadClient()
    client.pull["updated_at"] = tied_updated
    client.issue_events = [
        authority_event(2, "ready_for_review", tied_updated),
        authority_event(3, "reopened", tied_updated),
    ]
    current = generation(updated=tied_updated, action="reopened")
    current_run = workflow_run(
        run_id=1022,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [current_run]
    client.jobs[1022] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(current_run), client).outcome == "success"
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"

    stale = generation(updated=tied_updated, action="ready_for_review")
    stale_run = workflow_run(
        run_id=1023,
        generation=stale,
        status="completed",
        conclusion="failure",
    )
    client.runs[GATE_SPECS[0].workflow_id].append(stale_run)
    client.restoration_armed = True

    with pytest.raises(PublisherError, match="post-restoration inventory failure"):
        process_event(workflow_event(stale_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


@pytest.mark.parametrize(
    ("run_attempt", "event_conclusion"),
    [
        (2, "failure"),
        (1, "failure"),
        (1, "cancelled"),
    ],
)
def test_post_merge_unsafe_terminal_delivery_cannot_restore_success(
    run_attempt: int,
    event_conclusion: str,
) -> None:
    client = FakeGitHubClient()
    current = generation()
    first_attempt = workflow_run(
        run_id=1001,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [first_attempt]
    client.jobs[1001] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(first_attempt), client).outcome == "success"
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"

    unsafe_event = workflow_run(
        run_id=1001,
        run_attempt=run_attempt,
        generation=current,
        status="completed",
        conclusion=event_conclusion,
    )

    result = process_event(workflow_event(unsafe_event), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "stale"
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_post_merge_lower_id_newer_authority_cannot_restore_old_success() -> None:
    client = FakeGitHubClient()
    old_generation = generation()
    old_run = workflow_run(
        run_id=1010,
        generation=old_generation,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [old_run]
    client.jobs[1010] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(old_run), client).outcome == "success"

    reopened_updated = "2026-07-22T10:00:01Z"
    client.pull["updated_at"] = reopened_updated
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"
    client.issue_events.append(authority_event(2, "reopened", reopened_updated))
    newer_generation = generation(
        updated=reopened_updated,
        action="reopened",
    )
    newer_run = workflow_run(
        run_id=1009,
        generation=newer_generation,
        status="completed",
        conclusion="failure",
    )

    result = process_event(workflow_event(newer_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "stale"
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


def test_post_merge_stale_event_cannot_restore_an_even_older_terminal() -> None:
    client = FakeGitHubClient()
    oldest = generation()
    oldest_run = workflow_run(
        run_id=1011,
        generation=oldest,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [oldest_run]
    client.jobs[1011] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(oldest_run), client).outcome == "success"

    reopened_updated = "2026-07-22T10:00:01Z"
    draft_updated = "2026-07-22T10:00:02Z"
    client.pull["updated_at"] = draft_updated
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"
    client.issue_events.extend(
        [
            authority_event(2, "reopened", reopened_updated),
            authority_event(3, "convert_to_draft", draft_updated),
        ]
    )
    stale_middle = generation(
        updated=reopened_updated,
        action="reopened",
    )
    stale_run = workflow_run(
        run_id=1012,
        generation=stale_middle,
        status="completed",
        conclusion="failure",
    )

    result = process_event(workflow_event(stale_run), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "stale"
    assert check["status"] == "in_progress"
    assert "conclusion" not in check


@pytest.mark.parametrize("duplicate_mode", ["filtered", "full"])
def test_late_post_merge_source_delivery_preserves_terminal_success(
    duplicate_mode: str,
) -> None:
    client = FakeGitHubClient()
    current = generation()
    run = workflow_run(
        run_id=998,
        generation=current,
        status="completed",
        conclusion="success",
    )
    client.runs[GATE_SPECS[0].workflow_id] = [run]
    client.jobs[998] = [{"name": GATE_SPECS[0].attempt_job, "conclusion": "success"}]
    assert process_event(workflow_event(run), client).outcome == "success"
    client.pull["state"] = "closed"
    client.pull["merged"] = True
    client.pull["merged_at"] = "2026-07-22T10:05:00Z"

    duplicate = deepcopy(run)
    if duplicate_mode == "filtered":
        duplicate["display_title"] = str(duplicate["display_title"]).replace(
            "gate=full",
            "gate=filtered",
            1,
        )

    result = process_event(workflow_event(duplicate), client)

    check = client.checks[GATE_SPECS[0].context][0]
    assert result.outcome == "success"
    assert check["status"] == "completed"
    assert check["conclusion"] == "success"
