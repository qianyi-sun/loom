from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, TypeGuard
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

GITHUB_ACTIONS_APP_ID = 15368
AUTHORITATIVE_WORKFLOW_PATH = ".github/workflows/authoritative-gates.yml"
EXTERNAL_ID_PREFIX = "loom-authoritative-gate:"
FULL_TITLE_PREFIX = "gate=full /"

RELEVANT_LABEL_ORDER = (
    "ci:integration",
    "ci:integration-docker",
    "ci:images",
    "cluster-smoke",
    "staging-smoke",
    "ci:coverage-summary",
)
RELEVANT_LABELS = frozenset(RELEVANT_LABEL_ORDER)
AUTHORITY_EVENT_NAMES = frozenset(
    {
        "base_ref_changed",
        "convert_to_draft",
        "head_ref_force_pushed",
        "ready_for_review",
        "reopened",
    }
)


@dataclass(frozen=True)
class GateSpec:
    workflow_id: int
    workflow_name: str
    workflow_path: str
    context: str

    @property
    def attempt_job(self) -> str:
        return f"{self.context}-attempt"

    def external_id(self, *, repository: str, head_sha: str) -> str:
        return f"{EXTERNAL_ID_PREFIX}{repository}:{head_sha}:{self.context}"


GATE_SPECS = (
    GateSpec(
        workflow_id=302898379,
        workflow_name="CI",
        workflow_path=".github/workflows/ci.yml",
        context="repository-checks",
    ),
    GateSpec(
        workflow_id=302898384,
        workflow_name="images",
        workflow_path=".github/workflows/images.yml",
        context="images-gate",
    ),
    GateSpec(
        workflow_id=302898381,
        workflow_name="cluster-smoke",
        workflow_path=".github/workflows/cluster-smoke.yml",
        context="cluster-smoke-gate",
    ),
    GateSpec(
        workflow_id=302898388,
        workflow_name="staging-smoke",
        workflow_path=".github/workflows/staging-smoke.yml",
        context="staging-smoke-gate",
    ),
)

GATE_BY_WORKFLOW_ID = {spec.workflow_id: spec for spec in GATE_SPECS}


class PublisherError(RuntimeError):
    """The publisher could not establish one authoritative result."""


class DuplicateCustomCheckError(PublisherError):
    """More than one publisher-owned CheckRun exists for a context."""


class PullAuthorityError(PublisherError):
    """A head does not resolve to exactly one expected open pull request."""

    def __init__(self, head_sha: str, associated: set[int]) -> None:
        self.associated = frozenset(associated)
        rendered = ", ".join(str(number) for number in sorted(associated)) or "none"
        super().__init__(
            f"head {head_sha} is associated with ambiguous open pull requests: {rendered}"
        )


class GitHubAPIError(PublisherError):
    def __init__(self, method: str, path: str, status: int | None) -> None:
        status_text = "network error" if status is None else f"HTTP {status}"
        super().__init__(f"GitHub API {method} {path} failed: {status_text}")
        self.status = status


class PublisherClient(Protocol):
    repository: str

    def get_pull_request(self, number: int) -> Mapping[str, Any]: ...

    def base_contains_publisher(self, base_sha: str) -> bool: ...

    def get_workflow(self, workflow_id: int) -> Mapping[str, Any]: ...

    def list_check_runs(self, head_sha: str, context: str) -> Sequence[Mapping[str, Any]]: ...

    def create_check_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def update_check_run(
        self, check_run_id: int, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    def list_workflow_runs(
        self, workflow_id: int, head_sha: str, event: str
    ) -> Sequence[Mapping[str, Any]]: ...

    def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]: ...

    def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]: ...

    def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]: ...


class GitHubClient:
    """Small standard-library GitHub REST client used by the trusted workflow."""

    def __init__(
        self,
        *,
        token: str,
        repository: str,
        api_url: str = "https://api.github.com",
    ) -> None:
        if not token:
            raise ValueError("GITHUB_TOKEN is required")
        owner_repo = repository.split("/")
        if len(owner_repo) != 2 or not all(owner_repo):
            raise ValueError("GITHUB_REPOSITORY must be owner/name")
        self.repository = repository
        self._repo_path = "/".join(quote(part, safe="") for part in owner_repo)
        self._token = token
        self._api_url = api_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str | int] | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        url = f"{self._api_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(
            url,
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "loom-authoritative-gate",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise GitHubAPIError(method, path, exc.code) from exc
        except (URLError, TimeoutError) as exc:
            raise GitHubAPIError(method, path, None) from exc
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PublisherError(f"GitHub API {method} {path} returned invalid JSON") from exc

    def _paginated(
        self,
        path: str,
        key: str,
        *,
        query: Mapping[str, str | int] | None = None,
    ) -> list[Mapping[str, Any]]:
        items: list[Mapping[str, Any]] = []
        page = 1
        while True:
            page_query: dict[str, str | int] = dict(query or {})
            page_query.update({"per_page": 100, "page": page})
            response = self._request("GET", path, query=page_query)
            batch = response.get(key, []) if isinstance(response, Mapping) else []
            if not isinstance(batch, list) or not all(isinstance(item, Mapping) for item in batch):
                raise PublisherError(f"GitHub API {path} returned invalid {key}")
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1

    def get_pull_request(self, number: int) -> Mapping[str, Any]:
        response = self._request("GET", f"/repos/{self._repo_path}/pulls/{number}")
        if not isinstance(response, Mapping):
            raise PublisherError("GitHub pull request response is not an object")
        return response

    def base_contains_publisher(self, base_sha: str) -> bool:
        path = f"/repos/{self._repo_path}/contents/{quote(AUTHORITATIVE_WORKFLOW_PATH, safe='/')}"
        try:
            self._request("GET", path, query={"ref": base_sha})
        except GitHubAPIError as exc:
            if exc.status == 404:
                return False
            raise
        return True

    def get_workflow(self, workflow_id: int) -> Mapping[str, Any]:
        response = self._request("GET", f"/repos/{self._repo_path}/actions/workflows/{workflow_id}")
        if not isinstance(response, Mapping):
            raise PublisherError("GitHub workflow response is not an object")
        return response

    def list_check_runs(self, head_sha: str, context: str) -> Sequence[Mapping[str, Any]]:
        return self._paginated(
            f"/repos/{self._repo_path}/commits/{quote(head_sha, safe='')}/check-runs",
            "check_runs",
            query={"check_name": context, "filter": "all"},
        )

    def create_check_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        response = self._request("POST", f"/repos/{self._repo_path}/check-runs", payload=payload)
        if not isinstance(response, Mapping):
            raise PublisherError("GitHub create CheckRun response is not an object")
        return response

    def update_check_run(self, check_run_id: int, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        response = self._request(
            "PATCH",
            f"/repos/{self._repo_path}/check-runs/{check_run_id}",
            payload=payload,
        )
        if not isinstance(response, Mapping):
            raise PublisherError("GitHub update CheckRun response is not an object")
        return response

    def list_workflow_runs(
        self, workflow_id: int, head_sha: str, event: str
    ) -> Sequence[Mapping[str, Any]]:
        return self._paginated(
            f"/repos/{self._repo_path}/actions/workflows/{workflow_id}/runs",
            "workflow_runs",
            query={"head_sha": head_sha, "event": event},
        )

    def list_run_jobs(self, run_id: int, run_attempt: int) -> Sequence[Mapping[str, Any]]:
        return self._paginated(
            f"/repos/{self._repo_path}/actions/runs/{run_id}/attempts/{run_attempt}/jobs",
            "jobs",
        )

    def list_pull_requests_for_commit(self, head_sha: str) -> Sequence[Mapping[str, Any]]:
        response = self._request(
            "GET",
            f"/repos/{self._repo_path}/commits/{quote(head_sha, safe='')}/pulls",
            query={"per_page": 100},
        )
        if not isinstance(response, list) or not all(
            isinstance(item, Mapping) for item in response
        ):
            raise PublisherError("GitHub commit pull request response is invalid")
        return response

    def list_issue_events(self, number: int) -> Sequence[Mapping[str, Any]]:
        items: list[Mapping[str, Any]] = []
        page = 1
        path = f"/repos/{self._repo_path}/issues/{number}/events"
        while True:
            response = self._request(
                "GET",
                path,
                query={"per_page": 100, "page": page},
            )
            if not isinstance(response, list) or not all(
                isinstance(item, Mapping) for item in response
            ):
                raise PublisherError("GitHub issue events response is invalid")
            items.extend(response)
            if len(response) < 100:
                return items
            page += 1


@dataclass(frozen=True)
class Generation:
    head: str
    base: str
    pull: str
    updated: str
    action: str
    label: str
    labels: str

    def as_dict(self) -> dict[str, str]:
        return {
            "head": self.head,
            "base": self.base,
            "pull": self.pull,
            "updated": self.updated,
            "action": self.action,
            "label": self.label,
            "labels": self.labels,
        }


@dataclass(frozen=True)
class PublishResult:
    outcome: str
    contexts: tuple[str, ...] = ()


@dataclass(frozen=True)
class AuthorityEvent:
    event_id: int
    created_at: datetime
    name: str
    label: str
    commit_id: str


@dataclass(frozen=True)
class AuthorityMatch:
    epoch: int
    history_count: int
    source_ordinal: int = 1
    verified: bool = True


def _string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def is_relevant_pull_request_event(event: Mapping[str, Any]) -> bool:
    pull = event.get("pull_request")
    if not isinstance(pull, Mapping):
        return False
    action = _string(event.get("action"))
    if action == "converted_to_draft":
        return True
    if bool(pull.get("draft")):
        return False
    if action in {"opened", "ready_for_review", "reopened", "synchronize"}:
        return True
    if action in {"labeled", "unlabeled"}:
        label = event.get("label")
        return isinstance(label, Mapping) and label.get("name") in RELEVANT_LABELS
    if action == "edited":
        changes = event.get("changes")
        return isinstance(changes, Mapping) and "base" in changes
    return False


def parse_full_generation(display_title: Any) -> Generation | None:
    if not isinstance(display_title, str) or not display_title.startswith(FULL_TITLE_PREFIX):
        return None
    fields: dict[str, str] = {}
    for component in display_title.split(" / "):
        key, separator, value = component.partition("=")
        if not separator or not key or not value or key in fields:
            return None
        fields[key] = value
    if fields.get("gate") != "full":
        return None
    required = {"gate", "head", "base", "pull", "updated", "action", "label", "labels"}
    if set(fields) != required:
        return None
    label_mask = fields["labels"]
    if len(label_mask) != len(RELEVANT_LABEL_ORDER) or set(label_mask) - {"0", "1"}:
        return None
    pull_number = fields["pull"]
    if pull_number != "none" and (not pull_number.isdigit() or int(pull_number) <= 0):
        return None
    return Generation(
        head=fields["head"],
        base=fields["base"],
        pull=pull_number,
        updated=fields["updated"],
        action=fields["action"],
        label=fields["label"],
        labels=label_mask,
    )


def _matching_full_runs(
    runs: Sequence[Mapping[str, Any]],
    *,
    spec: GateSpec,
    head_sha: str,
    event: str,
    expected_head_repository: str,
) -> list[tuple[Mapping[str, Any], Generation]]:
    candidates: list[tuple[int, int, Mapping[str, Any], Generation]] = []
    for run in runs:
        head_repository = run.get("head_repository")
        if not isinstance(head_repository, Mapping):
            continue
        generation = parse_full_generation(run.get("display_title"))
        if (
            run.get("workflow_id") != spec.workflow_id
            or run.get("event") != event
            or head_repository.get("full_name") != expected_head_repository
            or run.get("head_sha") != head_sha
            or generation is None
            or generation.head != head_sha
            or (event == "merge_group" and generation.pull != "none")
        ):
            continue
        run_id = run.get("id")
        run_attempt = run.get("run_attempt", 1)
        if isinstance(run_id, int) and isinstance(run_attempt, int):
            candidates.append((run_id, run_attempt, run, generation))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return [(run, generation) for _, _, run, generation in candidates]


def select_latest_full_run(
    runs: Sequence[Mapping[str, Any]],
    *,
    spec: GateSpec,
    head_sha: str,
    event: str,
    expected_head_repository: str,
) -> tuple[Mapping[str, Any], Generation] | None:
    candidates = _matching_full_runs(
        runs,
        spec=spec,
        head_sha=head_sha,
        event=event,
        expected_head_repository=expected_head_repository,
    )
    return candidates[-1] if candidates else None


def attempt_conclusion(jobs: Sequence[Mapping[str, Any]], attempt_job_name: str) -> str:
    attempts = [job for job in jobs if job.get("name") == attempt_job_name]
    if len(attempts) != 1:
        return "failure"
    return "success" if attempts[0].get("conclusion") == "success" else "failure"


def matching_custom_checks(
    check_runs: Sequence[Mapping[str, Any]],
    spec: GateSpec,
    *,
    repository: str,
    head_sha: str,
) -> list[Mapping[str, Any]]:
    external_id = spec.external_id(repository=repository, head_sha=head_sha)
    return [
        check
        for check in check_runs
        if check.get("name") == spec.context
        and check.get("external_id") == external_id
        and isinstance(check.get("app"), Mapping)
        and check["app"].get("id") == GITHUB_ACTIONS_APP_ID
    ]


def _existing_custom_check(
    client: PublisherClient, head_sha: str, spec: GateSpec
) -> Mapping[str, Any] | None:
    matches = matching_custom_checks(
        client.list_check_runs(head_sha, spec.context),
        spec,
        repository=client.repository,
        head_sha=head_sha,
    )
    if len(matches) > 1:
        raise DuplicateCustomCheckError(
            f"duplicate custom CheckRuns for {spec.context} on {head_sha}"
        )
    return matches[0] if matches else None


def _state_summary(
    generation: Generation,
    *,
    authority_epoch: int,
    authority_history_count: int,
    pending: bool = False,
    run_id: int | None = None,
    run_attempt: int | None = None,
) -> str:
    state: dict[str, Any] = {
        "schema": "loom-authoritative-gate-v2",
        "authority_epoch": authority_epoch,
        "authority_history_count": authority_history_count,
        "generation": generation.as_dict(),
    }
    if pending:
        state["pending"] = True
    if run_id is not None:
        state["run_id"] = run_id
        state["run_attempt"] = run_attempt
    return json.dumps(state, sort_keys=True, separators=(",", ":"))


def _read_state(check: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if check is None:
        return None
    output = check.get("output")
    if not isinstance(output, Mapping):
        return None
    summary = output.get("summary")
    if not isinstance(summary, str):
        return None
    try:
        state = json.loads(summary)
    except json.JSONDecodeError:
        return None
    if not isinstance(state, Mapping) or state.get("schema") != "loom-authoritative-gate-v2":
        return None
    return state


def _state_matches_generation(check: Mapping[str, Any] | None, generation: Generation) -> bool:
    state = _read_state(check)
    return state is not None and state.get("generation") == generation.as_dict()


def _state_generation(check: Mapping[str, Any] | None) -> Generation | None:
    state = _read_state(check)
    value = state.get("generation") if state is not None else None
    if not isinstance(value, Mapping):
        return None
    fields = {key: _string(value.get(key)) for key in Generation.__dataclass_fields__}
    if not all(fields.values()):
        return None
    return Generation(**fields)


def _state_run_identity(check: Mapping[str, Any] | None) -> tuple[int, int] | None:
    state = _read_state(check)
    if state is None:
        return None
    run_id = state.get("run_id")
    run_attempt = state.get("run_attempt")
    if isinstance(run_id, int) and isinstance(run_attempt, int):
        return run_id, run_attempt
    return None


def _state_authority_epoch(check: Mapping[str, Any] | None) -> int | None:
    state = _read_state(check)
    epoch = state.get("authority_epoch") if state is not None else None
    return epoch if isinstance(epoch, int) and epoch >= 0 else None


def _state_authority_history_count(check: Mapping[str, Any] | None) -> int | None:
    state = _read_state(check)
    count = state.get("authority_history_count") if state is not None else None
    return count if isinstance(count, int) and count >= 0 else None


def _check_is_pending(check: Mapping[str, Any] | None) -> bool:
    if check is None:
        return False
    if check.get("status") == "in_progress":
        return True
    state = _read_state(check)
    return check.get("status") == "completed" and state is not None and state.get(
        "pending"
    ) is True


def _check_is_terminal(
    check: Mapping[str, Any] | None,
) -> TypeGuard[Mapping[str, Any]]:
    return (
        check is not None
        and check.get("status") == "completed"
        and not _check_is_pending(check)
    )


def _updated_at(value: str) -> datetime | None:
    if value == "none":
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _authority_events(events: Sequence[Mapping[str, Any]]) -> tuple[AuthorityEvent, ...]:
    relevant: list[AuthorityEvent] = []
    for event in events:
        name = _string(event.get("event"))
        label_name = "none"
        if name in {"labeled", "unlabeled"}:
            label = event.get("label")
            if not isinstance(label, Mapping):
                raise PublisherError(f"GitHub issue {name} event is missing its label")
            label_name = _string(label.get("name"))
            if label_name not in RELEVANT_LABELS:
                continue
        elif name not in AUTHORITY_EVENT_NAMES:
            continue
        event_id = event.get("id")
        created_at = _updated_at(_string(event.get("created_at")))
        if not isinstance(event_id, int) or event_id <= 0 or created_at is None:
            raise PublisherError(f"GitHub issue {name} event has invalid identity")
        relevant.append(
            AuthorityEvent(
                event_id=event_id,
                created_at=created_at,
                name=name,
                label=label_name,
                commit_id=_string(event.get("commit_id")),
            )
        )
    return tuple(sorted(relevant, key=lambda item: (item.created_at, item.event_id)))


def _latest_authority_epoch(events: Sequence[AuthorityEvent]) -> int:
    return events[-1].event_id if events else 0


def _generation_event_signature(generation: Generation) -> tuple[str, str] | None:
    if generation.action in {"labeled", "unlabeled"}:
        return generation.action, generation.label
    event_name = {
        "edited": "base_ref_changed",
        "ready_for_review": "ready_for_review",
        "reopened": "reopened",
        "synchronize": "head_ref_force_pushed",
    }.get(generation.action)
    return (event_name, "none") if event_name is not None else None


def _labels_from_mask(mask: str) -> frozenset[str]:
    return frozenset(
        label for label, selected in zip(RELEVANT_LABEL_ORDER, mask, strict=True) if selected == "1"
    )


def _event_post_label_masks(events: Sequence[AuthorityEvent], final_mask: str) -> Mapping[int, str]:
    """Reconstruct the relevant-label state immediately after every event."""

    labels = set(_labels_from_mask(final_mask))
    post_masks: dict[int, str] = {}
    for event in reversed(events):
        post_masks[event.event_id] = _relevant_label_mask(frozenset(labels))
        if event.name == "labeled":
            labels.discard(event.label)
        elif event.name == "unlabeled":
            labels.add(event.label)
    return post_masks


def _generation_authority_match(
    generation: Generation,
    events: Sequence[AuthorityEvent],
) -> AuthorityMatch:
    generation_time = _updated_at(generation.updated)
    if generation_time is None:
        return AuthorityMatch(0, len(events))
    signature = _generation_event_signature(generation)
    if signature is not None:
        post_masks = _event_post_label_masks(events, generation.labels)
        shape_matches = [
            event
            for event in events
            if (event.name, event.label) == signature
            and post_masks[event.event_id] == generation.labels
            and abs(event.created_at - generation_time) <= timedelta(seconds=5)
        ]
        possible_matches = shape_matches
        if generation.action == "synchronize":
            # The Issues Events API identifies the force-push destination in
            # commit_id. Bind the occurrence ordinal to this head so two
            # different heads updated in the same second cannot deadlock each
            # other's source inventory.
            possible_matches = [
                event for event in shape_matches if event.commit_id == generation.head
            ]
        if possible_matches:
            nearest_distance = min(
                abs(event.created_at - generation_time) for event in possible_matches
            )
            nearest = [
                event
                for event in possible_matches
                if abs(event.created_at - generation_time) == nearest_distance
            ]
            matched = nearest[-1]
            return AuthorityMatch(
                epoch=matched.event_id,
                history_count=len(events),
                source_ordinal=sum(event.created_at == matched.created_at for event in nearest),
            )
        # `synchronize` can be emitted for an ordinary non-force push, for which
        # the Issues Events API has no matching event. The new head SHA remains a
        # unique authority boundary. Other event types have a durable issue event
        # and fail closed until that event becomes visible.
        if generation.action != "synchronize":
            return AuthorityMatch(0, len(events), verified=False)
    eligible = [
        event
        for event in events
        if event.created_at < generation_time
        or (generation.action == "synchronize" and event.created_at == generation_time)
    ]
    return AuthorityMatch(_latest_authority_epoch(eligible), len(events))


def _generation_time_order(candidate: Generation, current: Generation) -> int | None:
    candidate_time = _updated_at(candidate.updated)
    current_time = _updated_at(current.updated)
    if candidate_time is None or current_time is None:
        return None
    return (candidate_time > current_time) - (candidate_time < current_time)


def _should_preempt_terminal_before_reads(
    existing: Mapping[str, Any],
    *,
    event_generation: Generation,
    event_run: Mapping[str, Any] | None = None,
) -> bool:
    existing_generation = _state_generation(existing)
    if existing_generation is None or existing_generation.pull != event_generation.pull:
        return False
    if _generation_time_order(event_generation, existing_generation) == -1:
        return False
    if event_run is not None and existing_generation == event_generation:
        existing_run = _state_run_identity(existing)
        event_run_id = event_run.get("id")
        event_run_attempt = event_run.get("run_attempt", 1)
        if (
            existing_run is not None
            and isinstance(event_run_id, int)
            and isinstance(event_run_attempt, int)
        ):
            event_identity = (event_run_id, event_run_attempt)
            if event_identity < existing_run:
                return False
            if event_identity == existing_run:
                return event_run.get("status") == "completed" and _string(
                    event_run.get("conclusion")
                ) != _string(existing.get("conclusion"))
    return True


def _upsert_check(
    client: PublisherClient,
    *,
    spec: GateSpec,
    head_sha: str,
    existing: Mapping[str, Any] | None,
    status: str,
    generation: Generation,
    authority_epoch: int,
    authority_history_count: int,
    details_url: str,
    conclusion: str | None = None,
    run_id: int | None = None,
    run_attempt: int | None = None,
) -> None:
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    verb = "awaiting" if status == "in_progress" else conclusion or "failure"
    external_id = spec.external_id(repository=client.repository, head_sha=head_sha)
    # GitHub rejects reopening an already-completed CheckRun. Replacing that
    # object with a new id also leaves auto-merge pinned to the retired id even
    # after the required-context rollup turns green. Preserve the one stable
    # CheckRun identity instead: a newer generation is represented by a
    # completed/failure sentinel whose signed state says it is still pending.
    # The exact source attempt later updates this same id to its real terminal
    # success or failure. This is fail-closed throughout and avoids duplicate
    # required contexts on one SHA.
    completed_pending = (
        status == "in_progress"
        and existing is not None
        and existing.get("status") == "completed"
    )
    effective_status = "completed" if completed_pending else status
    effective_conclusion = "failure" if completed_pending else conclusion
    payload: dict[str, Any] = {
        "name": spec.context,
        "head_sha": head_sha,
        "external_id": external_id,
        "status": effective_status,
        "details_url": details_url,
        "output": {
            "title": f"Authoritative {spec.context}: {verb}",
            "summary": _state_summary(
                generation,
                authority_epoch=authority_epoch,
                authority_history_count=authority_history_count,
                pending=status == "in_progress",
                run_id=run_id,
                run_attempt=run_attempt,
            ),
        },
    }
    if completed_pending:
        payload["conclusion"] = "failure"
        payload["completed_at"] = now
    elif status == "in_progress":
        payload["started_at"] = now
    else:
        payload["conclusion"] = conclusion
        payload["completed_at"] = now
    if existing is None:
        response = client.create_check_run(payload)
    else:
        check_id = existing.get("id")
        if not isinstance(check_id, int):
            raise PublisherError(f"custom CheckRun for {spec.context} has no integer id")
        payload.pop("head_sha")
        response = client.update_check_run(check_id, payload)
    if response:
        app = response.get("app")
        if (
            response.get("external_id") != external_id
            or not isinstance(app, Mapping)
            or app.get("id") != GITHUB_ACTIONS_APP_ID
            or response.get("status") != effective_status
            or (
                effective_status == "in_progress"
                and response.get("conclusion") is not None
            )
            or (
                effective_status == "completed"
                and response.get("conclusion") != effective_conclusion
            )
        ):
            raise PublisherError(
                f"GitHub did not return the publisher-owned {spec.context} CheckRun"
            )


def _pull_number_from_run(run: Mapping[str, Any], head_sha: str) -> int | None:
    pulls = run.get("pull_requests")
    if not isinstance(pulls, list):
        return None
    numbers: set[int] = set()
    for pull in pulls:
        if not isinstance(pull, Mapping):
            continue
        head = pull.get("head")
        if isinstance(head, Mapping) and head.get("sha") == head_sha:
            number = pull.get("number")
            if isinstance(number, int):
                numbers.add(number)
    return numbers.pop() if len(numbers) == 1 else None


def _pull_number_for_run(
    client: PublisherClient, run: Mapping[str, Any], head_sha: str
) -> int | None:
    number = _pull_number_from_run(run, head_sha)
    if number is not None:
        return number
    numbers: set[int] = set()
    for pull in client.list_pull_requests_for_commit(head_sha):
        head = pull.get("head")
        candidate = pull.get("number")
        if (
            pull.get("state") == "open"
            and isinstance(head, Mapping)
            and head.get("sha") == head_sha
            and isinstance(candidate, int)
        ):
            numbers.add(candidate)
    return numbers.pop() if len(numbers) == 1 else None


def _open_pull_numbers_for_commit(client: PublisherClient, head_sha: str) -> set[int]:
    numbers: set[int] = set()
    for pull in client.list_pull_requests_for_commit(head_sha):
        head = pull.get("head")
        number = pull.get("number")
        if (
            pull.get("state") == "open"
            and isinstance(head, Mapping)
            and head.get("sha") == head_sha
            and isinstance(number, int)
        ):
            numbers.add(number)
    return numbers


def _ensure_unique_pull_authority(
    client: PublisherClient,
    *,
    pull_number: int,
    head_sha: str,
) -> None:
    associated = _open_pull_numbers_for_commit(client, head_sha)
    if associated != {pull_number}:
        raise PullAuthorityError(head_sha, associated)


def _pull_ref(pull: Mapping[str, Any], side: str) -> str:
    value = pull.get(side)
    return _string(value.get("ref")) if isinstance(value, Mapping) else ""


def _pull_repository(pull: Mapping[str, Any], side: str) -> str:
    value = pull.get(side)
    repository = value.get("repo") if isinstance(value, Mapping) else None
    return _string(repository.get("full_name")) if isinstance(repository, Mapping) else ""


def _relevant_pull_labels(pull: Mapping[str, Any]) -> frozenset[str] | None:
    labels = pull.get("labels")
    if not isinstance(labels, list):
        return None
    return frozenset(
        name
        for label in labels
        if isinstance(label, Mapping)
        and isinstance((name := label.get("name")), str)
        and name in RELEVANT_LABELS
    )


def _relevant_label_mask(labels: frozenset[str]) -> str:
    return "".join("1" if label in labels else "0" for label in RELEVANT_LABEL_ORDER)


def _run_pull_ref(run: Mapping[str, Any], number: int, side: str) -> str:
    pulls = run.get("pull_requests")
    if not isinstance(pulls, list):
        return ""
    for pull in pulls:
        if not isinstance(pull, Mapping) or pull.get("number") != number:
            continue
        value = pull.get(side)
        if isinstance(value, Mapping):
            return _string(value.get("ref"))
    return ""


def _generation_from_pull_event(
    event: Mapping[str, Any], event_pull: Mapping[str, Any]
) -> Generation:
    head = event_pull.get("head")
    base = event_pull.get("base")
    label = event.get("label")
    if not isinstance(head, Mapping) or not isinstance(base, Mapping):
        raise PublisherError("pull request event is missing head/base")
    head_sha = _string(head.get("sha"))
    base_sha = _string(base.get("sha"))
    pull_number = event_pull.get("number") or event.get("number")
    if not head_sha or not base_sha or not isinstance(pull_number, int):
        raise PublisherError("pull request event is missing head/base SHA or PR number")
    relevant_labels = _relevant_pull_labels(event_pull)
    if relevant_labels is None:
        raise PublisherError("pull request event is missing labels")
    return Generation(
        head=head_sha,
        base=base_sha,
        pull=str(pull_number),
        updated=_string(event_pull.get("updated_at")) or "none",
        action=_string(event.get("action")) or "event",
        label=(
            _string(label.get("name"))
            if isinstance(label, Mapping) and label.get("name")
            else "none"
        ),
        labels=_relevant_label_mask(relevant_labels),
    )


def _generation_matches_pull_snapshot(generation: Generation, pull: Mapping[str, Any]) -> bool:
    head = pull.get("head")
    pull_number = pull.get("number")
    if (
        not isinstance(pull_number, int)
        or generation.pull != str(pull_number)
        or not isinstance(head, Mapping)
        or head.get("sha") != generation.head
    ):
        return False
    relevant_labels = _relevant_pull_labels(pull)
    if relevant_labels is None or _relevant_label_mask(relevant_labels) != generation.labels:
        return False
    if generation.action in {"labeled", "unlabeled"}:
        if (generation.label in relevant_labels) != (generation.action == "labeled"):
            return False
    return True


def _generation_matches_live_pull(generation: Generation, pull: Mapping[str, Any]) -> bool:
    return (
        pull.get("state") == "open"
        and not bool(pull.get("draft"))
        and _generation_matches_pull_snapshot(generation, pull)
    )


def _pull_is_merged(pull: Mapping[str, Any]) -> bool:
    return pull.get("state") == "closed" and (
        pull.get("merged") is True or isinstance(pull.get("merged_at"), str)
    )


def _is_trusted_promotion(pull: Mapping[str, Any], repository: str) -> bool:
    return (
        _pull_ref(pull, "head") == "dev"
        and _pull_ref(pull, "base") == "main"
        and _pull_repository(pull, "head") == repository
    )


def _publisher_active_for_pull(
    client: PublisherClient, generation: Generation, pull: Mapping[str, Any]
) -> bool:
    return client.base_contains_publisher(generation.base) or _is_trusted_promotion(
        pull, client.repository
    )


def _live_authority_generation(
    pull: Mapping[str, Any],
    authority_events: Sequence[AuthorityEvent],
) -> Generation | None:
    head = pull.get("head")
    base = pull.get("base")
    labels = _relevant_pull_labels(pull)
    if not isinstance(head, Mapping) or not isinstance(base, Mapping) or labels is None:
        return None
    head_sha = _string(head.get("sha"))
    base_sha = _string(base.get("sha"))
    pull_number = pull.get("number")
    if not head_sha or not base_sha or not isinstance(pull_number, int):
        return None
    if authority_events:
        latest = authority_events[-1]
        action = {
            "base_ref_changed": "edited",
            "convert_to_draft": "converted_to_draft",
            "head_ref_force_pushed": "synchronize",
        }.get(latest.name, latest.name)
        label = latest.label
        updated = latest.created_at.isoformat().replace("+00:00", "Z")
    else:
        action = "opened"
        label = "none"
        updated = _string(pull.get("updated_at")) or "none"
    return Generation(
        head=head_sha,
        base=base_sha,
        pull=str(pull_number),
        updated=updated,
        action=action,
        label=label,
        labels=_relevant_label_mask(labels),
    )


def _authority_match_is_live(match: AuthorityMatch, events: Sequence[AuthorityEvent]) -> bool:
    return (
        match.verified
        and match.history_count == len(events)
        and match.epoch == _latest_authority_epoch(events)
    )


def _same_source_occurrence_shape(candidate: Generation, current: Generation) -> bool:
    if current.action != "edited":
        return candidate == current
    # The Issues Events REST shape does not expose the destination base ref.
    # Count same-second base-retarget source runs across their base SHA while
    # retaining every other authority dimension. This allows A -> B -> A to
    # wait for the final source occurrence instead of accepting the first A.
    return (
        candidate.head == current.head
        and candidate.pull == current.pull
        and candidate.updated == current.updated
        and candidate.action == current.action
        and candidate.label == current.label
        and candidate.labels == current.labels
    )


def _live_pull_snapshot_candidates(
    candidates: Sequence[tuple[Mapping[str, Any], Generation]],
    *,
    pull_number: int,
    pull: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], Generation]]:
    live_base_ref = _pull_ref(pull, "base")
    return [
        (run, generation)
        for run, generation in candidates
        if generation.pull == str(pull_number)
        and isinstance((run_pulls := run.get("pull_requests")), list)
        and (not run_pulls or _pull_number_from_run(run, generation.head) == pull_number)
        and _generation_matches_live_pull(generation, pull)
        and (
            not (run_base_ref := _run_pull_ref(run, pull_number, "base"))
            or not live_base_ref
            or run_base_ref == live_base_ref
        )
    ]


def _current_pull_candidates(
    client: PublisherClient,
    *,
    candidates: Sequence[tuple[Mapping[str, Any], Generation]],
    pull_number: int,
    pull: Mapping[str, Any],
    authority_events: Sequence[AuthorityEvent],
) -> list[tuple[Mapping[str, Any], Generation, AuthorityMatch]]:
    """Return source runs belonging to the live PR authority generation."""

    live_base_ref = _pull_ref(pull, "base")
    live_head = pull.get("head")
    live_head_sha = _string(live_head.get("sha")) if isinstance(live_head, Mapping) else ""
    if not live_head_sha:
        raise PublisherError("live pull request has no head SHA")
    _ensure_unique_pull_authority(
        client,
        pull_number=pull_number,
        head_sha=live_head_sha,
    )
    current: list[tuple[Mapping[str, Any], Generation, AuthorityMatch]] = []
    publisher_by_base: dict[str, bool] = {}
    for run, generation in candidates:
        if generation.pull != str(pull_number):
            continue
        run_pulls = run.get("pull_requests")
        if not isinstance(run_pulls, list):
            continue
        if run_pulls and _pull_number_from_run(run, generation.head) != pull_number:
            continue
        if not _generation_matches_live_pull(generation, pull):
            continue
        run_base_ref = _run_pull_ref(run, pull_number, "base")
        if run_base_ref and live_base_ref and run_base_ref != live_base_ref:
            continue
        if generation.base not in publisher_by_base:
            publisher_by_base[generation.base] = _publisher_active_for_pull(
                client, generation, pull
            )
        publisher_active = publisher_by_base[generation.base]
        if not publisher_active:
            continue
        match = _generation_authority_match(generation, authority_events)
        if not _authority_match_is_live(match, authority_events):
            continue
        same_generation_count = len(
            {
                run_id
                for candidate_run, candidate_generation in candidates
                if _same_source_occurrence_shape(candidate_generation, generation)
                and isinstance((run_id := candidate_run.get("id")), int)
            }
        )
        if same_generation_count < match.source_ordinal:
            continue
        # Repeated events can produce byte-identical markers. Once the trusted
        # occurrence cardinality is present, they form one generation and the
        # highest run id/attempt is its replacement execution.
        current.append((run, generation, match))
    return current


def _pending_authority_version(
    existing: Mapping[str, Any] | None,
    *,
    authority_epoch: int,
    authority_history_count: int,
) -> tuple[int, int]:
    """Never lower the persisted issue-event watermark on a pending reset."""

    current_epoch = _state_authority_epoch(existing)
    current_count = _state_authority_history_count(existing)
    if (
        current_epoch is not None
        and current_count is not None
        and (
            current_count > authority_history_count
            or (current_count == authority_history_count and current_epoch != authority_epoch)
        )
    ):
        return current_epoch, current_count
    return authority_epoch, authority_history_count


def _set_pending_generation(
    client: PublisherClient,
    *,
    spec: GateSpec,
    generation: Generation,
    authority_epoch: int,
    authority_history_count: int,
    details_url: str,
    force: bool = False,
    superseded_run: tuple[int, int] | None = None,
) -> PublishResult:
    existing = _existing_custom_check(client, generation.head, spec)
    effective_epoch, effective_count = _pending_authority_version(
        existing,
        authority_epoch=authority_epoch,
        authority_history_count=authority_history_count,
    )
    exact_generation = _state_matches_generation(existing, generation)
    exact_version = (
        _state_authority_epoch(existing) == effective_epoch
        and _state_authority_history_count(existing) == effective_count
    )
    existing_run = _state_run_identity(existing)
    existing_count = _state_authority_history_count(existing)
    strictly_newer_terminal = (
        force
        and superseded_run is not None
        and existing_run is not None
        and existing_run > superseded_run
        and existing_count is not None
        and existing_count > authority_history_count
    )
    preserve_completed = (
        _check_is_terminal(existing)
        and (
            (
                exact_generation
                and exact_version
                and (
                    not force
                    or (
                        superseded_run is not None
                        and existing_run is not None
                        and existing_run > superseded_run
                    )
                )
            )
            or strictly_newer_terminal
        )
    )
    if preserve_completed:
        assert existing is not None
        existing_conclusion = _string(existing.get("conclusion"))
        return PublishResult(
            existing_conclusion if existing_conclusion in {"success", "failure"} else "current"
        )
    if (
        not force
        and _check_is_pending(existing)
        and exact_generation
        and exact_version
    ):
        return PublishResult("in_progress")
    _upsert_check(
        client,
        spec=spec,
        head_sha=generation.head,
        existing=existing,
        status="in_progress",
        generation=generation,
        authority_epoch=effective_epoch,
        authority_history_count=effective_count,
        details_url=details_url,
    )
    return PublishResult("in_progress", (spec.context,))


def _revoke_for_pull_authority_error(
    client: PublisherClient,
    *,
    spec: GateSpec,
    head_sha: str,
    event_generation: Generation | None,
    error: PullAuthorityError,
    details_url: str,
) -> None:
    existing = _existing_custom_check(client, head_sha, spec)
    if not _check_is_terminal(existing):
        return
    existing_generation = _state_generation(existing)
    existing_owner = (
        int(existing_generation.pull)
        if existing_generation is not None and existing_generation.pull.isdigit()
        else None
    )
    event_owner = (
        int(event_generation.pull)
        if event_generation is not None and event_generation.pull.isdigit()
        else None
    )
    if (
        existing_owner is not None
        and error.associated == frozenset({existing_owner})
        and event_owner != existing_owner
    ):
        # A delayed event from a closed PR can share the SHA with the one
        # current open PR (notably a dev-to-main promotion). It must not revoke
        # the current owner's check.
        return
    pending_generation = existing_generation or event_generation
    if pending_generation is None:
        return
    _set_pending_generation(
        client,
        spec=spec,
        generation=pending_generation,
        authority_epoch=_state_authority_epoch(existing) or 0,
        authority_history_count=_state_authority_history_count(existing) or 0,
        details_url=details_url,
        force=True,
        superseded_run=_state_run_identity(existing),
    )


def _invalidate_stale_pull_context(
    client: PublisherClient,
    *,
    spec: GateSpec,
    pull: Mapping[str, Any],
    authority_events: Sequence[AuthorityEvent],
    generation: Generation | None = None,
    force: bool = False,
    superseded_run: tuple[int, int] | None = None,
) -> PublishResult:
    if pull.get("state") != "open" or bool(pull.get("draft")):
        return PublishResult("stale")
    head = pull.get("head")
    live_head = _string(head.get("sha")) if isinstance(head, Mapping) else ""
    existing = _existing_custom_check(client, live_head, spec) if live_head else None
    existing_generation = _state_generation(existing)
    existing_run = _state_run_identity(existing)
    existing_epoch = _state_authority_epoch(existing)
    existing_count = _state_authority_history_count(existing)
    latest_epoch = _latest_authority_epoch(authority_events)
    version_not_behind = existing_count is not None and (
        existing_count > len(authority_events)
        or (existing_count == len(authority_events) and existing_epoch == latest_epoch)
    )
    existing_is_live = (
        existing_generation is not None
        and _generation_matches_live_pull(existing_generation, pull)
        and _publisher_active_for_pull(client, existing_generation, pull)
        and version_not_behind
    )
    if existing_is_live and existing is not None:
        if _check_is_pending(existing):
            return PublishResult("in_progress")
        if _check_is_terminal(existing) and (
            not force
            or (
                superseded_run is not None
                and existing_run is not None
                and existing_run > superseded_run
            )
        ):
            existing_conclusion = _string(existing.get("conclusion"))
            return PublishResult(
                existing_conclusion if existing_conclusion in {"success", "failure"} else "current"
            )
    generation = generation or _live_authority_generation(pull, authority_events)
    if generation is None or not _publisher_active_for_pull(client, generation, pull):
        return PublishResult("stale")
    return _set_pending_generation(
        client,
        spec=spec,
        generation=generation,
        authority_epoch=_latest_authority_epoch(authority_events),
        authority_history_count=len(authority_events),
        details_url=_string(pull.get("html_url")),
        force=force,
        superseded_run=superseded_run,
    )


def _publish_run_state(
    client: PublisherClient,
    *,
    spec: GateSpec,
    head_sha: str,
    run: Mapping[str, Any],
    generation: Generation,
    authority_match: AuthorityMatch,
    ensure_current: Callable[[], bool] | None = None,
    reconcile_stale: Callable[[], PublishResult] | None = None,
    compensate_stale: Callable[[], PublishResult] | None = None,
) -> PublishResult:
    status = run.get("status")
    run_id = run.get("id")
    run_attempt = run.get("run_attempt", 1)
    if not isinstance(run_id, int) or not isinstance(run_attempt, int):
        raise PublisherError("workflow run has invalid id/attempt")
    existing = _existing_custom_check(client, head_sha, spec)
    current_generation = _state_generation(existing)
    current_run = _state_run_identity(existing)
    current_epoch = _state_authority_epoch(existing)
    current_history_count = _state_authority_history_count(existing)
    candidate_run = (run_id, run_attempt)
    details_url = _string(run.get("html_url"))
    same_authority = (
        current_epoch == authority_match.epoch
        and current_history_count == authority_match.history_count
    )

    def still_current() -> bool:
        if ensure_current is None:
            return True
        try:
            return ensure_current()
        except PublisherError:
            if compensate_stale is not None:
                compensate_stale()
            raise

    if current_history_count is not None and (
        current_history_count > authority_match.history_count
        or (
            current_history_count == authority_match.history_count
            and current_epoch != authority_match.epoch
        )
    ):
        if _check_is_terminal(existing):
            if (
                current_generation == generation
                and current_run == candidate_run
                and current_history_count > authority_match.history_count
            ):
                existing_conclusion = _string(existing.get("conclusion"))
                return PublishResult(
                    existing_conclusion
                    if existing_conclusion in {"success", "failure"}
                    else "current"
                )
            if compensate_stale is not None:
                return compensate_stale()
            return _set_pending_generation(
                client,
                spec=spec,
                generation=generation,
                authority_epoch=authority_match.epoch,
                authority_history_count=authority_match.history_count,
                details_url=details_url,
                force=True,
                superseded_run=candidate_run,
            )
        return PublishResult("stale")

    if status in {"requested", "queued", "pending", "waiting", "in_progress"}:
        if (
            same_authority
            and current_generation == generation
            and current_run is not None
            and current_run > candidate_run
        ):
            return PublishResult("stale")
        if same_authority and current_generation is not None and current_generation != generation:
            generation_order = _generation_time_order(generation, current_generation)
            if generation_order is not None and generation_order < 0:
                return PublishResult("stale")
            if generation_order == 0 and (current_run is None or current_run >= candidate_run):
                return PublishResult("stale")
        if (
            _check_is_terminal(existing)
            and current_generation == generation
            and current_run == candidate_run
            and current_epoch == authority_match.epoch
            and current_history_count == authority_match.history_count
        ):
            return PublishResult("current")
        _upsert_check(
            client,
            spec=spec,
            head_sha=head_sha,
            existing=existing,
            status="in_progress",
            generation=generation,
            authority_epoch=authority_match.epoch,
            authority_history_count=authority_match.history_count,
            details_url=details_url,
            run_id=run_id,
            run_attempt=run_attempt,
        )
        return PublishResult("in_progress", (spec.context,))
    if status != "completed":
        raise PublisherError(f"unsupported workflow run status for {spec.context}")

    if (
        same_authority
        and current_generation == generation
        and current_run is not None
        and current_run > candidate_run
    ):
        return PublishResult("stale")
    if same_authority and current_generation is not None and current_generation != generation:
        generation_order = _generation_time_order(generation, current_generation)
        if generation_order is not None and generation_order < 0:
            return PublishResult("stale")
        if generation_order == 0 and (current_run is None or current_run >= candidate_run):
            return PublishResult("stale")
    if (
        _check_is_terminal(existing)
        and _state_matches_generation(existing, generation)
        and current_run == candidate_run
        and current_epoch == authority_match.epoch
        and current_history_count == authority_match.history_count
    ):
        if not still_current():
            return reconcile_stale() if reconcile_stale is not None else PublishResult("stale")
        existing_conclusion = _string(existing.get("conclusion"))
        return PublishResult(
            existing_conclusion if existing_conclusion in {"success", "failure"} else "current"
        )
    conclusion = (
        attempt_conclusion(client.list_run_jobs(run_id, run_attempt), spec.attempt_job)
        if run.get("conclusion") == "success"
        else "failure"
    )
    if not still_current():
        return reconcile_stale() if reconcile_stale is not None else PublishResult("stale")
    try:
        _upsert_check(
            client,
            spec=spec,
            head_sha=head_sha,
            existing=existing,
            status="completed",
            conclusion=conclusion,
            generation=generation,
            authority_epoch=authority_match.epoch,
            authority_history_count=authority_match.history_count,
            details_url=details_url,
            run_id=run_id,
            run_attempt=run_attempt,
        )
    except PublisherError:
        if compensate_stale is not None:
            compensate_stale()
        raise
    if not still_current():
        return reconcile_stale() if reconcile_stale is not None else PublishResult("stale")
    return PublishResult(conclusion, (spec.context,))


def _run_status_rank(run: Mapping[str, Any]) -> int:
    return {
        "requested": 0,
        "queued": 1,
        "pending": 1,
        "waiting": 1,
        "in_progress": 2,
        "completed": 3,
    }.get(_string(run.get("status")), -1)


def _advance_run_floor(
    current: Mapping[str, Any] | None,
    candidate: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    candidate_id = candidate.get("id")
    candidate_attempt = candidate.get("run_attempt", 1)
    if not isinstance(candidate_id, int) or not isinstance(candidate_attempt, int):
        return current
    if current is None:
        return candidate
    current_id = current.get("id")
    current_attempt = current.get("run_attempt", 1)
    if not isinstance(current_id, int) or not isinstance(current_attempt, int):
        return candidate
    candidate_identity = (candidate_id, candidate_attempt)
    current_identity = (current_id, current_attempt)
    if candidate_identity > current_identity or (
        candidate_identity == current_identity
        and _run_status_rank(candidate) >= _run_status_rank(current)
    ):
        return candidate
    return current


def _pull_run_candidates(
    client: PublisherClient,
    *,
    spec: GateSpec,
    pull: Mapping[str, Any],
    event_run_floor: Mapping[str, Any] | None = None,
) -> list[tuple[Mapping[str, Any], Generation]]:
    head = pull.get("head")
    head_sha = _string(head.get("sha")) if isinstance(head, Mapping) else ""
    expected_repository = _pull_repository(pull, "head") or client.repository
    if not head_sha:
        return []
    inventory: dict[tuple[int, int], Mapping[str, Any]] = {}
    source_runs = list(client.list_workflow_runs(spec.workflow_id, head_sha, "pull_request"))
    if event_run_floor is not None:
        source_runs.append(event_run_floor)
    for run in source_runs:
        run_id = run.get("id")
        run_attempt = run.get("run_attempt", 1)
        if not isinstance(run_id, int) or not isinstance(run_attempt, int):
            continue
        identity = (run_id, run_attempt)
        current = inventory.get(identity)
        if current is None or _run_status_rank(run) >= _run_status_rank(current):
            inventory[identity] = run
    return _matching_full_runs(
        tuple(inventory.values()),
        spec=spec,
        head_sha=head_sha,
        event="pull_request",
        expected_head_repository=expected_repository,
    )


def _reconcile_pull_context(
    client: PublisherClient,
    *,
    spec: GateSpec,
    pull_number: int,
    pull: Mapping[str, Any],
    authority_events: Sequence[AuthorityEvent],
    event_run_floor: Mapping[str, Any] | None = None,
) -> PublishResult:
    candidates = _pull_run_candidates(
        client,
        spec=spec,
        pull=pull,
        event_run_floor=event_run_floor,
    )
    live_candidates = _live_pull_snapshot_candidates(
        candidates,
        pull_number=pull_number,
        pull=pull,
    )
    current_candidates = _current_pull_candidates(
        client,
        candidates=candidates,
        pull_number=pull_number,
        pull=pull,
        authority_events=authority_events,
    )
    if live_candidates:
        observed_run, observed_generation = live_candidates[-1]
        observed_run_id = observed_run.get("id")
        observed_run_attempt = observed_run.get("run_attempt", 1)
        observed_identity = (
            (observed_run_id, observed_run_attempt)
            if isinstance(observed_run_id, int) and isinstance(observed_run_attempt, int)
            else None
        )
        current_identity: tuple[int, int] | None = None
        generation_order: int | None = None
        if current_candidates:
            current_run = current_candidates[-1][0]
            current_run_id = current_run.get("id")
            current_run_attempt = current_run.get("run_attempt", 1)
            if isinstance(current_run_id, int) and isinstance(current_run_attempt, int):
                current_identity = (current_run_id, current_run_attempt)
            generation_order = _generation_time_order(
                observed_generation,
                current_candidates[-1][1],
            )
        existing = _existing_custom_check(client, observed_generation.head, spec)
        existing_identity = _state_run_identity(existing)
        observed_is_newer = observed_identity is not None and (
            current_identity is None or observed_identity > current_identity
        )
        observed_match = _generation_authority_match(
            observed_generation,
            authority_events,
        )
        observed_is_definitively_stale = (
            observed_match.verified
            and observed_match.history_count == len(authority_events)
            and observed_match.epoch != _latest_authority_epoch(authority_events)
        )
        observed_is_not_stale = not observed_is_definitively_stale and (
            not current_candidates or generation_order is None or generation_order >= 0
        )
        terminal_has_not_seen_observation = observed_identity is not None and (
            existing_identity is None or observed_identity > existing_identity
        )
        if observed_is_newer and observed_is_not_stale and terminal_has_not_seen_observation:
            return _invalidate_stale_pull_context(
                client,
                spec=spec,
                pull=pull,
                authority_events=authority_events,
                generation=observed_generation,
                force=True,
                superseded_run=observed_identity,
            )
    if not current_candidates:
        return _invalidate_stale_pull_context(
            client,
            spec=spec,
            pull=pull,
            authority_events=authority_events,
        )
    run, generation, authority_match = current_candidates[-1]
    run_id = run.get("id")
    run_attempt = run.get("run_attempt", 1)
    if not isinstance(run_id, int) or not isinstance(run_attempt, int):
        raise PublisherError("selected workflow run has invalid id/attempt")
    selected_run_identity = (run_id, run_attempt)
    selected_base_ref = _run_pull_ref(run, pull_number, "base") or _pull_ref(pull, "base")
    observed_run_floor = _advance_run_floor(event_run_floor, run)
    observed_authority_pull: Mapping[str, Any] | None = None
    observed_authority_events: tuple[AuthorityEvent, ...] | None = None

    def ensure_current() -> bool:
        nonlocal observed_authority_events, observed_authority_pull, observed_run_floor
        fresh_pull = client.get_pull_request(pull_number)
        fresh_events = _authority_events(client.list_issue_events(pull_number))
        fresh_candidates = _pull_run_candidates(
            client,
            spec=spec,
            pull=fresh_pull,
            event_run_floor=observed_run_floor,
        )
        if _pull_is_merged(fresh_pull):
            associated = _open_pull_numbers_for_commit(client, generation.head)
            if associated:
                raise PullAuthorityError(generation.head, associated)
            merged_match = _generation_authority_match(generation, fresh_events)
            live_base_ref = _pull_ref(fresh_pull, "base")
            matching_snapshot = [
                (candidate_run, candidate_generation)
                for candidate_run, candidate_generation in fresh_candidates
                if _generation_matches_pull_snapshot(candidate_generation, fresh_pull)
                and (
                    not (candidate_base_ref := _run_pull_ref(candidate_run, pull_number, "base"))
                    or not live_base_ref
                    or candidate_base_ref == live_base_ref
                )
            ]
            latest_snapshot = matching_snapshot[-1] if matching_snapshot else None
            latest_snapshot_run = latest_snapshot[0] if latest_snapshot is not None else None
            latest_snapshot_generation = latest_snapshot[1] if latest_snapshot is not None else None
            if latest_snapshot_run is not None:
                observed_run_floor = _advance_run_floor(observed_run_floor, latest_snapshot_run)
            latest_snapshot_identity = (
                (
                    latest_snapshot_run.get("id"),
                    latest_snapshot_run.get("run_attempt", 1),
                )
                if latest_snapshot_run is not None
                else None
            )
            merged_is_current = (
                _generation_matches_pull_snapshot(generation, fresh_pull)
                and bool(selected_base_ref)
                and selected_base_ref == live_base_ref
                and merged_match == authority_match
                and _authority_match_is_live(merged_match, fresh_events)
                and latest_snapshot_identity == selected_run_identity
                and latest_snapshot_generation == generation
                and _publisher_active_for_pull(client, generation, fresh_pull)
            )
            if not merged_is_current:
                observed_authority_pull = fresh_pull
                observed_authority_events = fresh_events
            return merged_is_current
        fresh_current = _current_pull_candidates(
            client,
            candidates=fresh_candidates,
            pull_number=pull_number,
            pull=fresh_pull,
            authority_events=fresh_events,
        )
        if fresh_current:
            observed_run_floor = _advance_run_floor(observed_run_floor, fresh_current[-1][0])
        is_current = (
            bool(fresh_current)
            and (
                fresh_current[-1][0].get("id"),
                fresh_current[-1][0].get("run_attempt", 1),
            )
            == selected_run_identity
            and fresh_current[-1][1] == generation
            and fresh_current[-1][2] == authority_match
        )
        if not is_current and (
            not fresh_current
            or fresh_current[-1][1] != generation
            or fresh_current[-1][2] != authority_match
        ):
            observed_authority_pull = fresh_pull
            observed_authority_events = fresh_events
        return is_current

    def compensate_stale() -> PublishResult:
        return _set_pending_generation(
            client,
            spec=spec,
            generation=generation,
            authority_epoch=authority_match.epoch,
            authority_history_count=authority_match.history_count,
            details_url=_string(run.get("html_url")),
            force=True,
            superseded_run=selected_run_identity,
        )

    def reconcile_stale() -> PublishResult:
        try:
            if observed_authority_pull is not None and observed_authority_events is not None:
                compensated = compensate_stale()
                observed_head = observed_authority_pull.get("head")
                observed_head_sha = (
                    _string(observed_head.get("sha")) if isinstance(observed_head, Mapping) else ""
                )
                if observed_head_sha != generation.head:
                    return compensated
                return _invalidate_stale_pull_context(
                    client,
                    spec=spec,
                    pull=observed_authority_pull,
                    authority_events=observed_authority_events,
                    force=True,
                    superseded_run=selected_run_identity,
                )
            fresh_pull = client.get_pull_request(pull_number)
            fresh_events = _authority_events(client.list_issue_events(pull_number))
            return _reconcile_pull_context(
                client,
                spec=spec,
                pull_number=pull_number,
                pull=fresh_pull,
                authority_events=fresh_events,
                event_run_floor=observed_run_floor,
            )
        except PublisherError:
            compensate_stale()
            raise

    return _publish_run_state(
        client,
        spec=spec,
        head_sha=generation.head,
        run=run,
        generation=generation,
        authority_match=authority_match,
        ensure_current=ensure_current,
        reconcile_stale=reconcile_stale,
        compensate_stale=compensate_stale,
    )


def _handle_pull_request_target(
    event: Mapping[str, Any],
    client: PublisherClient,
    context: str | None,
) -> PublishResult:
    if not is_relevant_pull_request_event(event):
        return PublishResult("ignored")
    event_pull = event["pull_request"]
    assert isinstance(event_pull, Mapping)
    number = event_pull.get("number") or event.get("number")
    if not isinstance(number, int):
        raise PublisherError("pull_request_target event has no PR number")
    generation = _generation_from_pull_event(event, event_pull)
    specs = tuple(spec for spec in GATE_SPECS if context is None or spec.context == context)
    if not specs:
        raise PublisherError(f"unknown authoritative gate context: {context}")
    event_details_url = _string(event_pull.get("html_url"))
    # The webhook payload is sufficient to revoke a terminal result on its
    # head. Do this before any fallible live-state read so an observed relevant
    # transition cannot leave a stale success behind.
    for spec in specs:
        existing = _existing_custom_check(client, generation.head, spec)
        if not _check_is_terminal(existing):
            continue
        if not _should_preempt_terminal_before_reads(
            existing,
            event_generation=generation,
        ):
            continue
        _set_pending_generation(
            client,
            spec=spec,
            generation=generation,
            authority_epoch=_state_authority_epoch(existing) or 0,
            authority_history_count=_state_authority_history_count(existing) or 0,
            details_url=event_details_url,
            force=True,
            superseded_run=_state_run_identity(existing),
        )
    pull = client.get_pull_request(number)
    live_head = pull.get("head")
    live_head_sha = _string(live_head.get("sha")) if isinstance(live_head, Mapping) else ""
    if not live_head_sha:
        raise PublisherError("live pull request has no head SHA")
    if live_head_sha != generation.head:
        return PublishResult("stale")
    try:
        _ensure_unique_pull_authority(
            client,
            pull_number=number,
            head_sha=live_head_sha,
        )
    except PullAuthorityError as exc:
        for spec in specs:
            _revoke_for_pull_authority_error(
                client,
                spec=spec,
                head_sha=live_head_sha,
                event_generation=generation,
                error=exc,
                details_url=_string(pull.get("html_url")) or event_details_url,
            )
        raise
    event_base_ref = _pull_ref(event_pull, "base")
    live_base_ref = _pull_ref(pull, "base")
    event_labels = _relevant_pull_labels(event_pull)
    live_labels = _relevant_pull_labels(pull)
    event_is_live = not (
        not _generation_matches_live_pull(generation, pull)
        or (event_base_ref and live_base_ref and event_base_ref != live_base_ref)
        or (event_labels is not None and live_labels is not None and event_labels != live_labels)
    )
    publisher_probe = generation if event_is_live else _live_authority_generation(pull, ())
    if publisher_probe is None:
        return PublishResult("stale")
    try:
        publisher_active = _publisher_active_for_pull(client, publisher_probe, pull)
    except PublisherError:
        for spec in specs:
            existing = _existing_custom_check(client, publisher_probe.head, spec)
            if _check_is_terminal(existing):
                existing_generation = _state_generation(existing) or publisher_probe
                _set_pending_generation(
                    client,
                    spec=spec,
                    generation=existing_generation,
                    authority_epoch=_state_authority_epoch(existing) or 0,
                    authority_history_count=_state_authority_history_count(existing) or 0,
                    details_url=_string(pull.get("html_url")) or event_details_url,
                    force=True,
                    superseded_run=_state_run_identity(existing),
                )
        raise
    if not publisher_active:
        for spec in specs:
            existing = _existing_custom_check(client, publisher_probe.head, spec)
            if _check_is_terminal(existing):
                existing_generation = _state_generation(existing) or publisher_probe
                _set_pending_generation(
                    client,
                    spec=spec,
                    generation=existing_generation,
                    authority_epoch=_state_authority_epoch(existing) or 0,
                    authority_history_count=_state_authority_history_count(existing) or 0,
                    details_url=_string(pull.get("html_url")) or event_details_url,
                    force=True,
                    superseded_run=_state_run_identity(existing),
                )
        return PublishResult("legacy")

    authority_events = _authority_events(client.list_issue_events(number))
    authority_match = _generation_authority_match(generation, authority_events)
    final_results: list[PublishResult] = []
    changed: list[str] = []
    for spec in specs:
        if event_is_live and _authority_match_is_live(authority_match, authority_events):
            pending = _set_pending_generation(
                client,
                spec=spec,
                generation=generation,
                authority_epoch=authority_match.epoch,
                authority_history_count=authority_match.history_count,
                details_url=_string(pull.get("html_url")) or event_details_url,
            )
            changed.extend(pending.contexts)
        final_results.append(
            _reconcile_pull_context(
                client,
                spec=spec,
                pull_number=number,
                pull=pull,
                authority_events=authority_events,
            )
        )
    if len(specs) == 1:
        result = final_results[0]
        if changed and result.outcome in {"in_progress", "stale"}:
            return PublishResult("in_progress", tuple(changed))
        return result
    pending_contexts = [
        context_name
        for result in final_results
        if result.outcome in {"in_progress", "stale"}
        for context_name in result.contexts
    ]
    if changed or pending_contexts:
        return PublishResult("in_progress", tuple(dict.fromkeys(changed + pending_contexts)))
    if not final_results:
        return PublishResult("current")
    return PublishResult("current")


def _validate_workflow(event_run: Mapping[str, Any], client: PublisherClient) -> GateSpec | None:
    workflow_id = event_run.get("workflow_id")
    if not isinstance(workflow_id, int):
        return None
    spec = GATE_BY_WORKFLOW_ID.get(workflow_id)
    head_repository = event_run.get("head_repository")
    run_repository = event_run.get("repository")
    if (
        spec is None
        or event_run.get("event") not in {"pull_request", "merge_group"}
        or not isinstance(head_repository, Mapping)
        or not isinstance(head_repository.get("full_name"), str)
        or (
            isinstance(run_repository, Mapping)
            and run_repository.get("full_name") != client.repository
        )
    ):
        return None
    workflow = client.get_workflow(workflow_id)
    if (
        workflow.get("id") != workflow_id
        or workflow.get("name") != spec.workflow_name
        or workflow.get("path") != spec.workflow_path
    ):
        return None
    return spec


def _merge_group_run_candidates(
    client: PublisherClient,
    *,
    spec: GateSpec,
    head_sha: str,
    event_run_floor: Mapping[str, Any] | None = None,
) -> list[tuple[Mapping[str, Any], Generation]]:
    inventory: dict[tuple[int, int], Mapping[str, Any]] = {}
    source_runs = list(client.list_workflow_runs(spec.workflow_id, head_sha, "merge_group"))
    if event_run_floor is not None:
        source_runs.append(event_run_floor)
    for run in source_runs:
        run_id = run.get("id")
        run_attempt = run.get("run_attempt", 1)
        if not isinstance(run_id, int) or not isinstance(run_attempt, int):
            continue
        identity = (run_id, run_attempt)
        current = inventory.get(identity)
        if current is None or _run_status_rank(run) >= _run_status_rank(current):
            inventory[identity] = run
    return _matching_full_runs(
        tuple(inventory.values()),
        spec=spec,
        head_sha=head_sha,
        event="merge_group",
        expected_head_repository=client.repository,
    )


def _reconcile_merge_group_context(
    client: PublisherClient,
    *,
    spec: GateSpec,
    head_sha: str,
    event_run_floor: Mapping[str, Any] | None = None,
) -> PublishResult:
    candidates = _merge_group_run_candidates(
        client,
        spec=spec,
        head_sha=head_sha,
        event_run_floor=event_run_floor,
    )
    if not candidates:
        return PublishResult("ignored")
    run, generation = candidates[-1]
    if not client.base_contains_publisher(generation.base):
        return PublishResult("legacy")
    run_id = run.get("id")
    run_attempt = run.get("run_attempt", 1)
    if not isinstance(run_id, int) or not isinstance(run_attempt, int):
        raise PublisherError("selected merge-group run has invalid id/attempt")
    selected_run_identity = (run_id, run_attempt)
    authority_match = AuthorityMatch(0, 0)
    observed_run_floor = _advance_run_floor(event_run_floor, run)

    def ensure_current() -> bool:
        nonlocal observed_run_floor
        fresh_candidates = _merge_group_run_candidates(
            client,
            spec=spec,
            head_sha=head_sha,
            event_run_floor=observed_run_floor,
        )
        if fresh_candidates:
            observed_run_floor = _advance_run_floor(observed_run_floor, fresh_candidates[-1][0])
        return (
            bool(fresh_candidates)
            and (
                fresh_candidates[-1][0].get("id"),
                fresh_candidates[-1][0].get("run_attempt", 1),
            )
            == selected_run_identity
            and fresh_candidates[-1][1] == generation
        )

    def compensate_stale() -> PublishResult:
        return _set_pending_generation(
            client,
            spec=spec,
            generation=generation,
            authority_epoch=0,
            authority_history_count=0,
            details_url=_string(run.get("html_url")),
            force=True,
            superseded_run=selected_run_identity,
        )

    def reconcile_stale() -> PublishResult:
        try:
            return _reconcile_merge_group_context(
                client,
                spec=spec,
                head_sha=head_sha,
                event_run_floor=observed_run_floor,
            )
        except PublisherError:
            compensate_stale()
            raise

    return _publish_run_state(
        client,
        spec=spec,
        head_sha=head_sha,
        run=run,
        generation=generation,
        authority_match=authority_match,
        ensure_current=ensure_current,
        reconcile_stale=reconcile_stale,
        compensate_stale=compensate_stale,
    )


def _handle_workflow_run(
    event: Mapping[str, Any],
    client: PublisherClient,
    context: str | None,
) -> PublishResult:
    event_run = event.get("workflow_run")
    if not isinstance(event_run, Mapping):
        raise PublisherError("workflow_run event is missing workflow_run")
    workflow_id = event_run.get("workflow_id")
    prevalidated_spec = (
        GATE_BY_WORKFLOW_ID.get(workflow_id) if isinstance(workflow_id, int) else None
    )
    head_repository = event_run.get("head_repository")
    run_repository = event_run.get("repository")
    run_event = _string(event_run.get("event"))
    head_sha = _string(event_run.get("head_sha"))
    if (
        prevalidated_spec is None
        or run_event not in {"pull_request", "merge_group"}
        or not head_sha
        or not isinstance(head_repository, Mapping)
        or not isinstance(head_repository.get("full_name"), str)
        or (
            isinstance(run_repository, Mapping)
            and run_repository.get("full_name") != client.repository
        )
        or (context is not None and prevalidated_spec.context != context)
    ):
        return PublishResult("ignored")
    preempted_terminal: Mapping[str, Any] | None = None
    event_generation = parse_full_generation(event_run.get("display_title"))
    if event_generation is not None and event_generation.head == head_sha:
        existing = _existing_custom_check(client, head_sha, prevalidated_spec)
        if (
            _check_is_terminal(existing)
            and _should_preempt_terminal_before_reads(
                existing,
                event_generation=event_generation,
                event_run=event_run,
            )
        ):
            preempted_terminal = existing
            _set_pending_generation(
                client,
                spec=prevalidated_spec,
                generation=event_generation,
                authority_epoch=_state_authority_epoch(existing) or 0,
                authority_history_count=_state_authority_history_count(existing) or 0,
                details_url=_string(event_run.get("html_url")),
                force=True,
                superseded_run=_state_run_identity(existing),
            )
    spec = _validate_workflow(event_run, client)
    if spec is None or (context is not None and spec.context != context):
        return PublishResult("ignored")
    assert isinstance(head_repository, Mapping)
    event_head_repository = _string(head_repository.get("full_name"))
    if not head_sha:
        return PublishResult("ignored")

    if run_event == "pull_request":
        pull_number = _pull_number_for_run(client, event_run, head_sha)
        if pull_number is None:
            return PublishResult("ignored")
        pull = client.get_pull_request(pull_number)
        live_head = pull.get("head")
        live_head_sha = _string(live_head.get("sha")) if isinstance(live_head, Mapping) else ""
        if not live_head_sha:
            raise PublisherError("live pull request has no head SHA")
        if live_head_sha != head_sha:
            return PublishResult("stale")
        if _pull_is_merged(pull):
            associated = _open_pull_numbers_for_commit(client, head_sha)
            existing = _existing_custom_check(client, head_sha, spec)
            existing_generation = _state_generation(existing)
            existing_owner = (
                int(existing_generation.pull)
                if existing_generation is not None and existing_generation.pull.isdigit()
                else None
            )
            if not associated:
                if (
                    _check_is_terminal(existing)
                    and existing_owner == pull_number
                ):
                    existing_conclusion = _string(existing.get("conclusion"))
                    return PublishResult(
                        existing_conclusion
                        if existing_conclusion in {"success", "failure"}
                        else "current"
                    )
                restored_generation = _state_generation(preempted_terminal)
                restored_epoch = _state_authority_epoch(preempted_terminal)
                restored_count = _state_authority_history_count(preempted_terminal)
                restored_run = _state_run_identity(preempted_terminal)
                restored_conclusion = (
                    _string(preempted_terminal.get("conclusion"))
                    if preempted_terminal is not None
                    else ""
                )
                restored_owner = (
                    int(restored_generation.pull)
                    if restored_generation is not None and restored_generation.pull.isdigit()
                    else None
                )
                event_run_id = event_run.get("id")
                event_run_attempt = event_run.get("run_attempt", 1)
                event_identity = (
                    (event_run_id, event_run_attempt)
                    if isinstance(event_run_id, int) and isinstance(event_run_attempt, int)
                    else None
                )
                incoming_conclusion = (
                    "success"
                    if event_run.get("status") == "completed"
                    and event_run.get("conclusion") == "success"
                    else "failure"
                    if event_run.get("status") == "completed"
                    else ""
                )
                same_generation = (
                    restored_generation is not None and event_generation == restored_generation
                )
                incoming_is_stale = (
                    same_generation
                    and restored_run is not None
                    and event_identity is not None
                    and (
                        event_identity < restored_run
                        or (
                            event_identity == restored_run
                            and incoming_conclusion == restored_conclusion
                        )
                    )
                )
                if (
                    not incoming_is_stale
                    and not same_generation
                    and restored_generation is not None
                    and event_generation is not None
                ):
                    incoming_is_stale = (
                        _generation_time_order(event_generation, restored_generation) == -1
                    )
                merged_events: tuple[AuthorityEvent, ...] | None = None
                if (
                    not incoming_is_stale
                    and not same_generation
                    and event_generation is not None
                    and preempted_terminal is not None
                ):
                    merged_events = _authority_events(client.list_issue_events(pull_number))
                    event_match = _generation_authority_match(
                        event_generation,
                        merged_events,
                    )
                    incoming_is_stale = (
                        event_match.verified
                        and event_match.history_count == len(merged_events)
                        and event_match.epoch != _latest_authority_epoch(merged_events)
                    )
                observed_restoration_candidates: dict[
                    tuple[int, int],
                    tuple[Mapping[str, Any], Generation],
                ] = {}

                def restored_terminal_is_current() -> bool:
                    assert restored_generation is not None
                    assert restored_epoch is not None
                    assert restored_count is not None
                    fresh_pull = client.get_pull_request(pull_number)
                    if not _pull_is_merged(fresh_pull):
                        return False
                    if _open_pull_numbers_for_commit(client, restored_generation.head):
                        return False
                    fresh_events = _authority_events(client.list_issue_events(pull_number))
                    restored_match = _generation_authority_match(
                        restored_generation,
                        fresh_events,
                    )
                    if not (
                        _generation_matches_pull_snapshot(restored_generation, fresh_pull)
                        and _authority_match_is_live(restored_match, fresh_events)
                        and restored_epoch == restored_match.epoch
                        and restored_count == restored_match.history_count
                        and _publisher_active_for_pull(
                            client,
                            restored_generation,
                            fresh_pull,
                        )
                    ):
                        return False
                    restored_candidates = _pull_run_candidates(
                        client,
                        spec=spec,
                        pull=fresh_pull,
                        event_run_floor=event_run,
                    )
                    for candidate_run, candidate_generation in restored_candidates:
                        candidate_run_id = candidate_run.get("id")
                        candidate_attempt = candidate_run.get("run_attempt", 1)
                        if not isinstance(candidate_run_id, int) or not isinstance(
                            candidate_attempt,
                            int,
                        ):
                            continue
                        candidate_identity = (candidate_run_id, candidate_attempt)
                        observed = observed_restoration_candidates.get(candidate_identity)
                        if observed is None or _run_status_rank(candidate_run) >= _run_status_rank(
                            observed[0]
                        ):
                            observed_restoration_candidates[candidate_identity] = (
                                candidate_run,
                                candidate_generation,
                            )
                    monotonic_candidates = [
                        observed_restoration_candidates[identity]
                        for identity in sorted(observed_restoration_candidates)
                    ]
                    live_base_ref = _pull_ref(fresh_pull, "base")
                    owned_candidates = [
                        (candidate_run, candidate_generation)
                        for candidate_run, candidate_generation in monotonic_candidates
                        if isinstance(
                            (candidate_pulls := candidate_run.get("pull_requests")),
                            list,
                        )
                        and (
                            not candidate_pulls
                            or _pull_number_from_run(
                                candidate_run,
                                restored_generation.head,
                            )
                            == pull_number
                        )
                        and (
                            not (
                                candidate_base_ref := _run_pull_ref(
                                    candidate_run,
                                    pull_number,
                                    "base",
                                )
                            )
                            or not live_base_ref
                            or candidate_base_ref == live_base_ref
                        )
                    ]
                    potentially_current_candidates = []
                    latest_epoch = _latest_authority_epoch(fresh_events)
                    for candidate_run, candidate_generation in owned_candidates:
                        if not _generation_matches_pull_snapshot(
                            candidate_generation,
                            fresh_pull,
                        ):
                            continue
                        candidate_match = _generation_authority_match(
                            candidate_generation,
                            fresh_events,
                        )
                        candidate_is_stale = _generation_time_order(
                            candidate_generation,
                            restored_generation,
                        ) == -1 or (
                            candidate_match.verified
                            and candidate_match.history_count == len(fresh_events)
                            and candidate_match.epoch != latest_epoch
                        )
                        if not candidate_is_stale:
                            potentially_current_candidates.append(
                                (candidate_run, candidate_generation)
                            )
                    if any(
                        candidate_generation != restored_generation
                        for _, candidate_generation in potentially_current_candidates
                    ):
                        return False
                    restored_generation_runs = [
                        candidate_run
                        for candidate_run, candidate_generation in owned_candidates
                        if candidate_generation == restored_generation
                    ]
                    source_occurrences = {
                        run_id
                        for candidate_run, candidate_generation in owned_candidates
                        if _same_source_occurrence_shape(
                            candidate_generation,
                            restored_generation,
                        )
                        and isinstance((run_id := candidate_run.get("id")), int)
                    }
                    latest_restored_run = (
                        restored_generation_runs[-1] if restored_generation_runs else None
                    )
                    latest_restored_run_id = (
                        latest_restored_run.get("id") if latest_restored_run is not None else None
                    )
                    latest_restored_attempt = (
                        latest_restored_run.get("run_attempt", 1)
                        if latest_restored_run is not None
                        else None
                    )
                    latest_restored_identity = (
                        (latest_restored_run_id, latest_restored_attempt)
                        if isinstance(latest_restored_run_id, int)
                        and isinstance(latest_restored_attempt, int)
                        else None
                    )
                    latest_restored_verdict = ""
                    if (
                        latest_restored_run is not None
                        and latest_restored_run.get("status") == "completed"
                        and latest_restored_identity is not None
                    ):
                        latest_restored_verdict = (
                            attempt_conclusion(
                                client.list_run_jobs(
                                    latest_restored_identity[0],
                                    latest_restored_identity[1],
                                ),
                                spec.attempt_job,
                            )
                            if latest_restored_run.get("conclusion") == "success"
                            else "failure"
                        )
                    return (
                        len(source_occurrences) >= restored_match.source_ordinal
                        and latest_restored_identity == restored_run
                        and latest_restored_verdict == restored_conclusion
                    )

                restore_is_safe = (
                    incoming_is_stale
                    and restored_generation is not None
                    and restored_epoch is not None
                    and restored_count is not None
                    and restored_terminal_is_current()
                )
                if (
                    _check_is_terminal(preempted_terminal)
                    and restored_generation is not None
                    and restored_epoch is not None
                    and restored_count is not None
                    and restored_owner == pull_number
                    and restored_conclusion in {"success", "failure"}
                    and restore_is_safe
                    and not _check_is_terminal(existing)
                ):

                    def compensate_restoration() -> PublishResult:
                        return _set_pending_generation(
                            client,
                            spec=spec,
                            generation=restored_generation,
                            authority_epoch=restored_epoch,
                            authority_history_count=restored_count,
                            details_url=_string(preempted_terminal.get("details_url")),
                            force=True,
                            superseded_run=restored_run,
                        )

                    try:
                        _upsert_check(
                            client,
                            spec=spec,
                            head_sha=head_sha,
                            existing=existing,
                            status="completed",
                            conclusion=restored_conclusion,
                            generation=restored_generation,
                            authority_epoch=restored_epoch,
                            authority_history_count=restored_count,
                            details_url=_string(preempted_terminal.get("details_url")),
                            run_id=restored_run[0] if restored_run is not None else None,
                            run_attempt=restored_run[1] if restored_run is not None else None,
                        )
                        if not restored_terminal_is_current():
                            return compensate_restoration()
                    except PublisherError:
                        compensate_restoration()
                        raise
                    return PublishResult(restored_conclusion)
                return PublishResult("stale")
        try:
            _ensure_unique_pull_authority(
                client,
                pull_number=pull_number,
                head_sha=head_sha,
            )
        except PullAuthorityError as exc:
            # The PR can merge between the live snapshot above and the
            # commit-association read inside `_ensure_unique_pull_authority`.
            # In that case GitHub legitimately removes the PR from the open
            # association set. Re-enter with the now-merged snapshot so the
            # post-merge path preserves the already-authoritative terminal
            # result instead of revoking it as an ambiguous open authority.
            #
            # Only an empty association is eligible: a different or additional
            # open same-head PR remains a real authority conflict and must
            # continue to fail closed.
            if not exc.associated:
                fresh_pull = client.get_pull_request(pull_number)
                fresh_head = fresh_pull.get("head")
                fresh_head_sha = (
                    _string(fresh_head.get("sha")) if isinstance(fresh_head, Mapping) else ""
                )
                if fresh_head_sha == head_sha and _pull_is_merged(fresh_pull):
                    existing = _existing_custom_check(client, head_sha, spec)
                    existing_generation = _state_generation(existing)
                    existing_owner = (
                        int(existing_generation.pull)
                        if existing_generation is not None and existing_generation.pull.isdigit()
                        else None
                    )
                    if _check_is_terminal(existing) and existing_owner == pull_number:
                        existing_conclusion = _string(existing.get("conclusion"))
                        return PublishResult(
                            existing_conclusion
                            if existing_conclusion in {"success", "failure"}
                            else "current"
                        )
                    return _handle_workflow_run(event, client, context)
            _revoke_for_pull_authority_error(
                client,
                spec=spec,
                head_sha=head_sha,
                event_generation=event_generation,
                error=exc,
                details_url=_string(event_run.get("html_url")),
            )
            raise
        expected_head_repository = _pull_repository(pull, "head") or event_head_repository
        if event_head_repository != expected_head_repository:
            return PublishResult("ignored")
        event_run_floor = (
            event_run if parse_full_generation(event_run.get("display_title")) is not None else None
        )
        available_candidates = _pull_run_candidates(
            client,
            spec=spec,
            pull=pull,
            event_run_floor=event_run_floor,
        )
        if event_run_floor is None and not available_candidates:
            return PublishResult("ignored")
        live_candidates = _live_pull_snapshot_candidates(
            available_candidates,
            pull_number=pull_number,
            pull=pull,
        )
        if live_candidates and not any(
            _publisher_active_for_pull(client, candidate_generation, pull)
            for _, candidate_generation in live_candidates
        ):
            return PublishResult("legacy")
        authority_events = _authority_events(client.list_issue_events(pull_number))
        observed_run_floor = event_run_floor
        if live_candidates:
            observed_run_floor = _advance_run_floor(
                observed_run_floor,
                live_candidates[-1][0],
            )
        return _reconcile_pull_context(
            client,
            spec=spec,
            pull_number=pull_number,
            pull=pull,
            authority_events=authority_events,
            event_run_floor=observed_run_floor,
        )
    if event_head_repository != client.repository:
        return PublishResult("ignored")
    event_run_floor = (
        event_run if parse_full_generation(event_run.get("display_title")) is not None else None
    )
    available_merge_candidates = _merge_group_run_candidates(
        client,
        spec=spec,
        head_sha=head_sha,
        event_run_floor=event_run_floor,
    )
    if not available_merge_candidates:
        return PublishResult("ignored")
    observed_merge_floor = _advance_run_floor(
        event_run_floor,
        available_merge_candidates[-1][0],
    )
    return _reconcile_merge_group_context(
        client,
        spec=spec,
        head_sha=head_sha,
        event_run_floor=observed_merge_floor,
    )


def process_event(
    event: Mapping[str, Any],
    client: PublisherClient,
    *,
    context: str | None = None,
) -> PublishResult:
    event_name = _string(event.get("event_name"))
    if not event_name:
        if "workflow_run" in event:
            event_name = "workflow_run"
        elif "pull_request" in event:
            event_name = "pull_request_target"
    if event_name == "pull_request_target":
        return _handle_pull_request_target(event, client, context)
    if event_name == "workflow_run":
        return _handle_workflow_run(event, client, context)
    return PublishResult("ignored")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", type=Path, required=True)
    parser.add_argument(
        "--context",
        choices=tuple(spec.context for spec in GATE_SPECS),
        required=True,
    )
    args = parser.parse_args(argv)
    try:
        event = json.loads(args.event_path.read_text(encoding="utf-8"))
        if not isinstance(event, Mapping):
            raise PublisherError("event payload must be a JSON object")
        client = GitHubClient(
            token=os.environ.get("GITHUB_TOKEN", ""),
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        )
        result = process_event(event, client, context=args.context)
    except (OSError, json.JSONDecodeError, PublisherError, ValueError) as exc:
        print(f"authoritative gate publisher failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {"outcome": result.outcome, "contexts": list(result.contexts)},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
