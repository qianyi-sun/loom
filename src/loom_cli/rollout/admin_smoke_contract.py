"""Single-source admin-on-behalf smoke predicates for rehearsal and final gates."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from loom.security.redaction import redact_mapping, redact_text

_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_TASK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}(?:/[a-z0-9][a-z0-9._-]{0,127})+$")
_BATCH_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_NONRECOVERABLE_BATCH_RESULT_STATUSES = frozenset({"partial_failed", "all_failed"})
_NONRECOVERABLE_FANOUT_REASONS = frozenset({"required_worker_pool_incompatible"})


@dataclass(frozen=True, slots=True)
class AdminSmokeAuthority:
    """Strict non-secret identity and workload authority for one smoke run."""

    represented_username: str
    team_id: str
    admin_actor: str
    task_id: str
    required_worker_pool: str | None
    agent: str

    def __post_init__(self) -> None:
        try:
            parsed_team = uuid.UUID(self.team_id)
        except (AttributeError, ValueError) as exc:
            raise ValueError("admin smoke team identity is invalid") from exc
        if (
            parsed_team.version != 4
            or str(parsed_team) != self.team_id
            or _SAFE_NAME_RE.fullmatch(self.represented_username) is None
            or _SAFE_NAME_RE.fullmatch(self.admin_actor) is None
            or len(self.task_id) > 256
            or _TASK_ID_RE.fullmatch(self.task_id) is None
            or (
                self.required_worker_pool is not None
                and _SAFE_NAME_RE.fullmatch(self.required_worker_pool) is None
            )
            or _SAFE_NAME_RE.fullmatch(self.agent) is None
        ):
            raise ValueError("admin smoke authority is invalid")

    def to_record(self) -> dict[str, str | None]:
        return {
            "admin_actor": self.admin_actor,
            "agent": self.agent,
            "represented_username": self.represented_username,
            "required_worker_pool": self.required_worker_pool,
            "task_id": self.task_id,
            "team_id": self.team_id,
        }

    @classmethod
    def from_record(cls, value: Mapping[str, object]) -> AdminSmokeAuthority:
        expected = {
            "admin_actor",
            "agent",
            "represented_username",
            "required_worker_pool",
            "task_id",
            "team_id",
        }
        string_fields = expected - {"required_worker_pool"}
        if (
            set(value) != expected
            or any(not isinstance(value.get(field), str) for field in string_fields)
            or not (
                value.get("required_worker_pool") is None
                or isinstance(value.get("required_worker_pool"), str)
            )
        ):
            raise ValueError("admin smoke authority schema is invalid")
        return cls(
            represented_username=str(value["represented_username"]),
            team_id=str(value["team_id"]),
            admin_actor=str(value["admin_actor"]),
            task_id=str(value["task_id"]),
            required_worker_pool=(
                str(value["required_worker_pool"])
                if value["required_worker_pool"] is not None
                else None
            ),
            agent=str(value["agent"]),
        )


@dataclass(frozen=True, slots=True)
class AdminSmokeContract:
    """Pure payload, identity and result predicates shared by both stages."""

    authority: AdminSmokeAuthority

    def submission_payload(
        self,
        *,
        batch_name: str,
        n_per_task: int = 1,
    ) -> dict[str, object]:
        if _BATCH_NAME_RE.fullmatch(batch_name) is None or not 1 <= n_per_task <= 100:
            raise ValueError("admin smoke submission authority is invalid")
        payload: dict[str, object] = {
            "name": batch_name,
            "represented_username": self.authority.represented_username,
            "team_id": self.authority.team_id,
            "task_filter": {"task_ids": [self.authority.task_id]},
            "trial_config": {
                "agent_name": self.authority.agent,
                "agent_model": None,
            },
            "n_per_task": n_per_task,
        }
        if self.authority.required_worker_pool is not None:
            payload["required_worker_pools"] = [self.authority.required_worker_pool]
        return payload

    def validate_admin_identity(self, value: object) -> str | None:
        payload = _mapping(value)
        if payload is None:
            return "admin smoke whoami response is not JSON"
        scopes = payload.get("scopes")
        admin_scoped = isinstance(scopes, list) and any(
            isinstance(scope, str) and scope.startswith("admin:") for scope in scopes
        )
        if (
            payload.get("credential_type") == "admin_bearer_token"
            or payload.get("principal_type") == "admin"
            or admin_scoped
        ):
            return None
        return (
            "admin-on-behalf smoke requires an admin-capable token; "
            f"whoami credential_type={payload.get('credential_type')!r} "
            f"principal_type={payload.get('principal_type')!r}"
        )

    def validate_benchmark_catalog(self, value: object) -> str | None:
        payload = _mapping(value)
        if payload is None:
            return "benchmarks response not JSON"
        items = payload.get("items") or payload.get("data") or []
        return None if isinstance(items, list) and items else "benchmarks catalog is empty"

    def existing_batch_id(self, value: object, *, batch_name: str) -> str | None:
        if _BATCH_NAME_RE.fullmatch(batch_name) is None:
            raise ValueError("admin smoke batch identity is invalid")
        payload = _mapping(value)
        items = payload.get("items") if payload is not None else None
        if not isinstance(items, list):
            return None
        for item in items:
            if not isinstance(item, Mapping):
                continue
            submitted_by_user = item.get("submitted_by_user")
            task_filter = item.get("task_filter")
            if (
                item.get("name") != batch_name
                or item.get("team_id") != self.authority.team_id
                or not isinstance(submitted_by_user, Mapping)
                or not _same_username(
                    submitted_by_user.get("username"),
                    self.authority.represented_username,
                )
                or submitted_by_user.get("team_id") != self.authority.team_id
                or not isinstance(task_filter, Mapping)
                or task_filter.get("task_ids") != [self.authority.task_id]
            ):
                continue
            batch_id = item.get("id")
            if isinstance(batch_id, str) and batch_id:
                return batch_id
        return None

    def nonrecoverable_failure(self, value: object) -> str | None:
        payload = _mapping(value)
        if payload is None:
            return "admin smoke batch response is not JSON"
        result_status = payload.get("result_status")
        failure_reason = payload.get("failure_reason")
        fanout_errors = payload.get("fanout_errors")
        fanout_reasons = _fanout_error_reasons(fanout_errors)
        fanout_submit_failed = failure_reason == "fanout_submit_failed"
        incompatible_fanout = bool(fanout_reasons & _NONRECOVERABLE_FANOUT_REASONS)
        failed_result = result_status in _NONRECOVERABLE_BATCH_RESULT_STATUSES
        if not fanout_submit_failed and not (failed_result and incompatible_fanout):
            return None
        parts = [
            "admin-on-behalf smoke batch reported nonrecoverable fanout failure",
            f"state={payload.get('state')!r}",
            f"result_status={result_status!r}",
            f"failure_reason={failure_reason!r}",
        ]
        failure_message = payload.get("failure_message")
        if isinstance(failure_message, str) and failure_message.strip():
            parts.append(f"failure_message={redact_text(failure_message, limit=300)!r}")
        if fanout_errors:
            parts.append("fanout_errors=" + _compact_redacted_json(fanout_errors, limit=1000))
        return "; ".join(parts)

    def validate_admitted_batch(
        self,
        value: object,
        *,
        batch_id: str,
        batch_name: str,
    ) -> str | None:
        """Validate exact persisted admission without requiring worker completion."""
        if _BATCH_NAME_RE.fullmatch(batch_name) is None or not batch_id:
            raise ValueError("admin smoke batch identity is invalid")
        payload = _mapping(value)
        if payload is None:
            return "admin smoke batch response is not JSON"
        submitted_by_user = payload.get("submitted_by_user")
        task_filter = payload.get("task_filter")
        required_worker_pools = payload.get("required_worker_pools")
        expected_pools = (
            [self.authority.required_worker_pool]
            if self.authority.required_worker_pool is not None
            else []
        )
        if payload.get("id") != batch_id:
            return "admin smoke persisted batch id drifted"
        if payload.get("name") != batch_name:
            return "admin smoke persisted batch name drifted"
        if payload.get("team_id") != self.authority.team_id:
            return "admin smoke persisted team identity drifted"
        if not isinstance(submitted_by_user, Mapping):
            return "admin smoke persisted batch has no represented submitter"
        if (
            not _same_username(
                submitted_by_user.get("username"),
                self.authority.represented_username,
            )
            or submitted_by_user.get("team_id") != self.authority.team_id
        ):
            return "admin smoke persisted represented identity drifted"
        if not isinstance(task_filter, Mapping) or task_filter.get("task_ids") != [
            self.authority.task_id
        ]:
            return "admin smoke persisted task authority drifted"
        if required_worker_pools != expected_pools:
            return "admin smoke persisted worker-pool authority drifted"
        if payload.get("state") not in {"submitted", "pending", "running", "finished"}:
            return "admin smoke persisted batch state is invalid"
        return self.nonrecoverable_failure(payload)

    def validate_terminal_batch(self, value: object) -> str | None:
        payload = _mapping(value)
        if payload is None:
            return "batch poll response not JSON"
        if payload.get("state") != "finished":
            return f"batch terminal state {payload.get('state')!r} — expected finished"
        if payload.get("result_status") != "succeeded":
            return f"batch result_status {payload.get('result_status')!r} — expected succeeded"
        expected_count = _json_int(payload.get("expected_trial_count"))
        if expected_count is None:
            return "batch response has no numeric expected_trial_count"
        trial_summary = payload.get("trial_summary")
        if not isinstance(trial_summary, Mapping):
            return "batch response has no trial_summary"
        succeeded = _json_int(trial_summary.get("succeeded", 0))
        if succeeded is None:
            return "batch trial_summary.succeeded is not numeric"
        if succeeded < expected_count:
            return f"batch succeeded trials {succeeded} < expected {expected_count}"
        submitted_by_user = payload.get("submitted_by_user")
        if not isinstance(submitted_by_user, Mapping):
            return "batch response has no submitted_by_user"
        username = submitted_by_user.get("username")
        team_id = submitted_by_user.get("team_id")
        if (
            not _same_username(username, self.authority.represented_username)
            or team_id != self.authority.team_id
        ):
            return (
                "batch submitted_by_user does not match represented "
                f"user/team: username={username!r} team_id={team_id!r}"
            )
        return None


def decode_json_object(payload: bytes) -> Mapping[str, object] | None:
    """Decode one JSON object without ever admitting scalar or list evidence."""
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _same_username(left: object, right: str) -> bool:
    return isinstance(left, str) and left.strip().casefold() == right.strip().casefold()


def _json_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _fanout_error_reasons(value: object) -> set[str]:
    if isinstance(value, Mapping):
        reason = value.get("reason")
        return {reason} if isinstance(reason, str) and reason else set()
    if isinstance(value, list):
        reasons: set[str] = set()
        for item in value:
            reasons.update(_fanout_error_reasons(item))
        return reasons
    return set()


def _compact_redacted_json(value: object, *, limit: int) -> str:
    redacted = redact_mapping(value)
    try:
        rendered = json.dumps(redacted, sort_keys=True, separators=(",", ":"))
    except TypeError:
        rendered = str(redacted)
    return redact_text(rendered, limit=limit)


__all__ = [
    "AdminSmokeAuthority",
    "AdminSmokeContract",
    "decode_json_object",
]
