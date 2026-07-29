#!/usr/bin/env python3
"""Root authority for #1023's exact squash-merged staging promotion receipt.

The public command accepts only a broker request identity.  Every success
predicate is read from fixed, protected broker/evidence/candidate paths; callers
cannot provide a result, candidate, source path, or evidence document.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import pwd
import re
import secrets
import socket
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 1
SOURCE_HOST: Final = "trt-eai-oldlab-1"
APPROVED_REMOTE_URL: Final = "https://github.com/qianyi-sun/loom.git"
APPROVED_TARGET_REF: Final = "origin/dev"
STATE_ROOT: Final = Path("/var/lib/loom-staging-rollout")
ROLLOUT_ROOT: Final = Path("/data/loom-staging")
CANDIDATE_ROOT: Final = Path("/opt/loom-staging-runner/candidates")
ACCEPTANCE_ROOT: Final = STATE_ROOT / "acceptance"
PROMOTION_PATH: Final = ACCEPTANCE_ROOT / "promotion.json"
INSTALLED_PROGRAM: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-staging-promotion",
)
INSTALLED_SUDOERS: Final = Path(
    "/etc/sudoers.d/loom-developer-sandbox-staging-promotion",
)
GIT: Final = Path("/usr/bin/git")
MAX_JSON_BYTES: Final = 16 * 1024 * 1024
MAX_RECEIPTS: Final = 4096

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_RE = re.compile(r"^req-[0-9a-f]{16}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_ROUTE_RE = re.compile(r"^https://[a-z0-9.-]+/[a-z0-9/-]+$")
_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_SUDOERS = """\
%loom-staging-operators ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-developer-sandbox-staging-promotion produce --request-id req-* --execute
%loom-staging-operators ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-developer-sandbox-staging-promotion check
"""

_STEPS: Final[tuple[tuple[int, str], ...]] = (
    (0, "resolve-target"),
    (1, "worktree"),
    (2, "build-images"),
    (3, "kind-cluster"),
    (4, "kind-load-images"),
    (5, "backup"),
    (6, "audit"),
    (7, "render"),
    (8, "preflight"),
    (9, "migrate"),
    (10, "cluster-up"),
    (11, "env-state"),
    (12, "gb10-prep"),
    (13, "production-defaults"),
    (14, "release-gate"),
    (15, "smoke"),
    (16, "staging-admin-browser-acceptance"),
    (99, "summary"),
)

_BROWSER_CHECKS: Final[frozenset[str]] = frozenset(
    {
        "bootstrap_status_204",
        "bootstrap_empty_body",
        "bootstrap_no_store",
        "deployed_build_sha_present",
        "deployed_build_sha_matches_expected",
        "secure_http_only_lax_cookie",
        "authenticated_target_user",
        "platform_admin_authority",
        "audit_event_correlated",
        "admin_access_document_2xx",
        "authenticated_react_mount",
        "admin_tabs_accessibility",
        "admin_requests_apis_200",
        "admin_requests_ui_visible",
        "admin_accounts_apis_200",
        "admin_accounts_ui_visible",
        "admin_teams_api_200",
        "admin_teams_ui_visible",
        "admin_invites_apis_200",
        "admin_invites_ui_visible",
        "admin_tokens_api_200",
        "admin_tokens_ui_visible",
        "admin_audit_api_200",
        "all_admin_tabs_operable",
        "audit_tab_event_visible",
        "rate_cards_api_200",
        "rate_cards_ui_visible",
        "browser_console_clean",
        "browser_page_errors_clean",
        "browser_request_failures_clean",
        "browser_server_errors_clean",
    },
)


class PromotionError(RuntimeError):
    """The protected evidence cannot authorize a promotion receipt."""


@dataclass(frozen=True)
class Layout:
    state_root: Path = STATE_ROOT
    rollout_root: Path = ROLLOUT_ROOT
    candidate_root: Path = CANDIDATE_ROOT
    acceptance_root: Path = ACCEPTANCE_ROOT
    installed_program: Path = INSTALLED_PROGRAM
    installed_sudoers: Path = INSTALLED_SUDOERS
    git: Path = GIT

    @property
    def promotion(self) -> Path:
        return self.acceptance_root / "promotion.json"

    @property
    def state(self) -> Path:
        return self.acceptance_root / "state.json"

    @property
    def pending(self) -> Path:
        return self.acceptance_root / "pending.json"

    @property
    def lock(self) -> Path:
        return self.acceptance_root / ".lock"

    @property
    def receipts(self) -> Path:
        return self.acceptance_root / "receipts"


DEFAULT_LAYOUT: Final = Layout()


@dataclass(frozen=True)
class Snapshot:
    path: Path
    payload: bytes
    identity: tuple[int, ...]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()


@dataclass(frozen=True)
class ValidatedRollout:
    request_id: str
    attempt_number: int
    rollout_id: str
    candidate_sha: str
    candidate_tree: str
    observed_at: str
    source_snapshots: tuple[Snapshot, ...]


GitRunner = Callable[[Sequence[str], Path], str]
Failpoint = Callable[[str], None]


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise PromotionError(f"{field} is not a UTC timestamp")
    try:
        parsed = datetime.strptime(value, _UTC_FORMAT).replace(tzinfo=UTC)
    except ValueError as exc:
        raise PromotionError(f"{field} is not a canonical UTC timestamp") from exc
    return parsed


def _now_utc() -> str:
    return datetime.now(UTC).strftime(_UTC_FORMAT)


def _strict_json(payload: bytes, *, label: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite value {token!r}"),
            ),
        )
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise PromotionError(f"{label} is not strict JSON") from exc
    if not isinstance(value, dict):
        raise PromotionError(f"{label} must be a JSON object")
    return value


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _validate_directory_chain(path: Path, *, owner_uids: frozenset[int]) -> None:
    descriptor = _open_trusted_directory(path, owner_uids=owner_uids)
    os.close(descriptor)


def _directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if not nofollow or not directory:
        raise PromotionError("no-follow directory traversal is unavailable")
    return os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | nofollow | directory


def _open_trusted_directory(path: Path, *, owner_uids: frozenset[int]) -> int:
    absolute = Path(os.path.abspath(path))
    flags = _directory_flags()
    descriptor = os.open("/", flags)
    current = Path("/")
    try:
        root = os.fstat(descriptor)
        if root.st_uid not in owner_uids or stat.S_IMODE(root.st_mode) & 0o022:
            raise PromotionError("root directory authority is unsafe")
        for component in absolute.parts[1:]:
            current /= component
            child = os.open(component, flags, dir_fd=descriptor)
            metadata = os.fstat(child)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid not in owner_uids
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                os.close(child)
                raise PromotionError(f"protected directory authority is unsafe: {current}")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except PromotionError:
        os.close(descriptor)
        raise
    except OSError as exc:
        os.close(descriptor)
        raise PromotionError(f"protected directory is unavailable: {current}") from exc


def _snapshot(
    path: Path,
    *,
    owner_uids: frozenset[int],
    label: str,
    modes: frozenset[int] = frozenset({0o600}),
    max_bytes: int = MAX_JSON_BYTES,
) -> Snapshot:
    parent = _open_trusted_directory(path.parent, owner_uids=owner_uids)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        before_path = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        descriptor = os.open(path.name, flags, dir_fd=parent)
    except OSError as exc:
        os.close(parent)
        raise PromotionError(f"{label} is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in owner_uids
            or stat.S_IMODE(before.st_mode) not in modes
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > max_bytes
            or (before.st_dev, before.st_ino) != (before_path.st_dev, before_path.st_ino)
        ):
            raise PromotionError(f"{label} file authority is unsafe")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        after_path = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if (
            len(payload) > max_bytes
            or _metadata_identity(before) != _metadata_identity(after)
            or (after_path.st_dev, after_path.st_ino) != (before.st_dev, before.st_ino)
            or after_path.st_size != before.st_size
            or after_path.st_mtime_ns != before.st_mtime_ns
        ):
            raise PromotionError(f"{label} changed while it was read")
        return Snapshot(path=path, payload=payload, identity=_metadata_identity(before))
    finally:
        os.close(descriptor)
        os.close(parent)


def _assert_unchanged(snapshot: Snapshot) -> None:
    try:
        current = _snapshot(
            snapshot.path,
            owner_uids=frozenset({0, snapshot.identity[3]}),
            label=f"source {snapshot.path}",
            modes=frozenset({stat.S_IMODE(snapshot.identity[2])}),
            max_bytes=max(len(snapshot.payload), 1),
        )
    except PromotionError as exc:
        raise PromotionError(
            f"source changed before publication: {snapshot.path}",
        ) from exc
    if current.identity != snapshot.identity or current.payload != snapshot.payload:
        raise PromotionError(f"source changed before publication: {snapshot.path}")


def _exact_keys(value: Mapping[str, object], keys: set[str], *, label: str) -> None:
    if set(value) != keys:
        raise PromotionError(f"{label} does not use the closed schema")


def _sha(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise PromotionError(f"{field} is not a full lowercase git SHA")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PromotionError(f"{field} is not a SHA-256 digest")
    return value


def _safe_id(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise PromotionError(f"{field} is not a safe identifier")
    return value


def _validate_request(
    request: Mapping[str, object],
    *,
    request_id: str,
) -> tuple[str, str, str]:
    _exact_keys(
        request,
        {
            "request_id",
            "rollout_id",
            "caller",
            "candidate",
            "requested_at",
            "runner_config_sha256",
            "preflight_attestation_sha256",
            "preflight_registry_sha256",
            "preflight_coverage_sha256",
            "command",
            "status",
            "schema_version",
        },
        label="rollout request",
    )
    caller = request["caller"]
    candidate = request["candidate"]
    if not isinstance(caller, Mapping) or not isinstance(candidate, Mapping):
        raise PromotionError("rollout request nested bindings are invalid")
    _exact_keys(caller, {"username", "uid", "schema_version"}, label="request caller")
    _exact_keys(
        candidate,
        {
            "remote_url",
            "target_ref",
            "resolved_sha",
            "image_tag",
            "fetched_at",
            "schema_version",
            "resolved_tree",
        },
        label="request candidate",
    )
    candidate_sha = _sha(candidate["resolved_sha"], field="request candidate SHA")
    candidate_tree = _sha(candidate["resolved_tree"], field="request candidate tree")
    rollout_id = _safe_id(request["rollout_id"], field="rollout id")
    if (
        request["schema_version"] != 1
        or caller["schema_version"] != 1
        or not isinstance(caller["username"], str)
        or _USERNAME_RE.fullmatch(caller["username"]) is None
        or type(caller["uid"]) is not int
        or caller["uid"] < 0
        or candidate["schema_version"] != 1
        or request["request_id"] != request_id
        or request["command"] != "start"
        or request["status"] != "pending"
        or candidate["remote_url"] != APPROVED_REMOTE_URL
        or candidate["target_ref"] != APPROVED_TARGET_REF
        or candidate["image_tag"] != f"staging-{candidate_sha[:7]}"
    ):
        raise PromotionError("rollout request is not an exact merged-dev staging request")
    _utc(request["requested_at"], field="request requested_at")
    _utc(candidate["fetched_at"], field="candidate fetched_at")
    for field in (
        "runner_config_sha256",
        "preflight_attestation_sha256",
        "preflight_registry_sha256",
        "preflight_coverage_sha256",
    ):
        _sha256(request[field], field=field)
    return rollout_id, candidate_sha, candidate_tree


_ENVELOPE_KEYS = {
    "schema_version",
    "request_id",
    "rollout_id",
    "initiating_operator",
    "initiating_uid",
    "attempt_number",
    "attempt_operator",
    "attempt_uid",
    "remote_url",
    "target_ref",
    "resolved_sha",
    "image_tag",
    "fetched_at",
    "backup_manifest_path",
    "backup_manifest_sha256",
    "runner_config_sha256",
    "preflight_attestation_sha256",
    "preflight_registry_sha256",
    "preflight_coverage_sha256",
    "cluster_name",
    "namespace",
    "environment",
    "cp_url",
    "cluster_config_path",
    "rollout_root",
    "admin_token_source",
    "worker_token_source",
    "service_token_source",
    "expect_admin_token_fingerprint",
    "smoke_on_behalf_username",
    "smoke_on_behalf_team_id",
    "scope",
    "gb10_prep_concurrency",
    "resume",
    "resolved_tree",
}


def _validate_envelope(
    envelope: Mapping[str, object],
    *,
    request: Mapping[str, object],
    request_id: str,
    attempt_number: int,
    rollout_id: str,
    candidate_sha: str,
    candidate_tree: str,
    rollout_root: Path,
    candidate_root: Path,
) -> None:
    _exact_keys(envelope, _ENVELOPE_KEYS, label="driver envelope")
    candidate = request["candidate"]
    caller = request["caller"]
    assert isinstance(candidate, Mapping)
    assert isinstance(caller, Mapping)
    bindings = {
        "request_id": request_id,
        "rollout_id": rollout_id,
        "initiating_operator": caller["username"],
        "initiating_uid": caller["uid"],
        "remote_url": candidate["remote_url"],
        "target_ref": candidate["target_ref"],
        "resolved_sha": candidate_sha,
        "image_tag": candidate["image_tag"],
        "fetched_at": candidate["fetched_at"],
        "resolved_tree": candidate_tree,
        "runner_config_sha256": request["runner_config_sha256"],
        "preflight_attestation_sha256": request["preflight_attestation_sha256"],
        "preflight_registry_sha256": request["preflight_registry_sha256"],
        "preflight_coverage_sha256": request["preflight_coverage_sha256"],
    }
    if any(envelope.get(key) != value for key, value in bindings.items()):
        raise PromotionError("driver envelope does not match its immutable request")
    if (
        envelope["schema_version"] != 1
        or not isinstance(envelope["attempt_operator"], str)
        or _USERNAME_RE.fullmatch(envelope["attempt_operator"]) is None
        or type(envelope["attempt_uid"]) is not int
        or envelope["attempt_uid"] < 0
        or envelope["attempt_number"] != attempt_number
        or envelope["resume"] is not (attempt_number > 1)
        or envelope["environment"] != "staging"
        or envelope["cluster_name"] != "loom-staging"
        or envelope["namespace"] != "loom-staging"
        or envelope["cp_url"] != "http://127.0.0.1:18081"
        or envelope["scope"] != "current-gb10"
        or envelope["rollout_root"] != str(rollout_root)
        or envelope["cluster_config_path"]
        != str(
            candidate_root / candidate_sha / "repo/deploy/environments/staging.cluster.toml",
        )
    ):
        raise PromotionError("driver envelope is not the fixed staging authority")
    _sha256(envelope["backup_manifest_sha256"], field="backup manifest SHA-256")


def _attempt_number(request_directory: Path, *, owner_uids: frozenset[int]) -> int:
    attempts = request_directory / "attempts"
    _validate_directory_chain(attempts, owner_uids=owner_uids)
    values: list[int] = []
    try:
        entries = list(os.scandir(attempts))
    except OSError as exc:
        raise PromotionError("request attempts are unavailable") from exc
    for entry in entries:
        if (
            not entry.name.isdigit()
            or int(entry.name) < 1
            or not entry.is_dir(follow_symlinks=False)
        ):
            raise PromotionError("request attempts contain an unsafe entry")
        values.append(int(entry.name))
    if not values:
        raise PromotionError("request has no broker attempt")
    return max(values)


def _validate_state_and_results(
    state: Mapping[str, object],
    *,
    rollout_directory: Path,
    rollout_id: str,
    request_id: str,
    envelope: Mapping[str, object],
    owner_uids: frozenset[int],
    snapshots: list[Snapshot],
) -> tuple[str, Snapshot]:
    _exact_keys(
        state,
        {
            "version",
            "rollout_id",
            "status",
            "current_step",
            "driver",
            "request_id",
            "initiating_operator",
            "initiating_uid",
            "attempt_number",
            "attempt_operator",
            "attempt_uid",
            "steps",
        },
        label="rollout state",
    )
    steps = state["steps"]
    if (
        state["version"] != 2
        or state["rollout_id"] != rollout_id
        or state["request_id"] != request_id
        or state["status"] != "done"
        or state["current_step"] is not None
        or state["driver"] is not None
        or state["attempt_number"] != envelope["attempt_number"]
        or state["attempt_operator"] != envelope["attempt_operator"]
        or state["attempt_uid"] != envelope["attempt_uid"]
        or state["initiating_operator"] != envelope["initiating_operator"]
        or state["initiating_uid"] != envelope["initiating_uid"]
        or not isinstance(steps, list)
        or len(steps) != len(_STEPS)
    ):
        raise PromotionError("rollout state is not a completed broker attempt")

    browser_result: Mapping[str, object] | None = None
    browser_snapshot: Snapshot | None = None
    browser_finished: str | None = None
    previous_finished: datetime | None = None
    for raw_record, (number, name) in zip(steps, _STEPS, strict=True):
        if not isinstance(raw_record, Mapping):
            raise PromotionError("rollout step record is invalid")
        _exact_keys(
            raw_record,
            {"number", "name", "state", "inputs_hash", "started_at", "finished_at", "error"},
            label=f"step {number} state",
        )
        if (
            raw_record["number"] != number
            or raw_record["name"] != name
            or raw_record["state"] != "done"
            or raw_record["error"] is not None
        ):
            raise PromotionError(f"rollout step {number} is not complete")
        inputs_hash = _sha256(raw_record["inputs_hash"], field=f"step {number} inputs hash")
        started = _utc(raw_record["started_at"], field=f"step {number} started_at")
        finished = _utc(raw_record["finished_at"], field=f"step {number} finished_at")
        if finished < started or (previous_finished is not None and finished < previous_finished):
            raise PromotionError("rollout step timestamps regress")
        previous_finished = finished
        result_path = rollout_directory / f"{number:02d}-{name}" / "result.json"
        result_snapshot = _snapshot(
            result_path,
            owner_uids=owner_uids,
            label=f"step {number} result",
        )
        snapshots.append(result_snapshot)
        result = _strict_json(result_snapshot.payload, label=f"step {number} result")
        _exact_keys(
            result,
            {
                "number",
                "name",
                "state",
                "inputs_hash",
                "started_at",
                "finished_at",
                "exit_code",
                "error",
                "summary",
                "artifacts",
            },
            label=f"step {number} result",
        )
        if (
            result["number"] != number
            or result["name"] != name
            or result["state"] != "done"
            or result["inputs_hash"] != inputs_hash
            or result["started_at"] != raw_record["started_at"]
            or result["finished_at"] != raw_record["finished_at"]
            or result["exit_code"] != 0
            or result["error"] is not None
            or not isinstance(result["artifacts"], Mapping)
        ):
            raise PromotionError(f"step {number} result is not canonical success evidence")
        if number == 16:
            browser_result = result
            browser_finished = str(raw_record["finished_at"])

    if browser_result is None or browser_finished is None:
        raise PromotionError("candidate-bound browser regression evidence is missing")
    artifacts = browser_result["artifacts"]
    assert isinstance(artifacts, Mapping)
    browser_path = (
        rollout_directory
        / "16-staging-admin-browser-acceptance"
        / "browser-output"
        / "staging-admin-browser-acceptance.json"
    )
    if set(artifacts) != {
        "browser_report",
        "browser_report_sha256",
        "request_envelope_sha256",
    } or artifacts["browser_report"] != str(browser_path):
        raise PromotionError("browser regression artifact path is not fixed")
    browser_snapshot = _snapshot(
        browser_path,
        owner_uids=owner_uids,
        label="browser regression report",
    )
    snapshots.append(browser_snapshot)
    if artifacts["browser_report_sha256"] != browser_snapshot.sha256:
        raise PromotionError("browser regression report digest does not match its result")
    return browser_finished, browser_snapshot


def _validate_browser_report(
    report: Mapping[str, object],
    *,
    request_id: str,
    attempt_number: int,
    envelope_sha256: str,
    candidate_sha: str,
) -> None:
    top = {
        "schema_version",
        "status",
        "deployment_identity",
        "route",
        "request_id",
        "target",
        "audit_event_id",
        "browser",
        "checks",
        "cleanup",
        "failure_code",
        "rollout_binding",
    }
    _exact_keys(report, top, label="browser regression report")
    deployment = report["deployment_identity"]
    binding = report["rollout_binding"]
    target = report["target"]
    browser = report["browser"]
    checks = report["checks"]
    cleanup = report["cleanup"]
    if not all(
        isinstance(value, Mapping)
        for value in (deployment, binding, target, browser, checks, cleanup)
    ):
        raise PromotionError("browser regression report nested schema is invalid")
    assert isinstance(deployment, Mapping)
    assert isinstance(binding, Mapping)
    assert isinstance(target, Mapping)
    assert isinstance(browser, Mapping)
    assert isinstance(checks, Mapping)
    assert isinstance(cleanup, Mapping)
    expected_binding = {
        "request_id": request_id,
        "attempt_number": attempt_number,
        "request_envelope_sha256": envelope_sha256,
        "resolved_sha": candidate_sha,
    }
    expected_deployment = {
        "expected_deployed_sha": candidate_sha,
        "observed_deployed_sha": candidate_sha,
        "matched": True,
    }
    if (
        report["schema_version"] != 4
        or report["status"] != "pass"
        or report["failure_code"] is not None
        or report["request_id"] != request_id
        or not isinstance(report["route"], str)
        or _ROUTE_RE.fullmatch(report["route"]) is None
        or deployment != expected_deployment
        or binding != expected_binding
        or target.get("username") != "qianyi"
        or not isinstance(target.get("user_id"), str)
        or not str(target["user_id"]).strip()
        or browser.get("name") != "chromium"
        or not isinstance(browser.get("version"), str)
        or not str(browser["version"]).strip()
        or set(checks) != _BROWSER_CHECKS
        or any(value is not True for value in checks.values())
        or cleanup != {"logout_status": 204, "auth_me_after_logout_status": 401}
        or not isinstance(report["audit_event_id"], str)
        or not report["audit_event_id"].strip()
    ):
        raise PromotionError("candidate-bound browser regression did not pass")


def _default_git_runner(argv: Sequence[str], git: Path) -> str:
    environment = {
        "PATH": "/usr/bin:/bin",
        "LANG": "C",
        "LC_ALL": "C",
        "HOME": "/nonexistent",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
    }
    try:
        result = subprocess.run(
            [str(git), *argv],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PromotionError("fixed git object readback failed") from exc
    if result.returncode != 0:
        raise PromotionError("fixed git object readback failed")
    return result.stdout.strip()


def _validate_candidate_repository(
    layout: Layout,
    *,
    candidate_sha: str,
    candidate_tree: str,
    owner_uids: frozenset[int],
    git_runner: GitRunner,
) -> None:
    repository = layout.candidate_root / candidate_sha / "repo"
    _validate_directory_chain(repository, owner_uids=owner_uids)
    _validate_directory_chain(repository / ".git", owner_uids=owner_uids)
    prefix = [
        "-c",
        f"safe.directory={repository}",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "protocol.file.allow=never",
        "-C",
        str(repository),
    ]
    head = git_runner([*prefix, "rev-parse", "--verify", "HEAD"], layout.git)
    commit = git_runner(
        [*prefix, "rev-parse", "--verify", f"{candidate_sha}^{{commit}}"],
        layout.git,
    )
    tree = git_runner(
        [*prefix, "rev-parse", "--verify", f"{candidate_sha}^{{tree}}"],
        layout.git,
    )
    remote = git_runner([*prefix, "config", "--get", "remote.origin.url"], layout.git)
    dirty = git_runner([*prefix, "status", "--porcelain=v1", "--untracked-files=no"], layout.git)
    if (
        head != candidate_sha
        or commit != candidate_sha
        or tree != candidate_tree
        or remote != APPROVED_REMOTE_URL
        or dirty
    ):
        raise PromotionError("installed candidate git object/tree readback does not match")


def validate_rollout(
    request_id: str,
    *,
    layout: Layout,
    service_uid: int,
    git_runner: GitRunner = _default_git_runner,
) -> ValidatedRollout:
    if _REQUEST_RE.fullmatch(request_id) is None:
        raise PromotionError("request id must match the fixed broker request identity")
    owner_uids = frozenset({0, service_uid})
    request_directory = layout.state_root / "requests" / request_id
    request_snapshot = _snapshot(
        request_directory / "request.json",
        owner_uids=owner_uids,
        label="immutable rollout request",
    )
    request = _strict_json(request_snapshot.payload, label="immutable rollout request")
    rollout_id, candidate_sha, candidate_tree = _validate_request(
        request,
        request_id=request_id,
    )
    attempt_number = _attempt_number(request_directory, owner_uids=owner_uids)
    envelope_snapshot = _snapshot(
        request_directory / "attempts" / str(attempt_number) / "envelope.json",
        owner_uids=owner_uids,
        label="immutable driver envelope",
    )
    envelope = _strict_json(envelope_snapshot.payload, label="immutable driver envelope")
    _validate_envelope(
        envelope,
        request=request,
        request_id=request_id,
        attempt_number=attempt_number,
        rollout_id=rollout_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        rollout_root=layout.rollout_root,
        candidate_root=layout.candidate_root,
    )
    rollout_directory = layout.rollout_root / "rollouts" / rollout_id
    state_snapshot = _snapshot(
        rollout_directory / "state.json",
        owner_uids=owner_uids,
        label="completed rollout state",
    )
    state = _strict_json(state_snapshot.payload, label="completed rollout state")
    snapshots = [request_snapshot, envelope_snapshot, state_snapshot]
    observed_at, browser_snapshot = _validate_state_and_results(
        state,
        rollout_directory=rollout_directory,
        rollout_id=rollout_id,
        request_id=request_id,
        envelope=envelope,
        owner_uids=owner_uids,
        snapshots=snapshots,
    )
    browser = _strict_json(browser_snapshot.payload, label="browser regression report")
    _validate_browser_report(
        browser,
        request_id=request_id,
        attempt_number=attempt_number,
        envelope_sha256=envelope_snapshot.sha256,
        candidate_sha=candidate_sha,
    )
    artifacts = (
        _strict_json(
            snapshot.payload,
            label="browser step result",
        )["artifacts"]
        for snapshot in snapshots
        if snapshot.path.name == "result.json"
        and snapshot.path.parent.name == "16-staging-admin-browser-acceptance"
    )
    browser_artifacts = next(artifacts)
    assert isinstance(browser_artifacts, Mapping)
    if browser_artifacts["request_envelope_sha256"] != envelope_snapshot.sha256:
        raise PromotionError("browser regression is bound to a foreign request envelope")
    _validate_candidate_repository(
        layout,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        owner_uids=owner_uids,
        git_runner=git_runner,
    )
    for snapshot in snapshots:
        _assert_unchanged(snapshot)
    return ValidatedRollout(
        request_id=request_id,
        attempt_number=attempt_number,
        rollout_id=rollout_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        observed_at=observed_at,
        source_snapshots=tuple(snapshots),
    )


def _ensure_directory(path: Path, *, owner_uid: int, mode: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise PromotionError(f"installed directory is not safe: {path}")
    os.chmod(path, mode)
    if os.geteuid() == 0:
        os.chown(path, owner_uid, 0)
    metadata = path.lstat()
    if metadata.st_uid != owner_uid or stat.S_IMODE(metadata.st_mode) != mode:
        raise PromotionError(f"installed directory authority is invalid: {path}")


def _atomic_write(path: Path, payload: bytes, *, owner_uid: int, mode: int = 0o600) -> None:
    temporary = path.parent / f".{path.name}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, mode)
    try:
        os.fchmod(descriptor, mode)
        if os.geteuid() == 0:
            os.fchown(descriptor, owner_uid, 0)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise PromotionError("atomic authority write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    except Exception:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _write_exclusive(path: Path, payload: bytes, *, owner_uid: int) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        if os.geteuid() == 0:
            os.fchown(descriptor, owner_uid, 0)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise PromotionError("immutable receipt write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _authority_json(path: Path, *, owner_uid: int, label: str) -> dict[str, Any]:
    snapshot = _snapshot(
        path,
        owner_uids=frozenset({0, owner_uid}),
        label=label,
    )
    value = _strict_json(snapshot.payload, label=label)
    if snapshot.payload != _canonical_bytes(value):
        raise PromotionError(f"{label} is not canonical")
    return value


def _lexists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PromotionError(f"cannot inspect authority path: {path}") from exc
    return True


def _state_document(
    *,
    sequence: int,
    validated: ValidatedRollout,
    promotion_sha256: str,
    audit_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "loom.staging-rollout.acceptance-state",
        "sequence": sequence,
        "last_request_id": validated.request_id,
        "last_rollout_id": validated.rollout_id,
        "last_candidate_sha": validated.candidate_sha,
        "last_candidate_tree": validated.candidate_tree,
        "last_observed_at": validated.observed_at,
        "last_promotion_sha256": promotion_sha256,
        "last_audit_sha256": audit_sha256,
    }


def _load_state(layout: Layout, *, owner_uid: int) -> dict[str, Any] | None:
    if not _lexists(layout.state):
        return None
    value = _authority_json(layout.state, owner_uid=owner_uid, label="promotion high-water state")
    _exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "sequence",
            "last_request_id",
            "last_rollout_id",
            "last_candidate_sha",
            "last_candidate_tree",
            "last_observed_at",
            "last_promotion_sha256",
            "last_audit_sha256",
        },
        label="promotion high-water state",
    )
    if (
        value["schema_version"] != 1
        or value["kind"] != "loom.staging-rollout.acceptance-state"
        or type(value["sequence"]) is not int
        or value["sequence"] < 1
    ):
        raise PromotionError("promotion high-water state is invalid")
    _sha(value["last_candidate_sha"], field="high-water candidate SHA")
    _sha(value["last_candidate_tree"], field="high-water candidate tree")
    _sha256(value["last_promotion_sha256"], field="high-water promotion digest")
    _sha256(value["last_audit_sha256"], field="high-water audit digest")
    _utc(value["last_observed_at"], field="high-water observed_at")
    return value


def _validate_promotion(value: Mapping[str, object]) -> None:
    _exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "source_host",
            "rollout_id",
            "candidate_sha",
            "candidate_tree",
            "result",
            "observed_at",
        },
        label="promotion authority receipt",
    )
    if (
        value["schema_version"] != 1
        or value["kind"] != "loom.staging-rollout.acceptance"
        or value["source_host"] != SOURCE_HOST
        or value["result"] != "pass"
    ):
        raise PromotionError("promotion authority receipt is invalid")
    _safe_id(value["rollout_id"], field="promotion rollout id")
    _sha(value["candidate_sha"], field="promotion candidate SHA")
    _sha(value["candidate_tree"], field="promotion candidate tree")
    _utc(value["observed_at"], field="promotion observed_at")


def _receipt_count(layout: Layout) -> int:
    try:
        entries = list(os.scandir(layout.receipts))
    except OSError as exc:
        raise PromotionError("promotion receipt journal is unavailable") from exc
    count = 0
    for entry in entries:
        if not entry.is_file(follow_symlinks=False) or not entry.name.endswith(".json"):
            raise PromotionError("promotion receipt journal contains an unsafe entry")
        count += 1
    return count


def _recover(layout: Layout, *, owner_uid: int) -> None:
    if not _lexists(layout.pending):
        return
    pending = _authority_json(layout.pending, owner_uid=owner_uid, label="pending promotion")
    _exact_keys(
        pending,
        {
            "schema_version",
            "kind",
            "sequence",
            "audit_filename",
            "audit",
            "audit_sha256",
            "promotion",
            "promotion_sha256",
            "previous_promotion_sha256",
            "previous_state_sha256",
            "state",
        },
        label="pending promotion",
    )
    promotion = pending["promotion"]
    pending_audit = pending["audit"]
    state = pending["state"]
    if (
        not isinstance(promotion, Mapping)
        or not isinstance(pending_audit, Mapping)
        or not isinstance(state, Mapping)
    ):
        raise PromotionError("pending promotion payload is invalid")
    _validate_promotion(promotion)
    if (
        pending["schema_version"] != 1
        or pending["kind"] != "loom.staging-rollout.acceptance-pending"
        or pending["promotion_sha256"] != _digest(promotion)
        or pending["audit_sha256"]
        != _digest({key: value for key, value in pending_audit.items() if key != "audit_sha256"})
        or pending_audit.get("audit_sha256") != pending["audit_sha256"]
        or (
            pending["previous_promotion_sha256"] is not None
            and _SHA256_RE.fullmatch(str(pending["previous_promotion_sha256"])) is None
        )
        or (
            pending["previous_state_sha256"] is not None
            and _SHA256_RE.fullmatch(str(pending["previous_state_sha256"])) is None
        )
        or state.get("last_promotion_sha256") != pending["promotion_sha256"]
        or state.get("last_audit_sha256") != pending["audit_sha256"]
    ):
        raise PromotionError("pending promotion transaction is invalid")
    audit_path = layout.receipts / str(pending["audit_filename"])
    if not _lexists(audit_path):
        _write_exclusive(
            audit_path,
            _canonical_bytes(pending_audit),
            owner_uid=owner_uid,
        )
    audit = _authority_json(audit_path, owner_uid=owner_uid, label="pending audit receipt")
    if audit.get("audit_sha256") != pending["audit_sha256"]:
        raise PromotionError("pending promotion audit digest is invalid")
    unsigned = {key: value for key, value in audit.items() if key != "audit_sha256"}
    if _digest(unsigned) != pending["audit_sha256"]:
        raise PromotionError("pending promotion audit receipt is corrupt")
    _recover_cas(
        layout.promotion,
        promotion,
        previous_sha256=pending["previous_promotion_sha256"],
        owner_uid=owner_uid,
        label="promotion authority receipt",
    )
    _recover_cas(
        layout.state,
        state,
        previous_sha256=pending["previous_state_sha256"],
        owner_uid=owner_uid,
        label="promotion high-water state",
    )
    layout.pending.unlink()
    directory = os.open(layout.acceptance_root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _recover_cas(
    path: Path,
    target: Mapping[str, object],
    *,
    previous_sha256: object,
    owner_uid: int,
    label: str,
) -> None:
    target_sha256 = _digest(target)
    if _lexists(path):
        current = _authority_json(path, owner_uid=owner_uid, label=label)
        current_sha256 = _digest(current)
        if current_sha256 == target_sha256:
            return
        if previous_sha256 is None or current_sha256 != previous_sha256:
            raise PromotionError(f"{label} violates pending compare-and-swap")
    elif previous_sha256 is not None:
        raise PromotionError(f"{label} disappeared during pending compare-and-swap")
    _atomic_write(path, _canonical_bytes(target), owner_uid=owner_uid)


def _check_current(layout: Layout, *, owner_uid: int) -> dict[str, Any] | None:
    state = _load_state(layout, owner_uid=owner_uid)
    if state is None:
        if _lexists(layout.promotion) or any(layout.receipts.iterdir()):
            raise PromotionError("promotion authority has artifacts without high-water state")
        return None
    promotion = _authority_json(
        layout.promotion,
        owner_uid=owner_uid,
        label="promotion authority receipt",
    )
    _validate_promotion(promotion)
    if (
        _digest(promotion) != state["last_promotion_sha256"]
        or promotion["rollout_id"] != state["last_rollout_id"]
        or promotion["candidate_sha"] != state["last_candidate_sha"]
        or promotion["candidate_tree"] != state["last_candidate_tree"]
        or promotion["observed_at"] != state["last_observed_at"]
    ):
        raise PromotionError("promotion authority receipt regressed from its high-water state")
    audit_name = (
        f"{state['sequence']:020d}-{state['last_candidate_sha']}-{state['last_audit_sha256']}.json"
    )
    audit = _authority_json(
        layout.receipts / audit_name,
        owner_uid=owner_uid,
        label="promotion audit receipt",
    )
    unsigned = {key: value for key, value in audit.items() if key != "audit_sha256"}
    if audit.get("audit_sha256") != state["last_audit_sha256"] or _digest(unsigned) != audit.get(
        "audit_sha256",
    ):
        raise PromotionError("promotion audit receipt is corrupt")
    return state


def _open_lock(layout: Layout, *, owner_uid: int) -> int:
    descriptor = os.open(
        layout.lock,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    os.fchmod(descriptor, 0o600)
    if os.geteuid() == 0:
        os.fchown(descriptor, owner_uid, 0)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def ensure_runtime(layout: Layout, *, owner_uid: int) -> None:
    _ensure_directory(layout.acceptance_root, owner_uid=owner_uid, mode=0o700)
    _ensure_directory(layout.receipts, owner_uid=owner_uid, mode=0o700)


def produce(
    request_id: str,
    *,
    layout: Layout = DEFAULT_LAYOUT,
    service_uid: int,
    owner_uid: int = 0,
    hostname: str | None = None,
    now: str | None = None,
    git_runner: GitRunner = _default_git_runner,
    failpoint: Failpoint = lambda _name: None,
) -> dict[str, object]:
    source_host = (hostname or socket.gethostname()).split(".", 1)[0]
    if source_host != SOURCE_HOST:
        raise PromotionError("promotion producer is not running on the fixed source host")
    ensure_runtime(layout, owner_uid=owner_uid)
    lock = _open_lock(layout, owner_uid=owner_uid)
    try:
        _recover(layout, owner_uid=owner_uid)
        current = _check_current(layout, owner_uid=owner_uid)
        validated = validate_rollout(
            request_id,
            layout=layout,
            service_uid=service_uid,
            git_runner=git_runner,
        )
        for snapshot in validated.source_snapshots:
            _assert_unchanged(snapshot)
        if current is not None and current["last_request_id"] == validated.request_id:
            if (
                current["last_rollout_id"] == validated.rollout_id
                and current["last_candidate_sha"] == validated.candidate_sha
                and current["last_candidate_tree"] == validated.candidate_tree
                and current["last_observed_at"] == validated.observed_at
            ):
                existing_promotion = _authority_json(
                    layout.promotion,
                    owner_uid=owner_uid,
                    label="promotion authority receipt",
                )
                _validate_promotion(existing_promotion)
                return existing_promotion
            raise PromotionError("recorded request identity cannot be replayed with new evidence")
        if current is not None and _utc(
            validated.observed_at,
            field="candidate observed_at",
        ) <= _utc(current["last_observed_at"], field="high-water observed_at"):
            raise PromotionError("promotion candidate is older than the durable high-water mark")
        if _receipt_count(layout) >= MAX_RECEIPTS:
            raise PromotionError("promotion receipt journal reached its bounded limit")
        sequence = 1 if current is None else int(current["sequence"]) + 1
        promotion: dict[str, object] = {
            "schema_version": 1,
            "kind": "loom.staging-rollout.acceptance",
            "source_host": SOURCE_HOST,
            "rollout_id": validated.rollout_id,
            "candidate_sha": validated.candidate_sha,
            "candidate_tree": validated.candidate_tree,
            "result": "pass",
            "observed_at": validated.observed_at,
        }
        source_digests = {
            str(snapshot.path): snapshot.sha256 for snapshot in validated.source_snapshots
        }
        audit_unsigned: dict[str, object] = {
            "schema_version": 1,
            "kind": "loom.staging-rollout.acceptance-audit",
            "sequence": sequence,
            "request_id": validated.request_id,
            "attempt_number": validated.attempt_number,
            "rollout_id": validated.rollout_id,
            "candidate_sha": validated.candidate_sha,
            "candidate_tree": validated.candidate_tree,
            "source_host": SOURCE_HOST,
            "observed_at": validated.observed_at,
            "recorded_at": now or _now_utc(),
            "source_sha256": source_digests,
            "promotion_sha256": _digest(promotion),
            "previous_audit_sha256": (None if current is None else current["last_audit_sha256"]),
        }
        if _utc(audit_unsigned["recorded_at"], field="recorded_at") < _utc(
            validated.observed_at,
            field="observed_at",
        ):
            raise PromotionError("authority clock predates the trusted regression evidence")
        audit_sha256 = _digest(audit_unsigned)
        audit = {**audit_unsigned, "audit_sha256": audit_sha256}
        audit_filename = f"{sequence:020d}-{validated.candidate_sha}-{audit_sha256}.json"
        state = _state_document(
            sequence=sequence,
            validated=validated,
            promotion_sha256=_digest(promotion),
            audit_sha256=audit_sha256,
        )
        pending = {
            "schema_version": 1,
            "kind": "loom.staging-rollout.acceptance-pending",
            "sequence": sequence,
            "audit_filename": audit_filename,
            "audit": audit,
            "audit_sha256": audit_sha256,
            "promotion": promotion,
            "promotion_sha256": _digest(promotion),
            "previous_promotion_sha256": (
                None if current is None else current["last_promotion_sha256"]
            ),
            "previous_state_sha256": None if current is None else _digest(current),
            "state": state,
        }
        _atomic_write(layout.pending, _canonical_bytes(pending), owner_uid=owner_uid)
        failpoint("after-pending")
        _write_exclusive(
            layout.receipts / audit_filename,
            _canonical_bytes(audit),
            owner_uid=owner_uid,
        )
        failpoint("after-audit")
        for snapshot in validated.source_snapshots:
            _assert_unchanged(snapshot)
        _atomic_write(layout.promotion, _canonical_bytes(promotion), owner_uid=owner_uid)
        failpoint("after-promotion")
        _atomic_write(layout.state, _canonical_bytes(state), owner_uid=owner_uid)
        failpoint("after-state")
        layout.pending.unlink()
        directory = os.open(layout.acceptance_root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return promotion
    finally:
        os.close(lock)


def install(
    *,
    layout: Layout = DEFAULT_LAYOUT,
    owner_uid: int = 0,
    program_payload: bytes | None = None,
) -> None:
    payload = program_payload if program_payload is not None else Path(__file__).read_bytes()
    if not payload.startswith(b"#!/usr/bin/env python3\n"):
        raise PromotionError("promotion authority program payload is invalid")
    _ensure_directory(layout.installed_program.parent, owner_uid=owner_uid, mode=0o755)
    _ensure_directory(layout.installed_sudoers.parent, owner_uid=owner_uid, mode=0o755)
    ensure_runtime(layout, owner_uid=owner_uid)
    _atomic_write(layout.installed_program, payload, owner_uid=owner_uid, mode=0o755)
    _atomic_write(
        layout.installed_sudoers,
        _SUDOERS.encode(),
        owner_uid=owner_uid,
        mode=0o440,
    )


def _check_asset(path: Path, *, owner_uid: int, mode: int, payload: bytes | None = None) -> None:
    snapshot = _snapshot(
        path,
        owner_uids=frozenset({0, owner_uid}),
        label=f"installed asset {path}",
        modes=frozenset({mode}),
    )
    if payload is not None and snapshot.payload != payload:
        raise PromotionError(f"installed asset content drifted: {path}")


def check(
    *,
    layout: Layout = DEFAULT_LAYOUT,
    owner_uid: int = 0,
) -> dict[str, object] | None:
    _check_asset(layout.installed_program, owner_uid=owner_uid, mode=0o755)
    _check_asset(
        layout.installed_sudoers,
        owner_uid=owner_uid,
        mode=0o440,
        payload=_SUDOERS.encode(),
    )
    _validate_directory_chain(
        layout.acceptance_root,
        owner_uids=frozenset({0, owner_uid}),
    )
    lock = _open_lock(layout, owner_uid=owner_uid)
    try:
        _recover(layout, owner_uid=owner_uid)
        return _check_current(layout, owner_uid=owner_uid)
    finally:
        os.close(lock)


def _require_root() -> None:
    if os.geteuid() != 0:
        raise PromotionError("promotion authority commands require root")


def _service_uid() -> int:
    try:
        return pwd.getpwnam("loom-rollout").pw_uid
    except KeyError as exc:
        raise PromotionError("loom-rollout service identity is unavailable") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="loom-developer-sandbox-staging-promotion",
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    install_parser = subparsers.add_parser("install", allow_abbrev=False)
    install_parser.add_argument("--execute", action="store_true", required=True)
    subparsers.add_parser("check", allow_abbrev=False)
    produce_parser = subparsers.add_parser("produce", allow_abbrev=False)
    produce_parser.add_argument("--request-id", required=True)
    produce_parser.add_argument("--execute", action="store_true", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _require_root()
        if args.command == "install":
            install()
            result: object = {"status": "installed"}
        elif args.command == "check":
            result = {"status": "ok", "state": check()}
        elif args.command == "produce":
            result = produce(
                args.request_id,
                service_uid=_service_uid(),
            )
        else:  # pragma: no cover - argparse owns the closed command set
            raise PromotionError("unsupported command")
    except PromotionError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
