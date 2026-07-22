"""Prepare or verify the immutable shared GB10 candidate checkout.

This is an install-time artifact publication boundary.  It runs as the fixed
``loom-rollout`` service account, writes only the candidate-addressed shared
checkout, and never installs or activates a GB10 unit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.steps.s10_env_state import (
    ExternalSlurmPrereqMaterializationError,
    materialize_external_runner_repo,
    verify_external_runner_repo,
)

from .candidate import (
    CandidateBindingError,
    bind_configured_candidate,
    verify_bound_candidate,
)
from .config import ConfigError, OperatorConfig, candidate_sha_from_runner_repo
from .envelope import fixed_operator_config_path
from .installed_preflight_commands import InstalledPreflightCommands
from .model import CandidateBinding
from .policy import PolicyError, sanitized_child_environment

_SHARED_REPOSITORY_ROOT = Path("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")

Publication = Callable[..., Mapping[str, object]]


class CandidateSourcePublicationError(RuntimeError):
    """A bounded failure safe for installer output."""


def _service_uid(config: OperatorConfig) -> int:
    try:
        account = pwd.getpwnam(config.service_user)
    except (KeyError, OSError) as exc:
        raise CandidateSourcePublicationError(
            "candidate source service identity is unavailable"
        ) from exc
    if account.pw_uid <= 0 or os.geteuid() != account.pw_uid:
        raise CandidateSourcePublicationError("candidate source service identity is invalid")
    return account.pw_uid


def _repo_path(candidate: CandidateBinding) -> Path:
    path = _SHARED_REPOSITORY_ROOT / f"loom-remote-worker-{candidate.image_tag}"
    if path.parent != _SHARED_REPOSITORY_ROOT:
        raise CandidateSourcePublicationError("candidate source destination is invalid")
    return path


def publish_installed_candidate_source(
    *,
    config: OperatorConfig,
    candidate: CandidateBinding,
    candidate_tree: str | None = None,
    operation: str,
    materialize: Publication = materialize_external_runner_repo,
    verify: Publication = verify_external_runner_repo,
) -> dict[str, object]:
    """Prepare or check one candidate-addressed immutable shared checkout."""
    if operation not in {"prepare", "check"}:
        raise CandidateSourcePublicationError("candidate source operation is invalid")
    service_uid = _service_uid(config)
    try:
        configured_sha = candidate_sha_from_runner_repo(config.runner_repo)
    except ConfigError as exc:
        raise CandidateSourcePublicationError("candidate source runtime path is invalid") from exc
    resolved_tree = candidate.resolved_tree or candidate_tree
    if (
        config.environment != "staging"
        or config.namespace != "loom-staging"
        or candidate.source_mode != config.source_mode
        or candidate.resolved_sha != configured_sha
        or (
            candidate.resolved_tree is not None
            and candidate_tree not in {None, candidate.resolved_tree}
        )
        or (
            config.source_mode == "sealed-cumulative"
            and (
                config.source_commit_sha != candidate.resolved_sha
                or config.source_tree_sha != candidate.resolved_tree
                or config.source_base_sha != candidate.approved_base_sha
            )
        )
        or resolved_tree is None
        or len(resolved_tree) != 40
        or any(character not in "0123456789abcdef" for character in resolved_tree)
    ):
        raise CandidateSourcePublicationError("candidate source binding is invalid")
    destination = _repo_path(candidate)
    try:
        if operation == "prepare":
            raw_record = materialize(
                repo_dir=destination,
                source_repo=config.runner_repo,
                resolved_sha=candidate.resolved_sha,
                expected_ref=candidate.image_tag,
            )
        else:
            raw_record = verify(
                repo_dir=destination,
                resolved_sha=candidate.resolved_sha,
                expected_ref=candidate.image_tag,
            )
        record = dict(raw_record)
    except (ExternalSlurmPrereqMaterializationError, OSError, TypeError, ValueError) as exc:
        raise CandidateSourcePublicationError("candidate source publication failed safely") from exc
    action = record.get("repo_action")
    group_id = record.get("repo_group_id")
    allowed_actions = {"created", "matched"} if operation == "prepare" else {"matched"}
    if (
        set(record)
        != {
            "repo_action",
            "repo_dir",
            "repo_group_id",
            "repo_head",
            "repo_mode",
            "repo_status",
        }
        or action not in allowed_actions
        or record.get("repo_dir") != str(destination)
        or record.get("repo_head") != candidate.resolved_sha
        or record.get("repo_status") != "clean"
        or record.get("repo_mode") != "0750"
        or type(group_id) is not int
        or group_id <= 0
    ):
        raise CandidateSourcePublicationError("candidate source publication evidence is invalid")
    evidence = {
        "action": action,
        "candidate_sha": candidate.resolved_sha,
        "candidate_tree": resolved_tree,
        "image_tag": candidate.image_tag,
        "service_uid": service_uid,
        "status": "clean",
    }
    evidence["evidence_sha256"] = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="candidate-source-publication", allow_abbrev=False)
    parser.add_argument("operation", choices=("prepare", "check"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
        config = OperatorConfig.load(fixed_operator_config_path())
        service_uid = _service_uid(config)
        commands = InstalledPreflightCommands(
            config=config,
            child_environment=sanitized_child_environment(config, service_uid=service_uid),
        )
        candidate = bind_configured_candidate(
            config,
            run=commands.git,
            now=lambda: datetime.now(UTC),
        )
        candidate_tree = candidate.resolved_tree
        if candidate_tree is None:
            candidate_tree = verify_bound_candidate(
                config,
                candidate,
                run=commands.git,
            ).resolved_tree
        document = publish_installed_candidate_source(
            config=config,
            candidate=candidate,
            candidate_tree=candidate_tree,
            operation=args.operation,
        )
        sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except (
        CandidateBindingError,
        CandidateSourcePublicationError,
        ConfigError,
        PolicyError,
    ):
        sys.stderr.write('{"error":"candidate-source-publication-failed-safely"}\n')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CandidateSourcePublicationError",
    "main",
    "publish_installed_candidate_source",
]
