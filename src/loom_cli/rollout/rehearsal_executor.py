"""Concrete fixed-command executor for isolated exact-candidate rehearsal."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan
from loom_cli.rollout.rehearsal_journal_backend import RehearsalStepOutcome
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS

REHEARSAL_KUBECONFIG = Path("/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig")
_MAX_OUTPUT_BYTES = 1024 * 1024


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str], bytes | None, int], CommandResult]


def _default_run(
    argv: Sequence[str],
    payload: bytes | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    service_uid = os.geteuid()
    return subprocess.run(
        list(argv),
        input=None if payload is None else payload.decode("utf-8"),
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
        env={
            "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{service_uid}/bus",
            "HOME": "/var/lib/loom-staging-rollout",
            "KUBECONFIG": str(REHEARSAL_KUBECONFIG),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "LOGNAME": "loom-rollout",
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "USER": "loom-rollout",
            "XDG_RUNTIME_DIR": f"/run/user/{service_uid}",
        },
    )


@dataclass(frozen=True, slots=True)
class IsolatedRehearsalExecutor:
    """Run only allowlisted operations against rehearsal-scoped authority."""

    run: CommandRunner = _default_run
    kubeconfig: Path = REHEARSAL_KUBECONFIG

    def __post_init__(self) -> None:
        if not self.kubeconfig.is_absolute() or ".." in self.kubeconfig.parts:
            raise ValueError("rehearsal executor kubeconfig authority is invalid")

    def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalStepOutcome:
        if check_id not in REHEARSAL_CHECK_IDS:
            raise ValueError("rehearsal executor check identity is invalid")
        plan.resources.require_isolated()
        if check_id == "rehearsal.namespace":
            return self._namespace(plan)
        return RehearsalStepOutcome(
            passed=False,
            details={"status": "blocked"},
            blockers={"executor": "isolated-action-not-implemented"},
        )

    def _namespace(self, plan: RehearsalPlan) -> RehearsalStepOutcome:
        manifest = _namespace_manifest(plan)
        apply = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "apply",
                "--server-side=true",
                "--field-manager=loom-staging-preflight",
                "--request-timeout=30s",
                "-f",
                "-",
                "-o",
                "json",
            ),
            _json_bytes(manifest),
            timeout=45,
        )
        if apply is None or not _namespace_matches(apply, plan):
            return _blocked("namespace", "apply-failed")
        observed = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "namespace",
                plan.resources.namespace,
                "--request-timeout=15s",
                "-o",
                "json",
            ),
            None,
            timeout=20,
        )
        if observed is None or not _namespace_matches(observed, plan):
            return _blocked("namespace", "readback-drift")
        return RehearsalStepOutcome(
            passed=True,
            details={"namespace": plan.resources.namespace, "status": "ready"},
            blockers={},
        )

    def _command(
        self,
        argv: Sequence[str],
        payload: bytes | None,
        *,
        timeout: int,
    ) -> dict[str, object] | None:
        try:
            result = self.run(argv, payload, timeout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            return None
        if (
            result.returncode != 0
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
        ):
            return None
        try:
            value = json.loads(result.stdout, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, ValueError):
            return None
        return value if isinstance(value, dict) else None


def _namespace_manifest(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "annotations": {
                "loom.openai.dev/candidate-sha": plan.candidate_sha,
                "loom.openai.dev/candidate-tree": plan.candidate_tree,
                "loom.openai.dev/mutation-epoch": str(plan.mutation_epoch),
                "loom.openai.dev/plan-sha256": plan.plan_digest,
            },
            "labels": {
                "loom.openai.dev/authority": "staging-preflight",
                "loom.openai.dev/isolation": plan.resources.namespace.removeprefix(
                    "loom-rehearsal-"
                ),
            },
            "name": plan.resources.namespace,
        },
    }


def _namespace_matches(value: dict[str, object], plan: RehearsalPlan) -> bool:
    metadata = value.get("metadata")
    if value.get("apiVersion") != "v1" or value.get("kind") != "Namespace":
        return False
    if not isinstance(metadata, dict):
        return False
    expected = _namespace_manifest(plan)["metadata"]
    assert isinstance(expected, dict)
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    expected_labels = expected["labels"]
    expected_annotations = expected["annotations"]
    return bool(
        metadata.get("name") == plan.resources.namespace
        and isinstance(labels, dict)
        and isinstance(annotations, dict)
        and isinstance(expected_labels, dict)
        and isinstance(expected_annotations, dict)
        and all(labels.get(key) == item for key, item in expected_labels.items())
        and all(annotations.get(key) == item for key, item in expected_annotations.items())
    )


def _blocked(component: str, reason: str) -> RehearsalStepOutcome:
    return RehearsalStepOutcome(
        passed=False,
        details={"status": "blocked"},
        blockers={component: reason},
    )


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


__all__ = [
    "REHEARSAL_KUBECONFIG",
    "CommandResult",
    "CommandRunner",
    "IsolatedRehearsalExecutor",
]
