"""Acting reconciler loop for the rollout (#1097 / #1085 phase 4 — increment 5).

Composes the read-only primitives into a converging loop: read desired + live,
diff (`shadow_reconcile`), and — in APPLY mode — apply the desired declarative
objects to close the drift, then re-read and verify convergence. SHADOW mode
writes nothing and is identical to the read-only observer.

This is the write path, and it is **inert by default**: `reconcile_once` defaults
to SHADOW, and no live deploy path invokes APPLY mode. The cutover that routes the
live deploy onto the reconciler is a separate, gated switch — not flipped here.

`apply` and `read_live` are injected, so the core loop is pure and fully testable
without a cluster, and so the real applier can layer on the single-writer lease,
break-glass ownership adoption, and typed-check retry/block (later increments)
without changing this convergence logic. This loop covers the naturally
declarative resources only; imperative components (migration / epoch / supervisors)
converge through the version ledger, not by re-apply.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from loom_cli.rollout.shadow_reconcile import (
    DriftStatus,
    ResourceDrift,
    ShadowDriftReport,
    compute_drift,
)

# Drift states that APPLY mode converges by (re-)applying the desired object.
# ABSENT_FROM_DESIRED is deliberately excluded — the reconciler does not prune;
# deletion is not a re-apply and needs its own explicit, guarded path.
_APPLYABLE = (DriftStatus.ABSENT_FROM_LIVE, DriftStatus.MODIFIED)

ResourceKey = tuple[str, str, str]


class ReconcileMode(StrEnum):
    SHADOW = "shadow"  # observe + report drift, never write
    APPLY = "apply"  # converge declarative drift by applying desired objects


@dataclass(frozen=True)
class ReconcileResult:
    mode: ReconcileMode
    drift: ShadowDriftReport  # pre-apply desired-vs-live
    applied: tuple[ResourceKey, ...] = ()  # (kind, namespace, name) objects applied
    converged: bool = False  # live matches desired (post-apply in APPLY mode)
    residual: tuple[ResourceDrift, ...] = ()  # drift still present after the pass

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "converged": self.converged,
            "applied": [{"kind": k, "namespace": ns, "name": n} for (k, ns, n) in self.applied],
            "residual": [r.to_dict() for r in self.residual],
            "drift": self.drift.to_dict(),
        }


def _key(obj: Mapping[str, object]) -> ResourceKey:
    metadata = obj.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    return (
        str(obj.get("kind", "")),
        str(metadata.get("namespace", "")),
        str(metadata.get("name", "")),
    )


def _unsynced(report: ShadowDriftReport) -> tuple[ResourceDrift, ...]:
    return tuple(r for r in report.resources if r.status is not DriftStatus.IN_SYNC)


def reconcile_once(
    desired: Sequence[Mapping[str, object]],
    *,
    read_live: Callable[[], Sequence[Mapping[str, object]]],
    apply: Callable[[Mapping[str, object]], None],
    environment: str,
    target: str,
    mode: ReconcileMode = ReconcileMode.SHADOW,
) -> ReconcileResult:
    """Run one reconcile pass of `desired` against live.

    SHADOW (default): read + diff + report; **no writes**. APPLY: additionally
    (re-)apply every absent/modified desired object, then re-read and report
    whether live now matches desired (`converged`). Pure given the injected
    `read_live` / `apply`.
    """
    drift = compute_drift(desired, read_live(), environment=environment, target=target)
    if mode is ReconcileMode.SHADOW:
        return ReconcileResult(
            mode=mode, drift=drift, converged=drift.in_sync, residual=_unsynced(drift)
        )

    desired_by_key = {_key(obj): obj for obj in desired}
    applied: list[ResourceKey] = []
    for resource in drift.resources:
        if resource.status not in _APPLYABLE:
            continue
        obj = desired_by_key.get((resource.kind, resource.namespace, resource.name))
        if obj is not None:
            apply(obj)
            applied.append((resource.kind, resource.namespace, resource.name))

    post = compute_drift(desired, read_live(), environment=environment, target=target)
    return ReconcileResult(
        mode=mode,
        drift=drift,
        applied=tuple(applied),
        converged=post.in_sync,
        residual=_unsynced(post),
    )
