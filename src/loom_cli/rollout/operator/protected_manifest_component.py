"""Exact server-side manifest component for protected staging convergence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.credential_authority import read_trusted_file

from .final_gate_plan import FinalGatePlan
from .manifest_apply_contract import (
    MANIFEST_APPLY_CONTRACT_DIGEST,
    server_side_apply_argv,
    server_side_diff_argv,
)
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_DIFF_TIMEOUT_SECONDS = 120.0
_APPLY_TIMEOUT_SECONDS = 300.0
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    f"protected-manifest-component-v2|{MANIFEST_APPLY_CONTRACT_DIGEST}".encode()
).hexdigest()


class ProtectedManifestCommandRunner(Protocol):
    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class KubernetesProtectedManifestComponent:
    """Apply only the attested rendered resources after exact epoch ownership."""

    runner: ProtectedManifestCommandRunner
    environment: Mapping[str, str]
    service_uid: int
    epoch_guard: EpochGuard

    def __post_init__(self) -> None:
        if self.service_uid < 0 or "KUBECONFIG" not in self.environment:
            raise ValueError("protected manifest command authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="staging-manifests",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "baseline_digest": plan.protected_baseline_digest,
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "manifest_sha256": plan.rendered_manifest_sha256,
                    "namespace": plan.namespace,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(plan, ComponentState.DRIFTED, epoch.evidence_digest)
        payload = self._read_manifest(plan)
        status = self.runner.run_status(
            self._diff_argv(plan),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_DIFF_TIMEOUT_SECONDS,
        )
        state = ComponentState.EXACT if status == 0 else ComponentState.READY
        return self._observation(plan, state, epoch.evidence_digest)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected manifest epoch ownership changed before apply")
        payload = self._read_manifest(plan)
        if (
            self.runner.run_status(
                self._diff_argv(plan),
                env=self.environment,
                input_payload=payload,
                timeout_seconds=_DIFF_TIMEOUT_SECONDS,
            )
            != 1
        ):
            raise RuntimeError("protected manifest state changed before apply")
        self.runner.run_checked(
            server_side_apply_argv(plan.namespace),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_APPLY_TIMEOUT_SECONDS,
        )

    def _diff_argv(self, plan: FinalGatePlan) -> tuple[str, ...]:
        return server_side_diff_argv(plan.namespace)

    def _read_manifest(self, plan: FinalGatePlan) -> bytes:
        trusted = read_trusted_file(
            Path(plan.rendered_manifest_path),
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_MANIFEST_BYTES,
            require_nonempty=True,
        )
        if hashlib.sha256(trusted.payload).hexdigest() != plan.rendered_manifest_sha256:
            raise ValueError("protected rendered manifest content drifted")
        return trusted.payload

    def _observation(
        self,
        plan: FinalGatePlan,
        state: ComponentState,
        epoch_evidence_digest: str,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "baseline_digest": plan.protected_baseline_digest,
                    "epoch_evidence_digest": epoch_evidence_digest,
                    "manifest_sha256": plan.rendered_manifest_sha256,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "KubernetesProtectedManifestComponent",
    "ProtectedManifestCommandRunner",
]
