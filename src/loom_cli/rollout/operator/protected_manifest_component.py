"""Exact server-side manifest component for protected staging convergence."""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.manifest_readiness import (
    render_checkpoint_guard_field_ownership_payload,
)

from .final_gate_plan import FinalGatePlan
from .manifest_apply_contract import (
    MANIFEST_APPLY_CONTRACT_DIGEST,
    server_side_all_namespaces_apply_argv,
    server_side_all_namespaces_diff_argv,
)
from .protected_application_readiness import application_deployments, deployment_is_ready
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_DIFF_TIMEOUT_SECONDS = 120.0
_APPLY_TIMEOUT_SECONDS = 300.0
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    f"protected-manifest-component-v4-serving-generation|{MANIFEST_APPLY_CONTRACT_DIGEST}".encode()
).hexdigest()


class ProtectedManifestCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

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
    readiness_timeout_seconds: float = 600.0
    readiness_interval_seconds: float = 5.0
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or "KUBECONFIG" not in self.environment
            or not math.isfinite(self.readiness_timeout_seconds)
            or not 0 < self.readiness_timeout_seconds <= 900
            or not math.isfinite(self.readiness_interval_seconds)
            or not 0 < self.readiness_interval_seconds <= 30
            or not callable(self.monotonic)
            or not callable(self.sleep)
        ):
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
        payload = self._read_guarded_manifest(plan)
        status = self.runner.run_status(
            self._diff_argv(plan),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_DIFF_TIMEOUT_SECONDS,
        )
        if status == 1:
            return self._observation(plan, ComponentState.READY, epoch.evidence_digest)
        if status != 0:
            raise RuntimeError("protected manifest diff failed")
        deadline = self.monotonic() + self.readiness_timeout_seconds
        self._wait_for_applications(plan, payload, deadline)
        epoch_after = self.epoch_guard(plan)
        if epoch_after != epoch:
            raise RuntimeError("application readiness epoch changed while waiting")
        remaining = deadline - self.monotonic()
        if remaining <= 0:
            raise RuntimeError("application readiness timed out")
        if (
            self.runner.run_status(
                self._diff_argv(plan),
                env=self.environment,
                input_payload=self._read_guarded_manifest(plan),
                timeout_seconds=min(_DIFF_TIMEOUT_SECONDS, remaining),
            )
            != 0
        ):
            raise RuntimeError("application readiness manifest changed while waiting")
        if self.epoch_guard(plan) != epoch:
            raise RuntimeError("application readiness epoch changed during final diff")
        if self.monotonic() >= deadline:
            raise RuntimeError("application readiness timed out")
        return self._observation(plan, ComponentState.EXACT, epoch.evidence_digest)

    def _wait_for_applications(self, plan: FinalGatePlan, payload: bytes, deadline: float) -> None:
        deployments = application_deployments(payload, plan.namespace)
        while True:
            ready = True
            for deployment in deployments:
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    raise RuntimeError("application readiness timed out")
                ready = (
                    deployment_is_ready(
                        deployment,
                        runner=self.runner,
                        environment=self.environment,
                        timeout_seconds=min(30.0, remaining),
                    )
                    and ready
                )
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise RuntimeError("application readiness timed out")
            if ready:
                return
            self.sleep(min(self.readiness_interval_seconds, remaining))

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected manifest epoch ownership changed before apply")
        payload = self._read_guarded_manifest(plan)
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
            server_side_all_namespaces_apply_argv(),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_APPLY_TIMEOUT_SECONDS,
        )

    def _diff_argv(self, plan: FinalGatePlan) -> tuple[str, ...]:
        del plan
        return server_side_all_namespaces_diff_argv()

    def _read_guarded_manifest(self, plan: FinalGatePlan) -> bytes:
        trusted = read_trusted_file(
            Path(plan.rendered_manifest_path),
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_MANIFEST_BYTES,
            require_nonempty=True,
        )
        if hashlib.sha256(trusted.payload).hexdigest() != plan.rendered_manifest_sha256:
            raise ValueError("protected rendered manifest content drifted")
        try:
            rendered = trusted.payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("protected rendered manifest content is not UTF-8") from exc
        return render_checkpoint_guard_field_ownership_payload(rendered).encode("utf-8")

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
