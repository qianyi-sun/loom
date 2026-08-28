"""Journal-ready final-only external autoscaler supervisor component."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.external_supervisor_controller import (
    ExternalSupervisorControllerBinding,
    parse_external_supervisor_controller_bindings,
)
from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    ExternalSupervisorCanonicalPointer,
    ExternalSupervisorPredecessorAuthority,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
    protected_external_supervisor_script_paths_for_units,
)
from loom_cli.rollout.preflight_contract import external_supervisor_transition_digest

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)
from .protected_external_supervisor_transport import (
    PROTECTED_USER_UNIT_DIR,
    ExternalSupervisorLiveObservation,
    ProtectedExternalSupervisorTransport,
    classify_external_supervisor_live_state,
)

_IMPLEMENTATION_DIGEST = hashlib.sha256(b"loom-protected-external-supervisor-v4").hexdigest()
_COMPONENT_ID_BY_EXECUTION_HOST = {
    "gx10-01c7": "external-supervisors-gb10",
    "TRT-EAI-OLDLAB-1": "external-supervisors-oldlab",
}

EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class ProtectedExternalSupervisorComponent:
    candidate_root: Path
    transport: ProtectedExternalSupervisorTransport
    epoch_guard: EpochGuard
    execution_host: str | None = None
    unit_dir: Path = PROTECTED_USER_UNIT_DIR
    artifact_builder: Callable[..., ExternalSupervisorArtifact] = build_external_supervisor_artifact

    def __post_init__(self) -> None:
        if (
            not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or not callable(self.epoch_guard)
            or not callable(self.artifact_builder)
            or not self.unit_dir.is_absolute()
            or ".." in self.unit_dir.parts
            or (self.execution_host is not None and not self.execution_host)
        ):
            raise ValueError("protected external supervisor authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        artifact = self._artifact(plan)
        controller = self._controller_binding(plan, artifact)
        try:
            component_id = _COMPONENT_ID_BY_EXECUTION_HOST[controller.execution_host]
        except KeyError as exc:
            raise ValueError("protected external supervisor controller is unauthorized") from exc
        return ProtectedApplyComponent(
            component_id=component_id,
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "artifact_digest": artifact.artifact_digest,
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "profile_sha256": artifact.profile_sha256,
                    "script_sha256": dict(artifact.script_sha256),
                    "starting_epoch": plan.starting_mutation_epoch,
                    "execution_host": controller.execution_host,
                    "unit_directory": str(self.unit_dir),
                    "supervisor_predecessor_digest": controller.predecessor_digest,
                    "supervisor_predecessor_kind": controller.predecessor_kind,
                    "supervisor_predecessor_pending_transition_digest": (
                        controller.predecessor_pending_transition_digest
                    ),
                    "supervisor_predecessor_pointer_digest": (
                        controller.predecessor_pointer_digest
                    ),
                    "supervisor_predecessor_unit_set_digest": (
                        controller.predecessor_unit_set_digest
                    ),
                    "supervisor_predecessor_unit_sha256": dict(controller.predecessor_unit_sha256),
                    "supervisor_transition_digest": controller.transition_digest,
                    "unit_sha256": dict(artifact.unit_sha256),
                    "unit_set_digest": plan.systemd_unit_set_digest,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        artifact = self._artifact(plan)
        controller = self._controller_binding(plan, artifact)
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(
                plan,
                artifact,
                epoch,
                ComponentState.DRIFTED,
                "0" * 64,
            )
        try:
            live = self.transport.observe(artifact)
        except (RuntimeError, ValueError):
            # A bare observe intentionally refuses to self-authorize an absent
            # predecessor (absent is not self-authoritative -- it must be declared
            # by the attested plan). When the plan itself declares absent, re-observe
            # with the plan's absent authority so a genuinely-absent live state
            # classifies READY (ready to bootstrap the first canonical supervisor)
            # instead of DRIFTED, which would fail the protected-apply journal
            # before it ever reaches apply. The post-apply re-classify still uses the
            # bare observe above, which resolves the now-established canonical.
            if controller.predecessor_kind != "absent":
                return self._observation(plan, artifact, epoch, ComponentState.DRIFTED, "0" * 64)
            try:
                live = self.transport.observe(
                    artifact,
                    self._plan_authority(plan, controller),
                )
            except (RuntimeError, ValueError):
                return self._observation(plan, artifact, epoch, ComponentState.DRIFTED, "0" * 64)
        live_state = classify_external_supervisor_live_state(
            artifact,
            live,
            unit_dir=self.unit_dir,
        )
        state = ComponentState.READY if live_state == "repairable" else ComponentState(live_state)
        if state is ComponentState.EXACT:
            canonical = live.canonical_identity
            if canonical is None:
                state = ComponentState.DRIFTED
            elif (
                canonical.plan_digest != plan.plan_digest
                or canonical.attestation_digest != plan.attestation_digest
            ):
                # Exact target bytes may already belong to the canonical
                # predecessor. Re-attest them only through that complete,
                # plan-bound predecessor authority.
                try:
                    self._verify_predecessor_binding(plan, artifact, live, controller)
                except (RuntimeError, ValueError):
                    state = ComponentState.DRIFTED
                else:
                    state = ComponentState.READY
        elif state is ComponentState.READY:
            try:
                self._verify_predecessor_binding(plan, artifact, live, controller)
            except (RuntimeError, ValueError):
                state = ComponentState.DRIFTED
        return self._observation(
            plan,
            artifact,
            epoch,
            state,
            live.evidence_digest,
        )

    def apply(self, plan: FinalGatePlan) -> None:
        artifact = self._artifact(plan)
        controller = self._controller_binding(plan, artifact)
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected external supervisor epoch changed before apply")
        authority = self._plan_authority(plan, controller)
        live = self.transport.observe(artifact, authority)
        self._verify_predecessor_binding(plan, artifact, live, controller)
        live_state = classify_external_supervisor_live_state(
            artifact,
            live,
            unit_dir=self.unit_dir,
        )
        if live_state not in {"exact", "ready", "repairable"}:
            raise RuntimeError("protected external supervisor state changed before apply")
        self.transport.apply(
            artifact,
            live,
            plan_digest=plan.plan_digest,
            attestation_digest=plan.attestation_digest,
            transition_digest=controller.transition_digest,
        )

    @staticmethod
    def _plan_authority(
        plan: FinalGatePlan,
        controller: ExternalSupervisorControllerBinding | None = None,
    ) -> ExternalSupervisorPredecessorAuthority:
        authority = ExternalSupervisorPredecessorAuthority(
            kind=(
                plan.supervisor_predecessor_kind
                if controller is None
                else controller.predecessor_kind
            ),
            authority_digest=(
                plan.supervisor_predecessor_digest
                if controller is None
                else controller.predecessor_digest
            ),
            unit_sha256=(
                plan.supervisor_predecessor_unit_sha256
                if controller is None
                else controller.predecessor_unit_sha256
            ),
        )
        pointer_digest = (
            plan.supervisor_predecessor_pointer_digest
            if controller is None
            else controller.predecessor_pointer_digest
        )
        unit_set_digest = (
            plan.supervisor_predecessor_unit_set_digest
            if controller is None
            else controller.predecessor_unit_set_digest
        )
        if authority.kind == "absent":
            # An absent predecessor is the first-introduction bootstrap: a
            # post-0067 rollout that establishes the canonical supervisor when no
            # predecessor units are live and no canonical record exists yet.
            # Admission (`require_predecessor_kind`) and the transport authority
            # resolver both accept it, so the apply must too -- but only when it
            # is well formed: the absent sentinel digest, no predecessor units,
            # the tolerant empty unit-set digest, and an absent pointer. The
            # gb10-arm64->gb10 rename safety is enforced by the migration
            # component and the target pool identity, not by the predecessor, so
            # a fresh supervisor loses no guarantee. The strict
            # ``unit_set_digest`` property rejects an empty set, so the absent
            # unit-set is validated with the tolerant helper instead.
            if (
                authority.authority_digest != ABSENT_PREDECESSOR_DIGEST
                or authority.unit_sha256
                or pointer_digest != ABSENT_PREDECESSOR_DIGEST
                or unit_set_digest
                != external_supervisor_unit_set_digest_or_empty(authority.unit_sha256)
            ):
                raise ValueError("protected external supervisor predecessor binding is invalid")
            return authority
        if (
            authority.authority_digest == ABSENT_PREDECESSOR_DIGEST
            or authority.unit_set_digest != unit_set_digest
            or (authority.kind == "legacy-manifest" and pointer_digest != ABSENT_PREDECESSOR_DIGEST)
            or (authority.kind == "canonical" and pointer_digest == ABSENT_PREDECESSOR_DIGEST)
        ):
            raise ValueError("protected external supervisor predecessor binding is invalid")
        return authority

    @classmethod
    def _verify_predecessor_binding(
        cls,
        plan: FinalGatePlan,
        artifact: ExternalSupervisorArtifact,
        live: ExternalSupervisorLiveObservation,
        controller: ExternalSupervisorControllerBinding,
    ) -> None:
        # The concrete transport observation is intentionally read through its
        # public immutable fields.  A protocol implementation cannot bypass
        # any of these plan-bound comparisons.
        authority = cls._plan_authority(plan, controller)
        live_authority = live.predecessor_authority
        canonical = live.canonical_identity
        live_evidence_digest = live.evidence_digest
        pending_transition_digest = live.pending_transition_digest
        if live_authority != authority:
            raise RuntimeError("protected external supervisor predecessor authority drifted")
        if authority.kind == "canonical":
            if (
                canonical is None
                or ExternalSupervisorCanonicalPointer.build(canonical).pointer_digest
                != controller.predecessor_pointer_digest
            ):
                raise RuntimeError("protected external supervisor predecessor pointer drifted")
        elif canonical is not None:
            raise RuntimeError("protected external supervisor predecessor pointer appeared")
        if pending_transition_digest != controller.predecessor_pending_transition_digest:
            raise RuntimeError("protected external supervisor predecessor evidence drifted")
        target_unit_set_digest = external_supervisor_unit_set_digest(artifact.unit_sha256)
        calculated_transition = external_supervisor_transition_digest(
            unit_directory=controller.unit_directory,
            candidate_sha=plan.candidate_sha,
            candidate_tree=plan.candidate_tree,
            environment=plan.environment,
            predecessor_kind=authority.kind,
            predecessor_digest=authority.authority_digest,
            predecessor_pointer_digest=controller.predecessor_pointer_digest,
            predecessor_unit_sha256=authority.unit_sha256,
            predecessor_unit_set_digest=controller.predecessor_unit_set_digest,
            predecessor_live_evidence_digest=live_evidence_digest,
            predecessor_pending_transition_digest=pending_transition_digest,
            target_artifact_digest=artifact.artifact_digest,
            target_profile_sha256=artifact.profile_sha256,
            target_script_sha256=artifact.script_sha256,
            target_unit_sha256=artifact.unit_sha256,
            target_unit_set_digest=target_unit_set_digest,
        )
        if calculated_transition != controller.transition_digest:
            raise RuntimeError("protected external supervisor transition binding drifted")

    def _artifact(self, plan: FinalGatePlan) -> ExternalSupervisorArtifact:
        builder_kwargs: dict[str, object] = {
            "candidate_sha": plan.candidate_sha,
            "candidate_tree": plan.candidate_tree,
            "image_tag": f"staging-{plan.candidate_sha[:7]}",
            "environment": plan.environment,
        }
        if self.execution_host is not None:
            builder_kwargs["execution_host"] = self.execution_host
        artifact = self.artifact_builder(
            self.candidate_root,
            **builder_kwargs,
        )
        # Round-trip through the canonical parser so an injected or changed
        # builder cannot smuggle a partially validated object into final apply.
        artifact = ExternalSupervisorArtifact.from_bytes(artifact.to_bytes())
        execution_hosts = {supervisor.execution_host for supervisor in artifact.supervisors}
        if len(execution_hosts) != 1:
            raise ValueError("protected external supervisor artifact spans controllers")
        execution_host = next(iter(execution_hosts))
        unit_prefix = f"{execution_host}/"
        expected_dynamic_units = {
            key.removeprefix(unit_prefix): digest
            for key, digest in plan.supervisor_controller_unit_digests.items()
            if key.startswith(unit_prefix)
        }
        expected_script_digests = {
            path: plan.supervisor_script_digests[path]
            for path in protected_external_supervisor_script_paths_for_units(expected_dynamic_units)
        }
        calculated_set_digest = _hash_json({"failed": {}, "units": dict(plan.systemd_unit_digests)})
        if (
            artifact.candidate_sha != plan.candidate_sha
            or artifact.candidate_tree != plan.candidate_tree
            or artifact.environment != plan.environment
            or artifact.image_tag != f"staging-{plan.candidate_sha[:7]}"
            or artifact.artifact_digest
            != plan.supervisor_controller_artifact_digests.get(execution_host)
            or artifact.profile_sha256 != plan.supervisor_profile_sha256
            or dict(artifact.script_sha256) != expected_script_digests
            or dict(artifact.unit_sha256) != expected_dynamic_units
            or calculated_set_digest != plan.systemd_unit_set_digest
        ):
            raise ValueError("protected external supervisor artifact drifted from final plan")
        return artifact

    def _controller_binding(
        self,
        plan: FinalGatePlan,
        artifact: ExternalSupervisorArtifact,
    ) -> ExternalSupervisorControllerBinding:
        execution_hosts = {supervisor.execution_host for supervisor in artifact.supervisors}
        if len(execution_hosts) != 1:
            raise ValueError("protected external supervisor artifact spans controllers")
        execution_host = next(iter(execution_hosts))
        try:
            binding = parse_external_supervisor_controller_bindings(
                plan.supervisor_controller_bindings
            )[execution_host]
        except KeyError as exc:
            raise ValueError("protected external supervisor controller binding is missing") from exc
        if binding.unit_directory != str(self.unit_dir):
            raise ValueError("protected external supervisor controller unit directory drifted")
        return binding

    @staticmethod
    def _observation(
        plan: FinalGatePlan,
        artifact: ExternalSupervisorArtifact,
        epoch: ComponentObservation,
        state: ComponentState,
        live_digest: str,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "artifact_digest": artifact.artifact_digest,
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "epoch_evidence_digest": epoch.evidence_digest,
                    "live_digest": live_digest,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = ["ProtectedExternalSupervisorComponent"]
