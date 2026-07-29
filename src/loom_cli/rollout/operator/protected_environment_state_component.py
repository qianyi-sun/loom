"""Journaled Control Plane desired-state apply and live worker convergence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx

from loom_cli.environment_state import (
    EnvironmentStateProfile,
    autoscaler_blockers,
    autoscaler_policy_payload,
    diff_environment_state,
    gb10_desired_state_payload,
    load_environment_state_profile,
)
from loom_cli.gb10_release_gate import gb10_release_target_mismatches
from loom_cli.rollout.credential_authority import read_trusted_file

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_IMPLEMENTATION_DIGEST = hashlib.sha256(b"loom-protected-environment-state-v1").hexdigest()
_MAX_PROFILE_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_TOKEN_BYTES = 64 * 1024
_REQUEST_TIMEOUT_SECONDS = 10.0
_DESIRED_DRIFT_PREFIXES = (
    "worker_pool_autoscaler_policies[",
    "gb10_worker_pool_desired_states[",
)


@dataclass(frozen=True, slots=True)
class EnvironmentStateEvidence:
    desired_exact: bool
    runtime_exact: bool
    evidence_digest: str

    def __post_init__(self) -> None:
        if (
            type(self.desired_exact) is not bool
            or type(self.runtime_exact) is not bool
            or (self.runtime_exact and not self.desired_exact)
            or len(self.evidence_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.evidence_digest)
        ):
            raise ValueError("protected environment-state evidence is invalid")


class ProtectedEnvironmentStateTransport(Protocol):
    def observe(
        self,
        plan: FinalGatePlan,
        *,
        include_runtime: bool,
    ) -> EnvironmentStateEvidence: ...

    def apply(self, plan: FinalGatePlan) -> None: ...


@dataclass(frozen=True, slots=True)
class HttpxProtectedEnvironmentStateTransport:
    """Fixed CP admin transport bound to the attested candidate profile."""

    candidate_root: Path
    admin_token_path: Path
    cp_url: str
    service_uid: int

    def __post_init__(self) -> None:
        if (
            not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or not self.admin_token_path.is_absolute()
            or ".." in self.admin_token_path.parts
            or self.cp_url != "http://127.0.0.1:18081"
            or self.service_uid < 0
        ):
            raise ValueError("protected environment-state transport authority is invalid")

    def observe(
        self,
        plan: FinalGatePlan,
        *,
        include_runtime: bool,
    ) -> EnvironmentStateEvidence:
        profile = self._profile(plan)
        token = self._token(plan)
        live = self._fetch_live(profile, token)
        drift = diff_environment_state(profile, live)
        desired_drift = tuple(
            item for item in drift if item.path.startswith(_DESIRED_DRIFT_PREFIXES)
        )
        runtime_drift = tuple(drift) if include_runtime else desired_drift
        blockers = autoscaler_blockers(profile, live) if include_runtime else []
        gb10_mismatches = (
            gb10_release_target_mismatches(
                dict(live["gb10_status"]),
                release_image_tag=_image_tag(plan),
                release_env_config_version=_image_tag(plan),
            )
            if include_runtime
            else []
        )
        evidence = {
            "autoscaler_blocker_count": len(blockers),
            "desired_drift_paths": sorted(item.path for item in desired_drift),
            "gb10_mismatch_count": len(gb10_mismatches),
            "profile_sha256": plan.supervisor_profile_sha256,
            "runtime_drift_paths": sorted(item.path for item in runtime_drift),
        }
        return EnvironmentStateEvidence(
            desired_exact=not desired_drift,
            runtime_exact=not desired_drift
            and not runtime_drift
            and not blockers
            and not gb10_mismatches,
            evidence_digest=_hash_json(evidence),
        )

    def apply(self, plan: FinalGatePlan) -> None:
        profile = self._profile(plan)
        token = self._token(plan)
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(
            base_url=self.cp_url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            for policy in profile.autoscaler_policies:
                self._expect_ok(
                    client.put(
                        (
                            "/admin/worker-pool-autoscaler-policies/"
                            f"{policy['environment']}/{policy['pool_name']}"
                        ),
                        json=autoscaler_policy_payload(policy),
                    )
                )
            for state in profile.gb10_desired_states:
                self._expect_ok(
                    client.put(
                        (
                            "/admin/gb10-worker-pools/"
                            f"{state['environment']}/{state['pool_name']}/desired-state"
                        ),
                        json=gb10_desired_state_payload(state),
                    )
                )

    def _profile(self, plan: FinalGatePlan) -> EnvironmentStateProfile:
        path = self.candidate_root / "deploy/environment-state/staging.toml"
        first = read_trusted_file(
            path,
            service_uid=self.service_uid,
            private=False,
            max_bytes=_MAX_PROFILE_BYTES,
            require_nonempty=True,
        )
        if hashlib.sha256(first.payload).hexdigest() != plan.supervisor_profile_sha256:
            raise ValueError("protected environment-state profile drifted")
        profile = load_environment_state_profile(
            path,
            variables={
                "ENV_CONFIG_VERSION": _image_tag(plan),
                "GIT_SHA": plan.candidate_sha,
                "IMAGE_TAG": _image_tag(plan),
            },
            expected_environment=plan.environment,
        )
        second = read_trusted_file(
            path,
            service_uid=self.service_uid,
            private=False,
            max_bytes=_MAX_PROFILE_BYTES,
            require_nonempty=True,
        )
        if (
            second.payload != first.payload
            or second.metadata_fingerprint != first.metadata_fingerprint
            or profile.control_plane_environment != "staging"
        ):
            raise ValueError("protected environment-state profile authority changed")
        return profile

    def _token(self, plan: FinalGatePlan) -> str:
        trusted = read_trusted_file(
            self.admin_token_path,
            service_uid=self.service_uid,
            private=True,
            allow_qianyi_owner=True,
            max_bytes=_MAX_TOKEN_BYTES,
            require_nonempty=True,
        )
        if (
            plan.secret_metadata_fingerprints.get("admin")
            != f"sha256:{trusted.metadata_fingerprint}"
        ):
            raise ValueError("protected environment-state admin token metadata drifted")
        try:
            token = trusted.payload.strip().decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("protected environment-state admin token is invalid") from exc
        if not token or any(character.isspace() for character in token):
            raise ValueError("protected environment-state admin token is invalid")
        return token

    def _fetch_live(
        self,
        profile: EnvironmentStateProfile,
        token: str,
    ) -> dict[str, Mapping[str, Any]]:
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(
            base_url=self.cp_url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            autoscaler = client.get("/admin/worker-pool-autoscalers/status")
            gb10 = client.get(
                "/admin/gb10-worker-pools/status",
                params={"environment": profile.control_plane_environment},
            )
            slurm = client.get("/admin/slurm-worker-jobs/status")
        return {
            "autoscaler_status": self._response_json(autoscaler),
            "gb10_status": self._response_json(gb10),
            "slurm_status": self._response_json(slurm),
        }

    @staticmethod
    def _response_json(response: httpx.Response) -> Mapping[str, Any]:
        if response.status_code != 200 or len(response.content) > _MAX_RESPONSE_BYTES:
            raise RuntimeError("protected environment-state observation failed safely")
        try:
            value = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "protected environment-state observation returned invalid JSON"
            ) from exc
        if not isinstance(value, Mapping):
            raise RuntimeError("protected environment-state observation shape is invalid")
        return dict(value)

    @staticmethod
    def _expect_ok(response: httpx.Response) -> None:
        if response.status_code != 200 or len(response.content) > _MAX_RESPONSE_BYTES:
            raise RuntimeError("protected environment-state mutation failed safely")


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class ProtectedEnvironmentStateComponent:
    transport: ProtectedEnvironmentStateTransport
    epoch_guard: EpochGuard

    def __post_init__(self) -> None:
        if not callable(self.epoch_guard):
            raise ValueError("protected environment-state epoch authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="environment-state",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "profile_sha256": plan.supervisor_profile_sha256,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify_desired,
            apply=self.apply,
        )

    def classify_desired(self, plan: FinalGatePlan) -> ComponentObservation:
        return self._classify(plan, include_runtime=False)

    def classify_runtime(self, plan: FinalGatePlan) -> ComponentObservation:
        return self._classify(plan, include_runtime=True)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected environment-state epoch ownership changed before apply")
        evidence = self.transport.observe(plan, include_runtime=False)
        if evidence.desired_exact:
            raise RuntimeError("protected environment-state changed before apply")
        self.transport.apply(plan)

    def _classify(
        self,
        plan: FinalGatePlan,
        *,
        include_runtime: bool,
    ) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(
                plan,
                ComponentState.DRIFTED,
                epoch.evidence_digest,
                "0" * 64,
            )
        evidence = self.transport.observe(plan, include_runtime=include_runtime)
        if include_runtime and not evidence.desired_exact:
            state = ComponentState.DRIFTED
        else:
            exact = evidence.runtime_exact if include_runtime else evidence.desired_exact
            state = ComponentState.EXACT if exact else ComponentState.READY
        return self._observation(
            plan,
            state,
            epoch.evidence_digest,
            evidence.evidence_digest,
        )

    @staticmethod
    def _observation(
        plan: FinalGatePlan,
        state: ComponentState,
        epoch_digest: str,
        state_digest: str,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "epoch_evidence_digest": epoch_digest,
                    "state": state.value,
                    "state_evidence_digest": state_digest,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _image_tag(plan: FinalGatePlan) -> str:
    return f"staging-{plan.candidate_sha[:7]}"


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "EnvironmentStateEvidence",
    "HttpxProtectedEnvironmentStateTransport",
    "ProtectedEnvironmentStateComponent",
    "ProtectedEnvironmentStateTransport",
]
