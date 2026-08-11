"""Journaled Control Plane desired-state apply and live worker convergence."""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx

from loom.worker_token import DEFAULT_WORKER_TOKEN_ENV_KEY
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
from loom_cli.rollout.install_attestation import WORKER_ENV_TEMPLATE_PATH
from loom_cli.rollout.steps.s10_env_state import (
    ExternalSlurmPrereqMaterializationError,
    _materialize_env_file,
    materialize_external_runner_repo,
    verify_external_runner_env,
    verify_external_runner_repo,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_IMPLEMENTATION_DIGEST = hashlib.sha256(b"loom-protected-environment-state-v3").hexdigest()
_MAX_PROFILE_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_TOKEN_BYTES = 64 * 1024
_REQUEST_TIMEOUT_SECONDS = 10.0
_DESIRED_DRIFT_PREFIXES = (
    "worker_pool_autoscaler_policies[",
    "gb10_worker_pool_desired_states[",
)
_EXTERNAL_RUNNER_ROOTS: Mapping[str, tuple[Path, Path]] = {
    "gb10": (
        Path("/shared_work2/loom-staging-rollout/worker-repos"),
        Path("/shared_work2/loom-staging-rollout/worker-envs"),
    ),
    "oldlab": (
        Path("/shared_work/loom/staging-rollout/worker-repos"),
        Path("/shared_work/loom/staging-rollout/worker-envs"),
    ),
}
_STAGING_WORKER_SERVICE_ENV: Mapping[str, Mapping[str, str]] = {
    pool_name: {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://192.168.50.103:18081",
        "LOOM_WORKER_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_SUBPROCESS_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://192.168.50.103:19000",
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO": ("192.168.50.103:5443/loom-trial-cache"),
    }
    for pool_name in ("gb10", "oldlab")
}


@dataclass(frozen=True, slots=True)
class _ExternalRunnerTarget:
    pool_name: str
    env_file: Path
    repo_dir: Path
    repo_root: Path
    requested_concurrency: object
    settings: dict[str, Any]


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
    worker_token_path: Path
    expected_env_template_sha256: str
    cp_url: str
    service_uid: int

    def __post_init__(self) -> None:
        if (
            not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or not self.admin_token_path.is_absolute()
            or ".." in self.admin_token_path.parts
            or not self.worker_token_path.is_absolute()
            or ".." in self.worker_token_path.parts
            or len(self.expected_env_template_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.expected_env_template_sha256
            )
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
        targets = self._external_runner_targets(profile, plan)
        worker_token = self._worker_token(plan) if targets else None
        live = self._fetch_live(profile, token)
        drift = diff_environment_state(
            profile,
            live,
            expected_worker_token=worker_token,
        )
        desired_drift = tuple(
            item for item in drift if item.path.startswith(_DESIRED_DRIFT_PREFIXES)
        )
        prerequisite_drift, prerequisite_evidence = self._external_runner_prerequisite_evidence(
            plan,
            targets,
            worker_token,
        )
        desired_exact = not desired_drift and not prerequisite_drift
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
            "prerequisite_drift_paths": sorted(prerequisite_drift),
            "prerequisite_evidence": prerequisite_evidence,
            "profile_sha256": plan.supervisor_profile_sha256,
            "runtime_drift_paths": sorted(item.path for item in runtime_drift),
        }
        return EnvironmentStateEvidence(
            desired_exact=desired_exact,
            runtime_exact=desired_exact
            and not runtime_drift
            and not blockers
            and not gb10_mismatches,
            evidence_digest=_hash_json(evidence),
        )

    def apply(self, plan: FinalGatePlan) -> None:
        profile = self._profile(plan)
        targets = self._external_runner_targets(profile, plan)
        worker_token = self._worker_token(plan) if targets else None
        self._materialize_external_runner_prerequisites(
            plan,
            targets,
            worker_token,
        )
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
        return self._secret(plan, label="admin", path=self.admin_token_path)

    def _worker_token(self, plan: FinalGatePlan) -> str:
        return self._secret(plan, label="worker", path=self.worker_token_path)

    def _secret(self, plan: FinalGatePlan, *, label: str, path: Path) -> str:
        trusted = read_trusted_file(
            path,
            service_uid=self.service_uid,
            private=True,
            allow_qianyi_owner=True,
            max_bytes=_MAX_TOKEN_BYTES,
            require_nonempty=True,
        )
        if plan.secret_metadata_fingerprints.get(label) != f"sha256:{trusted.metadata_fingerprint}":
            raise ValueError(f"protected environment-state {label} token metadata drifted")
        try:
            token = trusted.payload.strip().decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError(f"protected environment-state {label} token is invalid") from exc
        if not token or any(character.isspace() for character in token):
            raise ValueError(f"protected environment-state {label} token is invalid")
        return token

    def _external_runner_targets(
        self,
        profile: EnvironmentStateProfile,
        plan: FinalGatePlan,
    ) -> tuple[_ExternalRunnerTarget, ...]:
        policies = tuple(profile.autoscaler_policies)
        policy_pools = {str(policy.get("pool_name")) for policy in policies}
        if (
            len(policies) != 2
            or policy_pools != {"gb10", "oldlab"}
            or any(
                policy.get("environment") != "staging"
                or policy.get("actuator") != "slurm"
                or policy.get("enabled") is not True
                or policy.get("min_slots") != 0
                or not isinstance(policy.get("actuator_config"), dict)
                or policy["actuator_config"].get("external_runner") is not True
                for policy in policies
            )
        ):
            raise ValueError("protected external runner policy authority is invalid")
        settings = profile.external_slurm_runner_prerequisites
        pool_names = tuple(str(policy["pool_name"]) for policy in policies)
        configured_pools = settings.get("pools") if settings else None
        worker_token_env_key = (
            settings.get("worker_token_env_key", DEFAULT_WORKER_TOKEN_ENV_KEY) if settings else None
        )
        if (
            not settings
            or settings.get("materialize") is not True
            or settings.get("require_clean_repo") is not True
            or settings.get("require_worker_token_parity") is not True
            or settings.get("expected_repo_ref") != _image_tag(plan)
            or not isinstance(configured_pools, list)
            or set(configured_pools) != set(pool_names)
            or len(configured_pools) != len(set(configured_pools))
            or worker_token_env_key != DEFAULT_WORKER_TOKEN_ENV_KEY
            or settings.get("env_template") != str(WORKER_ENV_TEMPLATE_PATH)
            or "env_template_glob" in settings
            or settings.get("worker_service_env") != _STAGING_WORKER_SERVICE_ENV
        ):
            raise ValueError("protected external runner policy authority is invalid")

        targets: list[_ExternalRunnerTarget] = []
        for policy in policies:
            pool_name = str(policy["pool_name"])
            roots = _EXTERNAL_RUNNER_ROOTS.get(pool_name)
            actuator_config = policy.get("actuator_config")
            if roots is None or not isinstance(actuator_config, dict):
                raise ValueError("protected external runner pool authority is invalid")
            repo_root, env_root = roots
            self._validate_external_runner_root(repo_root)
            self._validate_external_runner_root(env_root)
            env_file = Path(str(actuator_config.get("env_file", "")))
            repo_dir = Path(str(actuator_config.get("repo_dir", "")))
            image_tag = _image_tag(plan)
            if (
                env_file != env_root / f"staging-{pool_name}-worker-{image_tag}.env"
                or repo_dir != repo_root / f"loom-remote-worker-{image_tag}"
            ):
                raise ValueError("protected external runner destination is invalid")
            targets.append(
                _ExternalRunnerTarget(
                    pool_name=pool_name,
                    env_file=env_file,
                    repo_dir=repo_dir,
                    repo_root=repo_root,
                    requested_concurrency=actuator_config.get("requested_concurrency"),
                    settings=settings,
                )
            )
        return tuple(targets)

    def _validate_external_runner_root(self, path: Path) -> None:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ValueError("protected external runner root is unavailable") from exc
        if (
            not path.is_absolute()
            or ".." in path.parts
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise ValueError("protected external runner root authority is invalid")

    @staticmethod
    def _worker_token_env_key(target: _ExternalRunnerTarget) -> str:
        return str(target.settings.get("worker_token_env_key") or DEFAULT_WORKER_TOKEN_ENV_KEY)

    def _external_runner_prerequisite_evidence(
        self,
        plan: FinalGatePlan,
        targets: tuple[_ExternalRunnerTarget, ...],
        worker_token: str | None,
    ) -> tuple[tuple[str, ...], dict[str, dict[str, object]]]:
        drift: list[str] = []
        evidence: dict[str, dict[str, object]] = {}
        for target in targets:
            prefix = f"external_slurm_runner_prerequisites[staging/{target.pool_name}]"
            pool_evidence: dict[str, object] = {
                "env_exact": False,
                "repo_exact": False,
            }
            try:
                env_record = verify_external_runner_env(
                    env_file=target.env_file,
                    image_tag=_image_tag(plan),
                    pool_name=target.pool_name,
                    requested_concurrency=target.requested_concurrency,
                    worker_token=worker_token,
                    worker_token_env_key=self._worker_token_env_key(target),
                    settings=target.settings,
                    expected_template_sha256=self.expected_env_template_sha256,
                )
            except (ExternalSlurmPrereqMaterializationError, OSError, ValueError):
                drift.append(f"{prefix}.env_file")
            else:
                pool_evidence["env_exact"] = True
                pool_evidence["env_sha256"] = env_record["env_sha256"]
            try:
                repo_record = verify_external_runner_repo(
                    repo_dir=target.repo_dir,
                    resolved_sha=plan.candidate_sha,
                    expected_ref=_image_tag(plan),
                )
            except (ExternalSlurmPrereqMaterializationError, OSError, ValueError):
                drift.append(f"{prefix}.repo_dir")
            else:
                pool_evidence["repo_exact"] = True
                pool_evidence["repo_head"] = repo_record["repo_head"]
            evidence[target.pool_name] = pool_evidence
        return tuple(drift), evidence

    def _materialize_external_runner_prerequisites(
        self,
        plan: FinalGatePlan,
        targets: tuple[_ExternalRunnerTarget, ...],
        worker_token: str | None,
    ) -> None:
        for target in targets:
            _materialize_env_file(
                env_file=target.env_file,
                settings=target.settings,
                image_tag=_image_tag(plan),
                pool_name=target.pool_name,
                requested_concurrency=target.requested_concurrency,
                worker_token=worker_token,
                worker_token_env_key=self._worker_token_env_key(target),
                refresh_from_template=True,
                expected_template_sha256=self.expected_env_template_sha256,
            )
            materialize_external_runner_repo(
                repo_dir=target.repo_dir,
                source_repo=self.candidate_root,
                resolved_sha=plan.candidate_sha,
                expected_ref=_image_tag(plan),
            )

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
            # Idempotent: the desired environment-state is already at the target
            # for this candidate — e.g. a prior or concurrent same-candidate
            # apply advanced it, or this is a rollout re-run after the state was
            # already advanced (#1081). Applying is a no-op and the post-apply
            # re-classify confirms EXACT, so return rather than failing the whole
            # protected apply. The epoch guard above still fail-closes on any
            # real mutation-epoch change, so this cannot mask a conflicting
            # mutation — desired_exact means the live desired state equals this
            # plan's own target.
            return
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
