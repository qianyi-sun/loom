from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import loom_cli.rollout.operator.protected_environment_state_component as environment_state_component
from loom_cli.environment_state import load_environment_state_profile
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_environment_state_component import (
    EnvironmentStateEvidence,
    HttpxProtectedEnvironmentStateTransport,
    ProtectedEnvironmentStateComponent,
)
from loom_cli.rollout.steps.s10_env_state import (
    ExternalSlurmPrereqMaterializationError,
)


@pytest.fixture(autouse=True)
def _allow_test_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom_cli.environment_state.staging_gb10_external_activation_blockers",
        lambda **_kwargs: (),
    )


class _StateTransport:
    def __init__(self) -> None:
        self.desired_exact = False
        self.runtime_exact = False
        self.calls: list[str] = []

    def observe(self, _plan, *, include_runtime):
        self.calls.append("runtime-read" if include_runtime else "desired-read")
        return EnvironmentStateEvidence(
            desired_exact=self.desired_exact,
            runtime_exact=self.runtime_exact if include_runtime else self.desired_exact,
            evidence_digest="a" * 64,
        )

    def apply(self, _plan):
        self.calls.append("apply")
        self.desired_exact = True


def _epoch(_plan) -> ComponentObservation:
    return ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="b" * 64,
        observed_epoch=8,
    )


def _plan(**overrides):
    values = {
        "candidate_sha": "1" * 40,
        "candidate_tree": "2" * 40,
        "starting_mutation_epoch": 7,
        "supervisor_profile_sha256": "3" * 64,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_component_applies_desired_state_before_accepting_exact() -> None:
    transport = _StateTransport()
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=_epoch,
    )
    plan = _plan()

    assert component.classify_desired(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify_desired(plan).state is ComponentState.EXACT
    assert transport.calls == ["desired-read", "desired-read", "apply", "desired-read"]


def test_apply_is_idempotent_when_desired_state_already_at_target() -> None:
    # #1081: when the desired environment-state is already at the target for this
    # candidate (a prior/concurrent same-candidate apply advanced it, or a
    # re-run after it was advanced), apply must be a no-op, not a crash. The
    # protected apply reaches this because classify observed READY earlier and
    # the state became exact before apply ran.
    transport = _StateTransport()
    transport.desired_exact = True
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=_epoch,
    )
    plan = _plan()

    component.apply(plan)  # must not raise

    assert "apply" not in transport.calls  # no redundant mutation was issued
    assert component.classify_desired(plan).state is ComponentState.EXACT


def test_apply_still_fails_closed_when_epoch_ownership_changed() -> None:
    # The idempotent no-op must not weaken the epoch guard: a changed mutation
    # epoch before apply still fail-closes.
    transport = _StateTransport()
    transport.desired_exact = True

    def _drifted_epoch(_plan) -> ComponentObservation:
        return ComponentObservation(
            state=ComponentState.DRIFTED,
            evidence_digest="c" * 64,
            observed_epoch=8,
        )

    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=_drifted_epoch,
    )

    with pytest.raises(RuntimeError, match="epoch ownership changed before apply"):
        component.apply(_plan())


def test_desired_classification_retries_transient_control_plane_turnover() -> None:
    class TransientObservationTransport(_StateTransport):
        def observe(self, plan, *, include_runtime):
            if not self.calls:
                self.calls.append("transient-desired-read")
                raise httpx.ConnectError("control plane is restarting")
            self.desired_exact = True
            return super().observe(plan, include_runtime=include_runtime)

    transport = TransientObservationTransport()
    epoch_calls = 0

    def epoch(plan) -> ComponentObservation:
        nonlocal epoch_calls
        epoch_calls += 1
        return _epoch(plan)

    sleeps: list[float] = []
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=epoch,
    )

    with patch("time.sleep", side_effect=sleeps.append):
        observation = component.classify_desired(_plan())

    assert observation.state is ComponentState.EXACT
    assert transport.calls == ["transient-desired-read", "desired-read"]
    assert epoch_calls == 2
    assert len(sleeps) == 1 and 0 < sleeps[0] <= 30


def test_apply_recovers_when_idempotent_put_commits_before_transport_failure() -> None:
    class LostMutationResponseTransport(_StateTransport):
        def apply(self, _plan):
            self.calls.append("apply-response-lost")
            self.desired_exact = True
            raise httpx.ConnectError("mutation response was lost during restart")

    transport = LostMutationResponseTransport()
    epoch_calls = 0

    def epoch(plan) -> ComponentObservation:
        nonlocal epoch_calls
        epoch_calls += 1
        return _epoch(plan)

    sleeps: list[float] = []
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=epoch,
    )

    with patch("time.sleep", side_effect=sleeps.append):
        component.apply(_plan())

    assert transport.calls == [
        "desired-read",
        "apply-response-lost",
        "desired-read",
    ]
    assert epoch_calls == 3
    assert len(sleeps) == 1 and 0 < sleeps[0] <= 30


def test_transient_observation_retry_stops_when_epoch_ownership_changes() -> None:
    class TransientObservationTransport(_StateTransport):
        def observe(self, _plan, *, include_runtime):
            self.calls.append("transient-desired-read")
            raise httpx.ConnectError("control plane is restarting")

    transport = TransientObservationTransport()
    epoch_states = iter((ComponentState.EXACT, ComponentState.DRIFTED))
    epoch_calls = 0

    def epoch(_plan) -> ComponentObservation:
        nonlocal epoch_calls
        epoch_calls += 1
        return ComponentObservation(
            state=next(epoch_states),
            evidence_digest="b" * 64,
            observed_epoch=8,
        )

    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=epoch,
    )

    with patch("time.sleep"):
        observation = component.classify_desired(_plan())

    assert observation.state is ComponentState.DRIFTED
    assert transport.calls == ["transient-desired-read"]
    assert epoch_calls == 2


def test_transient_observation_retry_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    class UnavailableTransport(_StateTransport):
        def observe(self, _plan, *, include_runtime):
            self.calls.append("transient-desired-read")
            raise httpx.ConnectError("control plane remains unavailable")

    monkeypatch.setattr(environment_state_component, "_TRANSIENT_ATTEMPTS", 3)
    transport = UnavailableTransport()
    epoch_calls = 0

    def epoch(plan) -> ComponentObservation:
        nonlocal epoch_calls
        epoch_calls += 1
        return _epoch(plan)

    sleeps: list[float] = []
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=epoch,
    )

    with patch("time.sleep", side_effect=sleeps.append):
        with pytest.raises(httpx.ConnectError, match="remains unavailable"):
            component.classify_desired(_plan())

    assert transport.calls == ["transient-desired-read"] * 3
    assert epoch_calls == 3
    assert len(sleeps) == 2


@pytest.mark.parametrize("status_code", (408, 425, 429, 500, 502, 503, 504))
def test_http_transport_classifies_readiness_status_as_transient(status_code: int) -> None:
    response = httpx.Response(status_code, content=b"temporarily unavailable")

    with pytest.raises(httpx.TransportError):
        HttpxProtectedEnvironmentStateTransport._response_json(response)
    with pytest.raises(httpx.TransportError):
        HttpxProtectedEnvironmentStateTransport._expect_ok(response)


def test_runtime_classification_stays_nonexact_after_desired_state_is_written() -> None:
    transport = _StateTransport()
    transport.desired_exact = True
    component = ProtectedEnvironmentStateComponent(
        transport=transport,
        epoch_guard=_epoch,
    )

    assert component.classify_desired(_plan()).state is ComponentState.EXACT
    assert component.classify_runtime(_plan()).state is ComponentState.READY


def test_runtime_classification_fails_closed_when_desired_state_drifts() -> None:
    component = ProtectedEnvironmentStateComponent(
        transport=_StateTransport(),
        epoch_guard=_epoch,
    )

    assert component.classify_runtime(_plan()).state is ComponentState.DRIFTED


def test_http_transport_binds_profile_token_and_requests_only_active_slurm_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = tmp_path / "candidate"
    profile_path = candidate / "deploy/environment-state/staging.toml"
    profile_path.parent.mkdir(parents=True)
    profile_path.write_bytes(Path("deploy/environment-state/staging.toml").read_bytes())
    profile_path.chmod(0o644)
    token_path = tmp_path / "admin-token"
    token_path.write_text("admin-secret\n", encoding="ascii")
    token_path.chmod(0o600)
    trusted = read_trusted_file(
        token_path,
        service_uid=os.geteuid(),
        private=True,
        require_nonempty=True,
    )
    plan = _plan(
        supervisor_profile_sha256=hashlib.sha256(profile_path.read_bytes()).hexdigest(),
        secret_metadata_fingerprints={"admin": f"sha256:{trusted.metadata_fingerprint}"},
        environment="staging",
    )
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-1111111",
            "ENV_CONFIG_VERSION": "staging-1111111",
            "GIT_SHA": "1" * 40,
        },
        expected_environment="staging",
    )
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method == "PUT":
            assert request.headers["Authorization"] == "Bearer admin-secret"
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("worker-pool-autoscalers/status"):
            return httpx.Response(200, json={"policies": profile.autoscaler_policies})
        if request.url.path.endswith("gb10-worker-pools/status"):
            return httpx.Response(
                200,
                json={
                    "desired_states": profile.gb10_desired_states,
                    "nodes": [],
                    "unlinked_workers": [],
                },
            )
        if request.url.path.endswith("slurm-worker-jobs/status"):
            assert request.url.params.get("active_only") == "true"
            assert len(request.url.params) == 1
            return httpx.Response(200, json={"jobs": []})
        raise AssertionError(request.url)

    real_client = httpx.Client

    def client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_environment_state_component.httpx.Client",
        client,
    )
    monkeypatch.setattr(
        HttpxProtectedEnvironmentStateTransport,
        "_external_runner_targets",
        lambda *_args: (),
    )
    transport = HttpxProtectedEnvironmentStateTransport(
        candidate_root=candidate,
        admin_token_path=token_path,
        worker_token_path=token_path,
        expected_env_template_sha256="a" * 64,
        cp_url="http://127.0.0.1:18081",
        service_uid=os.geteuid(),
    )

    assert transport.observe(plan, include_runtime=False).desired_exact
    transport.apply(plan)
    assert requests == [
        ("GET", "/admin/worker-pool-autoscalers/status"),
        ("GET", "/admin/gb10-worker-pools/status"),
        ("GET", "/admin/slurm-worker-jobs/status"),
        ("PUT", "/admin/worker-pool-autoscaler-policies/staging/gb10"),
        ("PUT", "/admin/worker-pool-autoscaler-policies/staging/oldlab"),
        ("PUT", "/admin/gb10-worker-pools/staging/gb10/desired-state"),
    ]


def test_http_transport_materializes_both_external_runner_pools_before_enabling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = tmp_path / "candidate"
    profile_path = candidate / "deploy/environment-state/staging.toml"
    profile_path.parent.mkdir(parents=True)
    generated = tmp_path / "generated"
    generated.mkdir()
    template = generated / "staging-gb10-worker-staging-template.env"
    template.write_text(
        "\n".join(
            (
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
                "LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
                "LOOM_WORKER_TOKEN=old-worker-token",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
                "LOOM_WORKER_MINIO_ACCESS_KEY=access-key",
                "LOOM_WORKER_MINIO_SECRET_KEY=secret-key",
                "",
            )
        ),
        encoding="utf-8",
    )
    template.chmod(0o600)
    original_template = template.read_text(encoding="utf-8")

    roots: dict[str, tuple[Path, Path]] = {}
    policies: list[str] = []
    for pool_name, concurrency in (("gb10", 10), ("oldlab", 6)):
        pool_root = tmp_path / pool_name
        repo_root = pool_root / "worker-repos"
        env_root = pool_root / "worker-envs"
        repo_root.mkdir(parents=True)
        env_root.mkdir()
        repo_root.chmod(0o2750)
        env_root.chmod(0o2750)
        roots[pool_name] = (repo_root, env_root)
        policies.append(
            f"""
[[worker_pool_autoscaler_policies]]
pool_name = "{pool_name}"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 20

[worker_pool_autoscaler_policies.actuator_config]
external_runner = true
env_file = "{env_root}/staging-{pool_name}-worker-${{IMAGE_TAG}}.env"
repo_dir = "{repo_root}/loom-remote-worker-${{IMAGE_TAG}}"
requested_concurrency = {concurrency}
""".strip()
        )
    profile_path.write_text(
        "\n\n".join(
            (
                'environment = "staging"',
                *policies,
                f"""
[external_slurm_runner_prerequisites]
pools = ["gb10", "oldlab"]
expected_repo_ref = "${{IMAGE_TAG}}"
require_clean_repo = true
require_worker_token_parity = true
materialize = true
env_template = "{template}"

[external_slurm_runner_prerequisites.worker_service_env.gb10]
LOOM_WORKER_CONTROL_PLANE_URL = "http://192.168.50.103:18081"
LOOM_WORKER_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_SUBPROCESS_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_MINIO_ENDPOINT = "http://192.168.50.103:19000"
LOOM_WORKER_TRAJECTORIES_BUCKET = "loom-staging-trajectories"
LOOM_WORKER_ARTIFACTS_BUCKET = "loom-staging-artifacts"
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"

[external_slurm_runner_prerequisites.worker_service_env.oldlab]
LOOM_WORKER_CONTROL_PLANE_URL = "http://192.168.50.103:18081"
LOOM_WORKER_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_SUBPROCESS_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_MINIO_ENDPOINT = "http://192.168.50.103:19000"
LOOM_WORKER_TRAJECTORIES_BUCKET = "loom-staging-trajectories"
LOOM_WORKER_ARTIFACTS_BUCKET = "loom-staging-artifacts"
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"
""".strip(),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    profile_path.chmod(0o644)
    subprocess.run(("git", "init", "-q", str(candidate)), check=True)
    subprocess.run(("git", "-C", str(candidate), "config", "user.name", "Test"), check=True)
    subprocess.run(
        ("git", "-C", str(candidate), "config", "user.email", "test@example.com"),
        check=True,
    )
    subprocess.run(("git", "-C", str(candidate), "add", "."), check=True)
    subprocess.run(("git", "-C", str(candidate), "commit", "-qm", "candidate"), check=True)
    candidate_sha = subprocess.run(
        ("git", "-C", str(candidate), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_tree = subprocess.run(
        ("git", "-C", str(candidate), "rev-parse", "HEAD^{tree}"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    admin_token_path = tmp_path / "admin-token"
    worker_token_path = tmp_path / "worker-token"
    admin_token_path.write_text("admin-secret\n", encoding="ascii")
    worker_token_path.write_text("worker-secret\n", encoding="ascii")
    admin_token_path.chmod(0o600)
    worker_token_path.chmod(0o600)
    admin_trusted = read_trusted_file(
        admin_token_path,
        service_uid=os.geteuid(),
        private=True,
        require_nonempty=True,
    )
    worker_trusted = read_trusted_file(
        worker_token_path,
        service_uid=os.geteuid(),
        private=True,
        require_nonempty=True,
    )
    plan = _plan(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        supervisor_profile_sha256=hashlib.sha256(profile_path.read_bytes()).hexdigest(),
        secret_metadata_fingerprints={
            "admin": f"sha256:{admin_trusted.metadata_fingerprint}",
            "worker": f"sha256:{worker_trusted.metadata_fingerprint}",
        },
        environment="staging",
    )
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": f"staging-{candidate_sha[:7]}",
            "ENV_CONFIG_VERSION": f"staging-{candidate_sha[:7]}",
            "GIT_SHA": candidate_sha,
        },
        expected_environment="staging",
    )

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_environment_state_component._EXTERNAL_RUNNER_ROOTS",
        roots,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_environment_state_component.WORKER_ENV_TEMPLATE_PATH",
        template,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        roots["gb10"][0],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_OLDLAB_WORKER_REPO_ROOT",
        roots["oldlab"][0],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.grp.getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getegid()),
    )
    put_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            for policy in profile.autoscaler_policies:
                config = policy["actuator_config"]
                env_text = Path(config["env_file"]).read_text(encoding="utf-8")
                assert "LOOM_WORKER_TOKEN=worker-secret\n" in env_text
                assert f"LOOM_WORKER_POOL_NAME={policy['pool_name']}\n" in env_text
                assert "LOOM_WORKER_CONTROL_PLANE_URL=http://192.168.50.103:18081\n" in env_text
                assert "LOOM_WORKER_GATEWAY_URL=http://192.168.50.103:19100\n" in env_text
                assert (
                    "LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://192.168.50.103:19100\n" in env_text
                )
                assert "LOOM_WORKER_MINIO_ENDPOINT=http://192.168.50.103:19000\n" in env_text
                assert "LOOM_WORKER_TRAJECTORIES_BUCKET=loom-staging-trajectories\n" in env_text
                assert "LOOM_WORKER_ARTIFACTS_BUCKET=loom-staging-artifacts\n" in env_text
                assert (
                    "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO="
                    "192.168.50.103:5443/loom-trial-cache\n" in env_text
                )
                assert (
                    f"LOOM_WORKER_MAX_CONCURRENT={config['requested_concurrency']}\n"
                ) in env_text
                assert (
                    subprocess.run(
                        ("git", "-C", config["repo_dir"], "rev-parse", "HEAD"),
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip()
                    == candidate_sha
                )
            put_paths.append(request.url.path)
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("worker-pool-autoscalers/status"):
            return httpx.Response(200, json={"policies": profile.autoscaler_policies})
        if request.url.path.endswith("gb10-worker-pools/status"):
            return httpx.Response(
                200,
                json={"desired_states": [], "nodes": [], "unlinked_workers": []},
            )
        if request.url.path.endswith("slurm-worker-jobs/status"):
            return httpx.Response(200, json={"jobs": []})
        raise AssertionError(request.url)

    real_client = httpx.Client

    def client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_environment_state_component.httpx.Client",
        client,
    )
    transport = HttpxProtectedEnvironmentStateTransport(
        candidate_root=candidate,
        admin_token_path=admin_token_path,
        worker_token_path=worker_token_path,
        expected_env_template_sha256=hashlib.sha256(template.read_bytes()).hexdigest(),
        cp_url="http://127.0.0.1:18081",
        service_uid=os.geteuid(),
    )

    assert not transport.observe(plan, include_runtime=False).desired_exact
    transport.apply(plan)
    assert transport.observe(plan, include_runtime=False).desired_exact
    assert put_paths == [
        "/admin/worker-pool-autoscaler-policies/staging/gb10",
        "/admin/worker-pool-autoscaler-policies/staging/oldlab",
    ]

    for policy in profile.autoscaler_policies:
        env_file = Path(policy["actuator_config"]["env_file"])
        env_file.write_text(
            env_file.read_text(encoding="utf-8").replace(
                "LOOM_WORKER_MINIO_ENDPOINT=http://192.168.50.103:19000",
                "LOOM_WORKER_MINIO_ENDPOINT=http://coordinated-drift.example:9000",
            ),
            encoding="utf-8",
        )
        env_file.chmod(0o600)
    template.write_text(
        template.read_text(encoding="utf-8").replace(
            "LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
            "LOOM_WORKER_MINIO_ENDPOINT=http://coordinated-drift.example:9000",
        ),
        encoding="utf-8",
    )
    template.chmod(0o600)
    assert not transport.observe(plan, include_runtime=False).desired_exact

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="template digest drifted",
    ):
        transport.apply(plan)
    template.write_text(original_template, encoding="utf-8")
    template.chmod(0o600)
    transport.apply(plan)

    gb10_env = Path(profile.autoscaler_policies[0]["actuator_config"]["env_file"])
    gb10_env.write_text(
        gb10_env.read_text(encoding="utf-8").replace(
            "LOOM_WORKER_MINIO_ENDPOINT=http://192.168.50.103:19000",
            "LOOM_WORKER_MINIO_ENDPOINT=http://hostile.example:9000",
        ),
        encoding="utf-8",
    )
    gb10_env.chmod(0o600)
    assert not transport.observe(plan, include_runtime=False).desired_exact


@pytest.mark.parametrize(
    "mutation",
    (
        "missing-oldlab",
        "nonexternal-oldlab",
        "disabled-gb10",
        "nonzero-minimum-gb10",
        "alternate-token-key",
        "alternate-template",
        "template-glob",
        "missing-worker-service-env",
        "drifted-worker-service-env",
    ),
)
def test_external_runner_policy_authority_rejects_incomplete_or_static_contracts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-1111111",
            "ENV_CONFIG_VERSION": "staging-1111111",
            "GIT_SHA": "1" * 40,
        },
        expected_environment="staging",
    )
    policies = {policy["pool_name"]: policy for policy in profile.autoscaler_policies}
    private_service_env = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://192.168.50.103:18081",
        "LOOM_WORKER_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_SUBPROCESS_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://192.168.50.103:19000",
        "LOOM_WORKER_TRAJECTORIES_BUCKET": "loom-staging-trajectories",
        "LOOM_WORKER_ARTIFACTS_BUCKET": "loom-staging-artifacts",
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO": ("192.168.50.103:5443/loom-trial-cache"),
    }
    profile.external_slurm_runner_prerequisites["worker_service_env"] = {
        "gb10": dict(private_service_env),
        "oldlab": dict(private_service_env),
    }
    if mutation == "missing-oldlab":
        profile.autoscaler_policies.remove(policies["oldlab"])
    elif mutation == "nonexternal-oldlab":
        policies["oldlab"]["actuator_config"]["external_runner"] = False
    elif mutation == "disabled-gb10":
        policies["gb10"]["enabled"] = False
    elif mutation == "nonzero-minimum-gb10":
        policies["gb10"]["min_slots"] = 1
    elif mutation == "alternate-token-key":
        profile.external_slurm_runner_prerequisites["worker_token_env_key"] = (
            "LOOM_UNUSED_WORKER_TOKEN"
        )
    elif mutation == "alternate-template":
        profile.external_slurm_runner_prerequisites["env_template"] = (
            "/var/lib/loom-staging-rollout/generated/other.env"
        )
    elif mutation == "template-glob":
        profile.external_slurm_runner_prerequisites.pop("env_template")
        profile.external_slurm_runner_prerequisites["env_template_glob"] = (
            "/var/lib/loom-staging-rollout/generated/*.env"
        )
    elif mutation == "missing-worker-service-env":
        profile.external_slurm_runner_prerequisites.pop("worker_service_env")
    elif mutation == "drifted-worker-service-env":
        profile.external_slurm_runner_prerequisites["worker_service_env"]["gb10"][
            "LOOM_WORKER_CONTROL_PLANE_URL"
        ] = "http://192.168.50.13:18081"
    else:  # pragma: no cover - closed parametrization
        raise AssertionError(mutation)

    monkeypatch.setattr(
        HttpxProtectedEnvironmentStateTransport,
        "_validate_external_runner_root",
        lambda *_args: None,
    )
    transport = HttpxProtectedEnvironmentStateTransport(
        candidate_root=tmp_path / "candidate",
        admin_token_path=tmp_path / "admin-token",
        worker_token_path=tmp_path / "worker-token",
        expected_env_template_sha256="a" * 64,
        cp_url="http://127.0.0.1:18081",
        service_uid=os.geteuid(),
    )

    with pytest.raises(ValueError, match="external runner policy authority"):
        transport._external_runner_targets(profile, _plan())
