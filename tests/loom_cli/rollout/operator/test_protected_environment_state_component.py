from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

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


def test_http_transport_binds_profile_token_and_fixed_cp_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = tmp_path / "candidate"
    profile_path = candidate / "deploy/environment-state/staging.toml"
    profile_path.parent.mkdir(parents=True)
    profile_path.write_text(
        """
environment = "staging"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10"
actuator = "slurm"
enabled = false
min_slots = 0
max_slots = 14

[[gb10_worker_pool_desired_states]]
pool_name = "gb10"
image_tag = "${IMAGE_TAG}"
max_concurrent = 1
env_config_version = "${ENV_CONFIG_VERSION}"
source_git_commit = "${GIT_SHA}"

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "active"
""".strip()
        + "\n",
        encoding="utf-8",
    )
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
        admin_token_path=token_path,
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
        ("PUT", "/admin/gb10-worker-pools/staging/gb10/desired-state"),
    ]
