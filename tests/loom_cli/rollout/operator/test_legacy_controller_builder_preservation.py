"""Trial-controller retirement must not trigger independent image-builder work."""

import pytest

from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    ExternalSupervisorApplyError,
    FixedExternalSupervisorTransport,
    classify_external_supervisor_live_state,
)
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_component import (
    _Control,
    _Store,
    _absent_authority,
    _bound_artifact,
    _build_active_artifact,
)


class _IndependentControl:
    def __init__(self, artifact, store):
        self.calls = []
        self.controls = {}
        for supervisor in artifact.supervisors:
            control = _Control(artifact, store)
            control.calls = self.calls
            self.controls[supervisor.service_name] = control
            self.controls[supervisor.timer_name] = control

    def daemon_reload(self):
        self.calls.append("daemon-reload")
        for control in self.controls.values():
            control.loaded = True

    def __getattr__(self, name):
        def call(unit, **kwargs):
            return getattr(self.controls[unit], name)(unit, **kwargs)
        return call


@pytest.mark.parametrize("lost_promotion", [False, True])
def test_trial_retirement_and_compensation_preserve_unchanged_builder(tmp_path, monkeypatch, lost_promotion):
    plan, root, active = _bound_artifact(tmp_path)
    profile = root / "deploy/environment-state/staging.toml"
    profile.write_text(profile.read_text().replace("enabled = true\nactive = true", "enabled = false\nactive = false", 1))
    retired = _build_active_artifact(root, candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree, image_tag=f"staging-{plan.candidate_sha[:7]}",
        environment=plan.environment, execution_host="gx10-01c7")
    trial = next(item for item in retired.supervisors if item.pool_name == "gb10")
    builder = next(item for item in retired.supervisors if item.pool_name == "task-image-builder-gb10")
    assert not trial.enabled and not trial.active
    assert builder.enabled and builder.active
    for name in (builder.service_name, builder.timer_name):
        assert retired.unit_sha256[name] == active.unit_sha256[name]
    store = _Store()
    control = _IndependentControl(active, store)
    transport = FixedExternalSupervisorTransport(store, control)
    transport.apply(active, transport.observe(active, _absent_authority()),
        plan_digest="a" * 64, attestation_digest="b" * 64, transition_digest="c" * 64)
    control.calls.clear()
    if lost_promotion:
        promote = store.promote_canonical
        def fail_once(identity, *, expected_current):
            monkeypatch.setattr(store, "promote_canonical", promote)
            raise ConnectionError("lost promotion")
        monkeypatch.setattr(store, "promote_canonical", fail_once)
    def apply():
        transport.apply(retired, transport.observe(retired),
            plan_digest="d" * 64, attestation_digest="e" * 64, transition_digest="f" * 64)
    if lost_promotion:
        with pytest.raises(ExternalSupervisorApplyError):
            apply()
        assert classify_external_supervisor_live_state(active, transport.observe(active)) == "exact"
    else:
        apply()
        assert classify_external_supervisor_live_state(retired, transport.observe(retired)) == "exact"
    assert f"stop-service:{trial.service_name}" in control.calls
    assert not [call for call in control.calls if "task-image-builder" in call]
    assert store.compensation_blockers() == {}
