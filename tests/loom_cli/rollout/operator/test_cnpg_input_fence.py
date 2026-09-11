"""Rendering does not install a fence or authorize a database handoff."""

import json

import pytest


def _render(**overrides):
    from loom_cli.rollout.operator.protected_cnpg_input_fence import render_cnpg_input_fence

    return render_cnpg_input_fence(**{
        "intent_digest": "a" * 64,
        "target_pooler_names": ("existing-target",),
        **overrides,
    })


def test_fence_is_intent_bound_fail_closed_and_has_no_expiry_bypass():
    first, second = _render(), _render()
    assert first == second
    policies = [v for v in first if v["kind"] == "ValidatingAdmissionPolicy"]
    bindings = [v for v in first if v["kind"] == "ValidatingAdmissionPolicyBinding"]
    assert len(policies) == len(bindings) == 5
    assert {v["spec"]["policyName"] for v in bindings} == {v["metadata"]["name"] for v in policies}
    for policy in policies:
        assert policy["spec"]["failurePolicy"] == "Fail"
        assert policy["metadata"]["annotations"]["loom.dev/handoff-intent"] == "a" * 64
        assert "now(" not in json.dumps(policy)
    for binding in bindings:
        assert binding["spec"]["validationActions"] == ["Deny"]
    assert {v["metadata"]["name"] for v in first}.isdisjoint(
        {v["metadata"]["name"] for v in _render(intent_digest="b" * 64)}
    )


@pytest.mark.parametrize("overrides", [
    {"intent_digest": "x" * 64}, {"intent_digest": "a" * 63},
    {"target_pooler_names": ("x' || true",)},
    {"target_pooler_names": ("same", "same")},
    {"target_pooler_names": ["not-an-immutable-tuple"]},
])
def test_invalid_fence_authority_is_rejected(overrides):
    with pytest.raises(ValueError, match="CNPG"):
        _render(**overrides)
