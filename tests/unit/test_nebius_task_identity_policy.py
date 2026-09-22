"""Deployment identity claims must agree with an explicitly constrained target."""

from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import NebiusPlatformError, build_platform
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401

ROOT = Path(__file__).resolve().parents[2]


def test_identity_profile_cannot_claim_readiness_in_restricted_namespace(platform_inputs):  # noqa: F811
    config, candidate, profile = platform_inputs
    profile["supports_task_identity"] = True
    with pytest.raises(NebiusPlatformError, match="identity.*policy"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_identity_policy_is_target_bound_and_can_prepare_without_enabling_execution(platform_inputs):  # noqa: F811
    config, candidate, profile = platform_inputs
    config["task_identity_policy"] = {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    namespace = next(doc for doc in files["00-namespaces.yaml"]
                     if doc["metadata"]["name"] == config["execution_namespace"])
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "baseline"
    policy, binding = files["00-task-identity-policy.yaml"]
    assert policy["kind"] == "ValidatingAdmissionPolicy"
    assert policy["spec"]["failurePolicy"] == "Fail"
    assert binding["spec"]["validationActions"] == ["Deny"]
    assert binding["spec"]["matchResources"]["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": config["execution_namespace"],
    }
    different = deepcopy(config)
    different["task_identity_policy"]["target_id"] = "another-target"
    with pytest.raises(NebiusPlatformError, match="identity.*target"):
        build_platform(different, candidate, profile, {}, repo_root=ROOT)
