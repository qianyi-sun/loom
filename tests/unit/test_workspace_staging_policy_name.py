"""Named workspace staging policy resolution (#1263)."""

from __future__ import annotations

import pytest

from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
    resolve_trial_workspace_staging_policy,
    resolve_workspace_staging_policy_name,
)


def test_resolve_name_tb21_is_canonical() -> None:
    policy = resolve_workspace_staging_policy_name("tb21")
    assert policy == WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)


def test_resolve_name_none_is_no_filter() -> None:
    assert resolve_workspace_staging_policy_name("none") is None


def test_resolve_name_unknown_raises() -> None:
    with pytest.raises(ValueError, match="unknown workspace_staging_policy_name"):
        resolve_workspace_staging_policy_name("custom")


def test_trial_explicit_tb21_overrides_missing_provenance() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name="tb21",
        task_id="strict-pass-39-20260805/path-tracing/analyticrayrendererrepair",
        raw_provenance_policy=None,
    )
    assert policy == WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)


def test_trial_explicit_none_on_non_tb21() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name="none",
        task_id="strict-pass-39-20260805/path-tracing/analyticrayrendererrepair",
        raw_provenance_policy=TB21_AGENT_WORKSPACE_POLICY,
    )
    assert policy is None


def test_trial_explicit_none_on_tb21_fail_closed() -> None:
    with pytest.raises(ValueError, match="cannot use workspace_staging_policy_name=none"):
        resolve_trial_workspace_staging_policy(
            policy_name="none",
            task_id="terminal-bench-2@tb2.1-r6/hello-world",
            raw_provenance_policy=TB21_AGENT_WORKSPACE_POLICY,
        )


def test_trial_unset_tb21_requires_canonical_provenance() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name=None,
        task_id="terminal-bench-2@tb2.1-r6/hello-world",
        raw_provenance_policy=TB21_AGENT_WORKSPACE_POLICY,
    )
    assert policy == WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)


def test_trial_unset_non_tb21_without_provenance_is_none() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name=None,
        task_id="strict-pass-39-20260805/path-tracing/analyticrayrendererrepair",
        raw_provenance_policy=None,
    )
    assert policy is None
