"""Unit tests for the v1 workload trust contract."""

from __future__ import annotations

from dataclasses import replace
from importlib import import_module

import pytest


def _contract_class() -> type:
    return import_module("loom.workload_trust").WorkloadTrustContract


def _v1_contract() -> object:
    return _contract_class()(
        workload_trust_mode="internal_trusted",
        taskset_transforms_enabled=False,
        taskset_transform_network_isolated=False,
        untrusted_workload_isolation=False,
    )


def test_v1_contract_accepts_only_internal_trusted_tuple() -> None:
    assert _v1_contract().v1_violations() == []


@pytest.mark.parametrize(
    ("field", "value", "expected_violation"),
    [
        (
            "workload_trust_mode",
            "untrusted_isolated",
            "workload_trust_mode must be internal_trusted",
        ),
        (
            "taskset_transforms_enabled",
            True,
            "taskset_transforms_enabled must be false",
        ),
        (
            "taskset_transform_network_isolated",
            True,
            "taskset_transform_network_isolated must be false",
        ),
        (
            "untrusted_workload_isolation",
            True,
            "untrusted_workload_isolation must be false",
        ),
    ],
)
def test_v1_contract_rejects_each_non_v1_value(
    field: str,
    value: object,
    expected_violation: str,
) -> None:
    assert replace(_v1_contract(), **{field: value}).v1_violations() == [expected_violation]


def test_manifest_uses_exact_v1_wire_keys() -> None:
    assert _v1_contract().as_manifest() == {
        "workload_trust_mode": "internal_trusted",
        "taskset_transforms_enabled": False,
        "taskset_transform_network_isolated": False,
        "untrusted_workload_isolation": False,
    }
