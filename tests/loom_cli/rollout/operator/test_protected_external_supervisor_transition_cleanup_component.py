from __future__ import annotations

import json
from copy import deepcopy

import pytest

from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_external_supervisor_transition_cleanup_component import (
    KubernetesExternalSupervisorTransitionCleanupComponent,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan

NAME = "loom-external-slurm-autoscaler-manager-export"
NAMESPACE = "loom-dev"
POD_NAME = "loom-capacity-manager-abcdef1234-abcde"
POD_UID = "44e2299b-2ae1-45a5-bbab-c67409ea6e72"


def _owner_reference() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "v1",
            "blockOwnerDeletion": False,
            "controller": False,
            "kind": "Pod",
            "name": POD_NAME,
            "uid": POD_UID,
        }
    ]


def _role() -> dict[str, object]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": NAME,
            "namespace": NAMESPACE,
            "ownerReferences": _owner_reference(),
        },
        "rules": [
            {
                "apiGroups": ["apps"],
                "resourceNames": ["loom-capacity-manager"],
                "resources": ["deployments"],
                "verbs": ["get"],
            },
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get", "list"],
            },
            {
                "apiGroups": [""],
                "resourceNames": [POD_NAME],
                "resources": ["pods/exec"],
                "verbs": ["create"],
            },
        ],
    }


def _role_binding() -> dict[str, object]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": NAME,
            "namespace": NAMESPACE,
            "ownerReferences": _owner_reference(),
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": NAME,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-external-slurm-autoscaler",
                "namespace": "loom-staging",
            }
        ],
    }


def _policy() -> dict[str, object]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": NAME},
        "spec": {
            "failurePolicy": "Fail",
            "matchConditions": [
                {
                    "expression": (
                        "request.userInfo.username == "
                        "'system:serviceaccount:loom-staging:loom-external-slurm-autoscaler'"
                    ),
                    "name": "exact-external-autoscaler-principal",
                }
            ],
            "matchConstraints": {
                "resourceRules": [
                    {
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "operations": ["CONNECT"],
                        "resources": ["pods/exec"],
                    }
                ]
            },
            "validations": [
                {
                    "expression": (
                        "request.namespace == 'loom-dev' && "
                        "request.name.matches('^loom-capacity-manager-[a-z0-9]{1,10}-[a-z0-9]{5}$')"
                    ),
                    "message": (
                        "external Slurm autoscaler may only exec into a capacity manager "
                        "Deployment pod"
                    ),
                },
                {
                    "expression": "has(object.container) && object.container == 'manager'",
                    "message": "external Slurm autoscaler must select the manager container",
                },
                {
                    "expression": (
                        "has(object.command) && (object.command == "
                        "['python','-I','-B','-m','loom_capacity_manager.global_execution_witness',"
                        "'--pool-id','gb10'] || object.command == "
                        "['python','-I','-B','-m','loom_capacity_manager.global_execution_witness',"
                        "'--pool-id','oldlab'])"
                    ),
                    "message": (
                        "external Slurm autoscaler may only export a reviewed pool witness"
                    ),
                },
                {
                    "expression": (
                        "(!has(object.stdin) || object.stdin == false) && "
                        "(!has(object.tty) || object.tty == false) && "
                        "has(object.stdout) && object.stdout == true && "
                        "has(object.stderr) && object.stderr == true"
                    ),
                    "message": "external Slurm autoscaler exec must be non-interactive",
                },
            ],
        },
    }


def _policy_binding() -> dict[str, object]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": NAME},
        "spec": {"policyName": NAME, "validationActions": ["Deny"]},
    }


def _complete_objects() -> dict[str, dict[str, object]]:
    return {
        "role": _role(),
        "rolebinding": _role_binding(),
        "validatingadmissionpolicy": _policy(),
        "validatingadmissionpolicybinding": _policy_binding(),
    }


class _Runner:
    def __init__(self, objects: dict[str, dict[str, object]]) -> None:
        self.objects = deepcopy(objects)
        self.environment = {"KUBECONFIG": "/fixed"}
        self.calls: list[tuple[str, ...]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 30.0
        command = tuple(argv)
        self.calls.append(command)
        resource = command[command.index("get") + 1]
        singular = {
            "roles": "role",
            "rolebindings": "rolebinding",
            "validatingadmissionpolicies": "validatingadmissionpolicy",
            "validatingadmissionpolicybindings": "validatingadmissionpolicybinding",
        }[resource]
        items = []
        if singular == "role":
            items.extend(
                value for key, value in self.objects.items() if key.startswith("unrelated-role-")
            )
        if singular in self.objects:
            items.append(self.objects[singular])
        return json.dumps({"apiVersion": "v1", "items": items, "kind": "List"}).encode()

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert input_payload is None
        assert timeout_seconds == 30.0
        command = tuple(argv)
        self.calls.append(command)
        resource = command[command.index("delete") + 1]
        name = command[command.index("delete") + 2]
        assert name == NAME
        assert resource in self.objects
        del self.objects[resource]


def _epoch_exact(plan) -> ComponentObservation:
    return ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )


def _component(runner: _Runner) -> KubernetesExternalSupervisorTransitionCleanupComponent:
    return KubernetesExternalSupervisorTransitionCleanupComponent(
        runner=runner,
        environment=runner.environment,
        epoch_guard=_epoch_exact,
    )


@pytest.mark.parametrize(
    ("objects", "expected"),
    [
        (_complete_objects(), ComponentState.READY),
        (
            {
                "validatingadmissionpolicy": _policy(),
                "validatingadmissionpolicybinding": _policy_binding(),
            },
            ComponentState.READY,
        ),
        ({}, ComponentState.EXACT),
    ],
)
def test_transition_cleanup_classifies_only_complete_gc_reduced_or_absent(
    tmp_path, objects, expected
) -> None:
    plan = _plan(tmp_path)

    assert _component(_Runner(objects)).classify(plan).state is expected


def test_transition_cleanup_uses_exact_kubernetes_inventory_resources(tmp_path) -> None:
    runner = _Runner({})

    assert _component(runner).classify(_plan(tmp_path)).state is ComponentState.EXACT

    assert [call[call.index("get") + 1] for call in runner.calls] == [
        "roles",
        "rolebindings",
        "validatingadmissionpolicies",
        "validatingadmissionpolicybindings",
    ]


def test_transition_cleanup_accepts_known_api_server_defaults(tmp_path) -> None:
    objects = _complete_objects()
    policy = objects["validatingadmissionpolicy"]
    policy["spec"]["matchConstraints"]["matchPolicy"] = "Equivalent"  # type: ignore[index]
    for validation in policy["spec"]["validations"]:  # type: ignore[index]
        validation["reason"] = "Invalid"

    assert _component(_Runner(objects)).classify(_plan(tmp_path)).state is ComponentState.READY


@pytest.mark.parametrize(
    "objects",
    [
        {"role": _role()},
        {"validatingadmissionpolicy": _policy()},
        {
            "role": _role(),
            "rolebinding": _role_binding(),
            "validatingadmissionpolicy": _policy(),
        },
    ],
)
def test_transition_cleanup_rejects_every_other_partial_set(tmp_path, objects) -> None:
    assert _component(_Runner(objects)).classify(_plan(tmp_path)).state is ComponentState.DRIFTED


def test_transition_cleanup_rejects_wrong_identity_or_unrelated_exec_role(tmp_path) -> None:
    wrong_identity = _complete_objects()
    wrong_identity["rolebinding"]["subjects"] = [  # type: ignore[index]
        {"kind": "ServiceAccount", "name": "other", "namespace": "loom-staging"}
    ]
    unrelated = _complete_objects()
    unrelated["unrelated-role-other"] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "other-exec", "namespace": NAMESPACE},
        "rules": [{"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["create"]}],
    }

    assert (
        _component(_Runner(wrong_identity)).classify(_plan(tmp_path)).state
        is ComponentState.DRIFTED
    )
    assert _component(_Runner(unrelated)).classify(_plan(tmp_path)).state is ComponentState.DRIFTED


@pytest.mark.parametrize(
    ("objects", "expected_deletes"),
    [
        (
            _complete_objects(),
            [
                "rolebinding",
                "role",
                "validatingadmissionpolicybinding",
                "validatingadmissionpolicy",
            ],
        ),
        (
            {
                "validatingadmissionpolicy": _policy(),
                "validatingadmissionpolicybinding": _policy_binding(),
            },
            ["validatingadmissionpolicybinding", "validatingadmissionpolicy"],
        ),
    ],
)
def test_transition_cleanup_revokes_in_order_and_verifies_final_absence(
    tmp_path, objects, expected_deletes
) -> None:
    runner = _Runner(objects)
    component = _component(runner)
    plan = _plan(tmp_path)

    component.apply(plan)

    deletes = [call[call.index("delete") + 1] for call in runner.calls if "delete" in call]
    assert deletes == expected_deletes
    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT
    assert all("apply" not in call for call in runner.calls)
