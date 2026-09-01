"""Journal-ready revocation of the obsolete manager-exec transition authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_NAME = "loom-external-slurm-autoscaler-manager-export"
_NAMESPACE = "loom-dev"
_POD_NAME_RE = re.compile(r"^loom-capacity-manager-[a-z0-9]{1,10}-[a-z0-9]{5}$")
_UID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,127}$")
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    b"loom-protected-external-supervisor-transition-cleanup-v1"
).hexdigest()
_RESOURCE_ORDER = (
    "role",
    "rolebinding",
    "validatingadmissionpolicy",
    "validatingadmissionpolicybinding",
)
_DELETE_ORDER = (
    "rolebinding",
    "role",
    "validatingadmissionpolicybinding",
    "validatingadmissionpolicy",
)
_INVENTORY_RESOURCES = {
    "role": "roles",
    "rolebinding": "rolebindings",
    "validatingadmissionpolicy": "validatingadmissionpolicies",
    "validatingadmissionpolicybinding": "validatingadmissionpolicybindings",
}
_SAFE_PRESENT_SETS = frozenset(
    {
        frozenset(),
        frozenset(_RESOURCE_ORDER),
        frozenset({"validatingadmissionpolicy", "validatingadmissionpolicybinding"}),
    }
)


class TransitionCleanupCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class _TransitionObservation:
    state: ComponentState
    present: frozenset[str]
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class KubernetesExternalSupervisorTransitionCleanupComponent:
    """Remove only the exact predecessor authority, without any restore path."""

    runner: TransitionCleanupCommandRunner
    environment: Mapping[str, str]
    epoch_guard: EpochGuard

    def __post_init__(self) -> None:
        if not self.environment.get("KUBECONFIG") or not callable(self.epoch_guard):
            raise ValueError("external supervisor transition cleanup authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="external-supervisor-transition-cleanup",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "name": _NAME,
                    "namespace": _NAMESPACE,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._component_observation(
                plan,
                epoch,
                _TransitionObservation(ComponentState.DRIFTED, frozenset(), _hash_json({})),
            )
        try:
            observed = self._observe()
        except (OSError, RuntimeError, ValueError):
            observed = _TransitionObservation(
                ComponentState.DRIFTED,
                frozenset(),
                _hash_json({"observation": "failed"}),
            )
        return self._component_observation(plan, epoch, observed)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("external supervisor transition cleanup epoch changed")
        before = self._observe()
        if before.state is ComponentState.EXACT:
            return
        if before.state is not ComponentState.READY:
            raise RuntimeError("external supervisor transition cleanup state drifted")
        for resource in _DELETE_ORDER:
            if resource not in before.present:
                continue
            argv = ["kubectl"]
            if resource in {"role", "rolebinding"}:
                argv.extend(("--namespace", _NAMESPACE))
            argv.extend(("delete", resource, _NAME, "--wait=true"))
            self.runner.run_checked(
                tuple(argv),
                env=self.environment,
                input_payload=None,
                timeout_seconds=30.0,
            )
        after = self._observe()
        if after.state is not ComponentState.EXACT or after.present:
            raise RuntimeError("external supervisor transition cleanup did not converge")

    def _observe(self) -> _TransitionObservation:
        documents = {resource: self._list(resource) for resource in _RESOURCE_ORDER}
        roles = documents["role"]
        named: dict[str, Mapping[str, object]] = {}
        for role in roles:
            metadata = _mapping(role.get("metadata"))
            name = _string(metadata.get("name"))
            if _role_grants_pods_exec(role) and name != _NAME:
                raise ValueError("unrelated pods/exec Role is present")
        for resource, items in documents.items():
            matches = [item for item in items if _object_name(item) == _NAME]
            if len(matches) > 1:
                raise ValueError("transition cleanup object identity is ambiguous")
            if matches:
                named[resource] = matches[0]
        present = frozenset(named)
        if present not in _SAFE_PRESENT_SETS:
            state = ComponentState.DRIFTED
        elif not present:
            state = ComponentState.EXACT
        else:
            state = (
                ComponentState.READY if self._content_is_exact(named) else ComponentState.DRIFTED
            )
        return _TransitionObservation(
            state=state,
            present=present,
            evidence_digest=_hash_json(
                {
                    "objects": {
                        resource: _stable_object(item) for resource, item in sorted(named.items())
                    },
                    "present": sorted(present),
                    "state": state.value,
                }
            ),
        )

    def _list(self, resource: str) -> tuple[Mapping[str, object], ...]:
        argv = ["kubectl"]
        if resource in {"role", "rolebinding"}:
            argv.extend(("--namespace", _NAMESPACE))
        argv.extend(("get", _INVENTORY_RESOURCES[resource], "-o", "json"))
        payload = self.runner.capture_stdout(
            tuple(argv),
            env=self.environment,
            timeout_seconds=30.0,
        )
        try:
            document = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("transition cleanup inventory is invalid") from exc
        root = _mapping(document)
        items = root.get("items")
        if not isinstance(items, list):
            raise ValueError("transition cleanup inventory is invalid")
        return tuple(_mapping(item) for item in items)

    def _content_is_exact(self, named: Mapping[str, Mapping[str, object]]) -> bool:
        admission_exact = (
            _stable_object(named["validatingadmissionpolicy"]) == _expected_policy()
            and _stable_object(named["validatingadmissionpolicybinding"])
            == _expected_policy_binding()
        )
        if set(named) == {"validatingadmissionpolicy", "validatingadmissionpolicybinding"}:
            return admission_exact
        if set(named) != set(_RESOURCE_ORDER) or not admission_exact:
            return False
        role = _stable_object(named["role"])
        role_binding = _stable_object(named["rolebinding"])
        try:
            owner = _owner_reference(role)
            binding_owner = _owner_reference(role_binding)
        except ValueError:
            return False
        pod_name = _string(owner.get("name"))
        expected_role = _expected_role(pod_name=pod_name, owner=owner)
        expected_binding = _expected_role_binding(owner=owner)
        return owner == binding_owner and role == expected_role and role_binding == expected_binding

    def _component_observation(
        self,
        plan: FinalGatePlan,
        epoch: ComponentObservation,
        observed: _TransitionObservation,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=observed.state,
            evidence_digest=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "epoch_evidence_digest": epoch.evidence_digest,
                    "transition_evidence_digest": observed.evidence_digest,
                    "state": observed.state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _stable_object(value: Mapping[str, object]) -> dict[str, object]:
    metadata = _mapping(value.get("metadata"))
    stable_metadata: dict[str, object] = {"name": _string(metadata.get("name"))}
    if "namespace" in metadata:
        stable_metadata["namespace"] = _string(metadata.get("namespace"))
    if "ownerReferences" in metadata:
        owners = metadata.get("ownerReferences")
        if not isinstance(owners, list):
            raise ValueError("transition cleanup owner identity is invalid")
        stable_metadata["ownerReferences"] = [_mapping(owner) for owner in owners]
    stable: dict[str, object] = {
        "apiVersion": _string(value.get("apiVersion")),
        "kind": _string(value.get("kind")),
        "metadata": stable_metadata,
    }
    for field in ("rules", "roleRef", "subjects", "spec"):
        if field in value:
            stable[field] = deepcopy(value[field])
    if stable["kind"] == "ValidatingAdmissionPolicy" and "spec" in stable:
        spec = dict(_mapping(stable["spec"]))
        constraints = dict(_mapping(spec.get("matchConstraints")))
        if constraints.get("matchPolicy") == "Equivalent":
            constraints.pop("matchPolicy")
        spec["matchConstraints"] = constraints
        validations = spec.get("validations")
        if not isinstance(validations, list):
            raise ValueError("transition cleanup admission policy is invalid")
        normalized_validations: list[dict[str, object]] = []
        for item in validations:
            validation = dict(_mapping(item))
            if validation.get("reason") == "Invalid":
                validation.pop("reason")
            normalized_validations.append(validation)
        spec["validations"] = normalized_validations
        stable["spec"] = spec
    return stable


def _owner_reference(value: Mapping[str, object]) -> dict[str, object]:
    metadata = _mapping(value.get("metadata"))
    owners = metadata.get("ownerReferences")
    if not isinstance(owners, list) or len(owners) != 1:
        raise ValueError("transition cleanup owner identity is invalid")
    owner = dict(_mapping(owners[0]))
    if (
        set(owner)
        != {
            "apiVersion",
            "blockOwnerDeletion",
            "controller",
            "kind",
            "name",
            "uid",
        }
        or owner["apiVersion"] != "v1"
        or owner["kind"] != "Pod"
        or owner["controller"] is not False
        or owner["blockOwnerDeletion"] is not False
        or not isinstance(owner["name"], str)
        or _POD_NAME_RE.fullmatch(owner["name"]) is None
        or not isinstance(owner["uid"], str)
        or _UID_RE.fullmatch(owner["uid"]) is None
    ):
        raise ValueError("transition cleanup owner identity is invalid")
    return owner


def _expected_role(*, pod_name: str, owner: Mapping[str, object]) -> dict[str, object]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": _NAME,
            "namespace": _NAMESPACE,
            "ownerReferences": [dict(owner)],
        },
        "rules": [
            {
                "apiGroups": ["apps"],
                "resourceNames": ["loom-capacity-manager"],
                "resources": ["deployments"],
                "verbs": ["get"],
            },
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
            {
                "apiGroups": [""],
                "resourceNames": [pod_name],
                "resources": ["pods/exec"],
                "verbs": ["create"],
            },
        ],
    }


def _expected_role_binding(*, owner: Mapping[str, object]) -> dict[str, object]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": _NAME,
            "namespace": _NAMESPACE,
            "ownerReferences": [dict(owner)],
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": _NAME,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-external-slurm-autoscaler",
                "namespace": "loom-staging",
            }
        ],
    }


def _expected_policy() -> dict[str, object]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": _NAME},
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


def _expected_policy_binding() -> dict[str, object]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": _NAME},
        "spec": {"policyName": _NAME, "validationActions": ["Deny"]},
    }


def _role_grants_pods_exec(value: Mapping[str, object]) -> bool:
    rules = value.get("rules")
    if not isinstance(rules, list):
        raise ValueError("transition cleanup Role rules are invalid")
    for item in rules:
        rule = _mapping(item)
        groups = _string_list(rule.get("apiGroups"))
        resources = _string_list(rule.get("resources"))
        verbs = _string_list(rule.get("verbs"))
        if (
            any(group in {"", "*"} for group in groups)
            and any(resource in {"pods/exec", "*/exec", "*"} for resource in resources)
            and any(verb in {"create", "*"} for verb in verbs)
        ):
            return True
    return False


def _object_name(value: Mapping[str, object]) -> str:
    return _string(_mapping(value.get("metadata")).get("name"))


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("transition cleanup object is invalid")
    return value


def _string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("transition cleanup object is invalid")
    return value


def _string_list(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("transition cleanup object is invalid")
    return tuple(value)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("transition cleanup inventory has duplicate fields")
        value[key] = item
    return value


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = ["KubernetesExternalSupervisorTransitionCleanupComponent"]
