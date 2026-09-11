"""Render a temporary declared-input write fence; never install or authorize it.

The protected caller must journal exact owned policies/bindings, verify actual
enforcement, and exclude policy/authority writers before any handoff. This does
not drain prior API requests, cached CNPG actions, or privileged SQL sessions.
The caller must retire those under the fence and admit running processes/images.
No deadline or lost caller automatically releases the fence. Restart is denied:
postmaster-disruptive retirement must precede the surviving database-backed guard.
"""

from __future__ import annotations

import json
import re


def _validate_target_pooler_names(target_pooler_names: tuple[str, ...]) -> None:
    if (
        not isinstance(target_pooler_names, tuple)
        or any(not isinstance(name, str) or len(name) > 253
               or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", name) is None
               for name in target_pooler_names)
        or len(set(target_pooler_names)) != len(target_pooler_names)
    ):
        raise ValueError("CNPG input fence Pooler inventory is invalid")


def render_cnpg_input_fence(
    *, intent_digest: str,
    target_pooler_names: tuple[str, ...],
) -> tuple[dict[str, object], ...]:
    """Bind fixed staging scope without a restart exception during guarded handoff.

    Even installed administrators cannot change the restart annotation through
    this fence. The fence does not protect its own removal; other holders of the
    same Kubernetes identity and policy writers must be excluded separately.
    It blocks declared SQL-writer inputs, not all CNPG operator or pod mutation.
    The caller must bind a complete target Pooler name inventory and retire
    pre-fence API requests. Scale objects omit their parent cluster reference.
    The no-target-Pooler handoff profile must explicitly supply an empty tuple.
    """
    if (
        not isinstance(intent_digest, str) or re.fullmatch(r"[0-9a-f]{64}", intent_digest) is None
    ):
        raise ValueError("CNPG input fence authority is invalid")
    _validate_target_pooler_names(target_pooler_names)
    result: list[dict[str, object]] = []

    def pair(
        suffix: str, group: str, resources: list[str], scope: str,
        expression: str, variables: list[dict[str, str]] | None = None,
    ) -> None:
        name = f"loom-cnpg-fence-{intent_digest[:24]}-{suffix}"
        metadata = {"name": name, "annotations": {"loom.dev/handoff-intent": intent_digest}}
        spec: dict[str, object] = {
            "failurePolicy": "Fail",
            "matchConstraints": {"matchPolicy": "Equivalent", "namespaceSelector": {}, "objectSelector": {}, "resourceRules": [{
                "apiGroups": [group], "apiVersions": ["v1"],
                "operations": ["CREATE", "UPDATE", "DELETE"], "resources": resources, "scope": "Namespaced",
            }]},
            "matchConditions": [{"name": "fixed-staging-input", "expression":
                                  "request.namespace == 'loom-staging' && (" + scope + ")"}],
            "validations": [{"expression": expression,
                             "reason": "Forbidden",
                             "message": "loom-cnpg-fence: protected handoff input is frozen"}],
        }
        if variables:
            spec["variables"] = variables
        result.extend((
            {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
             "metadata": metadata, "spec": spec},
            {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
             "metadata": {"name": name, "annotations": {"loom.dev/handoff-intent": intent_digest}},
             "spec": {"policyName": name, "validationActions": ["Deny"]}},
        ))

    variables = []
    for prefix, obj in (("before", "oldObject"), ("after", "object")):
        for key in ("annotations", "labels"):
            variables.append({"name": prefix + key.capitalize(), "expression":
                              f"{obj} != null && has({obj}.metadata.{key}) ? {obj}.metadata.{key} : {{}}"})
    same_annotations = "variables.beforeAnnotations == variables.afterAnnotations"
    # Admission on /status must cover inputs used by in-pod pooler/role writers,
    # while normal health, primary selection and replication status can advance.
    # CNPG declares these as structural objects. Explicit dyn conversion makes
    # their absent->empty normalization well typed, preserving whole-value equality.
    writer_status_equal = " && ".join(
        f"(has(object.status) && has(object.status.{field}) ? dyn(object.status.{field}) : {{}}) == "
        f"(has(oldObject.status) && has(oldObject.status.{field}) ? dyn(oldObject.status.{field}) : {{}})"
        for field in ("poolerIntegrations", "managedRolesStatus")
    )
    pair("cluster", "postgresql.cnpg.io", ["clusters", "clusters/status"],
         "request.name == 'loom-postgres'",
         "request.operation == 'UPDATE' && object != null && oldObject != null && "
         "object.spec == oldObject.spec && "
         "(has(object.metadata.finalizers) ? object.metadata.finalizers : []) == "
         "(has(oldObject.metadata.finalizers) ? oldObject.metadata.finalizers : []) && "
         "variables.beforeLabels == variables.afterLabels && "
         f"({same_annotations}) && ({writer_status_equal})",
         variables)
    target = " || ".join(
        f"({obj} != null && has({obj}.spec) && has({obj}.spec.cluster) && "
        f"has({obj}.spec.cluster.name) && {obj}.spec.cluster.name == 'loom-postgres')"
        for obj in ("object", "oldObject")
    )
    pair("dependents", "postgresql.cnpg.io", ["databases", "databases/status", "poolers", "poolers/status",
                                              "publications", "publications/status",
                                              "subscriptions", "subscriptions/status"],
         target, "false")
    poolers = json.dumps(sorted(target_pooler_names))
    pair("scale", "postgresql.cnpg.io", ["clusters/scale", "poolers/scale"],
         "(request.resource.resource == 'clusters' && request.name == 'loom-postgres') || "
         f"(request.resource.resource == 'poolers' && request.name in {poolers})", "false")
    pair("credentials", "", ["secrets"],
         "request.name in ['loom-secrets', 'loom-postgres-cnpg-credentials']", "false")
    pair("monitoring", "", ["configmaps"], "request.name == 'cnpg-default-monitoring'", "false")
    return tuple(result)


def cnpg_input_fence_probe_commands(
    *, intent_digest: str, target_pooler_names: tuple[str, ...],
) -> tuple[tuple[str, tuple[str, ...], bytes | None], ...]:
    """Fixed server-dry-run requests; no supplied command, namespace or payload.

    These establish observed API enforcement only. They neither install a fence
    nor prove process retirement or that admission authority cannot change.
    """
    if not isinstance(intent_digest, str) or re.fullmatch(r"[0-9a-f]{64}", intent_digest) is None:
        raise ValueError("CNPG input fence probe intent is invalid")
    _validate_target_pooler_names(target_pooler_names)
    prefix = ("kubectl", "--namespace", "loom-staging")
    flags = ("--dry-run=server", "--output=json", "--request-timeout=30s")
    patch = json.dumps({"metadata": {"annotations": {"loom.dev/cnpg-fence-probe": intent_digest}}})
    requests: list[tuple[str, tuple[str, ...], bytes | None]] = []

    def add(suffix: str, command: tuple[str, ...], payload: bytes | None = None) -> None:
        requests.append((f"loom-cnpg-fence-{intent_digest[:24]}-{suffix}", prefix + command + flags, payload))

    add("cluster", ("patch", "cluster.postgresql.cnpg.io", "loom-postgres", "--type=merge", "--patch=" + patch))
    add("dependents", ("create", "--filename=-"), json.dumps({
        "apiVersion": "postgresql.cnpg.io/v1", "kind": "Database",
        "metadata": {"name": "loom-cnpg-fence-probe", "namespace": "loom-staging"},
        "spec": {"cluster": {"name": "loom-postgres"}, "name": "loom_cnpg_fence_probe", "owner": "loom"},
    }).encode())
    add("scale", ("scale", "cluster.postgresql.cnpg.io", "loom-postgres", "--replicas=1"))
    for name in ("loom-secrets", "loom-postgres-cnpg-credentials"):
        add("credentials", ("patch", "secret", name, "--type=merge", "--patch=" + patch))
    add("monitoring", ("patch", "configmap", "cnpg-default-monitoring", "--type=merge", "--patch=" + patch))
    for name in sorted(target_pooler_names):
        add("scale", ("scale", "pooler.postgresql.cnpg.io", name, "--replicas=1"))
    return tuple(requests)
