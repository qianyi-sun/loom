"""Permanent API admission fence for the inventoried staging legacy controllers.

The installed cutover must retain these objects and verify enforcement before
retiring old Pods and SQL sessions. Policy-writer exclusion remains external.
This closes the fixed controller restart paths; it is not evidence that old
processes have exited or that an unowned credential consumer does not exist.
"""

from __future__ import annotations

import json
import re

_RETIRED = ("loom-service", "loom-family-orchestrator", "loom-pipeline-orchestrator")
_SUCCESSORS = ("loom-control-plane", "loom-capacity-agent")
_CRONJOB = "loom-staging-data-lifecycle"


def _policy_pair(*, intent_digest: str, suffix: str, group: str, resources: list[str], scope: str, expression: str,
         variables: list[dict[str, str]] | None = None) -> tuple[dict[str, object], ...]:
    name = f"loom-legacy-writer-{intent_digest[:24]}-{suffix}"
    metadata = {"name": name, "annotations": {"loom.dev/legacy-writer-retirement": intent_digest}}
    spec: dict[str, object] = {
        "failurePolicy": "Fail",
        "matchConstraints": {"matchPolicy": "Equivalent", "namespaceSelector": {}, "objectSelector": {},
            "resourceRules": [{"apiGroups": [group], "apiVersions": ["v1"],
                "operations": ["CREATE", "UPDATE"], "resources": resources, "scope": "Namespaced"}]},
        "matchConditions": [{"name": "fixed-staging-writers", "expression":
            "request.namespace == 'loom-staging' && (" + scope + ")"}],
        "validations": [{"expression": expression, "reason": "Forbidden",
            "message": "loom-legacy-writer-retirement: legacy writer cannot resume"}],
    }
    if variables:
        spec["variables"] = variables
    return (
        {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
         "metadata": metadata, "spec": spec},
        {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
         "metadata": metadata, "spec": {"policyName": name, "validationActions": ["Deny"]}},
    )


def lifecycle_retirement_documents(intent_digest: str) -> tuple[dict[str, object], ...]:
    if not isinstance(intent_digest, str) or re.fullmatch(r"[0-9a-f]{64}", intent_digest) is None or intent_digest == "0" * 64:
        raise ValueError("lifecycle retirement authority is invalid")
    return _policy_pair(intent_digest=intent_digest, suffix="lifecycle", group="batch", resources=["cronjobs", "jobs"],
        scope=f"request.name == '{_CRONJOB}' || request.name.startsWith('{_CRONJOB}-')",
        expression="has(object.spec.suspend) && object.spec.suspend == true")


def render_legacy_writer_fence(*, intent_digest: str, control_plane_image: str) -> tuple[dict[str, object], ...]:
    if (
        not isinstance(intent_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", intent_digest) is None or intent_digest == "0" * 64
        or not isinstance(control_plane_image, str)
        or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.:/_-]*/loom-control-plane@sha256:[0-9a-f]{64}", control_plane_image) is None
    ):
        raise ValueError("legacy writer retirement authority is invalid")
    result: list[dict[str, object]] = []
    writers = json.dumps([*_RETIRED, *_SUCCESSORS])

    def pair(suffix: str, group: str, resources: list[str], scope: str, expression: str,
             variables: list[dict[str, str]] | None = None) -> None:
        result.extend(_policy_pair(intent_digest=intent_digest, suffix=suffix, group=group,
            resources=resources, scope=scope, expression=expression, variables=variables))

    # Scale's typed zero replica field may be omitted in admission serialization;
    # absence there means zero, unlike Deployment's default of one.
    # No scale-only request can prove a successor template. Its reviewed full
    # Deployment update remains available; old HPA/scale requests stay refused.
    pair("scale", "apps", ["deployments/scale"], f"request.name in {writers}",
         "!has(object.spec.replicas) || object.spec.replicas == 0")
    labels = "has(object.metadata.labels) ? object.metadata.labels : {}"
    workload = (
        "'app' in variables.labels ? variables.labels['app'] : "
        "('app.kubernetes.io/name' in variables.labels ? variables.labels['app.kubernetes.io/name'] : '')"
    )
    image = json.dumps(control_plane_image)
    successor = (
        "size(variables.pod.containers) == 1 && variables.pod.containers.all(c, "
        f"c.image == {image} && ("
        "(variables.workload == 'loom-control-plane' && c.name == 'control-plane' && "
        "(!has(c.command) || size(c.command) == 0) && (!has(c.args) || size(c.args) == 0) && "
        "has(c.env) && "
        "c.env.filter(e, e.name == 'LOOM_CP_PROTECTED_TRIAL_CUTOVER_ENABLED').size() == 1 && "
        "c.env.exists(e, e.name == 'LOOM_CP_PROTECTED_TRIAL_CUTOVER_ENABLED' && has(e.value) && e.value == 'true') && "
        "c.env.filter(e, e.name == 'LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE').size() == 1 && "
        "c.env.exists(e, e.name == 'LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE' && has(e.value) && "
        "e.value == '/run/loom/protected-worker-runtime/files/database-url')) || "
        "(variables.workload == 'loom-capacity-agent' && c.name == 'capacity-agent' && has(c.command) && "
        "size(c.command) >= 3 && c.command[0] == 'python' && c.command[1] == '-m' && "
        "c.command[2] == 'loom_capacity_agent.runtime')) )"
    )
    pair("deployments", "apps", ["deployments"], f"request.name in {writers}",
         "(has(object.spec.replicas) && object.spec.replicas == 0) || (" + successor + ")",
         [{"name": "pod", "expression": "object.spec.template.spec"},
          {"name": "workload", "expression": "request.name"}])
    prefix_scope = " || ".join(f"request.name.startsWith('{name}-')" for name in (*_RETIRED, *_SUCCESSORS))
    # Generated ReplicaSets carry the same app label as their Deployment. Match
    # known name prefixes too, so removing that label cannot reopen an old RS.
    pair("replicasets", "apps", ["replicasets"],
         f"({prefix_scope}) || (has(object.metadata.labels) && object.metadata.labels.exists(k, "
         f"k in ['app', 'app.kubernetes.io/name'] && object.metadata.labels[k] in {writers}))",
         "(has(object.spec.replicas) && object.spec.replicas == 0) || (" + successor + ")",
         [{"name": "labels", "expression": labels}, {"name": "workload", "expression": workload},
          {"name": "pod", "expression": "object.spec.template.spec"}])
    pair("replicaset-scale", "apps", ["replicasets/scale"], prefix_scope,
         "!has(object.spec.replicas) || object.spec.replicas == 0")
    pod_writers = json.dumps([*_RETIRED, *_SUCCESSORS, _CRONJOB])
    pair("pods", "", ["pods"],
         f"({prefix_scope}) || request.name.startsWith('{_CRONJOB}-') || "
         "(has(object.metadata.labels) && object.metadata.labels.exists(k, "
         f"k in ['app', 'app.kubernetes.io/name'] && object.metadata.labels[k] in {pod_writers}))",
         successor,
         [{"name": "labels", "expression": labels}, {"name": "workload", "expression": workload},
          {"name": "pod", "expression": "object.spec"}])
    result.extend(lifecycle_retirement_documents(intent_digest))
    return tuple(result)


def legacy_writer_fence_probe_commands(*, intent_digest: str, replica_set_name: str,
                                     ) -> tuple[tuple[str, tuple[str, ...], bytes | None], ...]:
    """Fixed server dry runs; the caller binds an existing inventoried ReplicaSet."""
    lifecycle_retirement_documents(intent_digest)
    prefixes = "|".join(re.escape(name) for name in (*_RETIRED, *_SUCCESSORS))
    if not isinstance(replica_set_name, str) or re.fullmatch(f"(?:{prefixes})-[a-z0-9][a-z0-9-]{{0,62}}", replica_set_name) is None:
        raise ValueError("legacy writer fence probe ReplicaSet is invalid")
    prefix = ("kubectl", "--namespace=loom-staging")
    flags = ("--dry-run=server", "--output=json", "--request-timeout=30s")
    result = []
    def add(suffix: str, command: tuple[str, ...], document: dict[str, object] | None = None) -> None:
        payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode() if document is not None else None
        result.append((f"loom-legacy-writer-{intent_digest[:24]}-{suffix}", prefix + command + flags, payload))
    add("scale", ("scale", "deployment/loom-service", "--replicas=1"))
    add("deployments", ("patch", "deployment/loom-service", "--type=merge", '--patch={"spec":{"replicas":1}}'))
    name = f"loom-service-fence-probe-{intent_digest[:12]}"
    pod: dict[str, object] = {"containers": [{"name": "retired-probe", "image": "invalid.example/retired-probe:never-run"}]}
    add("replicasets", ("create", "--filename=-"), {
        "apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {"name": name},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "loom-service"}},
            "template": {"metadata": {"labels": {"app": "loom-service"}}, "spec": pod}},
    })
    add("replicaset-scale", ("scale", "replicaset/" + replica_set_name, "--replicas=1"))
    add("pods", ("create", "--filename=-"), {"apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": {"app": "loom-service"}}, "spec": pod})
    add("lifecycle", ("create", "--filename=-"), {"apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": f"{_CRONJOB}-fence-probe-{intent_digest[:12]}"},
        "spec": {"suspend": False, "template": {"spec": {**pod, "restartPolicy": "Never"}}}})
    return tuple(result)
