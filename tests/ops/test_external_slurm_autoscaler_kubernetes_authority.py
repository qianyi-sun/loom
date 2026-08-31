"""Contracts for the external Slurm autoscaler's Kubernetes authority."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "deploy/k8s/external-slurm-autoscaler-authority.yaml"
MANAGER_BINDING = (
    ROOT / "deploy/k8s/external-slurm-autoscaler-manager-export-binding.yaml"
)
PUBLISHER = ROOT / "deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh"


def _documents() -> list[dict[str, object]]:
    return [
        document
        for document in yaml.safe_load_all(MANIFEST.read_text(encoding="utf-8"))
        if isinstance(document, dict)
    ]


def _manager_binding_documents() -> list[dict[str, object]]:
    return [
        document
        for document in yaml.safe_load_all(MANAGER_BINDING.read_text(encoding="utf-8"))
        if isinstance(document, dict)
    ]


def _document(*, kind: str, name: str, namespace: str) -> dict[str, object]:
    matches = [
        document
        for document in _documents()
        if document["kind"] == kind
        and document["metadata"]["name"] == name  # type: ignore[index]
        and document["metadata"]["namespace"] == namespace  # type: ignore[index]
    ]
    assert len(matches) == 1
    return matches[0]


def test_authority_is_namespace_scoped_and_uses_a_dedicated_token() -> None:
    documents = _documents()
    assert [document["kind"] for document in documents] == [
        "ServiceAccount",
        "Secret",
        "Role",
        "RoleBinding",
        "ValidatingAdmissionPolicy",
        "ValidatingAdmissionPolicyBinding",
        "Role",
    ]
    assert [document["metadata"].get("namespace") for document in documents] == [  # type: ignore[index]
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-staging",
        None,
        None,
        "loom-dev",
    ]
    assert all(document["kind"] != "ClusterRole" for document in documents)
    token = documents[1]
    assert token["type"] == "kubernetes.io/service-account-token"
    assert token["metadata"]["annotations"] == {  # type: ignore[index]
        "kubernetes.io/service-account.name": "loom-external-slurm-autoscaler",
    }


def test_authority_can_read_only_the_dedicated_db_secret() -> None:
    role = _document(
        kind="Role",
        name="loom-external-slurm-autoscaler",
        namespace="loom-staging",
    )
    rules = role["rules"]  # type: ignore[index]
    secret_rules = [rule for rule in rules if "secrets" in rule["resources"]]
    assert secret_rules == [
        {
            "apiGroups": [""],
            "resources": ["secrets"],
            "resourceNames": ["loom-external-slurm-autoscaler-db"],
            "verbs": ["get"],
        }
    ]
    assert all("loom-secrets" not in str(rule) for rule in rules)


def test_authority_has_only_the_reads_and_port_forward_needed_for_postgres() -> None:
    rules = _document(
        kind="Role",
        name="loom-external-slurm-autoscaler",
        namespace="loom-staging",
    )["rules"]  # type: ignore[index]
    assert {
        "apiGroups": [""],
        "resources": ["services", "endpoints", "pods"],
        "verbs": ["get", "list"],
    } in rules
    assert {
        "apiGroups": [""],
        "resources": ["pods/portforward"],
        "verbs": ["create"],
    } in rules
    verbs = {verb for rule in rules for verb in rule["verbs"]}
    assert verbs <= {"get", "list", "create"}


def test_manager_export_authority_is_bound_cross_namespace_and_narrow() -> None:
    role_name = "loom-external-slurm-autoscaler-manager-export"
    role = _document(kind="Role", name=role_name, namespace="loom-dev")
    assert role["rules"] == [
        {
            "apiGroups": ["apps"],
            "resources": ["deployments"],
            "resourceNames": ["loom-capacity-manager"],
            "verbs": ["get"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "list"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods/exec"],
            "resourceNames": ["loom-capacity-manager-publisher-must-replace"],
            "verbs": ["create"],
        },
    ]
    binding_documents = _manager_binding_documents()
    assert len(binding_documents) == 1
    binding = binding_documents[0]
    assert binding["kind"] == "RoleBinding"
    assert binding["metadata"] == {"name": role_name, "namespace": "loom-dev"}
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-external-slurm-autoscaler",
            "namespace": "loom-staging",
        }
    ]
    assert binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "Role",
        "name": role_name,
    }


def test_manager_export_exec_is_admission_confined_to_capacity_manager_pods() -> None:
    policy_name = "loom-external-slurm-autoscaler-manager-export"
    policies = [
        document
        for document in _documents()
        if document["kind"] == "ValidatingAdmissionPolicy"
        and document["metadata"]["name"] == policy_name  # type: ignore[index]
    ]
    assert len(policies) == 1
    spec = policies[0]["spec"]  # type: ignore[index]
    assert spec["failurePolicy"] == "Fail"
    assert spec["matchConstraints"]["resourceRules"] == [
        {
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "operations": ["CONNECT"],
            "resources": ["pods/exec"],
        }
    ]
    assert spec["matchConditions"] == [
        {
            "name": "exact-external-autoscaler-principal",
            "expression": (
                "request.userInfo.username == "
                "'system:serviceaccount:loom-staging:loom-external-slurm-autoscaler'"
            ),
        }
    ]
    assert spec["validations"] == [
        {
            "expression": (
                "request.namespace == 'loom-dev' && "
                "request.name.matches("
                "'^loom-capacity-manager-[a-z0-9]{1,10}-[a-z0-9]{5}$')"
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
                "['python','-I','-B','-m',"
                "'loom_capacity_manager.global_execution_witness',"
                "'--pool-id','gb10'] || object.command == "
                "['python','-I','-B','-m',"
                "'loom_capacity_manager.global_execution_witness',"
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
    ]
    bindings = [
        document
        for document in _documents()
        if document["kind"] == "ValidatingAdmissionPolicyBinding"
        and document["metadata"]["name"] == policy_name  # type: ignore[index]
    ]
    assert len(bindings) == 1
    assert bindings[0]["spec"] == {
        "policyName": policy_name,
        "validationActions": ["Deny"],
    }


def _write_fake_kubectl(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import base64
import json
import os
import pathlib
import sys

args = sys.argv[1:]
state_path = pathlib.Path(os.environ["KUBECTL_STATE"])
log_path = pathlib.Path(os.environ.get("KUBECTL_LOG", f"{state_path}.log"))


def log(event):
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(event + "\\n")


def state():
    try:
        return state_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


if "create" in args and "--raw" in args:
    payload = json.load(sys.stdin)
    if payload != {
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectRulesReview",
        "spec": {"namespace": "loom-dev"},
    }:
        print("invalid self-subject rules review", file=sys.stderr)
        raise SystemExit(7)
    observations = sum(
        line.startswith("revocation-")
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ) if log_path.exists() else 0
    delay = int(os.environ.get("REVOCATION_DELAY", "0"))
    mode = os.environ.get("RULE_REVIEW_MODE", "")
    incomplete = mode == "incomplete"
    evaluation_error = "authorizer review failed" if mode == "evaluation-error" else ""
    resource_rules = [{
        "apiGroups": [""],
        "resources": ["pods"],
        "verbs": ["get", "list"],
    }]
    if observations < delay:
        resource_rules.append({
            "apiGroups": [""],
            "resources": ["pods/exec"],
            "resourceNames": ["old-manager-pod"],
            "verbs": ["create"],
        })
        log("revocation-pending")
    elif mode:
        wildcard_rules = {
            "all-resources": {
                "apiGroups": [""],
                "resources": ["*"],
                "verbs": ["create"],
            },
            "all-exec-subresources": {
                "apiGroups": [""],
                "resources": ["*/exec"],
                "verbs": ["create"],
            },
            "all-api-groups": {
                "apiGroups": ["*"],
                "resources": ["pods/exec"],
                "verbs": ["create"],
            },
            "all-verbs": {
                "apiGroups": [""],
                "resources": ["pods/exec"],
                "verbs": ["*"],
            },
            "incomplete": {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get"],
            },
            "evaluation-error": {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get"],
            },
        }
        resource_rules.append(wildcard_rules[mode])
        log("revocation-pending")
    else:
        state_path.write_text("revoked-observed", encoding="utf-8")
        log("revocation-observed")
    print(json.dumps({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectRulesReview",
        "status": {
            "resourceRules": resource_rules,
            "nonResourceRules": [],
            "incomplete": incomplete,
            "evaluationError": evaluation_error,
        },
    }))
    raise SystemExit(0)
if "apply" in args and args[-1:] == ["-"]:
    payload = sys.stdin.buffer.read()
    if b"kind: RoleBinding" in payload:
        expected = (
            b"name: loom-capacity-manager-12345-abcde\\n"
            b"    uid: pod-uid\\n"
        )
        if state() != "role" or expected not in payload:
            print("manager binding was not owner-bound or role-ready", file=sys.stderr)
            raise SystemExit(8)
        state_path.write_text("binding", encoding="utf-8")
        log("binding")
    elif b"name: loom-external-slurm-autoscaler-manager-export" in payload:
        expected = b"resourceNames: [loom-capacity-manager-12345-abcde]"
        owner = (
            b"name: loom-capacity-manager-12345-abcde\\n"
            b"    uid: pod-uid\\n"
        )
        if state() != "policy-ready" or expected not in payload or owner not in payload:
            print("manager role was not exact, owner-bound, or policy-ready", file=sys.stderr)
            raise SystemExit(6)
        state_path.write_text("role", encoding="utf-8")
        log("role:loom-capacity-manager-12345-abcde")
    raise SystemExit(0)
if "delete" in args and "-f" in args:
    manifest = args[args.index("-f") + 1]
    if manifest.endswith("external-slurm-autoscaler-manager-export-binding.yaml"):
        state_path.write_text("revoked", encoding="utf-8")
        log("binding-revoked")
    raise SystemExit(0)
if "auth" in args and "can-i" in args and "--list" in args:
    observations = sum(
        line.startswith("revocation-")
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ) if log_path.exists() else 0
    delay = int(os.environ.get("REVOCATION_DELAY", "0"))
    print("Resources Non-Resource URLs Resource Names Verbs")
    if observations < delay:
        print("pods/exec [] [old-manager-pod] [create]")
        log("revocation-pending")
    else:
        print("pods [] [] [get list]")
        state_path.write_text("revoked-observed", encoding="utf-8")
        log("revocation-observed")
    raise SystemExit(0)
if "apply" in args and "-f" in args:
    manifest = args[args.index("-f") + 1]
    if manifest.endswith("external-slurm-autoscaler-authority.yaml"):
        post_binding_cleanup = state() == "revoked" and log_path.exists() and any(
            line == "binding"
            for line in log_path.read_text(encoding="utf-8").splitlines()
        )
        if state() != "revoked-observed" and not post_binding_cleanup:
            print("authority changed before effective manager binding revocation", file=sys.stderr)
            raise SystemExit(2)
        state_path.write_text("authority", encoding="utf-8")
        log("authority")
    elif manifest.endswith("external-slurm-autoscaler-manager-export-binding.yaml"):
        print("static manager binding must not be applied", file=sys.stderr)
        raise SystemExit(3)
    raise SystemExit(0)
if "get" in args and "validatingadmissionpolicy" in args:
    if state() not in {"authority", "policy-ready"}:
        raise SystemExit(4)
    output = args[-1]
    if "metadata.generation" in output:
        print("2", end="")
    elif "status.observedGeneration" in output:
        observations = sum(
            line == "policy-observation"
            for line in log_path.read_text(encoding="utf-8").splitlines()
        ) if log_path.exists() else 0
        log("policy-observation")
        if observations < int(os.environ.get("POLICY_STALE_ATTEMPTS", "0")):
            print("1|", end="")
        elif os.environ.get("POLICY_WARNING"):
            print("2|expression warning", end="")
        else:
            print("2|", end="")
            state_path.write_text("policy-ready", encoding="utf-8")
    raise SystemExit(0)
if (
    "auth" not in args
    and "get" in args
    and "deployment/loom-capacity-manager" in args
):
    print("deployment-uid", end="")
    raise SystemExit(0)
if "get" in args and "pods" in args and "--selector" in args:
    print("pod/loom-capacity-manager-12345-abcde")
    raise SystemExit(0)
if "wait" in args and "pod/loom-capacity-manager-12345-abcde" in args:
    raise SystemExit(0)
if "get" in args and "pod/loom-capacity-manager-12345-abcde" in args:
    output = args[-1]
    if "deletionTimestamp" in output:
        observations = sum(
            line == "pod-identity-observation"
            for line in log_path.read_text(encoding="utf-8").splitlines()
        ) if log_path.exists() else 0
        log("pod-identity-observation")
        if observations and os.environ.get("POD_IDENTITY_CHANGES"):
            print("|replacement-pod-uid", end="")
        elif observations >= 2 and os.environ.get("POD_IDENTITY_CHANGES_AFTER_BINDING"):
            print("|replacement-pod-uid", end="")
        else:
            print("|pod-uid", end="")
    elif os.environ.get("INVALID_POD_OWNER"):
        print("StatefulSet|foreign|foreign-uid|true", end="")
    elif os.environ.get("SPOOF_POD_REPLICASET_LINK"):
        print("ReplicaSet|loom-capacity-manager-67890|replicaset-uid|true", end="")
    else:
        print("ReplicaSet|loom-capacity-manager-12345|replicaset-uid|true", end="")
    raise SystemExit(0)
if "get" in args and any(
    value in args
    for value in (
        "replicaset.apps/loom-capacity-manager-12345",
        "replicaset.apps/loom-capacity-manager-67890",
    )
):
    print("replicaset-uid|Deployment|loom-capacity-manager|deployment-uid|true", end="")
    raise SystemExit(0)
if "auth" in args and "can-i" in args:
    can_i = args[args.index("can-i") + 1 :]
    namespace_flag = "--namespace"
    namespace = can_i[can_i.index(namespace_flag) + 1]
    permission = " ".join(can_i[: can_i.index(namespace_flag)])
    if namespace == "loom-dev" and state() != "binding":
        print("manager binding was not installed safely", file=sys.stderr)
        raise SystemExit(5)
    if namespace == "loom-dev" and permission == os.environ.get("DENY_AUTH"):
        print("no")
        raise SystemExit(1)
    print("yes")
    raise SystemExit(0)
if "get" in args and "secret" in args:
    secret_name = args[args.index("secret") + 1]
    output = args[-1]
    if secret_name == "loom-secrets":
        print(base64.b64encode(b"postgresql://test.invalid/loom").decode())
    elif secret_name == "loom-external-slurm-autoscaler-token" and "token" in output:
        print(base64.b64encode(b"test-token").decode())
    elif secret_name == "loom-external-slurm-autoscaler-token" and "ca" in output and "crt" in output:
        print(base64.b64encode(b"test-ca").decode())
    else:
        print(f"secret/{secret_name}")
    raise SystemExit(0)
if "create" in args and "secret" in args and "generic" in args:
    print("apiVersion: v1")
    print("kind: Secret")
    raise SystemExit(0)
raise SystemExit(0)
""",
        encoding="utf-8",
    )
    path.chmod(0o700)


@pytest.mark.parametrize(
    "denied_permission",
    [
        "get deployment/loom-capacity-manager",
        "list pods",
        "create pod/loom-capacity-manager-12345-abcde --subresource=exec",
    ],
)
def test_publisher_refuses_kubeconfig_without_manager_export_authority(
    tmp_path: Path,
    denied_permission: str,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env={
            "PATH": os.environ["PATH"],
            "KUBECONFIG": str(admin_kubeconfig),
            "KUBECTL": str(fake_kubectl),
            "KUBECTL_STATE": str(state),
            "DENY_AUTH": denied_permission,
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert (
        f"error: external autoscaler lacks required manager-export authority: {denied_permission}"
    ) in result.stderr
    assert not output.exists()


def test_publisher_grants_manager_export_only_after_policy_is_ready(
    tmp_path: Path,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env={
            "PATH": os.environ["PATH"],
            "KUBECONFIG": str(admin_kubeconfig),
            "KUBECTL": str(fake_kubectl),
            "KUBECTL_STATE": str(state),
            "DENY_AUTH": "",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert state.read_text(encoding="utf-8") == "binding"
    assert output.is_file()
    assert output.stat().st_mode & 0o777 == 0o600


def test_publisher_waits_for_effective_revocation_and_exact_live_pod(
    tmp_path: Path,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    fake_sleep = tmp_path / "sleep"
    fake_sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o700)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"
    log = tmp_path / "kubectl.log"

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env={
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "KUBECONFIG": str(admin_kubeconfig),
            "KUBECTL": str(fake_kubectl),
            "KUBECTL_STATE": str(state),
            "KUBECTL_LOG": str(log),
            "REVOCATION_DELAY": "2",
            "POLICY_STALE_ATTEMPTS": "2",
            "DENY_AUTH": "",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    events = log.read_text(encoding="utf-8").splitlines()
    assert events.index("revocation-observed") < events.index("authority")
    assert events.index("authority") < events.index("role:loom-capacity-manager-12345-abcde")
    assert events.index("role:loom-capacity-manager-12345-abcde") < events.index("binding")
    assert events.count("revocation-pending") == 2
    assert events.count("policy-observation") == 3


@pytest.mark.parametrize(
    ("extra_env", "expected_error"),
    [
        ({"REVOCATION_DELAY": "99"}, "manager-export exec authority was not revoked"),
        ({"POLICY_STALE_ATTEMPTS": "99"}, "admission policy was not observed"),
        ({"POLICY_WARNING": "1"}, "admission policy has type-checking warnings"),
        ({"INVALID_POD_OWNER": "1"}, "manager-export pod ownership is invalid"),
        ({"SPOOF_POD_REPLICASET_LINK": "1"}, "manager-export pod ownership is invalid"),
        (
            {"POD_IDENTITY_CHANGES": "1"},
            "manager-export pod identity changed during publication",
        ),
    ],
)
def test_publisher_fails_closed_before_binding_on_authority_transition_error(
    tmp_path: Path,
    extra_env: dict[str, str],
    expected_error: str,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    fake_sleep = tmp_path / "sleep"
    fake_sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o700)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"
    log = tmp_path / "kubectl.log"
    env = {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "KUBECONFIG": str(admin_kubeconfig),
        "KUBECTL": str(fake_kubectl),
        "KUBECTL_STATE": str(state),
        "KUBECTL_LOG": str(log),
        "DENY_AUTH": "",
        **extra_env,
    }

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert expected_error in result.stderr
    assert not output.exists()
    if log.exists():
        assert "binding" not in log.read_text(encoding="utf-8").splitlines()


@pytest.mark.parametrize(
    "rule_review_mode",
    [
        "all-resources",
        "all-exec-subresources",
        "all-api-groups",
        "all-verbs",
        "incomplete",
        "evaluation-error",
    ],
)
def test_publisher_does_not_trust_incomplete_or_wildcard_revocation(
    tmp_path: Path,
    rule_review_mode: str,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    fake_sleep = tmp_path / "sleep"
    fake_sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o700)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"
    log = tmp_path / "kubectl.log"

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env={
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "KUBECONFIG": str(admin_kubeconfig),
            "KUBECTL": str(fake_kubectl),
            "KUBECTL_STATE": str(state),
            "KUBECTL_LOG": str(log),
            "RULE_REVIEW_MODE": rule_review_mode,
            "DENY_AUTH": "",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "manager-export exec authority was not revoked" in result.stderr
    assert not output.exists()
    assert "binding" not in log.read_text(encoding="utf-8").splitlines()


def test_publisher_revokes_and_resets_authority_on_post_binding_identity_race(
    tmp_path: Path,
) -> None:
    fake_kubectl = tmp_path / "kubectl"
    _write_fake_kubectl(fake_kubectl)
    fake_sleep = tmp_path / "sleep"
    fake_sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o700)
    admin_kubeconfig = tmp_path / "admin-kubeconfig"
    admin_kubeconfig.write_text("admin", encoding="utf-8")
    output = tmp_path / "published-kubeconfig"
    state = tmp_path / "kubectl.state"
    log = tmp_path / "kubectl.log"

    result = subprocess.run(
        [str(PUBLISHER), str(output)],
        env={
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "KUBECONFIG": str(admin_kubeconfig),
            "KUBECTL": str(fake_kubectl),
            "KUBECTL_STATE": str(state),
            "KUBECTL_LOG": str(log),
            "POD_IDENTITY_CHANGES_AFTER_BINDING": "1",
            "DENY_AUTH": "",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "manager-export pod identity changed after binding" in result.stderr
    assert not output.exists()
    events = log.read_text(encoding="utf-8").splitlines()
    binding = events.index("binding")
    binding_revoked = events.index("binding-revoked", binding + 1)
    sentinel_reset = events.index("authority", binding_revoked + 1)
    revocation_observed = events.index("revocation-observed", sentinel_reset + 1)
    assert binding < binding_revoked < sentinel_reset < revocation_observed


def test_publisher_avoids_secret_values_in_process_arguments() -> None:
    source = PUBLISHER.read_text(encoding="utf-8")
    assert "loom-secrets" in source
    assert "cp-db-url" in source
    assert "loom-external-slurm-autoscaler-db" in source
    assert "mktemp -d" in source
    assert "trap " in source
    assert "--from-file=cp-db-url=" in source
    assert "--from-literal" not in source
    assert "chmod 0600" in source
    assert "https://192.168.50.103:6443" in source


def test_publisher_parses_as_bash() -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run(
        [bash, "-n", str(PUBLISHER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
