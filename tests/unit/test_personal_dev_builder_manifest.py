from __future__ import annotations

import json

from loom.personal_dev_builder_manifest import (
    PersonalDevBuilderManifestConfig,
    personal_dev_builder_manifest_documents,
)
from tests.unit.test_personal_dev_builder import _registration


def _config() -> PersonalDevBuilderManifestConfig:
    return PersonalDevBuilderManifestConfig(
        builder_image="registry.example/loom-personal-dev-builder@sha256:" + "a" * 64,
    )


def test_builder_manifest_is_attempt_bound_restricted_and_finite() -> None:
    registration = _registration()
    documents = personal_dev_builder_manifest_documents(
        registration,
        platform="linux/amd64",
        config=_config(),
    )
    namespace = documents[0]
    job = next(document for document in documents if document["kind"] == "Job")
    expected_labels = {
        "loom.dev/candidate": registration.candidate.candidate_sha[:12],
        "loom.dev/subject": str(registration.build_attempt.subject_id),
        "loom.dev/incarnation": str(registration.build_attempt.subject_incarnation),
        "loom.dev/operation": str(registration.build_attempt.operation_id),
        "loom.dev/attempt": str(registration.build_attempt.id),
        "loom.dev/operation-epoch": str(registration.build_attempt.operation_epoch),
        "loom.dev/build-attempt-sequence": str(registration.build_attempt.attempt_sequence),
        "loom.dev/build-lease-epoch": str(registration.build_attempt.lease_epoch),
    }

    assert namespace["metadata"]["name"] == (
        f"loom-build-{registration.build_attempt.id.hex}-"
        f"l{registration.build_attempt.lease_epoch:016x}"
    )
    assert namespace["metadata"]["labels"] | {
        "pod-security.kubernetes.io/enforce": "baseline",
        "pod-security.kubernetes.io/enforce-version": "v1.36",
        "pod-security.kubernetes.io/audit": "restricted",
        "pod-security.kubernetes.io/audit-version": "v1.36",
        "pod-security.kubernetes.io/warn": "restricted",
        "pod-security.kubernetes.io/warn-version": "v1.36",
    } == namespace["metadata"]["labels"]
    for document in documents:
        assert document["metadata"]["labels"] | expected_labels == document["metadata"]["labels"]
        assert document["kind"] != "Secret"

    pod = job["spec"]["template"]
    assert pod["metadata"]["labels"] | expected_labels == pod["metadata"]["labels"]
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["shareProcessNamespace"] is False
    assert "hostUsers" not in spec
    assert spec["runtimeClassName"] == "loom-personal-dev-builder"
    assert "nodeSelector" not in spec
    assert job["spec"]["activeDeadlineSeconds"] == 3600
    assert len(spec["containers"]) == 1
    assert len(spec["initContainers"]) == 1
    container = spec["containers"][0]
    assert container["name"] == "builder"
    assert container["image"].endswith("@sha256:" + "a" * 64)
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
    }
    assert container["resources"]["limits"] == {
        "cpu": "4",
        "ephemeral-storage": "20Gi",
        "memory": "8Gi",
    }
    assert {mount["name"] for mount in container["volumeMounts"]} == {
        "attempt-capability",
        "buildkit-run",
        "contract",
        "tmp-client",
        "workspace",
    }
    capability = next(
        volume for volume in spec["volumes"] if volume["name"] == "attempt-capability"
    )
    assert capability["secret"]["defaultMode"] == 0o400
    binding = next(
        document
        for document in documents
        if document["kind"] == "RoleBinding"
        and document["metadata"]["name"] == "loom-personal-dev-management"
    )
    assert binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": "loom-personal-dev-managed-namespace",
    }
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-personal-dev-management",
            "namespace": "loom-dev",
        }
    ]


def test_buildkit_sidecar_has_only_rootless_startup_authority() -> None:
    documents = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=_config(),
    )
    job = next(document for document in documents if document["kind"] == "Job")
    spec = job["spec"]["template"]["spec"]
    client = spec["containers"][0]
    sidecar = spec["initContainers"][0]

    assert sidecar["name"] == "buildkitd"
    assert sidecar["image"] == client["image"]
    assert sidecar["restartPolicy"] == "Always"
    assert sidecar["command"] == ["/usr/local/bin/loom-personal-dev-buildkitd"]
    assert "args" not in sidecar
    assert sidecar["securityContext"] == {
        "allowPrivilegeEscalation": True,
        "capabilities": {"drop": ["ALL"], "add": ["SETGID", "SETUID"]},
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
        "seccompProfile": {"type": "Unconfined"},
    }
    assert {mount["name"] for mount in sidecar["volumeMounts"]} == {
        "buildkit-run",
        "buildkit-state",
        "tmp-buildkit",
    }
    assert sidecar["startupProbe"] == {
        "exec": {
            "command": [
                "/usr/bin/buildctl",
                "--addr",
                "unix:///var/run/loom-buildkit/buildkitd.sock",
                "debug",
                "workers",
            ]
        },
        "failureThreshold": 60,
        "periodSeconds": 2,
        "timeoutSeconds": 1,
    }
    mounts = {
        container["name"]: {mount["name"]: mount for mount in container["volumeMounts"]}
        for container in (client, sidecar)
    }
    assert mounts["builder"]["buildkit-run"]["readOnly"] is True
    assert "readOnly" not in mounts["buildkitd"]["buildkit-run"]
    assert not {
        "attempt-capability",
        "contract",
        "workspace",
    } & set(mounts["buildkitd"])
    assert "env" not in sidecar


def test_builder_network_policy_denies_internal_authority_and_allows_exact_routes() -> None:
    documents = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/arm64",
        config=_config(),
    )
    policies = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "NetworkPolicy"
    }

    assert policies["default-deny"]["spec"] == {
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress"],
    }
    egress = policies["builder-egress"]["spec"]["egress"]
    assert any(
        peer.get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name")
        == "loom-dev"
        and rule["ports"] == [{"protocol": "TCP", "port": 9000}]
        for rule in egress
        for peer in rule["to"]
    )
    public_blocks = [peer["ipBlock"] for rule in egress for peer in rule["to"] if "ipBlock" in peer]
    ipv4 = next(block for block in public_blocks if block["cidr"] == "0.0.0.0/0")
    assert "10.0.0.0/8" in ipv4["except"]
    assert "169.254.0.0/16" in ipv4["except"]
    assert "192.0.2.0/24" in ipv4["except"]
    assert "198.18.0.0/15" in ipv4["except"]
    assert "198.51.100.0/24" in ipv4["except"]
    assert "203.0.113.0/24" in ipv4["except"]
    ipv6 = next(block for block in public_blocks if block["cidr"] == "::/0")
    assert "2001:db8::/32" in ipv6["except"]
    assert all(
        port["port"] in {80, 443}
        for rule in egress
        if any("ipBlock" in peer for peer in rule["to"])
        for port in rule["ports"]
    )


def test_builder_contract_is_canonical_and_contains_no_capability() -> None:
    documents = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=_config(),
    )
    config_map = next(document for document in documents if document["kind"] == "ConfigMap")
    contract_text = config_map["data"]["contract.json"]
    contract = json.loads(contract_text)

    assert contract_text == json.dumps(
        contract,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    assert contract["attempt_id"] == str(_registration().build_attempt.id)
    assert contract["lease_epoch"] == _registration().build_attempt.lease_epoch
    assert contract["platform"] == "linux/amd64"
    assert contract["archive_size_bytes"] == _registration().candidate.archive_size_bytes
    assert contract["max_artifact_bytes"] == 6 * 1024 * 1024 * 1024
    assert contract["max_image_archive_bytes"] == 2 * 1024 * 1024 * 1024
    assert set(contract["components"])
    assert "url" not in contract_text.casefold()
    assert "token" not in contract_text.casefold()
