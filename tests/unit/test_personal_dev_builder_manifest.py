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
        "loom.dev/build-attempt-sequence": str(
            registration.build_attempt.attempt_sequence
        ),
        "loom.dev/build-lease-epoch": str(registration.build_attempt.lease_epoch),
    }

    assert namespace["metadata"]["name"] == (
        f"loom-build-{registration.build_attempt.id.hex}-"
        f"l{registration.build_attempt.lease_epoch:016x}"
    )
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    for document in documents:
        assert document["metadata"]["labels"] | expected_labels == document["metadata"]["labels"]
        assert document["kind"] != "Secret"

    pod = job["spec"]["template"]
    assert pod["metadata"]["labels"] | expected_labels == pod["metadata"]["labels"]
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["hostUsers"] is False
    assert spec["runtimeClassName"] == "loom-personal-dev-builder"
    assert spec["nodeSelector"] == {"kubernetes.io/arch": "amd64"}
    assert job["spec"]["activeDeadlineSeconds"] == 3600
    container = spec["containers"][0]
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
    capability = next(
        volume for volume in spec["volumes"] if volume["name"] == "attempt-capability"
    )
    assert capability["secret"]["defaultMode"] == 0o400


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
        peer.get("namespaceSelector", {}).get("matchLabels", {}).get(
            "kubernetes.io/metadata.name"
        )
        == "loom-dev"
        and rule["ports"] == [{"protocol": "TCP", "port": 9000}]
        for rule in egress
        for peer in rule["to"]
    )
    public_blocks = [
        peer["ipBlock"]
        for rule in egress
        for peer in rule["to"]
        if "ipBlock" in peer
    ]
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
