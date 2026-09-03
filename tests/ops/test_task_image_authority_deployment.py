from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path
from typing import Any

import yaml

from loom_task_image_authority.auth import TaskImagePrincipalVerifier

_MANIFEST = Path("deploy/task-image-builder/authority-service-v1.yaml")
_EXAMPLE_PRINCIPALS = Path(
    "deploy/task-image-builder/authority-principals-v1.example.json"
)
_PROVIDER_POLICY = Path("deploy/task-image-builder/rootless-provider-v1.toml")
_PREREQUISITES = Path("deploy/task-image-builder/prerequisites-v1.toml")


def _objects() -> dict[tuple[str, str], dict[str, Any]]:
    documents = list(yaml.safe_load_all(_MANIFEST.read_text(encoding="utf-8")))
    assert all(isinstance(document, dict) for document in documents)
    objects = {
        (document["kind"], document["metadata"]["name"]): document
        for document in documents
    }
    assert len(objects) == len(documents)
    return objects


def test_manifest_is_exactly_zero_replica_internal_and_default_deny() -> None:
    objects = _objects()
    assert set(objects) == {
        ("ServiceAccount", "loom-task-image-authority"),
        ("Service", "loom-task-image-authority"),
        ("Deployment", "loom-task-image-authority"),
        ("NetworkPolicy", "loom-task-image-authority-default-deny"),
    }
    account = objects[("ServiceAccount", "loom-task-image-authority")]
    service = objects[("Service", "loom-task-image-authority")]
    deployment = objects[("Deployment", "loom-task-image-authority")]
    policy = objects[("NetworkPolicy", "loom-task-image-authority-default-deny")]

    assert account["automountServiceAccountToken"] is False
    assert service["spec"]["type"] == "ClusterIP"
    assert "externalIPs" not in service["spec"]
    assert "loadBalancerIP" not in service["spec"]
    assert deployment["spec"]["replicas"] == 0
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["egress"] == []


def test_pod_is_nonroot_readonly_resource_bounded_and_tokenless() -> None:
    deployment = _objects()[("Deployment", "loom-task-image-authority")]
    template = deployment["spec"]["template"]
    pod = template["spec"]
    assert pod["serviceAccountName"] == "loom-task-image-authority"
    assert pod["automountServiceAccountToken"] is False
    assert pod["enableServiceLinks"] is False
    assert pod["hostNetwork"] is False
    assert pod["hostPID"] is False
    assert pod["hostIPC"] is False
    assert pod["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "fsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert template["metadata"]["annotations"] == {
        "loom.qianyi.dev/activation": "disabled-phase2b1",
        "loom.qianyi.dev/required-pids-max": "256",
    }

    assert len(pod["containers"]) == 1
    container = pod["containers"][0]
    assert container["name"] == "authority"
    assert container["image"] == "ghcr.io/qianyi-sun/loom-control-plane:${IMAGE_TAG}"
    assert container["command"] == ["python", "-m", "loom_task_image_authority"]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    assert container["resources"] == {
        "requests": {"cpu": "100m", "memory": "128Mi", "ephemeral-storage": "32Mi"},
        "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "128Mi"},
    }
    assert pod["volumes"][-1] == {
        "name": "tmp",
        "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"},
    }
    assert {item["mountPath"]: item["readOnly"] for item in container["volumeMounts"]} == {
        "/run/loom-task-image-authority/runtime": True,
        "/run/loom-task-image-authority/tls": True,
        "/tmp": False,
    }


def test_manifest_references_only_uncreated_runtime_and_tls_secrets() -> None:
    objects = _objects()
    deployment = objects[("Deployment", "loom-task-image-authority")]
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    secret_volumes = [volume for volume in volumes if "secret" in volume]
    assert secret_volumes == [
        {
            "name": "runtime",
            "secret": {
                "secretName": "loom-task-image-authority-runtime",
                "defaultMode": 384,
            },
        },
        {
            "name": "tls",
            "secret": {
                "secretName": "loom-task-image-authority-tls",
                "defaultMode": 384,
            },
        },
    ]
    assert all(document[0] != "Secret" for document in objects)
    rendered = _MANIFEST.read_text(encoding="utf-8")
    for forbidden in (
        "token_sha256",
        "key_base64",
        "bootstrap_token",
        "session_token",
        "BEGIN CERTIFICATE",
        "BEGIN PRIVATE KEY",
        "hostPath",
        "privileged: true",
    ):
        assert forbidden not in rendered


def test_example_registry_contains_only_documented_public_example_digests(
    tmp_path: Path,
) -> None:
    raw = _EXAMPLE_PRINCIPALS.read_text(encoding="utf-8")
    document = json.loads(raw)
    oldlab_example = "example-only-oldlab-node-bearer-do-not-use"
    gb10_example = "example-only-gb10-node-bearer-do-not-use"
    expected = {
        "oldlab-trt-eai-oldlab-3-example": hashlib.sha256(
            oldlab_example.encode("ascii")
        ).hexdigest(),
        "gb10-trt-gb10-1-example": hashlib.sha256(
            gb10_example.encode("ascii")
        ).hexdigest(),
    }
    assert {
        principal["principal_id"]: principal["token_sha256"]
        for principal in document["principals"]
    } == expected
    assert oldlab_example not in raw
    assert gb10_example not in raw

    owner_copy = tmp_path / "principals.json"
    owner_copy.write_text(raw, encoding="utf-8")
    owner_copy.chmod(0o600)
    verifier = TaskImagePrincipalVerifier.from_file(owner_copy)
    assert verifier.verify_bearer(f"Bearer {oldlab_example}").node_name == (
        "trt-eai-oldlab-3"
    )
    assert verifier.verify_bearer(f"Bearer {gb10_example}").node_name == "trt-gb10-1"


def test_no_live_composition_references_the_inert_authority_artifact() -> None:
    manifest_name = _MANIFEST.name
    service_name = "loom-task-image-authority"
    referenced_by: list[Path] = []
    for root in (Path("deploy"), Path("src")):
        for path in root.rglob("*"):
            if not path.is_file() or path in {_MANIFEST, _EXAMPLE_PRINCIPALS}:
                continue
            if path.suffix not in {".json", ".py", ".sh", ".toml", ".yaml", ".yml"}:
                continue
            text = path.read_text(encoding="utf-8")
            if manifest_name in text or service_name in text:
                referenced_by.append(path)
    assert not referenced_by


def test_phase1_and_rootless_activation_guards_remain_closed() -> None:
    provider = tomllib.loads(_PROVIDER_POLICY.read_text(encoding="utf-8"))
    policies = provider["policies"]
    assert len(policies) == 2
    assert {policy["slurm_cluster_id"] for policy in policies} == {"oldlab", "gb10"}
    assert all(policy["enabled"] is False for policy in policies)
    assert all(
        policy["activation_blockers"]
        == [
            "allocation_executor_not_accepted",
            "node_guard_not_accepted",
            "publication_acceptance_not_complete",
            "renewable_registry_credential_broker_not_accepted",
        ]
        for policy in policies
    )

    prerequisites = tomllib.loads(_PREREQUISITES.read_text(encoding="utf-8"))
    assert prerequisites["production_certification_allowed"] is False
    assert prerequisites["certified_nodes"] == []
    assert prerequisites["unconditional_blockers"] == [
        "phase2_guard_provider_release_missing"
    ]
