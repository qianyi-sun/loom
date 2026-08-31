"""Contracts for the external Slurm autoscaler's Kubernetes authority."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "deploy/k8s/external-slurm-autoscaler-authority.yaml"
TRANSITION_BINDING = (
    ROOT / "deploy/k8s/external-slurm-autoscaler-manager-export-binding.yaml"
)
PUBLISHER = ROOT / "deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh"


def _documents() -> list[dict[str, object]]:
    return [
        document
        for document in yaml.safe_load_all(MANIFEST.read_text(encoding="utf-8"))
        if isinstance(document, dict)
    ]


def test_authority_is_namespace_scoped_and_uses_a_dedicated_token() -> None:
    documents = _documents()
    assert [document["kind"] for document in documents] == [
        "ServiceAccount",
        "Secret",
        "Role",
        "RoleBinding",
        "Role",
        "RoleBinding",
    ]
    assert [document["metadata"]["namespace"] for document in documents] == [  # type: ignore[index]
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-dev",
        "loom-dev",
    ]
    token = documents[1]
    assert token["type"] == "kubernetes.io/service-account-token"
    assert token["metadata"]["annotations"] == {  # type: ignore[index]
        "kubernetes.io/service-account.name": "loom-external-slurm-autoscaler",
    }


def test_authority_can_read_only_the_dedicated_db_secret() -> None:
    role = _documents()[2]
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
    rules = _documents()[2]["rules"]  # type: ignore[index]
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


def test_authority_reads_only_the_stable_witness_and_never_execs() -> None:
    documents = _documents()
    witness_role = documents[4]
    assert witness_role["metadata"]["name"] == (  # type: ignore[index]
        "loom-external-slurm-autoscaler-witness"
    )
    assert witness_role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["configmaps"],
            "resourceNames": ["loom-global-execution-witness-v1"],
            "verbs": ["get"],
        }
    ]
    binding = documents[5]
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-external-slurm-autoscaler",
            "namespace": "loom-staging",
        }
    ]
    assert all(
        "pods/exec" not in rule["resources"]
        for document in documents
        for rule in document.get("rules", [])  # type: ignore[union-attr]
    )


def test_publisher_cannot_recreate_transitional_manager_exec_authority() -> None:
    source = PUBLISHER.read_text(encoding="utf-8")

    assert not TRANSITION_BINDING.exists()
    assert "MANAGER_BINDING_MANIFEST" not in source
    assert "manager_pod_identity" not in source
    assert "--subresource=exec" not in source
    assert "loom-external-slurm-autoscaler-manager-export" not in source


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
