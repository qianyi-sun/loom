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
TRANSITION_BINDING = ROOT / "deploy/k8s/external-slurm-autoscaler-manager-export-binding.yaml"
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
        "Secret",
        "Role",
        "RoleBinding",
        "Role",
        "RoleBinding",
        "ClusterRole",
        "ClusterRoleBinding",
    ]
    assert [document["metadata"]["namespace"] for document in documents[:7]] == [  # type: ignore[index]
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-staging",
        "loom-dev",
        "loom-dev",
    ]
    service_account = documents[0]
    assert service_account["automountServiceAccountToken"] is False
    database = documents[1]
    assert database == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "loom-external-slurm-autoscaler-db",
            "namespace": "loom-staging",
        },
        "type": "Opaque",
    }
    assert "data" not in database
    assert "stringData" not in database
    token = documents[2]
    assert token["type"] == "kubernetes.io/service-account-token"
    assert token["metadata"]["annotations"] == {  # type: ignore[index]
        "kubernetes.io/service-account.name": "loom-external-slurm-autoscaler",
    }


def test_authority_can_read_only_the_dedicated_db_secret() -> None:
    role = _documents()[3]
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
    rules = _documents()[3]["rules"]  # type: ignore[index]
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
    witness_role = documents[5]
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
    binding = documents[6]
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


def test_authority_can_list_only_namespace_metadata_for_complete_exec_audit() -> None:
    documents = _documents()
    audit_role = documents[7]
    assert audit_role == {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "loom-external-slurm-autoscaler-namespace-audit"},
        "rules": [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["list"]}],
    }
    assert documents[8] == {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "loom-external-slurm-autoscaler-namespace-audit"},
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-external-slurm-autoscaler",
                "namespace": "loom-staging",
            }
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "loom-external-slurm-autoscaler-namespace-audit",
        },
    }


def test_publisher_cannot_recreate_transitional_manager_exec_authority() -> None:
    source = PUBLISHER.read_text(encoding="utf-8")

    assert not TRANSITION_BINDING.exists()
    assert "MANAGER_BINDING_MANIFEST" not in source
    assert "manager_pod_identity" not in source
    assert "--subresource=exec" not in source
    assert "loom-external-slurm-autoscaler-manager-export" not in source


def test_publisher_has_a_non_mutating_runtime_credential_check() -> None:
    source = PUBLISHER.read_text(encoding="utf-8")

    assert 'if [ "$1" = "--check" ]' in source
    assert "validate_runtime_kubeconfig" in source
    assert 'get configmap "$WITNESS_CONFIG_MAP"' in source
    assert "auth can-i create pods/exec" in source
    assert "get namespaces -o name" in source
    assert "while IFS= read -r authority_namespace; do" in source
    assert "--all-namespaces" not in source
    assert 'if [ "$exec_allowed" != "no" ]' in source


def test_publisher_rejects_pods_exec_in_any_enumerated_namespace(tmp_path: Path) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    kubeconfig = tmp_path / "external-supervisor.kubeconfig"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    kubeconfig.chmod(0o600)
    call_log = tmp_path / "kubectl.calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' \"$*\" >>\"${KUBECTL_LOG:?}\"
case \" $* \" in
  *\" get secret loom-external-slurm-autoscaler-db -o name \"*)
    printf 'resource/allowed\\n'
    ;;
  *\" get secret loom-external-slurm-autoscaler-db -o jsonpath={.data.cp-db-url} \"*)
    printf 'ZGF0YWJhc2UtdXJsCg=='
    ;;
  *\" get configmap loom-global-execution-witness-v1 -o name \"*)
    printf 'resource/allowed\\n'
    ;;
  *\" get namespaces -o name \"*)
    printf '%s\\n' namespace/loom-staging namespace/loom-dev namespace/loom-audit-third
    ;;
  *\" auth can-i create pods/exec \"*)
    if [[ \" $* \" == *\" -n loom-audit-third \"* ]]; then
      printf 'yes\\n'
    else
      printf 'no\\n'
    fi
    ;;
  *)
    exit 2
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o700)

    result = subprocess.run(
        [bash, str(PUBLISHER), "--check", str(kubeconfig)],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"KUBECTL": str(kubectl), "KUBECTL_LOG": str(call_log)},
    )

    assert result.returncode != 0
    assert "unexpected pods/exec authority" in result.stderr
    assert "-n loom-audit-third auth can-i create pods/exec" in call_log.read_text(encoding="utf-8")


def test_publisher_check_requires_nonempty_dedicated_database_key(tmp_path: Path) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    kubeconfig = tmp_path / "external-supervisor.kubeconfig"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    kubeconfig.chmod(0o600)
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *" get secret loom-external-slurm-autoscaler-db -o name "*)
    printf 'secret/loom-external-slurm-autoscaler-db\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-db -o jsonpath={.data.cp-db-url} "*)
    ;;
  *" get configmap loom-global-execution-witness-v1 -o name "*)
    printf 'configmap/loom-global-execution-witness-v1\n'
    ;;
  *" get namespaces -o name "*)
    printf 'namespace/loom-staging\n'
    ;;
  *" auth can-i create pods/exec "*)
    printf 'no\n'
    ;;
  *)
    exit 91
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o700)

    result = subprocess.run(
        [bash, str(PUBLISHER), "--check", str(kubeconfig)],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"KUBECTL": str(kubectl)},
    )

    assert result.returncode != 0
    assert "dedicated database credential is unavailable" in result.stderr


def _run_publisher_with_missing_prerequisite(
    tmp_path: Path,
    *,
    missing: str,
) -> tuple[subprocess.CompletedProcess[str], list[str], list[str]]:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    source = tmp_path / "rollout.kubeconfig"
    source.write_text("protected source\n", encoding="utf-8")
    source.chmod(0o600)
    output = tmp_path / "external-supervisor.kubeconfig"
    call_log = tmp_path / "kubectl.calls"
    mutation_log = tmp_path / "mutations"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${KUBECTL_LOG:?}"
case " $* " in
  *" get secret loom-external-slurm-autoscaler-db -o name "*)
    if [ "${MISSING_PREREQUISITE:?}" = db ]; then exit 42; fi
    printf 'secret/loom-external-slurm-autoscaler-db\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o name "*)
    if [ "${MISSING_PREREQUISITE:?}" = token ]; then exit 43; fi
    printf 'secret/loom-external-slurm-autoscaler-token\n'
    ;;
  *" get secret loom-secrets -o jsonpath={.data.cp-db-url} "*)
    printf 'source-read\n' >>"${MUTATION_LOG:?}"
    printf 'ZGF0YWJhc2UtdXJsCg=='
    ;;
  *" create secret generic loom-external-slurm-autoscaler-db "*)
    printf 'secret-create\n' >>"${MUTATION_LOG:?}"
    printf '%s\n' 'apiVersion: v1' 'kind: Secret'
    ;;
  *" apply -f "*)
    printf 'secret-apply\n' >>"${MUTATION_LOG:?}"
    cat >/dev/null
    ;;
  *)
    exit 93
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o700)

    result = subprocess.run(
        [bash, str(PUBLISHER), str(output)],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ
        | {
            "KUBECONFIG": str(source),
            "KUBECTL": str(kubectl),
            "KUBECTL_LOG": str(call_log),
            "MISSING_PREREQUISITE": missing,
            "MUTATION_LOG": str(mutation_log),
        },
    )

    return (
        result,
        call_log.read_text(encoding="utf-8").splitlines(),
        mutation_log.read_text(encoding="utf-8").splitlines()
        if mutation_log.exists()
        else [],
    )


def test_publisher_requires_missing_db_prerequisite_before_mutation(
    tmp_path: Path,
) -> None:
    result, calls, mutations = _run_publisher_with_missing_prerequisite(tmp_path, missing="db")

    assert result.returncode != 0
    assert not (tmp_path / "external-supervisor.kubeconfig").exists()
    assert mutations == []
    assert not any(
        "loom-secrets" in call
        or "create secret generic" in call
        or " apply -f " in call
        for call in calls
    )
    assert len(calls) == 1
    assert "get secret loom-external-slurm-autoscaler-db -o name" in calls[0]
    assert "external-slurm-autoscaler-authority.yaml" not in "\n".join(calls)


def test_publisher_requires_missing_token_prerequisite_before_mutation(
    tmp_path: Path,
) -> None:
    result, calls, mutations = _run_publisher_with_missing_prerequisite(tmp_path, missing="token")

    assert result.returncode != 0
    assert not (tmp_path / "external-supervisor.kubeconfig").exists()
    assert mutations == []
    assert not any(
        "loom-secrets" in call
        or "create secret generic" in call
        or " apply -f " in call
        for call in calls
    )
    assert len(calls) == 2
    assert "get secret loom-external-slurm-autoscaler-db -o name" in calls[0]
    assert "get secret loom-external-slurm-autoscaler-token -o name" in calls[1]


def test_publisher_only_reads_existing_prerequisites_before_local_publication(
    tmp_path: Path,
) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    source = tmp_path / "rollout.kubeconfig"
    source.write_text("protected source\n", encoding="utf-8")
    source.chmod(0o600)
    output = tmp_path / "external-supervisor.kubeconfig"
    call_log = tmp_path / "kubectl.calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${KUBECTL_LOG:?}"
case " $* " in
  *" get secret loom-external-slurm-autoscaler-db -o name "*)
    printf 'secret/loom-external-slurm-autoscaler-db\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o name "*)
    printf 'secret/loom-external-slurm-autoscaler-token\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o jsonpath={.data.token} "*)
    printf 'dG9rZW4='
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o jsonpath={.data.ca\\.crt} "*)
    printf 'Y2E='
    ;;
  *" get secret loom-external-slurm-autoscaler-db -o jsonpath={.data.cp-db-url} "*)
    printf 'ZGF0YWJhc2UtdXJsCg=='
    ;;
  *" get configmap loom-global-execution-witness-v1 -o name "*)
    printf 'configmap/loom-global-execution-witness-v1\n'
    ;;
  *" get namespaces -o name "*)
    printf '%s\n' namespace/loom-staging namespace/loom-dev
    ;;
  *" auth can-i create pods/exec "*)
    printf 'no\n'
    ;;
  *)
    exit 97
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o700)

    result = subprocess.run(
        [bash, str(PUBLISHER), str(output)],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ
        | {
            "KUBECONFIG": str(source),
            "KUBECTL": str(kubectl),
            "KUBECTL_LOG": str(call_log),
        },
    )

    calls = call_log.read_text(encoding="utf-8").splitlines()
    assert result.returncode == 0, result.stderr
    assert output.is_file() and not output.is_symlink()
    assert output.stat().st_mode & 0o777 == 0o600
    assert not any(
        "loom-secrets" in call or "create secret generic" in call or " apply " in call
        for call in calls
    )


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


@pytest.mark.parametrize("race_kind", ["regular", "symlink"])
def test_publisher_race_never_clobbers_a_concurrently_appearing_destination(
    tmp_path: Path,
    race_kind: str,
) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    source = tmp_path / "rollout.kubeconfig"
    source.write_text("protected source\n", encoding="utf-8")
    source.chmod(0o600)
    output = tmp_path / "external-supervisor.kubeconfig"
    symlink_target = tmp_path / "concurrent-target"
    symlink_target.write_text("concurrent-symlink-target\n", encoding="utf-8")
    marker = tmp_path / "race-created"
    call_log = tmp_path / "kubectl.calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${KUBECTL_LOG:?}"
case " $* " in
  *" apply -f "*)
    if [ "${*: -1}" = "-" ]; then cat >/dev/null; fi
    ;;
  *" get secret loom-secrets -o jsonpath={.data.cp-db-url} "*)
    printf 'ZGF0YWJhc2UtdXJsCg=='
    ;;
  *" create secret generic loom-external-slurm-autoscaler-db "*)
    printf '%s\n' 'apiVersion: v1' 'kind: Secret'
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o jsonpath={.data.token} "*)
    printf 'dG9rZW4='
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o jsonpath={.data.ca\\.crt} "*)
    printf 'Y2E='
    ;;
  *" get secret loom-external-slurm-autoscaler-token -o name "*)
    printf 'secret/loom-external-slurm-autoscaler-token\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-db -o name "*)
    printf 'secret/loom-external-slurm-autoscaler-db\n'
    ;;
  *" get secret loom-external-slurm-autoscaler-db -o jsonpath={.data.cp-db-url} "*|*" get configmap loom-global-execution-witness-v1 -o name "*)
    if [[ " $* " == *" --kubeconfig "*"/kubeconfig "* ]] && [ ! -e "${RACE_MARKER:?}" ]; then
      : >"$RACE_MARKER"
      if [ "${RACE_KIND:?}" = symlink ]; then
        ln -s -- "${SYMLINK_TARGET:?}" "${OUTPUT_PATH:?}"
      else
        printf 'concurrent-regular-file\n' >"${OUTPUT_PATH:?}"
        chmod 0644 "${OUTPUT_PATH:?}"
      fi
    fi
    printf 'resource/allowed\n'
    ;;
  *" get namespaces -o name "*)
    printf '%s\n' namespace/loom-staging namespace/loom-dev
    ;;
  *" auth can-i create pods/exec "*)
    printf 'no\n'
    ;;
  *)
    exit 93
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o700)

    result = subprocess.run(
        [bash, str(PUBLISHER), str(output)],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ
        | {
            "KUBECONFIG": str(source),
            "KUBECTL": str(kubectl),
            "KUBECTL_LOG": str(call_log),
            "OUTPUT_PATH": str(output),
            "RACE_KIND": race_kind,
            "RACE_MARKER": str(marker),
            "SYMLINK_TARGET": str(symlink_target),
        },
    )

    assert marker.exists()
    assert result.returncode != 0
    assert "external-slurm-autoscaler-authority.yaml" not in call_log.read_text(
        encoding="utf-8"
    )
    if race_kind == "symlink":
        assert output.is_symlink()
        assert output.readlink() == symlink_target
        assert symlink_target.read_text(encoding="utf-8") == "concurrent-symlink-target\n"
    else:
        assert not output.is_symlink()
        assert output.read_text(encoding="utf-8") == "concurrent-regular-file\n"
        assert output.stat().st_mode & 0o777 == 0o644
