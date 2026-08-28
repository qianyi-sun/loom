from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "ops" / "nebius_runtime_containment_smoke.py"
_SPEC = importlib.util.spec_from_file_location("nebius_runtime_containment_smoke", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def test_pod_is_digest_pinned_and_restricted() -> None:
    image = "ghcr.io/qianyi-sun/probe@sha256:" + "a" * 64
    document = _MODULE.pod("b" * 40, image)

    spec = document["spec"]
    container = spec["containers"][0]
    assert spec["runtimeClassName"] == "loom-sandbox"
    assert spec["automountServiceAccountToken"] is False
    assert spec["enableServiceLinks"] is False
    assert spec["serviceAccountName"] == "attempt"
    assert spec["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert container["image"] == image
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "privileged": False,
        "readOnlyRootFilesystem": True,
    }
    assert container["resources"]["limits"] == {"cpu": "500m", "memory": "256Mi"}


def test_network_policy_allows_only_dns() -> None:
    document = _MODULE.network_policy("b" * 40)
    spec = document["spec"]

    assert spec["ingress"] == []
    assert spec["policyTypes"] == ["Ingress", "Egress"]
    assert spec["podSelector"] == {"matchLabels": {"app": "runtime-containment"}}
    assert spec["egress"] == [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    },
                    "podSelector": {"matchLabels": {"k8s-app": "coredns"}},
                }
            ],
            "ports": [
                {"port": 53, "protocol": "UDP"},
                {"port": 53, "protocol": "TCP"},
            ],
        }
    ]


def test_namespace_pins_current_nebius_pod_security_version() -> None:
    labels = _MODULE.namespace("b" * 40)["metadata"]["labels"]
    assert labels["pod-security.kubernetes.io/enforce-version"] == "v1.35"
    assert labels["pod-security.kubernetes.io/audit-version"] == "v1.35"
    assert labels["pod-security.kubernetes.io/warn-version"] == "v1.35"


def test_forbidden_variants_cover_privilege_namespaces_and_host_path() -> None:
    variants = _MODULE.forbidden_pods("b" * 40, "ghcr.io/qianyi-sun/probe@sha256:" + "a" * 64)

    assert sorted(variants) == ["host_namespaces", "host_path", "privileged"]
    privileged = variants["privileged"]["spec"]
    assert privileged["securityContext"] == {"runAsUser": 0}
    assert privileged["containers"][0]["securityContext"] == {"privileged": True}
    assert variants["host_namespaces"]["spec"]["hostNetwork"] is True
    assert variants["host_namespaces"]["spec"]["hostPID"] is True
    assert variants["host_namespaces"]["spec"]["hostIPC"] is True
    assert variants["host_path"]["spec"]["volumes"] == [
        {"name": "host", "hostPath": {"path": "/", "type": "Directory"}}
    ]


@pytest.mark.parametrize(
    "image",
    [
        "ghcr.io/qianyi-sun/probe:latest",
        "ghcr.io/qianyi-sun/probe@sha256:abc",
        "ghcr.io/qianyi-sun/probe@sha512:" + "a" * 64,
    ],
)
def test_run_rejects_unpinned_image_before_kubectl(tmp_path: Path, image: str) -> None:
    args = _MODULE.parse_args(
        [
            "--kubeconfig",
            str(tmp_path / "missing"),
            "--candidate-sha",
            "b" * 40,
            "--image",
            image,
            "--runtime-class",
            str(tmp_path / "runtime.yaml"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ]
    )

    with pytest.raises(ValueError, match="exact sha256 digest"):
        _MODULE.run(args)


def test_checked_in_runtime_class_is_the_direct_input() -> None:
    runtime_class = (
        _ROOT
        / "deploy"
        / "terraform"
        / "nebius"
        / "modules"
        / "execution-target"
        / "runtime"
        / "loom-sandbox-runtime-class.yaml"
    )
    runtime_yaml = runtime_class.read_text()
    assert "name: loom-sandbox" in runtime_yaml
    assert "handler: runsc-loom-sandbox" in runtime_yaml
