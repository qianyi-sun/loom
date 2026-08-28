from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "ops" / "nebius_runtime_marker_smoke.py"
_SPEC = importlib.util.spec_from_file_location("nebius_runtime_marker_smoke", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def test_pod_is_a_bounded_runtime_marker_smoke() -> None:
    image = "ghcr.io/qianyi-sun/probe@sha256:" + "a" * 64
    document = _MODULE.pod("b" * 40, image)

    spec = document["spec"]
    container = spec["containers"][0]
    assert spec["runtimeClassName"] == "loom-sandbox"
    assert spec["automountServiceAccountToken"] is False
    assert spec["enableServiceLinks"] is False
    assert "dnsPolicy" not in spec
    assert "hostNetwork" not in spec
    assert "volumes" not in spec
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
        "readOnlyRootFilesystem": True,
    }
    assert container["resources"]["limits"] == {"cpu": "250m", "memory": "128Mi"}
    probe = container["command"][2]
    assert "/proc/gvisor/kernel_is_gvisor" in probe
    assert "socket" not in probe
    assert "mount" not in probe
    assert "setuid" not in probe


def test_namespace_uses_restricted_baseline() -> None:
    labels = _MODULE.namespace("b" * 40)["metadata"]["labels"]
    assert labels["pod-security.kubernetes.io/enforce"] == "restricted"
    assert labels["pod-security.kubernetes.io/enforce-version"] == "v1.35"
    assert "pod-security.kubernetes.io/audit" not in labels


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
