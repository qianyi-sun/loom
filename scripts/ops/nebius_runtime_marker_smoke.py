#!/usr/bin/env python3
"""Verify that one Pod starts with the checked-in Nebius gVisor RuntimeClass.

This smoke creates no provider resources and performs no network or escape
probes. It owns one temporary namespace plus the RuntimeClass and removes both
before exit. A pass proves only that the handler can start the pinned image on
an eligible execution node and exposes gVisor's in-sandbox marker.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_NAMESPACE = "loom-nebius-runtime-smoke"
_POD = "runtime-marker"
_RUNTIME_CLASS = "loom-sandbox"
_MANAGED_BY = "loom-nebius-runtime-smoke"
_POD_SECURITY_VERSION = "v1.35"
_DIGEST_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")

_PROBE = """
import json
import os
import pathlib

result = {
    "gvisor_marker": pathlib.Path("/proc/gvisor/kernel_is_gvisor").is_file(),
    "gid": os.getgid(),
    "uid": os.getuid(),
}
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
if not result["gvisor_marker"] or result["uid"] != 65532 or result["gid"] != 65532:
    raise SystemExit(1)
""".strip()


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class Kubectl:
    def __init__(self, kubeconfig: Path) -> None:
        self._prefix = (
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "--request-timeout=15s",
        )
        self.results: list[CommandResult] = []

    def run(
        self,
        args: Sequence[str],
        *,
        document: Mapping[str, Any] | None = None,
        check: bool = True,
    ) -> CommandResult:
        argv = (*self._prefix, *args)
        completed = subprocess.run(
            argv,
            input=None if document is None else json.dumps(document),
            text=True,
            capture_output=True,
            check=False,
        )
        result = CommandResult(
            argv=argv,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        self.results.append(result)
        if check and result.returncode != 0:
            raise RuntimeError(
                f"command failed ({result.returncode}): {' '.join(argv)}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result

    def create(self, document: Mapping[str, Any]) -> CommandResult:
        return self.run(("create", "-f", "-"), document=document)

    def get_json(self, *args: str) -> dict[str, Any]:
        result = self.run(("get", *args, "-o", "json"))
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise RuntimeError("kubectl JSON response is not an object")
        return value


def _metadata(name: str, candidate_sha: str) -> dict[str, Any]:
    return {
        "name": name,
        "labels": {"app.kubernetes.io/managed-by": _MANAGED_BY},
        "annotations": {"loom.nebius/candidate-sha": candidate_sha},
    }


def namespace(candidate_sha: str) -> dict[str, Any]:
    metadata = _metadata(_NAMESPACE, candidate_sha)
    metadata["labels"].update(
        {
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": _POD_SECURITY_VERSION,
        }
    )
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata}


def pod(candidate_sha: str, image: str) -> dict[str, Any]:
    metadata = _metadata(_POD, candidate_sha)
    metadata["namespace"] = _NAMESPACE
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {
            "activeDeadlineSeconds": 180,
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "restartPolicy": "Never",
            "runtimeClassName": _RUNTIME_CLASS,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 65532,
                "runAsGroup": 65532,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-c", _PROBE],
                    "resources": {
                        "requests": {"cpu": "25m", "memory": "32Mi"},
                        "limits": {"cpu": "250m", "memory": "128Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                    },
                }
            ],
        },
    }


def _assert_absent(kubectl: Kubectl, resource: str, name: str) -> None:
    result = kubectl.run(("get", resource, name), check=False)
    if result.returncode == 0:
        raise RuntimeError(f"refusing to overwrite existing {resource}/{name}")
    if "NotFound" not in result.stderr:
        raise RuntimeError(f"cannot establish absence of {resource}/{name}: {result.stderr}")


def _redacted_result(result: CommandResult) -> dict[str, Any]:
    return {
        "argv": list(result.argv),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if not _DIGEST_IMAGE.fullmatch(args.image):
        raise ValueError("--image must use an exact sha256 digest")
    if not _GIT_SHA.fullmatch(args.candidate_sha):
        raise ValueError("--candidate-sha must be a full lowercase Git SHA")
    if not args.kubeconfig.is_file() or args.kubeconfig.is_symlink():
        raise ValueError("--kubeconfig must be a regular non-symlink file")
    if args.evidence_dir.exists():
        raise ValueError("--evidence-dir must not already exist")

    runtime_class = yaml.safe_load(args.runtime_class.read_text())
    if not isinstance(runtime_class, dict):
        raise ValueError("runtime class asset must contain one Kubernetes object")
    if runtime_class.get("metadata", {}).get("name") != _RUNTIME_CLASS:
        raise ValueError("runtime class asset does not define loom-sandbox")

    kubectl = Kubectl(args.kubeconfig)
    started = time.time()
    created_runtime_class = False
    created_namespace = False
    try:
        _assert_absent(kubectl, "namespace", _NAMESPACE)
        _assert_absent(kubectl, "runtimeclass.node.k8s.io", _RUNTIME_CLASS)
        kubectl.create(runtime_class)
        created_runtime_class = True
        kubectl.create(namespace(args.candidate_sha))
        created_namespace = True
        kubectl.create(pod(args.candidate_sha, args.image))

        wait_result = kubectl.run(
            (
                "--namespace",
                _NAMESPACE,
                "wait",
                "--for=jsonpath={.status.phase}=Succeeded",
                "--timeout=5m",
                f"pod/{_POD}",
            ),
            check=False,
        )
        observed_pod = kubectl.get_json("--namespace", _NAMESPACE, "pod", _POD)
        log_result = kubectl.run(("--namespace", _NAMESPACE, "logs", _POD), check=False)
        if wait_result.returncode != 0:
            raise RuntimeError(
                "runtime marker Pod did not reach Succeeded: "
                f"{wait_result.stderr}\nlogs:\n{log_result.stdout}{log_result.stderr}"
            )

        node_name = observed_pod.get("spec", {}).get("nodeName")
        if not isinstance(node_name, str) or not node_name:
            raise RuntimeError("runtime marker Pod has no scheduled node")
        observed_node = kubectl.get_json("node", node_name)
        labels = observed_node.get("metadata", {}).get("labels", {})
        if labels.get("loom.nebius/node-role") != "execution":
            raise RuntimeError("runtime marker Pod did not run on a Nebius execution node")
        expected_selector = runtime_class.get("scheduling", {}).get("nodeSelector", {})
        if not expected_selector or any(
            labels.get(key) != value for key, value in expected_selector.items()
        ):
            raise RuntimeError("runtime marker node does not match the RuntimeClass selector")
        if observed_pod.get("spec", {}).get("runtimeClassName") != _RUNTIME_CLASS:
            raise RuntimeError("runtime marker Pod RuntimeClass readback drifted")
        if observed_pod.get("spec", {}).get("containers", [{}])[0].get("image") != args.image:
            raise RuntimeError("runtime marker Pod image readback drifted")
        if log_result.returncode != 0:
            raise RuntimeError(f"cannot read runtime marker log: {log_result.stderr}")

        probe = json.loads(log_result.stdout.strip())
        if probe != {"gvisor_marker": True, "gid": 65532, "uid": 65532}:
            raise RuntimeError(f"runtime marker result drifted: {probe}")
        container_status = observed_pod.get("status", {}).get("containerStatuses", [{}])[0]
        return {
            "candidate_sha": args.candidate_sha,
            "container_id": container_status.get("containerID"),
            "elapsed_seconds": round(time.time() - started, 3),
            "image": args.image,
            "image_id": container_status.get("imageID"),
            "node": node_name,
            "pod_uid": observed_pod.get("metadata", {}).get("uid"),
            "probe": probe,
            "runtime_class": _RUNTIME_CLASS,
        }
    finally:
        if created_namespace:
            kubectl.run(
                ("delete", "namespace", _NAMESPACE, "--wait=true", "--timeout=3m"),
                check=False,
            )
        if created_runtime_class:
            kubectl.run(
                (
                    "delete",
                    "runtimeclass.node.k8s.io",
                    _RUNTIME_CLASS,
                    "--wait=true",
                    "--timeout=1m",
                ),
                check=False,
            )
        args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        (args.evidence_dir / "kubectl.json").write_text(
            json.dumps([_redacted_result(item) for item in kubectl.results], indent=2) + "\n"
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--runtime-class", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = run(args)
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        if args.evidence_dir.is_dir():
            (args.evidence_dir / "error.json").write_text(
                json.dumps({"error": str(exc)}, indent=2, sort_keys=True) + "\n"
            )
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    (args.evidence_dir / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
