#!/usr/bin/env python3
"""Run a bounded, fail-closed containment smoke on the Nebius gVisor class.

The smoke owns a temporary namespace and creates no provider resources. It
expects the checked-in RuntimeClass handler to be installed on an eligible
execution node. All Kubernetes resources are removed before exit.
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
_POD = "runtime-containment"
_RUNTIME_CLASS = "loom-sandbox"
_MANAGED_BY = "loom-nebius-runtime-smoke"
_POD_SECURITY_VERSION = "v1.35"
_DIGEST_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")


_PROBE = r"""
import ctypes
import errno
import json
import os
import pathlib
import socket


def status_value(name):
    prefix = name + ":"
    for line in pathlib.Path("/proc/self/status").read_text().splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip().split()[0]
    raise RuntimeError("missing /proc/self/status field: " + name)


def cannot_open(path):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        return True
    else:
        os.close(fd)
        return False


def connect_blocked(host, port, family=socket.AF_UNSPEC):
    try:
        addresses = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
    except OSError:
        return True
    for af, socktype, proto, _, sockaddr in addresses:
        sock = socket.socket(af, socktype, proto)
        sock.settimeout(1.5)
        try:
            if sock.connect_ex(sockaddr) == 0:
                return False
        finally:
            sock.close()
    return True


def loopback_works():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname(), timeout=1)
    server, _ = listener.accept()
    client.sendall(b"x")
    ok = server.recv(1) == b"x"
    client.close()
    server.close()
    listener.close()
    return ok


def mount_is_denied():
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.mount(b"none", b"/tmp", b"tmpfs", 0, b"size=4096")
    return result == -1 and ctypes.get_errno() in (errno.EPERM, errno.EACCES)


def setuid_root_is_denied():
    try:
        os.setuid(0)
    except OSError as exc:
        return exc.errno in (errno.EPERM, errno.EACCES)
    return False


dns_answers = socket.getaddrinfo(
    "kubernetes.default.svc.cluster.local", 443, type=socket.SOCK_STREAM
)
api_ips = sorted({answer[4][0] for answer in dns_answers})
checks = {
    "gvisor_marker": pathlib.Path("/proc/gvisor/kernel_is_gvisor").is_file(),
    "uid_65532": os.getuid() == 65532,
    "gid_65532": os.getgid() == 65532,
    "cap_eff_zero": int(status_value("CapEff"), 16) == 0,
    "no_new_privs": status_value("NoNewPrivs") == "1",
    "seccomp_filter": status_value("Seccomp") == "2",
    "service_account_token_absent": not pathlib.Path(
        "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ).exists(),
    "containerd_socket_absent": not pathlib.Path(
        "/run/containerd/containerd.sock"
    ).exists(),
    "docker_socket_absent": not pathlib.Path("/var/run/docker.sock").exists(),
    "kubelet_state_absent": not pathlib.Path("/var/lib/kubelet").exists(),
    "kubernetes_config_absent": not pathlib.Path("/etc/kubernetes").exists(),
    "host_devices_absent": all(
        not pathlib.Path(path).exists() for path in ("/dev/kvm", "/dev/mem")
    ),
    "proc_kcore_unreadable": cannot_open("/proc/kcore"),
    "mount_denied": mount_is_denied(),
    "setuid_root_denied": setuid_root_is_denied(),
    "loopback_allowed": loopback_works(),
    "cluster_dns_allowed": bool(api_ips),
    "kubernetes_api_denied": all(connect_blocked(ip, 443) for ip in api_ips),
    "metadata_ipv4_denied": connect_blocked("169.254.169.254", 80),
    "private_ipv4_denied": connect_blocked("10.0.0.1", 443),
    "public_ipv4_denied": connect_blocked("1.1.1.1", 443),
    "metadata_ipv6_denied": connect_blocked("fe80::a9fe:a9fe", 80, socket.AF_INET6),
}
result = {
    "checks": checks,
    "dns_api_ips": api_ips,
    "gid": os.getgid(),
    "uid": os.getuid(),
}
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
if not all(checks.values()):
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
            "pod-security.kubernetes.io/audit": "restricted",
            "pod-security.kubernetes.io/audit-version": _POD_SECURITY_VERSION,
            "pod-security.kubernetes.io/warn": "restricted",
            "pod-security.kubernetes.io/warn-version": _POD_SECURITY_VERSION,
        }
    )
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata}


def namespaced_metadata(name: str, candidate_sha: str) -> dict[str, Any]:
    metadata = _metadata(name, candidate_sha)
    metadata["namespace"] = _NAMESPACE
    return metadata


def service_account(candidate_sha: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": namespaced_metadata("attempt", candidate_sha),
        "automountServiceAccountToken": False,
    }


def network_policy(candidate_sha: str) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": namespaced_metadata("runtime-smoke-egress", candidate_sha),
        "spec": {
            "podSelector": {"matchLabels": {"app": _POD}},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [],
            "egress": [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                            },
                            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                        }
                    ],
                    "ports": [
                        {"port": 53, "protocol": "UDP"},
                        {"port": 53, "protocol": "TCP"},
                    ],
                }
            ],
        },
    }


def pod(candidate_sha: str, image: str) -> dict[str, Any]:
    metadata = namespaced_metadata(_POD, candidate_sha)
    metadata["labels"]["app"] = _POD
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
            "serviceAccountName": "attempt",
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
                        "requests": {"cpu": "50m", "memory": "64Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "privileged": False,
                        "readOnlyRootFilesystem": True,
                    },
                }
            ],
        },
    }


def forbidden_pods(candidate_sha: str, image: str) -> dict[str, dict[str, Any]]:
    base = pod(candidate_sha, image)
    privileged = json.loads(json.dumps(base))
    privileged["metadata"]["name"] = "forbidden-privileged"
    privileged["spec"]["containers"][0]["securityContext"]["privileged"] = True

    host_namespaces = json.loads(json.dumps(base))
    host_namespaces["metadata"]["name"] = "forbidden-host-namespaces"
    host_namespaces["spec"].update({"hostIPC": True, "hostNetwork": True, "hostPID": True})

    host_path = json.loads(json.dumps(base))
    host_path["metadata"]["name"] = "forbidden-host-path"
    host_path["spec"]["volumes"] = [
        {"name": "host", "hostPath": {"path": "/", "type": "Directory"}}
    ]
    host_path["spec"]["containers"][0]["volumeMounts"] = [
        {"name": "host", "mountPath": "/host", "readOnly": True}
    ]
    return {
        "privileged": privileged,
        "host_namespaces": host_namespaces,
        "host_path": host_path,
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
        kubectl.create(service_account(args.candidate_sha))
        kubectl.create(network_policy(args.candidate_sha))

        denied: dict[str, str] = {}
        for name, document in forbidden_pods(args.candidate_sha, args.image).items():
            result = kubectl.run(
                ("create", "--dry-run=server", "-f", "-"),
                document=document,
                check=False,
            )
            if result.returncode == 0 or "violates PodSecurity" not in result.stderr:
                raise RuntimeError(
                    f"forbidden {name} Pod was not rejected by Pod Security: {result.stderr}"
                )
            denied[name] = result.stderr.strip()

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
                "runtime probe did not reach Succeeded: "
                f"{wait_result.stderr}\nlogs:\n{log_result.stdout}{log_result.stderr}"
            )
        node_name = observed_pod.get("spec", {}).get("nodeName")
        if not isinstance(node_name, str) or not node_name:
            raise RuntimeError("smoke Pod has no scheduled node")
        observed_node = kubectl.get_json("node", node_name)
        labels = observed_node.get("metadata", {}).get("labels", {})
        if labels.get("loom.nebius/node-role") != "execution":
            raise RuntimeError("smoke Pod did not run on a Nebius execution node")
        expected_node_selector = runtime_class.get("scheduling", {}).get("nodeSelector", {})
        if not expected_node_selector or any(
            labels.get(key) != value for key, value in expected_node_selector.items()
        ):
            raise RuntimeError("smoke node does not satisfy the exact RuntimeClass node selector")
        if observed_pod.get("spec", {}).get("runtimeClassName") != _RUNTIME_CLASS:
            raise RuntimeError("smoke Pod RuntimeClass readback drifted")
        if observed_pod.get("spec", {}).get("containers", [{}])[0].get("image") != args.image:
            raise RuntimeError("smoke Pod image readback drifted")

        if log_result.returncode != 0:
            raise RuntimeError(f"cannot read runtime probe log: {log_result.stderr}")
        probe = json.loads(log_result.stdout.strip())
        checks = probe.get("checks")
        if not isinstance(checks, dict) or not checks or not all(checks.values()):
            raise RuntimeError(f"runtime probe did not pass every check: {probe}")

        container_status = observed_pod.get("status", {}).get("containerStatuses", [{}])[0]
        return {
            "candidate_sha": args.candidate_sha,
            "elapsed_seconds": round(time.time() - started, 3),
            "forbidden_pod_admission": sorted(denied),
            "image": args.image,
            "image_id": container_status.get("imageID"),
            "container_id": container_status.get("containerID"),
            "node": node_name,
            "pod_uid": observed_pod.get("metadata", {}).get("uid"),
            "probe": probe,
            "runtime_class": _RUNTIME_CLASS,
        }
    finally:
        if created_namespace:
            kubectl.run(
                (
                    "delete",
                    "namespace",
                    _NAMESPACE,
                    "--wait=true",
                    "--timeout=3m",
                ),
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
