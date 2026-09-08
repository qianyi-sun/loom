#!/usr/bin/env python3
"""Render standard ARC Helm installations; never contact or mutate a cluster."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ARC_VERSION = "0.14.2"
CHART_ROOT = "oci://ghcr.io/actions/actions-runner-controller-charts/"
SYSTEM_NAMESPACE = "loom-nebius-arc-system"
CONTROLLER_ACCOUNT = "loom-nebius-arc-controller"
RUNNER_NAMES = {"ci": "loom-nebius-ci", "release": "loom-nebius-release"}
NAMESPACES = {role: f"loom-nebius-arc-{role}" for role in RUNNER_NAMES}
BUILDKIT_HOST = "tcp://127.0.0.1:1234"


def _name(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value
    ):
        raise ValueError("Secret reference must be a Kubernetes name")
    return value


def _image(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z0-9][a-z0-9.:/-]*(?::[A-Za-z0-9_.-]+)?@sha256:[0-9a-f]{64}",
        value,
    ):
        raise ValueError("Images must be immutable registry digest references")
    return value


def _placement(role: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "nodeSelector": {
            "kubernetes.io/arch": "amd64",
            "loom.nebius/platform": "integration",
            "loom.nebius/node-role": role,
        },
        "tolerations": [
            {
                "key": "loom.nebius/platform",
                "operator": "Equal",
                "value": "integration",
                "effect": "NoSchedule",
            }
        ],
    }
    if role != "system":
        result["tolerations"].append(
            {
                "key": "loom.nebius/runner",
                "operator": "Equal",
                "value": role,
                "effect": "NoSchedule",
            }
        )
    return result


def _runner_values(config: dict[str, Any], role: str) -> dict[str, Any]:
    runner_image = _image(
        config.get("release_runner_image", config["runner_image"])
        if role == "release"
        else config["runner_image"]
    )
    # ARC's documented custom DinD PodSpec: keep containerMode unset so Helm
    # cannot inject floating upstream images or replace these volume settings.
    spec = {
        **_placement(role),
        "automountServiceAccountToken": False,
        "securityContext": {"fsGroup": 123},
        "terminationGracePeriodSeconds": 30,
        "initContainers": [
            {
                "name": "init-dind-externals",
                "image": runner_image,
                "command": ["cp", "-r", "/home/runner/externals/.", "/home/runner/tmpDir/"],
                "volumeMounts": [{"name": "externals", "mountPath": "/home/runner/tmpDir"}],
            },
            {
                "name": "dind",
                "image": _image(config["dind_image"]),
                "restartPolicy": "Always",
                "args": ["dockerd", "--host=unix:///var/run/docker.sock", "--group=123"],
                "securityContext": {"privileged": True},
                "startupProbe": {
                    "exec": {"command": ["docker", "info"]},
                    "failureThreshold": 24,
                    "periodSeconds": 5,
                },
                "resources": {
                    "requests": {"cpu": "2", "memory": "8Gi", "ephemeral-storage": "40Gi"},
                    "limits": {"memory": "16Gi", "ephemeral-storage": "80Gi"},
                },
                "volumeMounts": [
                    {"name": "work", "mountPath": "/home/runner/_work"},
                    {"name": "socket", "mountPath": "/var/run"},
                    {"name": "externals", "mountPath": "/home/runner/externals"},
                    {"name": "docker", "mountPath": "/var/lib/docker"},
                ],
            },
        ],
        "containers": [
            {
                "name": "runner",
                "image": runner_image,
                "command": ["/home/runner/run.sh"],
                "env": [
                    {"name": "DOCKER_HOST", "value": "unix:///var/run/docker.sock"},
                    {"name": "RUNNER_WAIT_FOR_DOCKER_IN_SECONDS", "value": "120"},
                ],
                "resources": {
                    "requests": {"cpu": "2", "memory": "8Gi", "ephemeral-storage": "20Gi"},
                    "limits": {"memory": "12Gi", "ephemeral-storage": "40Gi"},
                },
                "volumeMounts": [
                    {"name": "work", "mountPath": "/home/runner/_work"},
                    {"name": "socket", "mountPath": "/var/run"},
                ],
            }
        ],
        "volumes": [
            {"name": name, "emptyDir": {}} for name in ("work", "socket", "externals", "docker")
        ],
    }
    if role == "release":
        buildkit_image = _image(config["buildkit_image"])
        runner = spec["containers"][0]
        runner["env"] = [
            {"name": "BUILDKIT_HOST", "value": BUILDKIT_HOST},
            {"name": "BUILDKIT_BIN", "value": "/opt/buildkit/buildctl"},
            {"name": "BUILDKIT_CLIENT", "value": "/opt/buildkit/buildctl"},
        ]
        runner["securityContext"] = {
            "runAsNonRoot": True,
            "runAsUser": 1001,
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
            "appArmorProfile": {"type": "RuntimeDefault"},
        }
        runner["volumeMounts"] = [
            {"name": "work", "mountPath": "/home/runner/_work"},
            {"name": "buildkit-tools", "mountPath": "/opt/buildkit", "readOnly": True},
        ]
        spec["securityContext"] = {"fsGroup": 1000}
        spec["initContainers"] = [
            {
                "name": "init-buildctl",
                "image": buildkit_image,
                "command": ["cp", "/usr/bin/buildctl", "/tools/buildctl"],
                "volumeMounts": [{"name": "buildkit-tools", "mountPath": "/tools"}],
            },
            {
                "name": "buildkit",
                "image": buildkit_image,
                "restartPolicy": "Always",
                "args": [f"--addr={BUILDKIT_HOST}", "--oci-worker-no-process-sandbox"],
                # Official BuildKit rootless Kubernetes configuration. Only the
                # build daemon needs user-namespace/mount syscalls; no host mounts.
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 1000,
                    "runAsGroup": 1000,
                    "privileged": False,
                    "seccompProfile": {"type": "Unconfined"},
                    "appArmorProfile": {"type": "Unconfined"},
                },
                "startupProbe": {
                    "exec": {"command": ["buildctl", "--addr", BUILDKIT_HOST, "debug", "workers"]},
                    "failureThreshold": 24,
                    "periodSeconds": 5,
                },
                "resources": spec["initContainers"][1]["resources"],
                "volumeMounts": [
                    {"name": "buildkit-cache", "mountPath": "/home/user/.local/share/buildkit"}
                ],
            },
        ]
        spec["volumes"] = [
            {"name": name, "emptyDir": {}} for name in ("work", "buildkit-tools", "buildkit-cache")
        ]
    pull_secret = config.get("image_pull_secret")
    if pull_secret:
        spec["imagePullSecrets"] = [{"name": _name(pull_secret)}]
    result: dict[str, Any] = {
        "githubConfigUrl": config["github_config_url"],
        "githubConfigSecret": _name(config[f"github_secret_{role}"]),
        "runnerScaleSetName": RUNNER_NAMES[role],
        "minRunners": 0,
        "maxRunners": config.get("ci_max_runners", 2)
        if role == "ci"
        else config.get("release_max_runners", 0),
        "controllerServiceAccount": {"namespace": SYSTEM_NAMESPACE, "name": CONTROLLER_ACCOUNT},
        "listenerTemplate": {
            "spec": {
                **_placement("system"),
                "containers": [
                    {
                        "name": "listener",
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"memory": "512Mi"},
                        },
                    }
                ],
            }
        },
        "template": {"metadata": {"labels": {"loom.nebius/runner": role}}, "spec": spec},
    }
    if group := config.get(f"runner_group_{role}"):
        result["runnerGroup"] = group
    if pull_secret:
        result["listenerTemplate"]["spec"]["imagePullSecrets"] = [{"name": _name(pull_secret)}]
    return result


def _install_script() -> str:
    header = """#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
mode="${1:---dry-run}"
case "$mode" in
  --dry-run) [[ $# -le 1 ]] ;;
  --apply) [[ $# == 3 && "$2" == --context && -n "$3" ]] ;;
  *) echo 'Usage: install.sh [--dry-run | --apply --context NAME]' >&2; exit 2 ;;
esac
command -v helm >/dev/null
if [[ "$mode" == --apply ]]; then
  context="$3"
  kubectl --context "$context" apply -f namespaces.yaml
"""
    for role in RUNNER_NAMES:
        header += (
            f'  secret=$(python3 -c \'import json; print(json.load(open("{role}.values.json"))["githubConfigSecret"])\')\n'
            f'  kubectl --context "$context" -n {NAMESPACES[role]} get secret "$secret" -o name >/dev/null\n'
        )
    header += "fi\n"
    for release, chart, namespace, values in (
        ("loom-nebius-arc", "gha-runner-scale-set-controller", SYSTEM_NAMESPACE, "controller"),
        *(
            (RUNNER_NAMES[role], "gha-runner-scale-set", NAMESPACES[role], role)
            for role in RUNNER_NAMES
        ),
    ):
        common = f"{release} {CHART_ROOT}{chart} --version {ARC_VERSION} --namespace {namespace} -f {values}.values.json"
        header += (
            'if [[ "$mode" == --dry-run ]]; then\n'
            f"  helm template {common} --include-crds --kubeconfig /dev/null > rendered-{values}.yaml\n"
            "else\n"
            f'  helm upgrade --install {common} --kube-context "$context" --wait --timeout 10m\n'
            "fi\n"
        )
    return header


def render(config: dict[str, Any]) -> dict[str, str]:
    allowed = {
        "github_config_url",
        "github_secret_ci",
        "github_secret_release",
        "controller_image",
        "runner_image",
        "dind_image",
        "image_pull_secret",
        "ci_max_runners",
        "runner_group_ci",
        "runner_group_release",
        "buildkit_image",
        "release_max_runners",
        "allow_external_images_for_bootstrap",
        "release_runner_image",
    }
    if not isinstance(config, dict) or set(config) - allowed:
        raise ValueError("Unsupported runner configuration fields")
    if not re.fullmatch(
        r"https://github\.com/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)?", config["github_config_url"]
    ):
        raise ValueError("Expected a GitHub organization or repository URL")
    if (
        type(config.get("ci_max_runners", 2)) is not int
        or not 1 <= config.get("ci_max_runners", 2) <= 4
    ):
        raise ValueError("CI runner limit must be between one and four")
    if type(config.get("release_max_runners", 0)) is not int or config.get(
        "release_max_runners", 0
    ) not in (0, 1):
        raise ValueError("Release runner limit must be zero or one")
    external = config.get("allow_external_images_for_bootstrap", False)
    if type(external) is not bool:
        raise ValueError("External image bootstrap flag must be boolean")
    image_keys = ["controller_image", "runner_image", "dind_image", "buildkit_image"]
    if "release_runner_image" in config:
        image_keys.append("release_runner_image")
    elif not external:
        raise ValueError("Pure Nebius release requires an explicit derivative runner image")
    for key in image_keys:
        image_ref = _image(config[key])
        if not external and not image_ref.split("/", 1)[0].endswith(".nebius.cloud"):
            raise ValueError(
                "Mirror images to Nebius or explicitly select external-image bootstrap"
            )
    for role in RUNNER_NAMES:
        group = config.get(f"runner_group_{role}")
        if group is not None and (
            not isinstance(group, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", group)
        ):
            raise ValueError("Invalid GitHub runner group")
    image = _image(config["controller_image"])
    repository_tag, digest = image.split("@")
    repository, _, tag = repository_tag.rpartition(":")
    if tag != ARC_VERSION:
        raise ValueError("Controller image tag must match the pinned Helm chart")
    controller = {
        **_placement("system"),
        "image": {"repository": repository, "tag": f"{tag}@{digest}", "pullPolicy": "IfNotPresent"},
        "serviceAccount": {"create": True, "name": CONTROLLER_ACCOUNT},
        "flags": {"logLevel": "info", "logFormat": "json"},
        "resources": {
            "requests": {"cpu": "250m", "memory": "256Mi"},
            "limits": {"memory": "1Gi"},
        },
    }
    if config.get("image_pull_secret"):
        controller["imagePullSecrets"] = [{"name": _name(config["image_pull_secret"])}]
    resources = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}
        for name in (SYSTEM_NAMESPACE, *NAMESPACES.values())
    ]
    # This policy selects runner pods only, not ARC listeners. The listener
    # needs Kubernetes API access; PR job containers do not.
    resources.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "ci-public-egress", "namespace": NAMESPACES["ci"]},
            "spec": {
                "podSelector": {"matchLabels": {"loom.nebius/runner": "ci"}},
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
                            {"protocol": protocol, "port": 53} for protocol in ("UDP", "TCP")
                        ],
                    },
                    {
                        "to": [
                            {
                                "ipBlock": {
                                    "cidr": "0.0.0.0/0",
                                    "except": [
                                        "10.0.0.0/8",
                                        "172.16.0.0/12",
                                        "192.168.0.0/16",
                                        "169.254.0.0/16",
                                        "100.64.0.0/10",
                                    ],
                                }
                            }
                        ]
                    },
                ],
            },
        }
    )
    resources.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "release-no-ingress", "namespace": NAMESPACES["release"]},
            "spec": {
                "podSelector": {"matchLabels": {"loom.nebius/runner": "release"}},
                "policyTypes": ["Ingress"],
                "ingress": [],
            },
        }
    )
    return {
        "controller.values.json": json.dumps(controller, indent=2) + "\n",
        **{
            f"{role}.values.json": json.dumps(_runner_values(config, role), indent=2) + "\n"
            for role in RUNNER_NAMES
        },
        "namespaces.yaml": yaml.safe_dump_all(resources, sort_keys=False),
        "install.sh": _install_script(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        files = render(json.loads(args.config.read_text()))
        args.output.mkdir(parents=True, exist_ok=False)
        for name, text in files.items():
            path = args.output / name
            path.write_text(text)
            path.chmod(0o755 if name == "install.sh" else 0o644)
    except (ValueError, KeyError, TypeError, OSError):
        print(
            "Runner render failed: invalid configuration or output must be a new directory",
            file=sys.stderr,
        )
        return 1
    print(f"Rendered ARC values and dry-run/install commands to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
