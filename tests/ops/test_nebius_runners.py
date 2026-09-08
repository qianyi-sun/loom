"""ARC bootstrap boundaries, using the real generated command path without a cluster."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from scripts.ops.render_nebius_runners import ARC_VERSION, NAMESPACES, SYSTEM_NAMESPACE, render

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def config() -> dict:
    return json.loads((ROOT / "deploy/nebius/runners.example.json").read_text())


@pytest.mark.parametrize("role", ["ci", "release"])
def test_ephemeral_runners_have_no_host_storage_or_cluster_credentials(config: dict, role: str) -> None:
    values = json.loads(render(config)[f"{role}.values.json"])
    pod = values["template"]["spec"]
    assert values["runnerScaleSetName"] == f"loom-nebius-{role}"
    assert values["minRunners"] == 0
    assert values["maxRunners"] == (2 if role == "ci" else 0)
    assert pod["nodeSelector"]["loom.nebius/node-role"] == role
    assert pod["nodeSelector"]["loom.nebius/platform"] == "integration"
    assert values["listenerTemplate"]["spec"]["nodeSelector"]["loom.nebius/node-role"] == "system"
    assert pod["automountServiceAccountToken"] is False
    assert all(set(volume) == {"name", "emptyDir"} for volume in pod["volumes"])
    assert "hostNetwork" not in pod and "hostPID" not in pod
    containers = {container["name"]: container for container in [*pod["initContainers"], *pod["containers"]]}
    assert containers["runner"]["image"] == config["runner_image"]
    if role == "ci":
        assert containers["dind"]["restartPolicy"] == "Always"
        assert "--host=unix:///var/run/docker.sock" in containers["dind"]["args"]
        assert containers["init-dind-externals"]["image"] == config["runner_image"]
    else:
        assert "dind" not in containers
        assert all(not item.get("securityContext", {}).get("privileged") for item in containers.values())
        assert containers["buildkit"]["securityContext"]["runAsUser"] == 1000
        assert containers["buildkit"]["securityContext"]["seccompProfile"] == {"type": "Unconfined"}
        assert containers["runner"]["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
        assert containers["runner"]["securityContext"]["allowPrivilegeEscalation"] is False
        assert "--addr=tcp://127.0.0.1:1234" in containers["buildkit"]["args"]
        assert "--oci-worker-no-process-sandbox" in containers["buildkit"]["args"]
        assert {item["name"]: item["value"] for item in containers["runner"]["env"]} == {
            "BUILDKIT_HOST": "tcp://127.0.0.1:1234", "BUILDKIT_BIN": "/opt/buildkit/buildctl",
            "BUILDKIT_CLIENT": "/opt/buildkit/buildctl",
        }
    assert all("secretKeyRef" not in json.dumps(container) for container in containers.values())
    assert "github_app_private_key" not in json.dumps(values)
    assert "containerMode" not in values


def test_controller_and_network_policy_keep_ci_away_from_release(config: dict) -> None:
    files = render(config)
    controller = json.loads(files["controller.values.json"])
    assert controller["nodeSelector"]["loom.nebius/node-role"] == "system"
    assert controller["image"]["tag"].startswith(ARC_VERSION + "@sha256:")
    objects = list(yaml.safe_load_all(files["namespaces.yaml"]))
    assert {item["metadata"]["name"] for item in objects if item["kind"] == "Namespace"} == {
        SYSTEM_NAMESPACE, *NAMESPACES.values(),
    }
    policy = next(item for item in objects if item["kind"] == "NetworkPolicy")
    assert policy["metadata"]["namespace"] == NAMESPACES["ci"]
    assert policy["spec"]["podSelector"] == {"matchLabels": {"loom.nebius/runner": "ci"}}
    assert policy["spec"]["ingress"] == []
    excluded = policy["spec"]["egress"][1]["to"][0]["ipBlock"]["except"]
    assert {"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16"} <= set(excluded)


@pytest.mark.parametrize(
    ("key", "value"),
    [("runner_image", "ghcr.io/actions/actions-runner:latest"),
     ("dind_image", "docker:dind"), ("controller_image", "controller:latest"),
     ("github_secret_ci", {"github_token": "never-serialize-this"}),
     ("github_config_url", "https://github.com/org/repo;injected"),
     ("ci_max_runners", 0), ("ci_max_runners", True), ("release_max_runners", 2),
     ("allow_external_images_for_bootstrap", False),
     ("github_app_private_key", "never-serialize-this")],
)
def test_invalid_or_secret_bearing_configuration_is_rejected(config: dict, key: str, value: object) -> None:
    config[key] = value
    with pytest.raises((ValueError, TypeError)):
        render(config)


def test_private_registry_mirror_and_pull_secret_reach_all_pods(config: dict) -> None:
    config["image_pull_secret"] = "nebius-registry-read"
    config["runner_image"] = "cr.eu-north1.nebius.cloud/example/runner@" + config["runner_image"].split("@")[1]
    files = render(config)
    expected = [{"name": "nebius-registry-read"}]
    assert json.loads(files["controller.values.json"])["imagePullSecrets"] == expected
    for role in NAMESPACES:
        values = json.loads(files[f"{role}.values.json"])
        assert values["template"]["spec"]["imagePullSecrets"] == expected
    assert values["listenerTemplate"]["spec"]["imagePullSecrets"] == expected


def test_pure_nebius_requires_all_image_mirrors_and_allows_explicit_release_smoke(config: dict) -> None:
    config["allow_external_images_for_bootstrap"] = False
    config["release_max_runners"] = 1
    for key in ("runner_image", "dind_image", "buildkit_image", "controller_image"):
        image, digest = config[key].split("@")
        config[key] = "cr.eu-north1.nebius.cloud/example/" + image.rsplit("/", 1)[1] + "@" + digest
    config["release_runner_image"] = "cr.eu-north1.nebius.cloud/example/loom-runner@sha256:" + "a" * 64
    values = json.loads(render(config)["release.values.json"])
    assert values["maxRunners"] == 1
    assert values["template"]["spec"]["containers"][0]["image"] == config["release_runner_image"]
    ci = json.loads(render(config)["ci.values.json"])
    assert ci["template"]["spec"]["containers"][0]["image"] == config["runner_image"]
    assert all(item["image"].startswith("cr.eu-north1.nebius.cloud/")
               for item in values["template"]["spec"]["initContainers"])
    del config["release_runner_image"]
    with pytest.raises(ValueError, match="explicit derivative"):
        render(config)


def _bootstrap(tmp_path: Path, config: dict) -> tuple[Path, dict[str, str]]:
    output = tmp_path / "rendered"
    output.mkdir()
    for name, text in render(config).items():
        (output / name).write_text(text)
    commands = tmp_path / "commands"
    bin_path = tmp_path / "bin"
    bin_path.mkdir()
    for tool in ("helm", "kubectl"):
        executable = bin_path / tool
        executable.write_text(
            "#!/usr/bin/env python3\nimport json,os,sys\n"
            "with open(os.environ['COMMANDS'],'a') as stream:\n"
            " stream.write(json.dumps([os.path.basename(sys.argv[0]),*sys.argv[1:]])+'\\n')\n"
            "if os.environ.get('FAIL_SECRET') and 'secret' in sys.argv: sys.exit(1)\n"
        )
        executable.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_path}:{os.environ['PATH']}", "COMMANDS": str(commands)}
    return output, env


def test_bootstrap_defaults_to_helm_render_without_cluster_access(tmp_path: Path, config: dict) -> None:
    output, env = _bootstrap(tmp_path, config)
    result = subprocess.run(["bash", str(output / "install.sh")], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    commands = [json.loads(line) for line in Path(env["COMMANDS"]).read_text().splitlines()]
    assert len(commands) == 3
    assert all(command[:2] == ["helm", "template"] for command in commands)
    assert all(command[command.index("--version") + 1] == ARC_VERSION for command in commands)


def test_apply_requires_context_and_existing_registration_secret(tmp_path: Path, config: dict) -> None:
    output, env = _bootstrap(tmp_path, config)
    command = ["bash", str(output / "install.sh"), "--apply"]
    assert subprocess.run(command, env=env, capture_output=True).returncode != 0
    assert not Path(env["COMMANDS"]).exists()
    result = subprocess.run([*command, "--context", "integration"], env={**env, "FAIL_SECRET": "1"}, capture_output=True)
    assert result.returncode != 0
    commands = [json.loads(line) for line in Path(env["COMMANDS"]).read_text().splitlines()]
    assert all(item[0] == "kubectl" and item[1:3] == ["--context", "integration"] for item in commands)
    assert not any(item[0] == "helm" for item in commands)
