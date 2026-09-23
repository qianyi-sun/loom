"""Only the pinned, hash-verified ingress image may reach the region registry."""
from __future__ import annotations

import hashlib
import importlib
import json
import subprocess

import pytest


@pytest.fixture
def mirror(tmp_path, monkeypatch):
    module = importlib.import_module("scripts.ops.nebius_ingress_image")
    config = json.dumps({"architecture": "amd64", "os": "linux", "config": {"Labels": {
        "org.opencontainers.image.version": "v3.7.13",
    }}}).encode()
    manifest = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                           "config": {"digest": "sha256:" + hashlib.sha256(config).hexdigest(), "size": len(config)}}).encode()
    pin = "sha256:" + hashlib.sha256(manifest).hexdigest()
    monkeypatch.setattr(module, "DIGEST", pin)
    prefix = "cr.eu-north1.nebius.cloud/testregistry"
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"auths": {"cr.eu-north1.nebius.cloud": {"auth": "aWFtOnNlY3JldA=="}}}))
    auth.chmod(0o600)
    state = tmp_path / "mirror-state"
    data = {"source": (manifest, config), "destination": None, "copy_failure": None, "commands": []}

    def run(command, **kwargs):
        data["commands"].append(command)
        assert kwargs["capture_output"] and not kwargs["check"] and 0 < kwargs["timeout"] <= 900
        if command[1] == "copy":
            record = json.loads((state / "image-mirror.json").read_bytes())
            assert record["status"] == "copy_intent"
            assert "--preserve-digests" in command
            assert command[-2:] == ["docker://docker.io/library/traefik@" + pin,
                                    "docker://" + prefix + "/loom-shared-ingress@" + pin]
            if data["copy_failure"] != "before":
                data["destination"] = data["source"]
            if data["copy_failure"]:
                raise subprocess.TimeoutExpired(command, 900, output=b"private-registry-token")
            return subprocess.CompletedProcess(command, 0, b"private-registry-token", b"")
        image = data["source"] if "docker.io/library/traefik@" in command[-1] else data["destination"]
        if image is None:
            return subprocess.CompletedProcess(command, 1, b"", b"private-registry-token")
        if "--config" in command and "--raw" not in command:
            # skopeo's default --config output is reserialized, not blob bytes.
            output = json.dumps(json.loads(image[1]), indent=4).encode() + b"\n"
        else:
            output = image[1 if "--config" in command else 0]
        return subprocess.CompletedProcess(command, 0, output, b"")

    monkeypatch.setattr(module.subprocess, "run", run)
    options = dict(registry_prefix=prefix, region="eu-north1", auth_file=auth, state_dir=state)
    return module, data, options, pin


def test_copy_returns_only_digest_bound_evidence_and_replay_rechecks_without_write(mirror):
    module, data, options, pin = mirror
    result = module.mirror_ingress_image(**options)
    assert result == {"status": "mirrored", "image": options["registry_prefix"] + "/loom-shared-ingress@" + pin,
                      "platform": "linux/amd64", "version": "v3.7.13"}
    assert module.mirror_ingress_image(**options) == result
    assert sum(command[1] == "copy" for command in data["commands"]) == 1
    assert "private-registry-token" not in json.dumps(result)
    assert (options["state_dir"] / "image-mirror.json").stat().st_mode & 0o077 == 0


@pytest.mark.parametrize("exists", [True, False])
def test_new_runner_without_copy_grant_can_only_read_destination(mirror, exists):
    module, data, options, _ = mirror
    if exists:
        data["destination"] = data["source"]
        assert module.mirror_ingress_image(**options, allow_copy=False)["status"] == "mirrored"
    else:
        with pytest.raises(module.ImageError):
            module.mirror_ingress_image(**options, allow_copy=False)
    assert data["commands"] and all(command[1] == "inspect" for command in data["commands"])
    assert all("docker.io/library/traefik" not in command[-1] for command in data["commands"])


@pytest.mark.parametrize("failure", ["before", "after"])
def test_unknown_copy_outcome_requires_exact_readback_never_a_second_copy(mirror, failure):
    module, data, options, _ = mirror
    data["copy_failure"] = failure
    if failure == "after":
        assert module.mirror_ingress_image(**options)["status"] == "mirrored"
    else:
        for _ in range(2):
            with pytest.raises(module.ImageError) as error:
                module.mirror_ingress_image(**options)
            assert "private-registry-token" not in str(error.value)
    assert sum(command[1] == "copy" for command in data["commands"]) == 1


@pytest.mark.parametrize("part", [0, 1])
def test_corrupt_source_content_is_rejected_before_registry_write(mirror, part):
    module, data, options, _ = mirror
    image = list(data["source"])
    image[part] += b" "
    data["source"] = tuple(image)
    with pytest.raises(module.ImageError):
        module.mirror_ingress_image(**options)
    assert not any(command[1] == "copy" for command in data["commands"])


@pytest.mark.parametrize("change", ["index", "architecture", "os", "version"])
def test_a_future_pin_still_requires_the_qualified_platform_and_version(mirror, change, monkeypatch):
    module, data, options, _ = mirror
    manifest, config = (json.loads(value) for value in data["source"])
    if change == "index":
        manifest["mediaType"] = "application/vnd.oci.image.index.v1+json"
    elif change == "version":
        config["config"]["Labels"]["org.opencontainers.image.version"] = "v2.0.0"
    else:
        config[change] = "arm64" if change == "architecture" else "windows"
    config_bytes = json.dumps(config).encode()
    manifest["config"] = {"digest": "sha256:" + hashlib.sha256(config_bytes).hexdigest(), "size": len(config_bytes)}
    manifest_bytes = json.dumps(manifest).encode()
    monkeypatch.setattr(module, "DIGEST", "sha256:" + hashlib.sha256(manifest_bytes).hexdigest())
    data["source"] = manifest_bytes, config_bytes
    with pytest.raises(module.ImageError):
        module.mirror_ingress_image(**options)
    assert not any(command[1] == "copy" for command in data["commands"])


@pytest.mark.parametrize("destination", [None, (b"{}", b"{}")])
def test_replay_rejects_missing_or_changed_destination_without_recopy(mirror, destination):
    module, data, options, _ = mirror
    module.mirror_ingress_image(**options)
    data["destination"] = destination
    with pytest.raises(module.ImageError):
        module.mirror_ingress_image(**options)
    assert sum(command[1] == "copy" for command in data["commands"]) == 1


@pytest.mark.parametrize("change", ["region", "prefix", "public-auth", "extra-auth", "symlink-auth", "journal"])
def test_invalid_authority_fails_before_network(mirror, change, tmp_path):
    module, data, options, _ = mirror
    if change == "region":
        options["region"] = "us-central1"
    elif change == "prefix":
        options["registry_prefix"] += "/foreign"
    elif change == "public-auth":
        options["auth_file"].chmod(0o644)
    elif change == "extra-auth":
        options["auth_file"].write_text('{"auths":{"docker.io":{"auth":"aWFtOnNlY3JldA=="}}}')
    elif change == "symlink-auth":
        link = tmp_path / "linked.json"
        link.symlink_to(options["auth_file"])
        options["auth_file"] = link
    else:
        options["state_dir"].mkdir(mode=0o700)
        journal = options["state_dir"] / "image-mirror.json"
        journal.write_text('{"status":"mirrored"}')
        journal.chmod(0o600)
    with pytest.raises(module.ImageError):
        module.mirror_ingress_image(**options)
    assert not data["commands"]
