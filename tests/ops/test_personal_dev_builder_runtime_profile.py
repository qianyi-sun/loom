from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ops.personal_dev_builder_runtime_profile import (
    RuntimeProfileError,
    load_runtime_profile,
    render_runtime_class,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_PATH = _REPO_ROOT / "deploy/dev-fleet/personal-dev-builder-runtime-profile.json"
_RUNTIME_CLASS_PATH = _REPO_ROOT / "deploy/dev-fleet/personal-dev-builder-runtime-class.yaml"
_ARCHIVE_SHA512 = (
    "3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe"
    "33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62"
)
_MEMBERS = {
    "containerd-shim-runsc-v1": (
        43_208_193,
        "71b9e90897f39ee51fee8e0345cf675956d95bd1d6458c92f49d984097ffa327",
    ),
    "gvisor-bin/checkpointgofer": (
        68_743_833,
        "a4f6837a9837a8c3499c7e2d1d58931babb140bf228762f1c2b13469256b2bda",
    ),
    "gvisor-bin/gvisor_sentry": (
        47_910_193,
        "871a4b5ca197d37fae7d30ab0aa356fe3156c1f9836e8a40122f7f08c6b46f62",
    ),
    "gvisor-bin/runsc-metric-server": (
        52_294_519,
        "ff3476a1f28cb684bd7340e183e80f8af7a5be5b0b3ca4bdb79bc2a6d92b6cb4",
    ),
    "runsc": (
        104_854_508,
        "670bcd3cbc103f00d8bb5098edc370f32397ee4c134231436bafa659bb3c068e",
    ),
}


def _value() -> dict[str, Any]:
    return json.loads(_PROFILE_PATH.read_text(encoding="ascii"))


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("ascii")


def test_checked_in_profile_binds_the_measured_release_and_host() -> None:
    profile = load_runtime_profile(_PROFILE_PATH)

    assert profile.archive_sha512 == _ARCHIVE_SHA512
    assert {
        name: (member.size, member.sha256)
        for name, member in profile.members.items()
    } == _MEMBERS
    assert all(member.archive_mode == 0o755 for member in profile.members.values())
    assert all(member.install_mode == 0o555 for member in profile.members.values())
    assert profile.version == "release-20260810.0"
    assert profile.tag_commit == "5ceb9a5fd5750d6c73dd166441f28306039300d0"
    assert profile.k3s_version == "v1.36.2+k3s1"
    assert profile.containerd_version == "v2.3.2-k3s2"
    assert profile.selector == {
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "loom.dev/personal-dev-runtime-profile-a": profile.sha256[:32],
        "loom.dev/personal-dev-runtime-profile-b": profile.sha256[32:],
    }


def test_profile_renders_exact_k3s_and_runsc_configuration() -> None:
    profile = load_runtime_profile(_PROFILE_PATH)

    assert profile.k3s_template == (
        '{{ template "base" . }}\n\n'
        "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
        "'runsc-personal-dev']\n"
        '  runtime_type = "io.containerd.runsc.v1"\n'
        "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
        "'runsc-personal-dev'.options]\n"
        '  TypeUrl = "io.containerd.runsc.v1.options"\n'
        '  ConfigPath = "/etc/containerd/runsc-personal-dev.toml"\n'
    ).encode("ascii")
    assert profile.runsc_toml == (
        'binary_name = "/opt/loom/gvisor/release-20260810.0/runsc"\n'
        "\n"
        "[runsc_config]\n"
        '  allow-flag-override = "false"\n'
        '  allow-packet-socket-write = "false"\n'
        '  allow-suid = "false"\n'
        '  debug = "false"\n'
        '  directfs = "false"\n'
        '  file-access = "exclusive"\n'
        '  file-access-mounts = "shared"\n'
        '  gvisor-marker-file = "true"\n'
        '  host-fifo = "none"\n'
        '  host-settings = "check"\n'
        '  host-uds = "none"\n'
        '  net-raw = "false"\n'
        '  network = "sandbox"\n'
        '  oci-seccomp = "true"\n'
        '  platform = "kvm"\n'
        '  platform_device_path = "/dev/kvm"\n'
        '  profile = "false"\n'
        '  restore-spec-validation = "enforce"\n'
        '  sidecar-release-enforcement-policy = "ALWAYS"\n'
        '  strace = "false"\n'
        '  watchdog-action = "panic"\n'
    ).encode("ascii")


def test_checked_in_runtime_class_is_derived_only_from_the_profile() -> None:
    profile = load_runtime_profile(_PROFILE_PATH)

    assert yaml.safe_load(_RUNTIME_CLASS_PATH.read_text(encoding="ascii")) == (
        render_runtime_class(profile)
    )


def _mutate_unknown(value: dict[str, Any]) -> None:
    value["unexpected"] = True


def _mutate_hash(value: dict[str, Any]) -> None:
    value["archive"]["members"]["runsc"]["sha256"] = "a" * 64


def _mutate_member(value: dict[str, Any]) -> None:
    value["archive"]["members"]["extra"] = {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "a" * 64,
        "size": 1,
    }


def _mutate_path(value: dict[str, Any]) -> None:
    value["installation"]["release_root"] = "/usr/local/gvisor"


def _mutate_flag(value: dict[str, Any]) -> None:
    value["runtime"]["flags"]["network"] = "host"


def _mutate_version(value: dict[str, Any]) -> None:
    value["host"]["k3s_version"] = "v1.36.3+k3s1"


@pytest.mark.parametrize(
    "mutation",
    (
        _mutate_unknown,
        _mutate_hash,
        _mutate_member,
        _mutate_path,
        _mutate_flag,
        _mutate_version,
    ),
)
def test_profile_rejects_any_authority_or_release_drift(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    value = _value()
    mutation(value)
    path = tmp_path / "profile.json"
    path.write_bytes(_canonical(value))

    with pytest.raises(RuntimeProfileError, match="profile"):
        load_runtime_profile(path)


def test_profile_rejects_duplicate_keys_and_noncanonical_bytes(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"schema":"one","schema":"two"}\n')
    noncanonical = tmp_path / "noncanonical.json"
    noncanonical.write_bytes(json.dumps(_value(), sort_keys=True).encode("ascii"))

    for path in (duplicate, noncanonical):
        with pytest.raises(RuntimeProfileError, match="profile"):
            load_runtime_profile(path)


def test_profile_rejects_excessive_json_nesting_with_the_public_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested.json"
    path.write_bytes(b"[" * 2_000 + b"]" * 2_000)

    with pytest.raises(RuntimeProfileError, match="profile"):
        load_runtime_profile(path)


@pytest.mark.parametrize("constant", (b"NaN", b"Infinity", b"-Infinity"))
def test_profile_rejects_nonfinite_json_constants_with_the_public_error(
    tmp_path: Path,
    constant: bytes,
) -> None:
    path = tmp_path / "nonfinite.json"
    path.write_bytes(b'{"schema":' + constant + b"}\n")

    with pytest.raises(RuntimeProfileError, match="profile"):
        load_runtime_profile(path)
