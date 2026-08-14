#!/usr/bin/env python3
"""Build and verify the closed OLDLAB Stage 1 simulator image contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from importlib.metadata import distributions
from pathlib import Path
from typing import Any


class ImageContractError(ValueError):
    """The image contract or runtime observation is unsafe or inconsistent."""


_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_GPU_UUID = re.compile(r"^GPU-[0-9a-fA-F-]{8,64}$")
_MANIFEST_KEYS = {
    "application_features",
    "base_image_index_digest",
    "base_image_platform_manifest_digest",
    "build_sha",
    "build_tree_sha",
    "gpu_contract",
    "minimum_nvidia_driver",
    "loom_runtime_lock_sha256",
    "platform",
    "pipeline_rng_patch_sha256",
    "preflight_argv",
    "provider_assets",
    "schema_version",
    "sim_python_version",
    "sim_distribution_freeze_sha256",
    "sim_requirements_lock_sha256",
    "source_evidence_sha256",
    "source_lock_sha256",
    "vla_python_version",
    "vla_distribution_freeze_sha256",
    "vla_uv_lock_sha256",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _distribution_freeze_digest() -> str:
    """Digest the effective, import-order authoritative Python distributions."""
    effective: dict[str, str] = {}
    for distribution in distributions():
        raw_name = distribution.metadata["Name"]
        if not raw_name:
            continue
        name = re.sub(r"[-_.]+", "-", str(raw_name)).lower()
        # importlib.metadata follows sys.path order.  Keep the first visible
        # distribution when a system-site package is shadowed by the venv.
        effective.setdefault(name, str(distribution.version))
    payload = _canonical([{"name": name, "version": effective[name]} for name in sorted(effective)])
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _load_canonical(path: Path, *, max_bytes: int) -> dict[str, Any]:
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not 1 <= before.st_size <= max_bytes
    ):
        raise ImageContractError(f"{path.name} is not a bounded private regular file")
    payload = path.read_bytes()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImageContractError(f"{path.name} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict) or payload != _canonical(value):
        raise ImageContractError(f"{path.name} is not canonical JSON plus LF")
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ImageContractError(f"{path.name} changed while reading")
    return value


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ImageContractError(f"{label} is not a canonical digest")
    return value


def _require_git_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_SHA.fullmatch(value) is None:
        raise ImageContractError(f"{label} is not a full lowercase Git SHA")
    return value


def build_manifest(args: argparse.Namespace) -> None:
    source_evidence = _load_canonical(args.source_evidence, max_bytes=1_048_576)
    if set(source_evidence) != {
        "integration_patches",
        "schema_version",
        "source_lock_sha256",
        "sources",
    }:
        raise ImageContractError("source evidence keys are not closed")
    if source_evidence["schema_version"] != "loom.behavior-stage1-image-source-evidence.v1":
        raise ImageContractError("source evidence schema drift")
    source_lock_digest = _sha256(args.source_lock)
    if source_evidence["source_lock_sha256"] != source_lock_digest:
        raise ImageContractError("source evidence does not bind the source lock")
    if source_evidence["integration_patches"] != [
        {
            "name": "openpi-transformers-cache-type",
            "path": "openpi/src/openpi/models_pytorch/gemma_pytorch.py",
            "result_sha256": "sha256:4f75d3647fadb7d00c0fee884579cf5a3ef33a6af53a3908fc237358d9606cf5",
            "source_sha256": "sha256:08fd8d750519f0fb44fc5173311e50a30f4c8f32c02e51244b4f8e47b32cd52f",
        }
    ]:
        raise ImageContractError("source evidence integration patch drift")
    try:
        source_lock = json.loads(args.source_lock.read_bytes())
        sim_freeze = source_lock["sim_python"]["accepted_freeze_sha256"]
        vla_freeze = source_lock["vla_python"]["accepted_freeze_sha256"]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ImageContractError("source lock has no closed Python freeze authority") from exc
    manifest = {
        "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
        "base_image_index_digest": "sha256:f3563cb2ba0c18af0b2fb321360dcb73a917b899f879e3213623d6bee484fa54",
        "base_image_platform_manifest_digest": "sha256:93b0f99635ab126fb5b33298d513c11520f119f0ee60ff8414ccef67ea977829",
        "build_sha": _require_git_sha(args.build_sha, "build SHA"),
        "build_tree_sha": _require_git_sha(args.build_tree_sha, "build tree SHA"),
        "gpu_contract": {
            "count_exact": 2,
            "memory_mib_min_each": 16000,
            "model_exact": "NVIDIA GeForce RTX 5080",
            "ordered_roles": ["sim", "vla"],
        },
        "minimum_nvidia_driver": "570.00",
        "loom_runtime_lock_sha256": _sha256(args.loom_runtime_lock),
        "platform": "linux/amd64",
        "pipeline_rng_patch_sha256": _sha256(args.pipeline_rng_patch),
        "preflight_argv": [
            "/opt/loom/bin/sim-python",
            "-I",
            "/opt/loom/bin/behavior_stage1_image_contract.py",
            "preflight",
            "--json",
        ],
        "provider_assets": [],
        "schema_version": "loom.behavior-stage1-image-compatibility.v1",
        "sim_distribution_freeze_sha256": _require_digest(sim_freeze, "sim freeze"),
        "sim_python_version": "3.11.13",
        "sim_requirements_lock_sha256": _sha256(args.sim_lock),
        "source_evidence_sha256": _sha256(args.source_evidence),
        "source_lock_sha256": source_lock_digest,
        "vla_distribution_freeze_sha256": _require_digest(vla_freeze, "VLA freeze"),
        "vla_python_version": "3.11.13",
        "vla_uv_lock_sha256": _sha256(args.vla_lock),
    }
    payload = _canonical(manifest)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(args.output, flags, 0o444)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _version_tuple(value: str) -> tuple[int, ...]:
    try:
        return tuple(int(item) for item in value.split("."))
    except ValueError as exc:
        raise ImageContractError("NVIDIA driver version is invalid") from exc


def _gpu_observation(minimum_driver: str) -> tuple[list[dict[str, object]], str]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,uuid,name,memory.total,driver_version",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ImageContractError("NVIDIA device observation failed") from exc
    devices: list[dict[str, object]] = []
    driver: str | None = None
    for expected_index, line in enumerate(completed.stdout.splitlines()):
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 5:
            raise ImageContractError("NVIDIA device observation is malformed")
        index_text, uuid, name, memory_text, observed_driver = fields
        try:
            index = int(index_text)
            memory_mib = int(memory_text)
        except ValueError as exc:
            raise ImageContractError("NVIDIA device numbers are malformed") from exc
        if index != expected_index or _GPU_UUID.fullmatch(uuid) is None:
            raise ImageContractError("NVIDIA device order or UUID is invalid")
        if name != "NVIDIA GeForce RTX 5080" or memory_mib < 16000:
            raise ImageContractError("NVIDIA device does not satisfy the OLDLAB contract")
        if driver is not None and driver != observed_driver:
            raise ImageContractError("NVIDIA driver version differs across devices")
        driver = observed_driver
        devices.append(
            {
                "device_uuid": uuid,
                "logical_index": index,
                "memory_mib": memory_mib,
                "model": name,
                "role": ("sim", "vla")[index] if index < 2 else "invalid",
            }
        )
    if len(devices) != 2 or driver is None:
        raise ImageContractError("OLDLAB Stage 1 requires exactly two GPUs")
    if _version_tuple(driver) < _version_tuple(minimum_driver):
        raise ImageContractError("NVIDIA driver is older than the image contract")
    return devices, driver


def _probe(
    argv: list[str],
    label: str,
    *,
    environment: dict[str, str] | None = None,
) -> dict[str, object]:
    probe_environment = {
        "HOME": "/scratch/home",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    }
    if environment is not None:
        if set(probe_environment) & set(environment):
            raise ImageContractError(f"{label} probe environment overlaps fixed keys")
        probe_environment.update(environment)
    try:
        completed = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            timeout=90,
            env=probe_environment,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ImageContractError(f"{label} runtime probe failed") from exc
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ImageContractError(f"{label} runtime probe output is invalid") from exc
    if not isinstance(value, dict):
        raise ImageContractError(f"{label} runtime probe output is not an object")
    return value


def _external_freeze_digest(interpreter: str) -> str:
    try:
        completed = subprocess.run(
            [
                interpreter,
                "-I",
                "/opt/loom/bin/behavior_stage1_image_contract.py",
                "freeze-digest",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                "HOME": "/scratch/home",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            },
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ImageContractError("VLA distribution freeze probe failed") from exc
    value = completed.stdout.strip()
    return _require_digest(value, "observed VLA distribution freeze")


def _verify_distribution_freezes(manifest: dict[str, Any]) -> None:
    if _distribution_freeze_digest() != manifest["sim_distribution_freeze_sha256"]:
        raise ImageContractError("simulator distribution freeze drifted")
    if (
        _external_freeze_digest("/opt/loom/venv-vla/bin/python")
        != manifest["vla_distribution_freeze_sha256"]
    ):
        raise ImageContractError("VLA distribution freeze drifted")


def preflight(args: argparse.Namespace) -> None:
    manifest_path = Path("/opt/loom/contracts/compatibility-manifest.json")
    source_evidence_path = Path("/opt/loom/contracts/source-evidence.json")
    manifest = _load_canonical(manifest_path, max_bytes=1_048_576)
    if set(manifest) != _MANIFEST_KEYS:
        raise ImageContractError("compatibility manifest keys are not closed")
    if manifest["schema_version"] != "loom.behavior-stage1-image-compatibility.v1":
        raise ImageContractError("compatibility manifest schema drift")
    if manifest["platform"] != "linux/amd64":
        raise ImageContractError("compatibility platform drift")
    if manifest["source_evidence_sha256"] != _sha256(source_evidence_path):
        raise ImageContractError("installed source evidence digest drift")
    if manifest["pipeline_rng_patch_sha256"] != _sha256(
        Path("/opt/loom/sources/omnigibson/omnigibson/utils/pipeline_rng.py")
    ):
        raise ImageContractError("installed Pipeline RNG patch digest drift")
    _require_digest(manifest["source_lock_sha256"], "source lock")
    _verify_distribution_freezes(manifest)
    devices, driver = _gpu_observation(str(manifest["minimum_nvidia_driver"]))
    simulator = _probe(
        [
            "/opt/loom/bin/sim-python",
            "-I",
            "-c",
            (
                "import json,sys; import omnigibson; import bddl; "
                "from loom.integrations.behavior.stages.rollout_backend import _load_runtime; "
                "runtime=_load_runtime(); print(json.dumps({'omnigibson':runtime.version,"
                "'python':'.'.join(map(str,sys.version_info[:3]))},sort_keys=True,separators=(',',':')))"
            ),
        ],
        "simulator",
        environment={
            "OMNIGIBSON_APPDATA_PATH": "/scratch/omnigibson/appdata",
            "OMNIGIBSON_DATA_PATH": "/inputs/dataset/payload/omnigibson",
            "OMNIGIBSON_HEADLESS": "1",
            "OMNI_KIT_ACCEPT_EULA": "YES",
        },
    )
    vla = _probe(
        [
            "/opt/loom/venv-vla/bin/python",
            "-I",
            "-c",
            (
                "import json,sys,torch; "
                "from loom.integrations.behavior.vla.policy_backend import _load_runtime; "
                "runtime=_load_runtime(); print(json.dumps({'b1k':runtime.version,"
                "'cuda':torch.version.cuda,'python':'.'.join(map(str,sys.version_info[:3]))},"
                "sort_keys=True,separators=(',',':')))"
            ),
        ],
        "VLA",
    )
    if simulator != {"omnigibson": "3.8.0", "python": "3.11.13"}:
        raise ImageContractError("simulator runtime versions drifted")
    if vla.get("b1k") != "0.1.0" or vla.get("python") != "3.11.13":
        raise ImageContractError("VLA runtime versions drifted")
    observation = {
        "compatibility_manifest_sha256": _sha256(manifest_path),
        "devices": devices,
        "driver_version": driver,
        "schema_version": "loom.behavior-stage1-image-preflight.v1",
        "simulator": simulator,
        "source_evidence_sha256": _sha256(source_evidence_path),
        "vla": vla,
    }
    if args.json:
        sys.stdout.buffer.write(_canonical(observation))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build-manifest")
    build.add_argument("--source-lock", type=Path, required=True)
    build.add_argument("--source-evidence", type=Path, required=True)
    build.add_argument("--sim-lock", type=Path, required=True)
    build.add_argument("--loom-runtime-lock", type=Path, required=True)
    build.add_argument("--vla-lock", type=Path, required=True)
    build.add_argument("--build-sha", required=True)
    build.add_argument("--build-tree-sha", required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--pipeline-rng-patch", type=Path, required=True)
    runtime = subparsers.add_parser("preflight")
    runtime.add_argument("--json", action="store_true")
    verify = subparsers.add_parser("verify-freezes")
    verify.add_argument("--manifest", type=Path, required=True)
    subparsers.add_parser("freeze-digest")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "build-manifest":
            build_manifest(args)
        elif args.command == "preflight":
            preflight(args)
        elif args.command == "verify-freezes":
            manifest = _load_canonical(args.manifest, max_bytes=1_048_576)
            if set(manifest) != _MANIFEST_KEYS:
                raise ImageContractError("compatibility manifest keys are not closed")
            _verify_distribution_freezes(manifest)
        else:
            print(_distribution_freeze_digest())
        return 0
    except ImageContractError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
