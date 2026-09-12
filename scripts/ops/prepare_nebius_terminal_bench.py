#!/usr/bin/env python3
"""Prepare one local Terminal-Bench TaskSet; never build, publish, or submit it."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import tarfile
import tomllib
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from loom.models.task import TaskConfig
from loom.models.taskset import UserTaskSetManifest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "deploy/catalog/nebius-terminal-bench/file-archive-manifest"
_IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}\Z")


def task_config(image: str | None = None) -> bytes:
    if image is not None and not _IMAGE.fullmatch(image):
        raise ValueError("image must be an immutable OCI reference ending in @sha256:<64 hex>")
    environment_source = (
        f'docker_image = "{image}"'
        if image is not None
        else 'dockerfile = "environment/Dockerfile"\n'
        'docker_build_context = "environment"\nbuild_timeout_sec = 600'
    )
    raw = f'''schema_version = "1"

[task]
id = "file-archive-manifest"
name = "File archive manifest (Nebius AMD64 offline adaptation)"
labels = ["terminal-bench-2-harbor-90", "file-archive-manifest", "nebius-amd64", "offline-adaptation"]

[environment]
os = "linux"
cpu_arch = "x86_64"
{environment_source}
workdir = "/app"
user = "agent"
cpus = 2
memory_mb = 4096
storage_mb = 8192
network_policies_supported = ["gateway-only"]

[environment.baseline_network_policy]
kind = "gateway-only"

[agent]
name = "terminus-2"
timeout_sec = 900

[verifier]
name = "script"
timeout_sec = 900
env_mode = "shared"

[verifier.args]
script_path = "verifier/run.sh"

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["archive_manifest.json", "build_manifest.py"]
'''
    TaskConfig.model_validate(tomllib.loads(raw))
    return raw.encode()


def prepare(*, output: Path, image: str | None = None, source: Path = SOURCE) -> dict[str, Any]:
    config = task_config(image)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("output must be absent or an empty directory")
    provenance = json.loads((source / "source-provenance.json").read_text())
    manifest = {
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {
            "name": "nebius-terminal-bench-file-archive-manifest" + (
                "-dockerfile" if image is None else ""
            ),
            "display_name": "Terminal Bench 2 Harbor: File Archive Manifest (Nebius adaptation)",
        },
        "intents": ["evaluation"],
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz", "subset": "tasks"},
        "limits": {"max_instances": 1, "timeout_per_task_s": 1800},
    }
    UserTaskSetManifest.model_validate(manifest)
    files = {
        "task.toml": config,
        "instruction.md": (source / "original/instruction.md").read_bytes(),
        "tests/test_outputs.py": (source / "original/tests/test_outputs.py").read_bytes(),
        "verifier/run.sh": (source / "verifier/run.sh").read_bytes(),
        "source-provenance.json": (source / "source-provenance.json").read_bytes(),
    }
    if image is None:
        # Only the declared Dockerfile enters the build context. Private verifier
        # inputs stay outside it, and no oracle is included in either input path.
        files["environment/Dockerfile"] = (source / "Dockerfile").read_bytes()
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for name, payload in sorted(files.items()):
                info = tarfile.TarInfo(f"tasks/file-archive-manifest/{name}")
                info.size = len(payload)
                info.mode = 0o755 if name == "verifier/run.sh" else 0o644
                archive.addfile(info, io.BytesIO(payload))
    evidence = {
        "task_count": 1,
        "environment_source": "dockerfile" if image is None else "prebuilt",
        "task_image_ref": image,
        "verifier_image_ref": image,
        "adaptation_profile": "nebius-amd64-nonroot-offline",
        "original_profile_supported": False,
        "agent": "terminus-2",
        "model": "glm-5.2",
        "source_provenance": provenance,
        "task_container_allocation": {"cpus": 2, "memory_mb": 4096, "storage_mb": 8192},
        "trial_submissions": 0,
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))
    (output / "bundle.tar.gz").write_bytes(buffer.getvalue())
    (output / "taskset-build.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image", help="Use an already admitted task/verifier digest instead of Dockerfile preparation"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        prepare(image=args.image, output=args.output.resolve())
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Prepared one local TaskSet in {args.output.resolve()}; no Trial submitted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
