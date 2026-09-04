"""Build the candidate-bound TaskSet used by staged Nebius acceptance."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
import tomllib
from pathlib import Path
from typing import Any

from loom.models.task import TaskConfig
from loom.service_execution_materialization import load_service_execution_runtime_profile


class NebiusAcceptanceTaskSetError(ValueError):
    pass


def _add_file(archive: tarfile.TarFile, name: str, payload: bytes, *, mode: int = 0o644) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mode = mode
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    archive.addfile(info, io.BytesIO(payload))


def _task_toml(image: str) -> bytes:
    return f'''schema_version = "1"

[task]
id = "nebius-acceptance/canonical-output"
name = "Nebius canonical output acceptance"
description = "Candidate-bound acceptance task for artifacts, trajectory, usage, and verifier output."

[environment]
os = "linux"
cpu_arch = "x86_64"
gpu_vendor = "none"
docker_image = "{image}"
cpus = 2
memory_mb = 8192
storage_mb = 8192
workdir = "/workspace"
user = "agent"
network_policies_supported = ["gateway-only"]

[environment.baseline_network_policy]
kind = "gateway-only"

[agent]
name = "direct-completion"
timeout_sec = 300

[verifier]
name = "script"
timeout_sec = 300
env_mode = "shared"

[verifier.args]
script_path = "verifier/check.sh"

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["answer.txt", "reasoning.md"]
required_artifacts = ["answer.txt", "reasoning.md"]
'''.encode()


_INSTRUCTION = b"""Return a short deterministic answer ending in ACCEPTED.

The runtime will preserve the response as both answer.txt and reasoning.md.
These declared outputs, the complete model-call trajectory, attributed usage,
and the verifier result must all survive canonical materialization.
"""

_VERIFIER = b"""#!/bin/sh
set -eu
# Keep the execution unit alive long enough for staged acceptance to observe
# true simultaneous Running state after a scale-from-zero node launch.
sleep 180
test -s answer.txt
test -s reasoning.md
grep -q 'ACCEPTED' answer.txt
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
printf '%s\n' '{"rewards":{"artifact_complete":1.0}}' > "$LOOM_VERIFIER_OUTPUT"
"""

_MANIFEST = b"""apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: nebius-canonical-acceptance
  display_name: Nebius Canonical Acceptance
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
limits:
  max_instances: 1
  timeout_per_task_s: 900
"""


def build_nebius_acceptance_taskset(
    *, runtime_profile_path: Path, output_dir: Path
) -> dict[str, Any]:
    if output_dir.exists():
        if not output_dir.is_dir():
            raise NebiusAcceptanceTaskSetError(f"output path is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise NebiusAcceptanceTaskSetError(f"output directory is not empty: {output_dir}")
    try:
        profile_raw = runtime_profile_path.read_text(encoding="utf-8")
        profile = load_service_execution_runtime_profile(profile_raw)
    except (OSError, ValueError) as exc:
        raise NebiusAcceptanceTaskSetError(f"invalid runtime profile: {exc}") from exc
    if profile is None:
        raise NebiusAcceptanceTaskSetError("runtime profile cannot be empty")

    task_toml = _task_toml(profile.task_image_ref)
    try:
        TaskConfig.model_validate(tomllib.loads(task_toml.decode()))
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise NebiusAcceptanceTaskSetError(f"generated acceptance task is invalid: {exc}") from exc

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as archive:
            _add_file(archive, "tasks/canonical-output/task.toml", task_toml)
            _add_file(archive, "tasks/canonical-output/instruction.md", _INSTRUCTION)
            _add_file(
                archive,
                "tasks/canonical-output/verifier/check.sh",
                _VERIFIER,
                mode=0o755,
            )
    bundle = buffer.getvalue()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.yaml").write_bytes(_MANIFEST)
    (output_dir / "bundle.tar.gz").write_bytes(bundle)
    evidence: dict[str, Any] = {
        "schema_version": "loom.nebius-acceptance-taskset.v1",
        "candidate_sha": profile.candidate_sha,
        "task_image_ref": profile.task_image_ref,
        "runtime_image_ref": profile.runtime_image_ref,
        "manifest_sha256": hashlib.sha256(_MANIFEST).hexdigest(),
        "bundle_sha256": hashlib.sha256(bundle).hexdigest(),
        "task_count": 1,
        "required_outputs": [
            "artifacts/answer.txt",
            "artifacts/reasoning.md",
            "trajectory/events.jsonl",
            "accounting/usage.json",
            "verifier/output.json",
        ],
    }
    evidence_bytes = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode()
    (output_dir / "taskset-build.json").write_bytes(evidence_bytes)
    (output_dir / "taskset-build.json.sha256").write_text(
        f"{hashlib.sha256(evidence_bytes).hexdigest()}  taskset-build.json\n",
        encoding="utf-8",
    )
    return evidence


__all__ = [
    "NebiusAcceptanceTaskSetError",
    "build_nebius_acceptance_taskset",
]
