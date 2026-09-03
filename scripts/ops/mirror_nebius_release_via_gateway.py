#!/usr/bin/env python3
"""Mirror one digest-pinned Loom release into Nebius Registry via its gateway."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path

CRANE_VERSION = "0.22.0"
CRANE_LINUX_X86_64_SHA256 = "edb74d53fad9a596860f59d1c5d04a43dfb5f441dc71f57060dd0bf39483c833"
_SHA = re.compile(r"^[0-9a-f]{40}$")
_GATEWAY = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$")
_DIGEST_REF = re.compile(r"^([a-z0-9.-]+(?::[0-9]+)?(?:/[a-z0-9._-]+)+)@(sha256:[0-9a-f]{64})$")
_TARGET_REGISTRY = re.compile(r"^cr\.eu-north1\.nebius\.cloud/[a-z0-9]+$")
_COMPONENTS = (
    ("gateway", "loom-llm-gateway"),
    ("control_plane", "loom-control-plane"),
    ("service", "loom-service"),
    ("execution_actuator", "loom-execution-actuator"),
    ("execution_runtime", "loom-execution-runtime"),
)

_REMOTE_SCRIPT = r"""
set -euo pipefail

candidate_sha=$1
target_registry=$2
shift 2
work_dir=$(mktemp -d)
cleanup() {
  find "$work_dir" -type f -delete
  find "$work_dir" -depth -type d -empty -delete
}
trap cleanup EXIT

curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  -o "$work_dir/crane.tar.gz" \
  "https://github.com/google/go-containerregistry/releases/download/v__CRANE_VERSION__/go-containerregistry_Linux_x86_64.tar.gz"
printf '%s  %s\n' '__CRANE_SHA256__' "$work_dir/crane.tar.gz" \
  | sha256sum -c - >/dev/null
tar -xzf "$work_dir/crane.tar.gz" -C "$work_dir" crane

mkdir -m 700 "$work_dir/bin" "$work_dir/docker"
cat >"$work_dir/bin/docker-credential-nebius" <<'WRAPPER'
#!/bin/sh
exec nebius registry docker-credential "$@"
WRAPPER
chmod 700 "$work_dir/bin/docker-credential-nebius"
printf '%s\n' '{"credHelpers":{"cr.eu-north1.nebius.cloud":"nebius"}}' \
  >"$work_dir/docker/config.json"
chmod 600 "$work_dir/docker/config.json"
export PATH="$work_dir/bin:$PATH"
export DOCKER_CONFIG="$work_dir/docker"

for component in loom-llm-gateway loom-control-plane loom-service loom-execution-actuator loom-execution-runtime; do
  source_ref=$1
  shift
  source_digest=${source_ref##*@}
  target_name="$target_registry/$component"
  target_tag_ref="$target_name:release-$candidate_sha-amd64"
  "$work_dir/crane" copy "$source_ref" "$target_tag_ref" >/dev/null
  target_digest=$("$work_dir/crane" digest "$target_tag_ref")
  if [[ "$target_digest" != "$source_digest" ]]; then
    echo "mirrored digest mismatch for $component" >&2
    exit 1
  fi
  printf '%s\t%s\t%s@%s\n' "$component" "$source_ref" "$target_name" "$target_digest"
done
test "$#" -eq 0
""".replace("__CRANE_VERSION__", CRANE_VERSION).replace(
    "__CRANE_SHA256__", CRANE_LINUX_X86_64_SHA256
)


def _owner_only(path: Path, *, label: str) -> None:
    if not path.is_file():
        raise ValueError(f"{label} must be a file")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ValueError(f"{label} must not be group/world accessible")


def _digest_ref(value: str, *, component: str) -> str:
    match = _DIGEST_REF.fullmatch(value)
    if match is None or not match.group(1).endswith(f"/{component}"):
        raise ValueError(f"{component} source must be its digest-pinned image")
    return value


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _parse_result(
    stdout: str,
    *,
    candidate_sha: str,
    target_registry: str,
    expected_sources: dict[str, str],
) -> dict[str, object]:
    rows = stdout.splitlines()
    if len(rows) != len(_COMPONENTS):
        raise ValueError("gateway returned an incomplete mirror result")
    images: dict[str, dict[str, str]] = {}
    expected_by_component = {component: key for key, component in _COMPONENTS}
    for row in rows:
        fields = row.split("\t")
        if len(fields) != 3:
            raise ValueError("gateway returned a malformed mirror result")
        component, source_ref, target_ref = fields
        key = expected_by_component.get(component)
        if key is None or key in images or source_ref != expected_sources[key]:
            raise ValueError("gateway returned an unexpected mirror binding")
        source_digest = source_ref.rsplit("@", 1)[1]
        if target_ref != f"{target_registry}/{component}@{source_digest}":
            raise ValueError("gateway returned a changed mirror digest")
        images[key] = {"source_ref": source_ref, "target_ref": target_ref}
    if set(images) != {key for key, _ in _COMPONENTS}:
        raise ValueError("gateway returned duplicate mirror bindings")
    return {
        "schema_version": "loom.nebius-release-mirror.v1",
        "candidate_sha": candidate_sha,
        "target_registry": target_registry,
        "images": images,
    }


def mirror(args: argparse.Namespace) -> dict[str, object]:
    if _SHA.fullmatch(args.candidate_sha) is None:
        raise ValueError("candidate SHA must contain exactly 40 lowercase hex characters")
    if _GATEWAY.fullmatch(args.gateway) is None:
        raise ValueError("gateway must be an IPv4 address or DNS hostname")
    if _TARGET_REGISTRY.fullmatch(args.target_registry) is None:
        raise ValueError("target registry must be the eu-north1 Nebius registry prefix")
    ssh_key = args.ssh_key.resolve()
    known_hosts = args.known_hosts.resolve()
    _owner_only(ssh_key, label="SSH key")
    if not known_hosts.is_file() or not known_hosts.read_text(encoding="utf-8").strip():
        raise ValueError("known-hosts must be a non-empty file")
    expected_sources = {
        key: _digest_ref(getattr(args, f"{key}_image"), component=component)
        for key, component in _COMPONENTS
    }
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "ConnectTimeout=15",
        "-i",
        str(ssh_key),
        f"codex@{args.gateway}",
        "bash",
        "-s",
        "--",
        args.candidate_sha,
        args.target_registry,
        *(expected_sources[key] for key, _ in _COMPONENTS),
    ]
    completed = subprocess.run(
        command,
        input=_REMOTE_SCRIPT,
        text=True,
        check=True,
        capture_output=True,
    )
    return _parse_result(
        completed.stdout,
        candidate_sha=args.candidate_sha,
        target_registry=args.target_registry,
        expected_sources=expected_sources,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--ssh-key", required=True, type=Path)
    parser.add_argument("--known-hosts", required=True, type=Path)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--target-registry", required=True)
    for key, _ in _COMPONENTS:
        parser.add_argument(f"--{key.replace('_', '-')}-image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        payload = mirror(args)
        _write_json(args.output.resolve(), payload)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(f"Nebius release mirror converged: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
