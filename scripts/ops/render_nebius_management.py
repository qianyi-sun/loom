#!/usr/bin/env python3
"""Render a protected management deployment without credentials or cluster writes."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.nebius_candidate import validate_source_identity  # noqa: E402

from loom_service.environment_management.candidates import _json  # noqa: E402
from loom_service.environment_management.deployment import (  # noqa: E402
    ManagementDeployment,
    render_management,
)


def read_input(path: Path) -> dict:
    with path.open("rb") as stream:
        payload = stream.read(2 * 1024 * 1024 + 1)
    if len(payload) > 2 * 1024 * 1024:
        raise ValueError("input too large")
    return _json(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("deployment", "candidate", "runtime-profile", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    status = "invalid-input"
    try:
        deployment = ManagementDeployment.model_validate(read_input(args.deployment))
        candidate, profile = read_input(args.candidate), read_input(args.runtime_profile)
        validate_source_identity(candidate)
        if candidate["registry_prefix"] != deployment.installation.registry_prefix:
            raise ValueError("registry mismatch")
        result = render_management(deployment, candidate=candidate, profile=profile, repo_root=ROOT)
        status = "output-unavailable"
        # A fresh private directory avoids overwriting operator input/evidence,
        # following pre-existing symlinks or making protected configuration public.
        args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        for name, docs in result.files.items():
            path = args.output / name
            with path.open("x", encoding="utf-8") as handle:
                path.chmod(0o600)
                yaml.safe_dump_all(docs, handle, sort_keys=False)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        # Pydantic and parser errors can echo operator-supplied values.
        print(json.dumps({"status": status, "error_type": type(exc).__name__}), file=sys.stderr)
        return 1
    print(json.dumps({"status": "rendered-not-installed", "namespace": deployment.namespace,
                      "candidate_sha": candidate["candidate_sha"], "revision": result.revision,
                      "platform_envelope": asdict(result.platform_envelope), "files": list(result.files)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
