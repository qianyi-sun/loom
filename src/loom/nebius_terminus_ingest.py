"""Opt-in Nebius Terminus ingest adaptations for Harbor/TB packs (#1996).

Keeps ``automatic_service_execution_rejections`` contracts unchanged. When
``publish-local --execution-profile nebius-terminus`` (or validate-local with
the same flag) is set, adapt staged bundles + Loom-schema config so admission
reasons become satisfiable without silently rewriting every publish.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
)

NEBIUS_TERMINUS_PROFILE = "nebius-terminus"
KNOWN_EXECUTION_PROFILES: frozenset[str] = frozenset({NEBIUS_TERMINUS_PROFILE})

DEFAULT_CPUS = 1
DEFAULT_MEMORY_MB = 2048
DEFAULT_STORAGE_MB = 4096
VERIFIER_SCRIPT_PATH = "verifier/run.sh"
_GLOB_MAGIC = re.compile(r"[][*?]")


# Harbor grader bridge. Calls tests/test.sh and only translates reward.txt
# into Loom's verifier JSON. Not loaded from the Harbor90 catalog file: that
# copy still runs /opt/verifier/bin/pytest and is provenance, not this profile.
_TEST_SH_VERIFIER_RUN_SH = b"""#!/usr/bin/env bash
# Harbor verifier bridge for Nebius Terminus publish.
#
# Native tasks write a numeric reward to /logs/verifier/reward.txt. A numeric
# zero is a valid benchmark outcome. Missing, empty, malformed, or non-finite
# evidence is a verifier failure: this script exits non-zero without emitting a
# result JSON, so ScriptVerifier records its execution failure rather than
# coercing it into a model score of zero.

set -u

TASK_DIR="${LOOM_TASK_DIR:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}"
LOG_DIR="${TB21_VERIFIER_LOG_DIR:-/logs/verifier}"
REWARD_PATH="${TB21_REWARD_PATH:-$LOG_DIR/reward.txt}"
TEST_MOUNT_DIR="${TB21_TEST_MOUNT_DIR:-/tests}"
: "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT must be set}"

mkdir -p "$LOG_DIR" "$TEST_MOUNT_DIR" "$(dirname "$LOOM_VERIFIER_OUTPUT")"
rm -f "$REWARD_PATH"

if [ ! -f "$TASK_DIR/tests/test.sh" ]; then
    echo "tb_reward_error=missing_tests" >&2
    exit 1
fi

cp -R "$TASK_DIR/tests/." "$TEST_MOUNT_DIR/"
set +e
(
    cd "$TASK_DIR" || exit 1
    bash "$TASK_DIR/tests/test.sh"
)
verifier_rc=$?
set -e

if [ "$verifier_rc" -eq 124 ]; then
    echo "tb_reward_error=timeout" >&2
    exit 1
fi

python3 - "$LOOM_VERIFIER_OUTPUT" "$REWARD_PATH" "$LOG_DIR" "$verifier_rc" <<'PY'
import json
import math
import sys
from pathlib import Path

output_path = Path(sys.argv[1])
reward_path = Path(sys.argv[2])
log_dir = Path(sys.argv[3])
test_returncode = int(sys.argv[4])

try:
    raw = reward_path.read_text(encoding="utf-8")
except OSError:
    print("tb_reward_error=missing_reward", file=sys.stderr)
    raise SystemExit(1)

stripped = raw.strip()
if not stripped:
    print("tb_reward_error=empty_reward", file=sys.stderr)
    raise SystemExit(1)
try:
    reward = float(stripped)
except ValueError:
    print("tb_reward_error=malformed_reward", file=sys.stderr)
    raise SystemExit(1)
if not math.isfinite(reward):
    print("tb_reward_error=malformed_reward", file=sys.stderr)
    raise SystemExit(1)

ctrf_path = log_dir / "ctrf.json"
output_log_path = log_dir / "output.log"
output_log_tail = None
if output_log_path.exists():
    output_log_tail = output_log_path.read_text(
        encoding="utf-8", errors="replace",
    )[-4000:]

output_path.write_text(json.dumps({
    "rewards": {"resolved": reward},
    "checks": [{
        "name": "harbor_tests",
        "passed": reward > 0.0,
        "score": reward,
        "message": f"tests/test.sh rc={test_returncode}; reward={stripped}",
    }],
    "structured": {
        "reward_raw": raw,
        "test_sh_returncode": test_returncode,
        "output_log_tail": output_log_tail,
        "artifacts": {
            "reward_path": str(reward_path),
            "ctrf_path": str(ctrf_path) if ctrf_path.exists() else None,
            "output_log_path": str(output_log_path) if output_log_path.exists() else None,
        },
    },
}) + "\\n", encoding="utf-8")
PY
"""

_ONLINE_BOOTSTRAP_MARKERS = (
    b"pip install",
    b"uv pip",
    b"uvx ",
    b"curl ",
    b"wget ",
    b"apt-get",
)
_REPLACE_VERIFIER_MARKERS = (
    b"/opt/verifier/bin/pytest",
    b"harbor loom bridge",
)


@dataclass(frozen=True)
class NebiusTerminusAdaptStats:
    resources_filled: bool = False
    network_forced_gateway_only: bool = False
    verifier_identity_stripped: bool = False
    verifier_path_forced: bool = False
    cpu_arch_forced: bool = False
    workspace_identity_forced: bool = False
    verifier_wrapper_installed: bool = False
    artifact_globs_stripped: bool = False


def resolve_execution_profile(value: str | None) -> str | None:
    """Return a known profile name, or raise ``ValueError`` for unknowns."""

    if value is None or value == "":
        return None
    if value not in KNOWN_EXECUTION_PROFILES:
        known = ", ".join(sorted(KNOWN_EXECUTION_PROFILES))
        raise ValueError(
            f"unknown execution profile {value!r}; known profiles: {known}",
        )
    return value


def offline_verifier_run_sh_bytes() -> bytes:
    """Return the Nebius ``verifier/run.sh`` template.

    The script calls Harbor's ``tests/test.sh`` and translates ``reward.txt``.
    The Harbor90 catalog copy under ``deploy/`` is left untouched.
    """

    return _TEST_SH_VERIFIER_RUN_SH


def _needs_verifier_wrapper(existing: bytes | None) -> bool:
    """Install when missing, online-bootstrapping, or still the pytest-only wrapper.

    A script that already runs ``tests/test.sh`` is left alone.
    """

    if existing is None:
        return True
    if any(marker in existing for marker in _ONLINE_BOOTSTRAP_MARKERS):
        return True
    if any(marker in existing for marker in _REPLACE_VERIFIER_MARKERS):
        return True
    return False


def _ensure_offline_verifier_wrapper(staged: Path) -> bool:
    target = staged / "verifier" / "run.sh"
    existing = target.read_bytes() if target.is_file() else None
    if not _needs_verifier_wrapper(existing):
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(offline_verifier_run_sh_bytes())
    target.chmod(0o755)
    return True


def _is_exact_relative_artifact(path: str) -> bool:
    posix = PurePosixPath(path)
    return not (
        posix.is_absolute()
        or ".." in posix.parts
        or _GLOB_MAGIC.search(path)
        or posix.parts[:1] == (".loom",)
    )


def _strip_inexact_artifacts(steps: list[Any]) -> tuple[list[dict[str, Any]], bool]:
    """Drop Harbor TB2.1 verifier globs that Nebius admission rejects."""

    changed = False
    cleaned: list[dict[str, Any]] = []
    for step in steps:
        if not isinstance(step, dict):
            cleaned.append(step)
            continue
        item = dict(step)
        for field in ("artifacts", "required_artifacts"):
            values = item.get(field)
            if not isinstance(values, list):
                continue
            kept = [path for path in values if isinstance(path, str) and _is_exact_relative_artifact(path)]
            if kept != values:
                changed = True
            if kept:
                item[field] = kept
            else:
                item.pop(field, None)
        cleaned.append(item)
    return cleaned, changed


def adapt_bundle_for_nebius_terminus(
    staged: Path,
    config: dict[str, Any],
) -> tuple[dict[str, Any], NebiusTerminusAdaptStats]:
    """Mutate *config* + staged verifier for Nebius Terminus admission.

    Returns a shallow-copied config dict (nested sections that are mutated are
    also copied) and counters describing what changed.
    """

    adapted = dict(config)
    environment = dict(adapted.get("environment") or {})
    verifier = dict(adapted.get("verifier") or {})
    verifier_args = dict(verifier.get("args") or {})

    resources_filled = False
    if environment.get("cpus") is None:
        environment["cpus"] = DEFAULT_CPUS
        resources_filled = True
    if environment.get("memory_mb") is None:
        environment["memory_mb"] = DEFAULT_MEMORY_MB
        resources_filled = True
    if environment.get("storage_mb") is None:
        environment["storage_mb"] = DEFAULT_STORAGE_MB
        resources_filled = True

    network_forced = (
        environment.get("network_policies_supported") != ["gateway-only"]
        or environment.get("baseline_network_policy") != {"kind": "gateway-only"}
    )
    environment["network_policies_supported"] = ["gateway-only"]
    environment["baseline_network_policy"] = {"kind": "gateway-only"}

    cpu_arch_forced = environment.get("cpu_arch") != "x86_64"
    environment["cpu_arch"] = "x86_64"
    if environment.get("os") is None:
        environment["os"] = "linux"

    workdir = environment.get("workdir")
    workdir_ok = workdir in {"/app", "/workspace", PurePosixPath("/app"), PurePosixPath("/workspace")}
    workspace_forced = environment.get("user") != "agent" or not workdir_ok
    environment["user"] = "agent"
    if not workdir_ok:
        environment["workdir"] = "/app"

    verifier_identity_stripped = verifier.get("user") is not None
    verifier.pop("user", None)
    verifier["name"] = verifier.get("name") or "script"
    verifier["env_mode"] = "shared"
    verifier_path_forced = verifier_args.get("script_path") != VERIFIER_SCRIPT_PATH
    verifier_args["script_path"] = VERIFIER_SCRIPT_PATH
    verifier["args"] = verifier_args

    steps_raw = adapted.get("steps")
    artifact_globs_stripped = False
    if isinstance(steps_raw, list):
        cleaned_steps, artifact_globs_stripped = _strip_inexact_artifacts(steps_raw)
        adapted["steps"] = cleaned_steps

    adapted["environment"] = environment
    adapted["verifier"] = verifier

    wrapper_installed = _ensure_offline_verifier_wrapper(staged)
    return adapted, NebiusTerminusAdaptStats(
        resources_filled=resources_filled,
        network_forced_gateway_only=network_forced,
        verifier_identity_stripped=verifier_identity_stripped,
        verifier_path_forced=verifier_path_forced,
        cpu_arch_forced=cpu_arch_forced,
        workspace_identity_forced=workspace_forced,
        verifier_wrapper_installed=wrapper_installed,
        artifact_globs_stripped=artifact_globs_stripped,
    )


def preflight_nebius_terminus_admission(
    config: dict[str, Any],
    source_provenance: dict[str, Any],
) -> tuple[str, ...]:
    """Run Nebius Terminus admission gates against an adapted config + SEI."""

    task = TaskConfig.model_validate(config)
    trial = TrialConfig(
        agent_name="terminus-2",
        agent_model=ModelSpec(provider="openai", name="gpt-5", source="api"),
        workspace_staging_policy_name="tb21",
    )
    return automatic_service_execution_rejections(
        task,
        trial,
        source_provenance=source_provenance,
        allow_task_image_preparation=True,
    )


@dataclass(frozen=True)
class NebiusTerminusProfileStats:
    adapted_tasks: int = 0
    verifier_wrappers_installed: int = 0
    resources_filled_tasks: int = 0
    preflight_passed: int = 0


def merge_profile_stats(
    total: NebiusTerminusProfileStats,
    adapt: NebiusTerminusAdaptStats,
    *,
    preflight_ok: bool,
) -> NebiusTerminusProfileStats:
    return NebiusTerminusProfileStats(
        adapted_tasks=total.adapted_tasks + 1,
        verifier_wrappers_installed=(
            total.verifier_wrappers_installed + int(adapt.verifier_wrapper_installed)
        ),
        resources_filled_tasks=(
            total.resources_filled_tasks + int(adapt.resources_filled)
        ),
        preflight_passed=total.preflight_passed + int(preflight_ok),
    )
