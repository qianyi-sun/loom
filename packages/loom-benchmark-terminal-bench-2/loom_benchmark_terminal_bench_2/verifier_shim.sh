#!/usr/bin/env bash
# Harbor-native TB2.1 verifier bridge.
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
    echo "tb21_reward_error=missing_tests" >&2
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
    echo "tb21_reward_error=timeout" >&2
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
    print("tb21_reward_error=missing_reward", file=sys.stderr)
    raise SystemExit(1)

stripped = raw.strip()
if not stripped:
    print("tb21_reward_error=empty_reward", file=sys.stderr)
    raise SystemExit(1)
try:
    reward = float(stripped)
except ValueError:
    print("tb21_reward_error=malformed_reward", file=sys.stderr)
    raise SystemExit(1)
if not math.isfinite(reward):
    print("tb21_reward_error=malformed_reward", file=sys.stderr)
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
        "name": "tb21_native_tests",
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
}) + "\n", encoding="utf-8")
PY
