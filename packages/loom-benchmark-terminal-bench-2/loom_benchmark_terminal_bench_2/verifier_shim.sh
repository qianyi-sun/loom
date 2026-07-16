#!/bin/sh
# Loom <-> TB-2 verifier bridge.
#
# Runs TB-2's run-tests.sh (copied into the container at
# /app/environment/tb2-tests/run-tests.sh by Loom's prepare phase) and translates its
# exit code into the JSON shape ScriptVerifier consumes from
# $LOOM_VERIFIER_OUTPUT.
#
# Outputs:
#   {"rewards": {"resolved": <0.0|1.0>},
#    "checks": [{"name": "tb2_run_tests", "passed": <bool>,
#                "score": <0.0|1.0>, "message": "exit=<N>"}]}
#
# #865: best-effort tee of test output into
# ``$LOOM_TASK_DIR/.loom/verifier/pytest.log`` (or ``/app/.loom/verifier``
# when that workdir is writable). Audit persistence must never mask the
# scoring JSON written to $LOOM_VERIFIER_OUTPUT.

set -u

TEST_DIR="${TEST_DIR:-/app/environment/tb2-tests}"
export TEST_DIR
MAX_LOG_BYTES="${LOOM_VERIFIER_LOG_MAX_BYTES:-1048576}"

cd /app 2>/dev/null || true

RAW_LOG="$(mktemp)"
# shellcheck disable=SC2064
trap 'rm -f "$RAW_LOG"' EXIT

set +e
bash "$TEST_DIR/run-tests.sh" >"$RAW_LOG" 2>&1
rc=$?
set -e

# Resolve a writable audit directory without failing the scoring path.
LOG_DIR=""
if [ -n "${LOOM_TASK_DIR:-}" ]; then
  candidate="${LOOM_TASK_DIR%/}/.loom/verifier"
  if mkdir -p "$candidate" 2>/dev/null; then
    LOG_DIR="$candidate"
  fi
fi
if [ -z "$LOG_DIR" ] && [ -d /app ] && [ -w /app ]; then
  candidate="/app/.loom/verifier"
  if mkdir -p "$candidate" 2>/dev/null; then
    LOG_DIR="$candidate"
  fi
fi

if [ -n "$LOG_DIR" ]; then
  LOG_FILE="$LOG_DIR/pytest.log"
  raw_bytes=$(wc -c <"$RAW_LOG" | tr -d ' ')
  if [ "$raw_bytes" -le "$MAX_LOG_BYTES" ]; then
    cp "$RAW_LOG" "$LOG_FILE"
    truncated=false
    kept_bytes=$raw_bytes
  else
    head_bytes=360000
    marker_file="$LOG_DIR/.truncation_marker"
    printf '\n...[truncated verifier log; preserved trailing output]...\n' >"$marker_file"
    marker_bytes=$(wc -c <"$marker_file" | tr -d ' ')
    tail_bytes=$((MAX_LOG_BYTES - head_bytes - marker_bytes))
    if [ "$tail_bytes" -lt 0 ]; then
      tail_bytes=0
    fi
    {
      head -c "$head_bytes" "$RAW_LOG"
      cat "$marker_file"
      if [ "$tail_bytes" -gt 0 ]; then
        tail -c "$tail_bytes" "$RAW_LOG"
      fi
    } >"$LOG_FILE"
    rm -f "$marker_file"
    truncated=true
    kept_bytes=$(wc -c <"$LOG_FILE" | tr -d ' ')
  fi

  cat >"$LOG_DIR/pytest.log.meta.json" <<EOF
{
  "schema_version": "1",
  "truncated": $truncated,
  "original_bytes": $raw_bytes,
  "kept_bytes": $kept_bytes,
  "return_code": $rc,
  "script_path": "$TEST_DIR/run-tests.sh",
  "log_path": ".loom/verifier/pytest.log"
}
EOF
fi

if [ "$rc" -eq 0 ]; then
    reward="1.0"
    passed="true"
    score="1.0"
else
    reward="0.0"
    passed="false"
    score="0.0"
fi

mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
cat > "$LOOM_VERIFIER_OUTPUT" <<EOF
{
  "rewards": {"resolved": $reward},
  "checks": [
    {"name": "tb2_run_tests", "passed": $passed, "score": $score, "message": "exit=$rc"}
  ]
}
EOF
