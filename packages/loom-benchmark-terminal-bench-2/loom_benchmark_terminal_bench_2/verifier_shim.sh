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
# #865: stream test output to the caller while retaining a bounded audit copy
# under ``$LOOM_TASK_DIR/.loom/verifier``. Audit persistence is best-effort and
# must never mask the scoring JSON written to $LOOM_VERIFIER_OUTPUT.

set -u

TASK_DIR="${LOOM_TASK_DIR:-/app}"
TEST_DIR="${TEST_DIR:-${TASK_DIR%/}/environment/tb2-tests}"
export TEST_DIR
MAX_LOG_BYTES=1048576
LOG_DIR="${TASK_DIR%/}/.loom/verifier"
LOG_FILE="$LOG_DIR/pytest.log"
META_FILE="$LOG_DIR/pytest.log.meta.json"

cd "$TASK_DIR" 2>/dev/null || true

run_tests_with_bounded_audit() {
  # A pre-existing directory can become unwritable after mkdir succeeds. Test
  # the actual log open separately and fall back to the observable scoring path.
  if ! mkdir -p "$LOG_DIR" 2>/dev/null || ! ( : >"$LOG_FILE" ) 2>/dev/null; then
    bash "$TEST_DIR/run-tests.sh" 2>&1
    return $?
  fi

  stream_dir="$(mktemp -d)" || {
    bash "$TEST_DIR/run-tests.sh" 2>&1
    return $?
  }
  stdout_fifo="$stream_dir/stdout"
  stderr_fifo="$stream_dir/stderr"
  audit_fifo="$stream_dir/audit"
  state_file="$stream_dir/state"
  if ! mkfifo "$stdout_fifo" "$stderr_fifo" "$audit_fifo"; then
    rm -rf "$stream_dir"
    bash "$TEST_DIR/run-tests.sh" 2>&1
    return $?
  fi

  # Retain at most MAX_LOG_BYTES while continuing to drain the audit stream.
  # Keeping fd 3 open across head + wc avoids a reader gap that could SIGPIPE
  # either tee process. Redirection failures still enter the drain-only branch.
  (
    exec 3<"$audit_fifo"
    if head -c "$MAX_LOG_BYTES" <&3 >"$LOG_FILE" 2>/dev/null; then
      kept_bytes=$(wc -c <"$LOG_FILE" | tr -d ' ')
      remainder_bytes=$(wc -c <&3 | tr -d ' ')
      printf 'ok %s %s\n' "$kept_bytes" "$remainder_bytes" >"$state_file"
    else
      wc -c <&3 >/dev/null
      printf 'failed 0 0\n' >"$state_file"
    fi
    exec 3<&-
  ) &
  audit_pid=$!

  # Preserve each original stream byte-for-byte while sending copies to the
  # bounded audit consumer. The two audit copies may interleave, as expected
  # for a combined subprocess log, but stdout and stderr remain distinct.
  tee "$audit_fifo" <"$stdout_fifo" &
  stdout_tee_pid=$!
  tee "$audit_fifo" <"$stderr_fifo" >&2 &
  stderr_tee_pid=$!

  bash "$TEST_DIR/run-tests.sh" >"$stdout_fifo" 2>"$stderr_fifo" &
  test_pid=$!
  wait "$test_pid"
  test_rc=$?
  wait "$stdout_tee_pid" 2>/dev/null || true
  wait "$stderr_tee_pid" 2>/dev/null || true
  wait "$audit_pid" 2>/dev/null || true

  audit_status=failed
  kept_bytes=0
  remainder_bytes=0
  if IFS=' ' read -r audit_status kept_bytes remainder_bytes <"$state_file" \
    && [ "$audit_status" = ok ]; then
    original_bytes=$((kept_bytes + remainder_bytes))
    if [ "$remainder_bytes" -gt 0 ]; then
      truncated=true
    else
      truncated=false
    fi
    if ! (cat >"$META_FILE" <<EOF
{
  "schema_version": "1",
  "truncated": $truncated,
  "original_bytes": $original_bytes,
  "kept_bytes": $kept_bytes,
  "return_code": $test_rc,
  "script_path": "$TEST_DIR/run-tests.sh",
  "log_path": ".loom/verifier/pytest.log"
}
EOF
    ) 2>/dev/null; then
      rm -f "$LOG_FILE" "$META_FILE" 2>/dev/null || true
    fi
  else
    rm -f "$LOG_FILE" "$META_FILE" 2>/dev/null || true
  fi
  rm -rf "$stream_dir"

  return "$test_rc"
}

run_tests_with_bounded_audit
rc=$?

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
