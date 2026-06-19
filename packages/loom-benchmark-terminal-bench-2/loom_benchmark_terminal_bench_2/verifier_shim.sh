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

set -u

TEST_DIR="${TEST_DIR:-/app/environment/tb2-tests}"
export TEST_DIR

cd /app 2>/dev/null || true

set +e
bash "$TEST_DIR/run-tests.sh"
rc=$?
set -e

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
