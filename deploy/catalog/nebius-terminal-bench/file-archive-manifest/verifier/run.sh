#!/bin/sh
# Offline environment preparation for the unchanged Harbor test assertions.
set -eu
: "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT is required}"
task_dir="${LOOM_TASK_DIR:-/app}"
mkdir -p /tests /logs/verifier /loom/verifier
cp "$task_dir/tests/test_outputs.py" /tests/test_outputs.py
rm -f /logs/verifier/reward.txt /logs/verifier/ctrf.json
set +e
/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
rc=$?
set -e
reward=0
passed=false
if [ "$rc" -eq 0 ]; then reward=1; passed=true; fi
printf '%s\n' "$reward" > /logs/verifier/reward.txt
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
cat > "$LOOM_VERIFIER_OUTPUT" <<JSONEOF
{"rewards":{"resolved":$reward,"passed":$reward},"checks":[{"name":"harbor_test_sh","passed":$passed,"score":$reward,"message":"exit=$rc"}],"structured":{"exit_code":$rc,"reward":$reward}}
JSONEOF
