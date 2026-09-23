#!/bin/sh
# Loom native Harbor runner: preserve the original offline test semantics.
set -eu
: "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT is required}"
task_dir="${LOOM_TASK_DIR:-/app}"
mkdir -p /tests /logs/verifier /loom/verifier
cp -R "$task_dir/tests/." /tests/
rm -f /logs/verifier/reward.txt /logs/verifier/ctrf.json
set +e
bash "$task_dir/verifier/harbor-offline.sh"
rc=$?
set -e
if [ "$rc" -ne 0 ]; then
    echo "offline Harbor verifier script failed" >&2
    exit "$rc"
fi
reward=$(cat /logs/verifier/reward.txt)
case "$reward" in
    0) passed=false ;;
    1) passed=true ;;
    *) echo "Harbor verifier reward must be 0 or 1" >&2; exit 1 ;;
esac
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
cat > "$LOOM_VERIFIER_OUTPUT" <<JSONEOF
{"rewards":{"resolved":$reward,"passed":$reward},"checks":[{"name":"harbor_test_sh","passed":$passed,"score":$reward,"message":"exit=$rc"}],"structured":{"exit_code":$rc,"reward":$reward}}
JSONEOF
