#!/bin/sh
# Harbor tests/test.sh → Loom ScriptVerifier JSON bridge.
set +e
mkdir -p /logs/verifier /loom/verifier
task_dir="${LOOM_TASK_DIR:-/app}"
if [ ! -e /tests ] && [ -d "$task_dir/tests" ]; then
  ln -s "$task_dir/tests" /tests 2>/dev/null \
    || { mkdir -p /tests && cp -R "$task_dir/tests"/. /tests/; }
fi
if [ ! -d /tests ] && [ -d /workspace/tests ]; then
  ln -s /workspace/tests /tests 2>/dev/null \
    || { mkdir -p /tests && cp -R /workspace/tests/. /tests/; }
fi
if [ -f /tests/test.sh ]; then
  bash /tests/test.sh
  rc=$?
elif [ -f "$task_dir/tests/test.sh" ]; then
  bash "$task_dir/tests/test.sh"
  rc=$?
else
  echo "harbor loom bridge: tests/test.sh not found" >&2
  rc=2
fi
reward=0
if [ -f /logs/verifier/reward.txt ]; then
  reward=$(head -n 1 /logs/verifier/reward.txt | tr -cd "0-9.")
fi
case "$reward" in
  "") reward=0 ;;
esac
case "$reward" in
  1|1.0|1.00)
    passed=true
    resolved=1.0
    ;;
  *)
    passed=false
    resolved=0.0
    ;;
esac
cat > "$LOOM_VERIFIER_OUTPUT" <<JSONEOF
{"rewards":{"resolved":$resolved,"passed":$reward},"checks":[{"name":"harbor_test_sh","passed":$passed,"score":$resolved,"message":"exit=$rc"}],"structured":{"exit_code":$rc,"reward":$reward}}
JSONEOF
exit 0
