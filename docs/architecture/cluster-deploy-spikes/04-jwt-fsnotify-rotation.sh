#!/usr/bin/env bash
# 04-jwt-fsnotify-rotation.sh — verify the JWT refresh mechanism.
#
# Spec claims under test (cluster-deploy.md §Sandbox→gateway, JWT lifetime):
#   1. Worker writes JWT to a host file; container sees the new content
#      via a bind mount in real time.
#   2. Atomic-rename rotation (write to .tmp + mv into place) is visible
#      to the container with no partial-read window.
#
# IMPORTANT EMPIRICAL FINDING from running this spike:
#
#   The rev 7-11 spec said "worker writes new JWT via `docker cp` to a
#   tmpfs-mounted `/run/loom/step-jwt`." That doesn't work. `docker cp`
#   into a tmpfs mount of a running container exits 0 BUT the file
#   inside the container is unchanged. This is because tmpfs mounts
#   are kernel-managed in-memory filesystems owned by the running
#   process's mount namespace; `docker cp` writes to container layers,
#   not to active mount points overlaid on top of them.
#
#   This spike caught the bug. The corrected mechanism (verified below)
#   is bind-mount + host-side write + atomic rename. The spec has been
#   updated accordingly.
#
# Runs against plain Docker. No k8s required.

set -euo pipefail

WATCHER="loom-jwt-watcher-spike"
HOST_DIR="/tmp/spike-04-jwt-host"
INITIAL_JWT="initial-jwt-payload"
ROTATED_JWT="rotated-jwt-payload-with-fresh-claims"

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

cleanup() {
  set +e
  docker rm -f "$WATCHER" >/dev/null 2>&1
  rm -rf "$HOST_DIR"
  set -e
}
trap cleanup EXIT

# --- Setup: bind-mount a host dir, write initial JWT ------------------------

mkdir -p "$HOST_DIR"
echo "$INITIAL_JWT" > "$HOST_DIR/step-jwt"

docker run -d --name "$WATCHER" \
  --mount "type=bind,source=$HOST_DIR,target=/run/loom" \
  alpine sh -c '
    # Tight poll loop: re-read step-jwt every 100 ms; log content on change.
    last=""
    while true; do
      now=$(cat /run/loom/step-jwt 2>/dev/null || echo "MISSING")
      if [ "$now" != "$last" ]; then
        echo "$(date -u +%H:%M:%S.%N | head -c14) read: $now"
        last="$now"
      fi
      sleep 0.1
    done
  ' >/dev/null

sleep 0.5

# --- Claim 1: container sees initial JWT via bind mount --------------------

logs=$(docker logs "$WATCHER" 2>&1)
if ! echo "$logs" | grep -q "$INITIAL_JWT"; then
  echo "  watcher logs:"
  echo "$logs" | sed 's/^/    /'
  fail "claim 1: watcher did not see initial JWT via bind mount"
fi
pass "claim 1: bind-mount delivers initial JWT to container"

# --- Claim 2: host-side overwrite is visible to container ------------------

echo "intermediate-overwrite" > "$HOST_DIR/step-jwt"
sleep 0.5

logs=$(docker logs "$WATCHER" 2>&1)
if ! echo "$logs" | grep -q "intermediate-overwrite"; then
  echo "  watcher logs:"
  echo "$logs" | sed 's/^/    /'
  fail "claim 2: watcher did not see host-side overwrite"
fi
pass "claim 2: host-side direct overwrite visible to container"

# --- Claim 3: atomic-rename rotation visible, no partial reads -------------

# Standard atomic-rename pattern: write to .tmp, then mv into place.
# Worker MUST use this pattern to avoid the watcher seeing a half-written
# file mid-rotation.
echo "$ROTATED_JWT" > "$HOST_DIR/step-jwt.tmp"
mv "$HOST_DIR/step-jwt.tmp" "$HOST_DIR/step-jwt"
sleep 0.5

logs=$(docker logs "$WATCHER" 2>&1)
if ! echo "$logs" | grep -q "$ROTATED_JWT"; then
  echo "  watcher logs:"
  echo "$logs" | sed 's/^/    /'
  fail "claim 3: watcher did not see atomic-rename rotation"
fi
pass "claim 3: atomic-rename rotation visible to container"

# --- Claim 4: no empty / partial reads observed ----------------------------

invalid_reads=$(echo "$logs" | grep "read:" | grep -vE "$INITIAL_JWT|intermediate-overwrite|$ROTATED_JWT|MISSING" || true)
if [ -n "$invalid_reads" ]; then
  echo "  unexpected reads found:"
  echo "$invalid_reads" | sed 's/^/    /'
  fail "claim 4: watcher observed partial / unexpected reads"
fi
pass "claim 4: no partial reads observed — bind-mount + atomic rename is safe"

# --- Negative result: confirm the BROKEN mechanism is broken ----------------
# This is the bug the spike caught. Documenting it inline so future
# readers see the evidence. If this assertion ever stops failing, the
# spec can revert to tmpfs+cp; until then, bind-mount stands.

NEGATIVE="loom-jwt-negative-spike"
docker run -d --name "$NEGATIVE" \
  --mount type=tmpfs,destination=/run/loom \
  alpine sh -c 'echo BEFORE > /run/loom/step-jwt; while true; do sleep 1; done' >/dev/null
sleep 0.5

echo "BROKEN_MECHANISM_AFTER" > /tmp/spike-04-broken.txt
docker cp /tmp/spike-04-broken.txt "$NEGATIVE:/run/loom/step-jwt" >/dev/null 2>&1
sleep 0.5

inside=$(docker exec "$NEGATIVE" cat /run/loom/step-jwt 2>/dev/null)
docker rm -f "$NEGATIVE" >/dev/null
rm -f /tmp/spike-04-broken.txt

if [ "$inside" = "BROKEN_MECHANISM_AFTER" ]; then
  echo "  HUH: docker cp into tmpfs actually worked this time."
  echo "  This is unexpected; the rev-7 mechanism may now be viable."
  echo "  Verify on multiple Docker versions before reverting the spec."
else
  pass "negative: docker cp into tmpfs of running container is BROKEN as expected (got '$inside')"
fi

echo ""
echo "All claims verified. JWT refresh = host-side bind-mount + atomic"
echo "rename. The rev 7-11 spec's tmpfs+docker-cp mechanism doesn't work;"
echo "spec updated accordingly."
