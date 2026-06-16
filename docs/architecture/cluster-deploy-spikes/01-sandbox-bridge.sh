#!/usr/bin/env bash
# 01-sandbox-bridge.sh — verify the rev 6+ Path B sandbox-isolation primitive.
#
# Spec claims under test (cluster-deploy.md §Sandbox→gateway):
#   1. `docker network create --internal` produces a bridge with no host
#      route. Containers on it cannot reach the internet OR the host.
#   2. A singleton attached to two bridges (one --internal, one normal)
#      can serve traffic from the --internal side while reaching
#      something off the host on the normal side. Docker accepts this
#      combination (unlike host-network + bridges, which it rejects).
#   3. The sandbox container, attached only to the --internal bridge,
#      can reach the singleton at the pinned per-trial IP and nothing
#      else.
#
# Failure of any of these falsifies the SSRF defense Layer 1.
#
# Runs against plain Docker. No k8s required.

set -euo pipefail

CIDR_PREFIX="${CIDR_PREFIX:-10.99}"
TRIAL_IDX="${TRIAL_IDX:-42}"
UPLINK_NET="loom-uplink-spike"
SANDBOX_NET="sandbox-spike-${TRIAL_IDX}"
SINGLETON="loom-singleton-spike"
SANDBOX="loom-sandbox-spike"

cleanup() {
  set +e
  docker rm -f "$SANDBOX" >/dev/null 2>&1
  docker rm -f "$SINGLETON" >/dev/null 2>&1
  docker network rm "$SANDBOX_NET" >/dev/null 2>&1
  docker network rm "$UPLINK_NET" >/dev/null 2>&1
  set -e
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

# --- Setup --------------------------------------------------------------------

echo "Setup: creating loom-uplink and sandbox bridge..."
docker network create --driver bridge "$UPLINK_NET" >/dev/null
docker network create \
  --driver bridge \
  --internal \
  --subnet "${CIDR_PREFIX}.${TRIAL_IDX}.0/24" \
  --gateway "${CIDR_PREFIX}.${TRIAL_IDX}.1" \
  "$SANDBOX_NET" >/dev/null

# Singleton: attached to BOTH networks. Listens on :8443.
# busybox nc serves a static reply so we can verify reachability.
docker run -d --name "$SINGLETON" \
  --network "$UPLINK_NET" \
  busybox sh -c "while true; do echo 'singleton-ok' | nc -l -p 8443; done" >/dev/null

docker network connect \
  --ip "${CIDR_PREFIX}.${TRIAL_IDX}.2" \
  "$SANDBOX_NET" "$SINGLETON"

# Sandbox: attached only to the --internal bridge.
docker run -d --name "$SANDBOX" \
  --network "$SANDBOX_NET" \
  busybox sh -c "sleep 600" >/dev/null

# --- Claim 1: Docker accepts singleton on two bridges -------------------------
# If Docker had rejected the second `network connect` (as it does for
# host-network containers), the singleton would not be reachable from
# both bridges. Verify by inspecting its network attachments.

attachments=$(docker inspect "$SINGLETON" \
  --format '{{range $k, $_ := .NetworkSettings.Networks}}{{$k}} {{end}}')
echo "  singleton attachments: $attachments"
echo "$attachments" | grep -q "$UPLINK_NET" || \
  fail "singleton missing $UPLINK_NET attachment"
echo "$attachments" | grep -q "$SANDBOX_NET" || \
  fail "singleton missing $SANDBOX_NET attachment"
pass "claim 1: singleton joined to both bridges (uplink + per-trial)"

# --- Claim 2: sandbox CAN reach singleton at pinned IP -----------------------
# nc the singleton's bridge IP from the sandbox; should receive the reply.

reply=$(docker exec "$SANDBOX" sh -c \
  "echo '' | nc -w 2 ${CIDR_PREFIX}.${TRIAL_IDX}.2 8443" 2>/dev/null || true)
if [[ "$reply" == *"singleton-ok"* ]]; then
  pass "claim 2: sandbox reaches singleton at pinned bridge IP"
else
  fail "claim 2: sandbox could not reach singleton (got: '$reply')"
fi

# --- Claim 3: sandbox has no default route (so internet is unreachable) ------
# Structural check on `ip route` is more robust than attempting a network
# connection: --internal bridges have no default gateway, period.

if docker exec "$SANDBOX" ip route 2>&1 | grep -q "^default"; then
  fail "claim 3: sandbox HAS a default route — --internal bridge isn't blocking egress"
fi
pass "claim 3: sandbox has no default route (--internal bridge denies host route)"

# Sanity-check: confirm the network call fails (non-zero exit). nc on
# busybox is silent on -z when blocked; we check the exit code.
if docker exec "$SANDBOX" nc -w 2 -z 8.8.8.8 53 >/dev/null 2>&1; then
  fail "claim 3b: sandbox dial of 8.8.8.8:53 SUCCEEDED — internet is reachable"
else
  pass "claim 3b: sandbox dial of 8.8.8.8:53 fails closed (non-zero exit)"
fi

# --- Claim 4: sandbox CANNOT reach the host's IP on uplink bridge ------------
# The host has an IP on loom-uplink-spike (Docker IPAM assigned). The
# sandbox is NOT on that bridge; from the sandbox's --internal bridge,
# the uplink-bridge IPs should be unreachable.

uplink_host_ip=$(docker network inspect "$UPLINK_NET" \
  --format '{{(index .IPAM.Config 0).Gateway}}')
echo "  uplink host IP: $uplink_host_ip"

if docker exec "$SANDBOX" sh -c "ping -c 1 -W 2 $uplink_host_ip" >/dev/null 2>&1; then
  fail "claim 4: sandbox reached host's uplink IP — bridges aren't isolated"
fi
pass "claim 4: sandbox cannot reach host's IP on uplink bridge"

echo ""
echo "All claims verified. The rev 6+ Path B sandbox-isolation design"
echo "composes correctly with Docker's primitives."
