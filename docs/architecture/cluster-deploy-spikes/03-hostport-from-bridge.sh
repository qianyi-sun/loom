#!/usr/bin/env bash
# 03-hostport-from-bridge.sh — verify hostPort traffic from a Docker bridge.
#
# This is the spike for the rev 9 weakened claim. The spec says:
#   "hostPort is reachable from Docker bridges in most kubeadm/k3s setups,
#    but the guarantee is conditional: CNI's portmap plugin installs
#    iptables DNAT rules that match destination IPs the CNI considers
#    'node-local,' and Docker bridge IPs aren't always in that set."
#
# This spike confirms whether the path actually works on kind. If it
# does on kind (which uses kindnet, similar portmap behavior to common
# CNIs), the design is more likely to work on real clusters. If it
# DOESN'T even on kind, the design needs the NodePort fallback or a
# host-routing escape hatch.
#
# Topology:
#   real host
#   ├─ Docker daemon
#   │  ├─ kind node container (= k8s node)
#   │  │  └─ gateway-router pod (hostPort 30443 on kind node IP)
#   │  ├─ loom-uplink bridge (created by this script)
#   │  │  └─ singleton-mock container
#   │  └─ kind node's hostPort exposed to host via kind portMapping
#   └─ host's IP on loom-uplink
#
# Test: singleton-mock on loom-uplink dials <host-bridge-gw>:30443.
# Expected: traffic reaches the gateway-router pod (or doesn't, in
# which case we know the design needs a different mechanism).

set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"

CLUSTER="loom-spike-03"
UPLINK_NET="loom-uplink-spike-03"
SINGLETON="loom-singleton-spike-03"

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
warn() { echo "WARN: $*" >&2; }

cleanup() {
  set +e
  docker rm -f "$SINGLETON" >/dev/null 2>&1
  docker network rm "$UPLINK_NET" >/dev/null 2>&1
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
  set -e
}
trap cleanup EXIT

# --- Setup: kind cluster with port mapping ------------------------------------

echo "Setup: kind cluster with hostPort 30443 mapped to host..."
cat <<EOF | kind create cluster --name "$CLUSTER" --config=- >/dev/null 2>&1
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30443
        hostPort: 30443
        protocol: TCP
EOF

# Wait for default ServiceAccount to be created (kind sometimes races).
echo "  waiting for default ServiceAccount..."
for i in {1..30}; do
  if kubectl --context "kind-${CLUSTER}" get serviceaccount default -n default >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# Apply a gateway-router stand-in: a small pod with hostPort 30443.
# nginx returns 200 OK on /; perfect for reachability testing.
cat <<EOF | kubectl --context "kind-${CLUSTER}" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: gateway-router-mock
  namespace: default
spec:
  containers:
    - name: nginx
      image: nginx:alpine
      ports:
        - containerPort: 80
          hostPort: 30443
EOF

echo "  waiting for gateway-router-mock to be Ready..."
kubectl --context "kind-${CLUSTER}" wait --for=condition=Ready pod/gateway-router-mock --timeout=60s >/dev/null
pass "gateway-router-mock running with hostPort 30443"

# --- Setup: loom-uplink Docker bridge + singleton mock -----------------------

docker network create --driver bridge "$UPLINK_NET" >/dev/null
docker run -d --name "$SINGLETON" \
  --network "$UPLINK_NET" \
  busybox sh -c "sleep 600" >/dev/null

# Discover the host's IP on the uplink bridge (Docker IPAM assigns).
GW=$(docker network inspect "$UPLINK_NET" --format '{{(index .IPAM.Config 0).Gateway}}')
echo "  uplink bridge gateway IP: $GW"

# --- Claim: singleton-on-uplink can reach the kind-mapped hostPort -----------
# kind's extraPortMappings publishes hostPort 30443 on the actual host.
# The singleton on the bridge dials <bridge-gw>:30443. The bridge-gw
# IS the host's IP on the bridge, so this is the same as "singleton
# dials host:30443". Question: does the publish-on-host route apply
# regardless of source interface (the bridge), or does it only apply
# to the host's primary NIC?

reply=$(docker exec "$SINGLETON" sh -c \
  "wget -q -O - --timeout=5 http://${GW}:30443/ 2>&1 | head -c 100" || echo "")

echo "  reply from singleton's wget: '${reply:0:80}'"

if echo "$reply" | grep -q "nginx\|Welcome"; then
  pass "claim: hostPort reachable from Docker bridge container via host's bridge IP"
  echo ""
  echo "EMPIRICAL FINDING: On kind+kindnet, hostPort traffic DOES traverse"
  echo "from a Docker bridge container to a k8s hostPort pod. This is good"
  echo "evidence for the rev 9 design (preflight probe is still warranted"
  echo "since CNI behavior varies on real clusters)."
else
  warn "hostPort NOT reachable from Docker bridge container on kind."
  echo ""
  echo "EMPIRICAL FINDING: The hostPort-from-bridge path doesn't survive"
  echo "the journey on kind. The design needs ONE of:"
  echo "  - explicit host-routing rule on the loom-uplink bridge"
  echo "  - alternative mechanism (NodePort with --nodeport-addresses tuning,"
  echo "    or running the singleton on host network — but rev 6 rejected"
  echo "    that)"
  echo "  - per-trial bridge connected to a kind/k3s NodePort range"
  echo ""
  echo "This is why the rev 9 preflight TCP-connect probe is mandatory:"
  echo "the path may not work on the operator's specific CNI."
  fail "claim falsified on kind+kindnet"
fi

# --- Sanity check: confirm the host itself can reach hostPort ----------------
# This proves the kind portmapping works at all, isolating the bridge
# question from "is kind itself broken."

if curl -sf --max-time 5 "http://localhost:30443/" >/dev/null; then
  pass "sanity: host itself reaches localhost:30443 (kind portMapping works)"
else
  warn "sanity: even the host can't reach localhost:30443 — kind portMapping issue"
fi
