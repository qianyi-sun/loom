#!/bin/bash
# Supervised public-entry proxy. Each accepted connection opens a new local
# NodePort connection, allowing kube-proxy to select any ready ingress endpoint
# without a host-owned reference to kube-proxy's generated chains.
set -euo pipefail

K3S_INGRESS_IP="${K3S_INGRESS_IP:-192.168.50.103}"
SOCAT_BIN="${SOCAT_BIN:-/usr/bin/socat}"
mapping="${1:-}"

if [[ ! "${mapping}" =~ ^([0-9]+)-([0-9]+)$ ]]; then
  echo "expected <listen-port>-<node-port>, got: ${mapping}" >&2
  exit 2
fi

listen_port="${BASH_REMATCH[1]}"
node_port="${BASH_REMATCH[2]}"

exec "${SOCAT_BIN}" -d \
  "TCP-LISTEN:${listen_port},bind=${K3S_INGRESS_IP},reuseaddr,fork" \
  "TCP:${K3S_INGRESS_IP}:${node_port},connect-timeout=5"
