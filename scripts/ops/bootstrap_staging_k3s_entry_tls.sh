#!/bin/bash
# One-time (idempotent) bootstrap of the k3s staging public entry + native TLS,
# so a fresh bring-up self-provisions both with zero manual steps (#1114).
#
# Run as root on the k3s control-plane host (bb8-1), from a repo checkout:
#   sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml scripts/ops/bootstrap_staging_k3s_entry_tls.sh
#
# It:
#  1. installs the entry-cutover script + systemd unit (host :443/:80 -> k3s
#     ingress :8443/:8080), enabled + started;
#  2. patches the cert-manager controller to hostNetwork (its ACME API calls
#     need the host's clean egress — the CNI pod net MITMs outbound HTTPS);
#  3. applies the LE ClusterIssuer + Certificate (deploy/staging-k3s/tls-acme.yaml).
#
# Safe to re-run. Does NOT touch the kind cluster (rollback anchor).
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"        # repo root
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"; export KUBECONFIG
log() { echo "[bootstrap-entry-tls] $*"; }

# 1. entry cutover (host iptables -> k3s ingress), persisted via systemd.
log "installing entry-cutover script + systemd unit"
install -m 0755 "${here}/deploy/staging-k3s/loom-staging-k3s-cutover.sh" \
  /usr/local/sbin/loom-staging-k3s-cutover.sh
install -m 0644 "${here}/deploy/staging-k3s/loom-staging-k3s-cutover.service" \
  /etc/systemd/system/loom-staging-k3s-cutover.service
systemctl daemon-reload
systemctl enable --now loom-staging-k3s-cutover.service
log "entry cutover: $(systemctl is-active loom-staging-k3s-cutover.service)"

# 2. cert-manager controller needs host-network egress to reach Let's Encrypt.
log "patching cert-manager controller -> hostNetwork (clean ACME egress)"
kubectl -n cert-manager patch deploy cert-manager --type merge -p \
  '{"spec":{"template":{"spec":{"hostNetwork":true,"dnsPolicy":"ClusterFirstWithHostNet"}}}}'
kubectl -n cert-manager rollout status deploy/cert-manager --timeout=180s

# 3. LE issuer + Certificate (self-issues + auto-renews loom-staging-tls).
log "applying LE ClusterIssuer + Certificate"
kubectl apply -f "${here}/deploy/staging-k3s/tls-acme.yaml"

log "done. Certificate issues within ~1 min; verify:"
echo "  kubectl -n loom-staging get certificate yylx-world"
echo "  # external (bb8-1->own-IP uses OUTPUT and bypasses the DNAT, so resolve elsewhere):"
echo "  curl --resolve yylx.world:443:192.168.50.103 https://yylx.world/staging/api/v1/health"
