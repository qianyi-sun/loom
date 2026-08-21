#!/bin/bash
# One-time (idempotent) bootstrap of the k3s staging public entry + native TLS,
# so a fresh bring-up self-provisions both with zero manual steps.
#
# Run as root on the k3s control-plane host (bb8-1), from a repo checkout:
#   sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml scripts/ops/bootstrap_staging_k3s_entry_tls.sh
#
# It:
#  1. applies the dedicated NodePort Service, starts supervised host proxies,
#     then installs its legacy-route verifier + systemd unit;
#  2. patches the cert-manager controller to hostNetwork (its ACME API calls
#     need the host's clean egress — the CNI pod net MITMs outbound HTTPS);
#  3. applies the LE ClusterIssuer + Certificate (deploy/staging-k3s/tls-acme.yaml).
#
# Safe to re-run. Does not mutate application workloads or stateful data.
set -euo pipefail
here="$(cd "$(dirname "$0")/../.." && pwd)"        # repo root
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"; export KUBECONFIG
log() { echo "[bootstrap-entry-tls] $*"; }

# 1. Create the Kubernetes route and start Loom-owned proxy listeners before
#    removing any predecessor path.
log "applying the pinned public-entry NodePort Service"
kubectl apply -f "${here}/deploy/staging-k3s/loom-staging-public-entry.yaml"
log "installing public-entry proxy + cutover units"
install -m 0755 "${here}/deploy/staging-k3s/loom-staging-public-proxy.sh" \
  /usr/local/sbin/loom-staging-public-proxy.sh
install -m 0644 "${here}/deploy/staging-k3s/loom-staging-public-proxy@.service" \
  /etc/systemd/system/loom-staging-public-proxy@.service
install -m 0755 "${here}/deploy/staging-k3s/loom-staging-k3s-cutover.sh" \
  /usr/local/sbin/loom-staging-k3s-cutover.sh
install -m 0644 "${here}/deploy/staging-k3s/loom-staging-k3s-cutover.service" \
  /etc/systemd/system/loom-staging-k3s-cutover.service
systemctl daemon-reload
systemctl reenable \
  loom-staging-public-proxy@18080-32080.service \
  loom-staging-public-proxy@18443-32443.service \
  loom-staging-k3s-cutover.service
systemctl restart \
  loom-staging-public-proxy@18080-32080.service \
  loom-staging-public-proxy@18443-32443.service
# The cutover is a RemainAfterExit oneshot; restart revalidates after replacing
# its executable and after both proxy listeners are active.
if ! systemctl restart loom-staging-k3s-cutover.service; then
  log "initial entry cutover attempt is waiting for proxy/kube-proxy readiness"
fi
cutover_active=false
for _attempt in {0..12}; do
  if systemctl is-active --quiet loom-staging-k3s-cutover.service; then
    cutover_active=true
    break
  fi
  if (( _attempt < 12 )); then
    sleep 5
  fi
done
if [[ "${cutover_active}" != true ]]; then
  systemctl status loom-staging-k3s-cutover.service --no-pager -l || true
  echo "entry cutover did not become active within 60 seconds" >&2
  exit 1
fi
log "entry path: $(systemctl is-active \
  loom-staging-public-proxy@18080-32080.service \
  loom-staging-public-proxy@18443-32443.service \
  loom-staging-k3s-cutover.service | paste -sd' ')"

# 2. cert-manager controller needs host-network egress to reach Let's Encrypt.
log "patching cert-manager controller -> hostNetwork (clean ACME egress)"
kubectl -n cert-manager patch deploy cert-manager --type merge -p \
  '{"spec":{"template":{"spec":{"hostNetwork":true,"dnsPolicy":"ClusterFirstWithHostNet"}}}}'
kubectl -n cert-manager rollout status deploy/cert-manager --timeout=180s

# 3. LE issuer + Certificate (self-issues + auto-renews loom-staging-tls).
log "applying LE ClusterIssuer + Certificate"
kubectl apply -f "${here}/deploy/staging-k3s/tls-acme.yaml"

# 4. GB10 fleet connectivity: socat forwards from the ports the nodes tunnel to
#    (bb8-1 :18081/:18082/:19000/:19100) onto the k3s router hostPorts.
log "installing GB10 fleet forward units (k3s connectivity bridge)"
install -m 0755 "${here}/deploy/staging-k3s/loom-k3s-fleet-fwd.sh" \
  /usr/local/sbin/loom-k3s-fleet-fwd.sh
install -m 0644 "${here}/deploy/staging-k3s/loom-k3s-fleet-fwd@.service" \
  /etc/systemd/system/loom-k3s-fleet-fwd@.service
systemctl daemon-reload
for map in 18081-30080 18082-30080 19000-30900 19100-30443; do
  systemctl enable --now "loom-k3s-fleet-fwd@${map}.service"
done
log "fleet forwards: $(systemctl is-active 'loom-k3s-fleet-fwd@18081-30080' 'loom-k3s-fleet-fwd@19000-30900' 'loom-k3s-fleet-fwd@19100-30443' | paste -sd' ')"

log "done. Certificate issues within ~1 min; verify:"
echo "  kubectl -n loom-staging get certificate yylx-world"
echo "  # external (bb8-1->own-IP uses OUTPUT and bypasses the DNAT, so resolve elsewhere):"
echo "  curl --resolve yylx.world:443:192.168.50.103 https://yylx.world/staging/api/v1/health"
