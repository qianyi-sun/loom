#!/bin/bash
# Route the public entry address (bb8-1 :443 + :80) to the k3s ingress (:8443
# https / :8080 http), inserted ABOVE docker's DOCKER nat chain.  The explicit
# destination match is required: an unscoped PREROUTING rule also captures
# container egress to arbitrary Internet :443/:80 destinations. :80 is needed
# for cert-manager's HTTP-01 renewal (deploy/staging-k3s/tls-acme.yaml).
#
# Idempotent (`-C` check before `-I`); re-applied on boot by
# loom-staging-k3s-cutover.service. Installed by
# scripts/ops/bootstrap_staging_k3s_entry_tls.sh.
#
# Rollback (fall back to kind): delete both PREROUTING rules and disable the
# service; the kind cluster still binds :443/:80 beneath.
set -e
K3S_INGRESS_IP="${K3S_INGRESS_IP:-192.168.50.103}"
apply() { iptables -t nat -C "$@" 2>/dev/null || iptables -t nat -I "$@"; }
remove_legacy() {
  while iptables -t nat -C "$@" 2>/dev/null; do
    iptables -t nat -D "$@"
  done
}

# Install both bounded routes before removing the former unscoped rules. If
# either insertion fails, ``set -e`` leaves the working legacy routes intact.
apply PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 443 -j DNAT --to-destination "${K3S_INGRESS_IP}:8443"
apply PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 80 -j DNAT --to-destination "${K3S_INGRESS_IP}:8080"

# Loop so duplicate legacy rules cannot survive a prior retry.
remove_legacy PREROUTING -p tcp --dport 443 -j DNAT --to-destination "${K3S_INGRESS_IP}:8443"
remove_legacy PREROUTING -p tcp --dport 80 -j DNAT --to-destination "${K3S_INGRESS_IP}:8080"
