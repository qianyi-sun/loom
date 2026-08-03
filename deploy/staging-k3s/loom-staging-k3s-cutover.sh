#!/bin/bash
# Route the public entry (bb8-1 :443 + :80) to the k3s ingress (:8443 https /
# :8080 http), inserted ABOVE docker's DOCKER nat chain which would otherwise
# DNAT :443/:80 to the legacy kind cluster (192.168.80.2). :80 is required for
# cert-manager's HTTP-01 renewal on k3s (deploy/staging-k3s/tls-acme.yaml).
#
# Idempotent (`-C` check before `-I`); re-applied on boot by
# loom-staging-k3s-cutover.service. Installed by
# scripts/ops/bootstrap_staging_k3s_entry_tls.sh.
#
# Rollback (fall back to kind): delete both PREROUTING rules and disable the
# service; the kind cluster still binds :443/:80 beneath.
set -e
K3S_INGRESS_IP="${K3S_INGRESS_IP:-192.168.50.103}"
apply() { iptables -t nat -C $1 2>/dev/null || iptables -t nat -I $1; }
apply "PREROUTING -p tcp --dport 443 -j DNAT --to-destination ${K3S_INGRESS_IP}:8443"
apply "PREROUTING -p tcp --dport 80 -j DNAT --to-destination ${K3S_INGRESS_IP}:8080"
