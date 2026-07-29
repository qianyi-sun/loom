#!/usr/bin/env bash
set -euo pipefail

# This firewall is outside the guest. PR code can become root inside its
# disposable VM but cannot change these rules or reach RFC1918/link-local
# services on oldlab-5's private networks.
nft -f - <<'NFT'
table inet loom_ci_egress {
  chain output {
    type filter hook output priority 0; policy accept;

    ip daddr 127.0.0.11 udp dport 53 accept
    ip daddr 127.0.0.11 tcp dport 53 accept

    ip daddr {
      0.0.0.0/8,
      10.0.0.0/8,
      100.64.0.0/10,
      127.0.0.0/8,
      169.254.0.0/16,
      172.16.0.0/12,
      192.0.0.0/24,
      192.0.2.0/24,
      192.168.0.0/16,
      198.18.0.0/15,
      198.51.100.0/24,
      203.0.113.0/24,
      224.0.0.0/4,
      240.0.0.0/4
    } reject

    ip6 daddr != ::1 reject
  }
}
NFT

# QEMU needs /dev/kvm and its slot files, not ambient container capabilities.
exec setpriv \
  --bounding-set=-all \
  --inh-caps=-all \
  --ambient-caps=-all \
  -- "$@"
