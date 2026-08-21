#!/bin/bash
# Route public :443/:80 to supervised Loom proxy listeners. Each proxy opens a
# local connection to the dedicated ingress NodePort Service, so kube-proxy
# selects any ready ingress endpoint without Loom referring to disposable
# KUBE-* chains. After both replacement paths exist, remove obsolete rules left
# by the staging entry migration and the emergency HA repair.
#
# Idempotent; re-applied with the k3s lifecycle by
# loom-staging-k3s-cutover.service. Installed by
# scripts/ops/bootstrap_staging_k3s_entry_tls.sh.
#
# Rollback requires a separately verified replacement route before disabling
# the proxy units or removing these destination-scoped DNAT rules.
set -euo pipefail

K3S_INGRESS_IP="${K3S_INGRESS_IP:-192.168.50.103}"
K3S_INGRESS_SERVICE="${K3S_INGRESS_SERVICE:-ingress-nginx/loom-staging-public-entry}"
CURL_BIN="${CURL_BIN:-/usr/bin/curl}"
HTTP_NODE_PORT="${HTTP_NODE_PORT:-32080}"
HTTPS_NODE_PORT="${HTTPS_NODE_PORT:-32443}"
HTTP_PROXY_PORT="${HTTP_PROXY_PORT:-18080}"
HTTPS_PROXY_PORT="${HTTPS_PROXY_PORT:-18443}"

iptables_nat() {
  iptables --wait -t nat "$@"
}

nodeport_route_exists() {
  local port_name="$1"
  local node_port="$2"
  local line
  while IFS= read -r line; do
    if [[ "${line}" == "-A KUBE-NODEPORTS "* \
      && "${line}" == *" -p tcp "* \
      && "${line}" == *" --comment \"${K3S_INGRESS_SERVICE}:${port_name}\" "* \
      && "${line}" == *" --dport ${node_port} "* \
      && "${line}" == *" -j KUBE-"* ]]; then
      return 0
    fi
  done <<<"${KUBE_NODEPORT_RULES}"
  return 1
}

require_nodeport_route() {
  local port_name="$1"
  local node_port="$2"
  if ! nodeport_route_exists "${port_name}" "${node_port}"; then
    echo "kube-proxy NodePort route not ready for ${K3S_INGRESS_SERVICE}:${port_name}" >&2
    return 1
  fi
}

probe_proxy() {
  local scheme="$1"
  local proxy_port="$2"
  local path="$3"
  local -a tls_options=()
  if [[ "${scheme}" == "https" ]]; then
    # This transport probe runs before bootstrap creates the ACME Certificate.
    # Public post-cutover checks still validate the issued certificate.
    tls_options+=(--insecure)
  fi
  "${CURL_BIN}" \
    --fail \
    --silent \
    --show-error \
    --output /dev/null \
    --noproxy '*' \
    --connect-timeout 2 \
    --max-time 5 \
    --retry 5 \
    --retry-all-errors \
    --retry-delay 1 \
    --retry-max-time 15 \
    "${tls_options[@]}" \
    --resolve "yylx.world:${proxy_port}:${K3S_INGRESS_IP}" \
    "${scheme}://yylx.world:${proxy_port}${path}"
}

ensure_rule() {
  local check_output
  local check_status
  if check_output="$(iptables_nat -C "$@" 2>&1)"; then
    return 0
  else
    check_status=$?
  fi
  case "${check_status}" in
    1) iptables_nat -I "$@" ;;
    *)
      printf '%s\n' "${check_output}" >&2
      return "${check_status}"
      ;;
  esac
}

remove_legacy() {
  local check_output
  local check_status
  while true; do
    if check_output="$(iptables_nat -C "$@" 2>&1)"; then
      iptables_nat -D "$@"
      continue
    else
      check_status=$?
    fi
    case "${check_status}" in
      1) return 0 ;;
      *)
        printf '%s\n' "${check_output}" >&2
        return "${check_status}"
        ;;
    esac
  done
}

legacy_external_chain() {
  local port="$1"
  local line
  local target
  while IFS= read -r line; do
    if [[ "${line}" == "-A PREROUTING "* \
      && "${line}" == *" -d ${K3S_INGRESS_IP}/32 "* \
      && "${line}" == *" -p tcp "* \
      && "${line}" == *" --dport ${port} "* \
      && "${line}" == *" -j KUBE-EXT-"* ]]; then
      target="${line##* -j }"
      if [[ ! "${target}" =~ ^KUBE-EXT-[A-Z0-9]+$ ]]; then
        echo "refusing malformed legacy kube-proxy target: ${target}" >&2
        return 2
      fi
      printf '%s\n' "${target}"
      return 0
    fi
  done <<<"${PREROUTING_RULES}"
}

remove_legacy_external_jumps() {
  local port="$1"
  local target
  while true; do
    PREROUTING_RULES="$(iptables_nat -S PREROUTING)"
    target="$(legacy_external_chain "${port}")"
    if [[ -z "${target}" ]]; then
      return 0
    fi
    remove_legacy PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport "${port}" -j "${target}"
  done
}

# Validate both Kubernetes routes before changing the working emergency path.
KUBE_NODEPORT_RULES="$(iptables_nat -S KUBE-NODEPORTS)"
require_nodeport_route https "${HTTPS_NODE_PORT}"
require_nodeport_route http "${HTTP_NODE_PORT}"

# Type=simple only proves that systemd forked socat. Exercise both complete
# proxy-to-NodePort paths before changing any packet route; retry briefly to
# absorb the listener bind and kube-proxy reconciliation races at boot.
probe_proxy https "${HTTPS_PROXY_PORT}" /staging/api/v1/health
probe_proxy http "${HTTP_PROXY_PORT}" /staging/

# Install both destination-scoped proxy routes before removing any predecessor.
# If the second insertion fails, the first proxy route is already a valid path
# and the untouched predecessor continues serving the other port.
ensure_rule PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 443 -j DNAT --to-destination "${K3S_INGRESS_IP}:${HTTPS_PROXY_PORT}"
ensure_rule PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 80 -j DNAT --to-destination "${K3S_INGRESS_IP}:${HTTP_PROXY_PORT}"

# Remove direct references created by the emergency repair, including stale
# chain hashes from any earlier Service incarnation.
remove_legacy_external_jumps 443
remove_legacy_external_jumps 80

# Remove both destination-scoped and historical unscoped local-pod DNAT rules.
remove_legacy PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 443 -j DNAT --to-destination "${K3S_INGRESS_IP}:8443"
remove_legacy PREROUTING -d "${K3S_INGRESS_IP}/32" -p tcp --dport 80 -j DNAT --to-destination "${K3S_INGRESS_IP}:8080"
remove_legacy PREROUTING -p tcp --dport 443 -j DNAT --to-destination "${K3S_INGRESS_IP}:8443"
remove_legacy PREROUTING -p tcp --dport 80 -j DNAT --to-destination "${K3S_INGRESS_IP}:8080"
