#!/usr/bin/env bash
# Publish the narrow staging credential consumed by an external Slurm controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTHORITY_MANIFEST="$SCRIPT_DIR/../k8s/external-slurm-autoscaler-authority.yaml"
KUBECTL="${KUBECTL:-/usr/local/bin/kubectl}"
NAMESPACE="loom-staging"
SOURCE_SECRET="loom-secrets"
TARGET_SECRET="loom-external-slurm-autoscaler-db"
TOKEN_SECRET="loom-external-slurm-autoscaler-token"
API_SERVER="https://192.168.50.103:6443"

if [ "$#" -ne 1 ]; then
  echo "usage: KUBECONFIG=/path/to/admin-kubeconfig $0 OUTPUT_KUBECONFIG" >&2
  exit 2
fi
output_path="$1"
if [ -z "${KUBECONFIG:-}" ] || [ ! -r "$KUBECONFIG" ]; then
  echo "error: a readable source KUBECONFIG is required" >&2
  exit 1
fi
if [ ! -f "$AUTHORITY_MANIFEST" ] || [ -L "$AUTHORITY_MANIFEST" ]; then
  echo "error: external autoscaler authority manifest is unavailable" >&2
  exit 1
fi
if [ -L "$output_path" ]; then
  echo "error: output kubeconfig must not be a symlink" >&2
  exit 1
fi

umask 077
temporary_dir="$(mktemp -d)"
trap 'rm -rf -- "$temporary_dir"' EXIT

"$KUBECTL" --kubeconfig "$KUBECONFIG" apply -f "$AUTHORITY_MANIFEST" >/dev/null

# Keep the broad source credential off command lines and out of output. The
# generated Kubernetes Secret contains only the one key the runner consumes.
"$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" get secret \
  "$SOURCE_SECRET" -o 'jsonpath={.data.cp-db-url}' \
  | base64 --decode >"$temporary_dir/cp-db-url"
if [ ! -s "$temporary_dir/cp-db-url" ]; then
  echo "error: source database credential is empty" >&2
  exit 1
fi
"$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" create secret generic \
  "$TARGET_SECRET" --from-file=cp-db-url="$temporary_dir/cp-db-url" \
  --dry-run=client -o yaml \
  | "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" apply -f - >/dev/null

token_data=""
ca_data=""
for _attempt in $(seq 1 30); do
  token_data="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" get secret \
      "$TOKEN_SECRET" -o 'jsonpath={.data.token}' 2>/dev/null || true
  )"
  ca_data="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" get secret \
      "$TOKEN_SECRET" -o 'jsonpath={.data.ca\.crt}' 2>/dev/null || true
  )"
  if [ -n "$token_data" ] && [ -n "$ca_data" ]; then
    break
  fi
  sleep 1
done
if [ -z "$token_data" ] || [ -z "$ca_data" ]; then
  echo "error: service-account token was not populated" >&2
  exit 1
fi
printf '%s' "$token_data" | base64 --decode >"$temporary_dir/token"
printf '%s' "$ca_data" | base64 --decode >"$temporary_dir/ca.crt"
bearer_token="$(<"$temporary_dir/token")"
embedded_ca="$(base64 --wrap=0 "$temporary_dir/ca.crt")"

printf '%s\n' \
  'apiVersion: v1' \
  'kind: Config' \
  'clusters:' \
  '- name: loom-staging' \
  '  cluster:' \
  "    server: $API_SERVER" \
  "    certificate-authority-data: $embedded_ca" \
  'users:' \
  '- name: loom-external-slurm-autoscaler' \
  '  user:' \
  "    token: $bearer_token" \
  'contexts:' \
  '- name: loom-staging-external-slurm-autoscaler' \
  '  context:' \
  '    cluster: loom-staging' \
  '    namespace: loom-staging' \
  '    user: loom-external-slurm-autoscaler' \
  'current-context: loom-staging-external-slurm-autoscaler' \
  >"$temporary_dir/kubeconfig"
chmod 0600 "$temporary_dir/kubeconfig"

"$KUBECTL" --kubeconfig "$temporary_dir/kubeconfig" -n "$NAMESPACE" \
  get secret "$TARGET_SECRET" -o name >/dev/null
install -m 0600 "$temporary_dir/kubeconfig" "$output_path"
printf 'published namespace-scoped external Slurm autoscaler kubeconfig: %s\n' \
  "$output_path"
