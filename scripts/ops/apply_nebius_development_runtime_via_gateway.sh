#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --gateway HOST --ssh-key PATH --known-hosts PATH --cluster-id ID --nebius-credentials PATH --model-provider-api-key-file PATH --control-plane-image DIGEST_REF --service-image DIGEST_REF" >&2
  exit 2
}

gateway=
ssh_key=
known_hosts=
cluster_id=
nebius_credentials=
model_provider_api_key_file=
control_plane_image=
service_image=
while (($#)); do
  case "$1" in
    --gateway)
      (($# >= 2)) || usage
      gateway=$2
      shift 2
      ;;
    --ssh-key)
      (($# >= 2)) || usage
      ssh_key=$2
      shift 2
      ;;
    --known-hosts)
      (($# >= 2)) || usage
      known_hosts=$2
      shift 2
      ;;
    --cluster-id)
      (($# >= 2)) || usage
      cluster_id=$2
      shift 2
      ;;
    --nebius-credentials)
      (($# >= 2)) || usage
      nebius_credentials=$2
      shift 2
      ;;
    --model-provider-api-key-file)
      (($# >= 2)) || usage
      model_provider_api_key_file=$2
      shift 2
      ;;
    --control-plane-image)
      (($# >= 2)) || usage
      control_plane_image=$2
      shift 2
      ;;
    --service-image)
      (($# >= 2)) || usage
      service_image=$2
      shift 2
      ;;
    *) usage ;;
  esac
done

[[ $gateway =~ ^[A-Za-z0-9.-]+$ ]] || usage
[[ $cluster_id =~ ^mk8scluster-[a-z0-9]+$ ]] || usage
[[ $control_plane_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ $service_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ -f $ssh_key && -f $known_hosts && -f $nebius_credentials ]] || usage
[[ -s $model_provider_api_key_file ]] || usage

if ! credential_mode=$(stat -c '%a' "$nebius_credentials" 2>/dev/null); then
  credential_mode=$(stat -f '%Lp' "$nebius_credentials")
fi
if ((10#$credential_mode % 100 != 0)); then
  echo "Nebius credential file must not be group/world accessible" >&2
  exit 1
fi
if ! provider_key_mode=$(stat -c '%a' "$model_provider_api_key_file" 2>/dev/null); then
  provider_key_mode=$(stat -f '%Lp' "$model_provider_api_key_file")
fi
if ((10#$provider_key_mode % 100 != 0)); then
  echo "Model provider API key file must not be group/world accessible" >&2
  exit 1
fi

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
local_stage=$(mktemp -d)
remote_stage="/tmp/loom-nebius-runtime-$$"
trap 'rm -rf "$local_stage"' EXIT

tar -C "$repo_root" -czf "$local_stage/runtime.tar.gz" \
  scripts/ops/apply_nebius_development_runtime.sh \
  deploy/k8s/nebius-gateway-development-patch.yaml \
  deploy/k8s/nebius-control-plane-development-patch.yaml \
  deploy/k8s/nebius-service-development-patch.yaml \
  deploy/k8s/nebius-execution-actuator.yaml \
  deploy/k8s/nebius-capacity-collector.yaml \
  deploy/k8s/network-policies.yaml

ssh_options=(
  -o BatchMode=yes
  -o StrictHostKeyChecking=yes
  -o "UserKnownHostsFile=$known_hosts"
  -o ConnectTimeout=15
  -i "$ssh_key"
)

ssh "${ssh_options[@]}" "codex@$gateway" "install -d -m 700 '$remote_stage'"
scp "${ssh_options[@]}" \
  "$local_stage/runtime.tar.gz" \
  "$nebius_credentials" \
  "$model_provider_api_key_file" \
  "codex@$gateway:$remote_stage/"

ssh "${ssh_options[@]}" "codex@$gateway" bash -s -- \
  "$remote_stage" "$cluster_id" "$(basename "$nebius_credentials")" \
  "$(basename "$model_provider_api_key_file")" \
  "$control_plane_image" "$service_image" <<'REMOTE'
set -euo pipefail
remote_stage=$1
cluster_id=$2
credential_name=$3
provider_key_name=$4
control_plane_image=$5
service_image=$6
trap 'rm -rf "$remote_stage"' EXIT

chmod 600 "$remote_stage/$credential_name"
chmod 600 "$remote_stage/$provider_key_name"
tar -C "$remote_stage" -xzf "$remote_stage/runtime.tar.gz"
nebius mk8s cluster get-credentials \
  --id "$cluster_id" \
  --internal \
  --kubeconfig "$remote_stage/kubeconfig" \
  --no-progress
chmod 600 "$remote_stage/kubeconfig"

kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom set image \
  deployment/loom-control-plane "control-plane=$control_plane_image"
kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom rollout status \
  deployment/loom-control-plane --timeout=300s
kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom set image \
  deployment/loom-service "service=$service_image"
kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom rollout status \
  deployment/loom-service --timeout=300s

"$remote_stage/scripts/ops/apply_nebius_development_runtime.sh" \
  --kubeconfig "$remote_stage/kubeconfig" \
  --nebius-credentials "$remote_stage/$credential_name" \
  --model-provider-api-key-file "$remote_stage/$provider_key_name"
REMOTE
