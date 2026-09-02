#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --gateway HOST --ssh-key PATH --known-hosts PATH --cluster-id ID --nebius-credentials PATH --model-provider-api-key-file PATH --service-execution-runtime-profile PATH --gateway-image DIGEST_REF --control-plane-image DIGEST_REF --service-image DIGEST_REF --execution-runtime-image DIGEST_REF" >&2
  exit 2
}

gateway=
ssh_key=
known_hosts=
cluster_id=
nebius_credentials=
model_provider_api_key_file=
service_execution_runtime_profile=
gateway_image=
control_plane_image=
service_image=
execution_runtime_image=
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
    --service-execution-runtime-profile)
      (($# >= 2)) || usage
      service_execution_runtime_profile=$2
      shift 2
      ;;
    --gateway-image)
      (($# >= 2)) || usage
      gateway_image=$2
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
    --execution-runtime-image)
      (($# >= 2)) || usage
      execution_runtime_image=$2
      shift 2
      ;;
    *) usage ;;
  esac
done

[[ $gateway =~ ^[A-Za-z0-9.-]+$ ]] || usage
[[ $cluster_id =~ ^mk8scluster-[a-z0-9]+$ ]] || usage
[[ $gateway_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ $control_plane_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ $service_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ $execution_runtime_image =~ ^[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}$ ]] || usage
[[ -f $ssh_key && -f $known_hosts && -f $nebius_credentials ]] || usage
[[ -s $model_provider_api_key_file ]] || usage
[[ -s $service_execution_runtime_profile ]] || usage

python3 - "$nebius_credentials" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
subject = document.get("subject-credentials")
if not isinstance(subject, dict):
    raise SystemExit("Nebius credentials must contain subject-credentials")
required = {"alg", "private-key", "kid", "iss", "sub"}
if not required <= subject.keys() or subject.get("alg") != "RS256":
    raise SystemExit("Nebius credentials must be a complete RS256 service-account document")
if subject.get("iss") != subject.get("sub"):
    raise SystemExit("Nebius credential issuer and subject must match")
PY

python3 - "$service_execution_runtime_profile" "$service_image" "$execution_runtime_image" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if payload.get("task_image_ref") != sys.argv[2]:
    raise SystemExit("runtime profile task_image_ref must equal --service-image")
if payload.get("runtime_image_ref") != sys.argv[3]:
    raise SystemExit("runtime profile runtime_image_ref must equal --execution-runtime-image")
PY

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
  scripts/ops/normalize_secret_file.py \
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

# This intentionally expands the locally generated numeric PID into a fixed,
# quoted remote path rather than accepting a remote-shell expression.
# shellcheck disable=SC2029
ssh "${ssh_options[@]}" "codex@$gateway" "install -d -m 700 '$remote_stage'"
scp "${ssh_options[@]}" \
  "$local_stage/runtime.tar.gz" \
  "$nebius_credentials" \
  "$model_provider_api_key_file" \
  "$service_execution_runtime_profile" \
  "codex@$gateway:$remote_stage/"

ssh "${ssh_options[@]}" "codex@$gateway" bash -s -- \
  "$remote_stage" "$cluster_id" "$(basename "$nebius_credentials")" \
  "$(basename "$model_provider_api_key_file")" \
  "$(basename "$service_execution_runtime_profile")" \
  "$gateway_image" "$control_plane_image" "$service_image" <<'REMOTE'
set -euo pipefail
remote_stage=$1
cluster_id=$2
credential_name=$3
provider_key_name=$4
runtime_profile_name=$5
gateway_image=$6
control_plane_image=$7
service_image=$8
trap 'rm -rf "$remote_stage"' EXIT

chmod 600 "$remote_stage/$credential_name"
chmod 600 "$remote_stage/$provider_key_name"
chmod 600 "$remote_stage/$runtime_profile_name"
tar -C "$remote_stage" -xzf "$remote_stage/runtime.tar.gz"
nebius mk8s cluster get-credentials \
  --id "$cluster_id" \
  --internal \
  --kubeconfig "$remote_stage/kubeconfig" \
  --no-progress
chmod 600 "$remote_stage/kubeconfig"

# Create the referenced Secret before a first rollout.  The inner idempotent
# apply validates and replaces the same object, then stamps its digest on both
# consuming Deployments so future profile changes always roll them.
kubectl --kubeconfig "$remote_stage/kubeconfig" create secret generic \
  loom-service-execution-runtime-profile -n loom \
  --from-file=profile-json="$remote_stage/$runtime_profile_name" \
  --dry-run=client -o yaml \
  | kubectl --kubeconfig "$remote_stage/kubeconfig" apply -f - >/dev/null

kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom set image \
  deployment/loom-llm-gateway "gateway=$gateway_image"
kubectl --kubeconfig "$remote_stage/kubeconfig" -n loom rollout status \
  deployment/loom-llm-gateway --timeout=300s
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
  --model-provider-api-key-file "$remote_stage/$provider_key_name" \
  --service-execution-runtime-profile "$remote_stage/$runtime_profile_name"
REMOTE
