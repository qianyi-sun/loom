#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --kubeconfig PATH --nebius-credentials PATH --model-provider-api-key-file PATH" >&2
  exit 2
}

kubeconfig=
nebius_credentials=
model_provider_api_key_file=
while (($#)); do
  case "$1" in
    --kubeconfig)
      (($# >= 2)) || usage
      kubeconfig=$2
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
    *) usage ;;
  esac
done

[[ -n "$kubeconfig" && -f "$kubeconfig" ]] || usage
[[ -n "$nebius_credentials" && -f "$nebius_credentials" ]] || usage
[[ -n "$model_provider_api_key_file" && -s "$model_provider_api_key_file" ]] || usage

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

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
export KUBECONFIG=$kubeconfig

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

normalized_provider_key=$(mktemp)
token_file=
cleanup() {
  rm -f "$normalized_provider_key"
  [[ -z "$token_file" ]] || rm -f "$token_file"
}
trap cleanup EXIT
chmod 600 "$normalized_provider_key"
python3 "$repo_root/scripts/ops/normalize_secret_file.py" \
  "$model_provider_api_key_file" "$normalized_provider_key"
provider_key_sha256=$(python3 - "$normalized_provider_key" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

print(sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)

kubectl get namespace loom >/dev/null
kubectl get secret -n loom loom-admin-secret >/dev/null
kubectl get secret -n loom loom-image-admission >/dev/null
kubectl get secret -n loom-nebius-development loom-execution-actuator-db >/dev/null

kubectl create secret generic loom-nebius-model-provider \
  -n loom \
  --from-file=api-key="$normalized_provider_key" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl patch deployment -n loom loom-llm-gateway --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-gateway-development-patch.yaml" >/dev/null
kubectl patch deployment -n loom loom-llm-gateway --type=merge \
  --patch "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"loom.ca/model-provider-secret-sha256\":\"$provider_key_sha256\"}}}}}" \
  >/dev/null
kubectl rollout status -n loom deployment/loom-llm-gateway --timeout=180s

kubectl patch deployment -n loom loom-control-plane --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-control-plane-development-patch.yaml" >/dev/null
kubectl rollout status -n loom deployment/loom-control-plane --timeout=180s
kubectl patch deployment -n loom loom-service --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-service-development-patch.yaml" >/dev/null
kubectl rollout status -n loom deployment/loom-service --timeout=180s

kubectl apply --dry-run=server -f "$repo_root/deploy/k8s/nebius-execution-actuator.yaml" >/dev/null
kubectl apply --dry-run=server -f "$repo_root/deploy/k8s/nebius-capacity-collector.yaml" >/dev/null

kubectl create secret generic loom-execution-capacity-collector-nebius \
  -n loom-nebius-development \
  --from-file=credentials.json="$nebius_credentials" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

if ! kubectl get secret -n loom-nebius-development \
  loom-execution-capacity-collector-control-plane >/dev/null 2>&1; then
  token_file=$(mktemp)
  chmod 600 "$token_file"
  kubectl exec -n loom deploy/loom-control-plane -- python -c '
import json
import tomllib
import urllib.request

with open("/var/run/loom/admin/secrets.toml", "rb") as handle:
    admin_token = tomllib.load(handle)["admin"]["token"]
request = urllib.request.Request(
    "http://127.0.0.1:8080/admin/execution-capacity-collector-tokens",
    data=b"{}",
    headers={
        "Authorization": f"Bearer {admin_token}",
        "Content-Type": "application/json",
    },
    method="POST",
)
with urllib.request.urlopen(request, timeout=15) as response:
    payload = json.load(response)
token = payload.get("token", "")
if not token.startswith("loom_ecc_"):
    raise SystemExit("collector token mint returned an invalid response")
print(token, end="")
' >"$token_file"
  kubectl create secret generic loom-execution-capacity-collector-control-plane \
    -n loom-nebius-development \
    --from-file=token="$token_file" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

kubectl apply -f "$repo_root/deploy/k8s/network-policies.yaml" >/dev/null
kubectl apply -f "$repo_root/deploy/k8s/nebius-execution-actuator.yaml" >/dev/null
kubectl apply -f "$repo_root/deploy/k8s/nebius-capacity-collector.yaml" >/dev/null
kubectl rollout status -n loom-nebius-development deployment/loom-execution-actuator --timeout=180s

deadline=$((SECONDS + 180))
while ((SECONDS < deadline)); do
  desired_collector_image=$(kubectl get cronjob -n loom-nebius-development \
    loom-execution-capacity-collector \
    -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].image}')
  latest_job=$(kubectl get jobs -n loom-nebius-development \
    -l app.kubernetes.io/name=loom-execution-capacity-collector \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.succeeded}{"|"}{.spec.template.spec.containers[0].image}{"\n"}{end}' \
    | tail -n 1)
  if [[ $latest_job == *"|1|$desired_collector_image" ]]; then
    echo "Nebius development runtime is reconciled and the recurring collector has succeeded"
    exit 0
  fi
  sleep 5
done

echo "capacity collector did not complete within 180 seconds" >&2
kubectl get cronjob,jobs,pods -n loom-nebius-development >&2
exit 1
