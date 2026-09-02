#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --kubeconfig PATH --nebius-credentials PATH [--model-provider-api-key-file PATH] [--image-admission-keyring PATH] --service-execution-runtime-profile PATH" >&2
  exit 2
}

kubeconfig=
nebius_credentials=
model_provider_api_key_file=
image_admission_keyring=
service_execution_runtime_profile=
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
    --image-admission-keyring)
      (($# >= 2)) || usage
      image_admission_keyring=$2
      shift 2
      ;;
    --service-execution-runtime-profile)
      (($# >= 2)) || usage
      service_execution_runtime_profile=$2
      shift 2
      ;;
    *) usage ;;
  esac
done

[[ -n "$kubeconfig" && -f "$kubeconfig" ]] || usage
[[ -n "$nebius_credentials" && -f "$nebius_credentials" ]] || usage
[[ -n "$service_execution_runtime_profile" && -s "$service_execution_runtime_profile" ]] || usage
if [[ -n $model_provider_api_key_file && ! -s $model_provider_api_key_file ]]; then
  usage
fi
if [[ -n $image_admission_keyring && ! -s $image_admission_keyring ]]; then
  usage
fi

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

python3 - "$service_execution_runtime_profile" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
required = {
    "schema_version", "logical_pool_id", "candidate_sha", "execution_class_id",
    "task_image_ref", "runtime_image_ref", "runtime_binary_sha256", "image_admission",
}
if not isinstance(payload, dict) or not required <= payload.keys():
    raise SystemExit("service execution runtime profile is incomplete")
digest_image = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
sha256 = re.compile(r"^sha256:[0-9a-f]{64}$")
if (
    payload["schema_version"] != "loom.service-execution-runtime-profile.v1"
    or payload["logical_pool_id"] != "nebius-cpu"
    or payload["execution_class_id"] != "linux-amd64-cpu-pod-v1"
    or not re.fullmatch(r"[0-9a-f]{40}", str(payload["candidate_sha"]))
    or not digest_image.fullmatch(str(payload["task_image_ref"]))
    or not digest_image.fullmatch(str(payload["runtime_image_ref"]))
    or not sha256.fullmatch(str(payload["runtime_binary_sha256"]))
):
    raise SystemExit("service execution runtime profile schema is invalid")
admission = payload["image_admission"]
if not isinstance(admission, dict) or admission.get("schema_version") != "loom.execution-image-admission.v1":
    raise SystemExit("service execution runtime profile admission is invalid")
rows = admission.get("admissions")
if not isinstance(rows, list) or {
    row.get("statement", {}).get("image_ref")
    for row in rows
    if isinstance(row, dict) and isinstance(row.get("statement"), dict)
} != {payload["task_image_ref"], payload["runtime_image_ref"]}:
    raise SystemExit("service execution runtime profile image coverage is invalid")
PY

if [[ -n $image_admission_keyring ]]; then
  python3 - "$image_admission_keyring" <<'PY'
import base64
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
keys = payload.get("keys") if isinstance(payload, dict) else None
if payload.get("schema_version") != 1 or not isinstance(keys, list) or not keys:
    raise SystemExit("image admission keyring is invalid")
seen_ids = set()
seen_keys = set()
for row in keys:
    if not isinstance(row, dict) or set(row) != {"signing_key_id", "public_key_base64"}:
        raise SystemExit("image admission keyring entry is invalid")
    key_id = row["signing_key_id"]
    encoded = row["public_key_base64"]
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (TypeError, ValueError):
        raise SystemExit("image admission public key is invalid") from None
    if (
        not isinstance(key_id, str)
        or re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", key_id) is None
        or len(raw) != 32
        or key_id in seen_ids
        or raw in seen_keys
    ):
        raise SystemExit("image admission key binding is invalid")
    seen_ids.add(key_id)
    seen_keys.add(raw)
PY
fi

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
export KUBECONFIG=$kubeconfig

if ! credential_mode=$(stat -c '%a' "$nebius_credentials" 2>/dev/null); then
  credential_mode=$(stat -f '%Lp' "$nebius_credentials")
fi
if ((10#$credential_mode % 100 != 0)); then
  echo "Nebius credential file must not be group/world accessible" >&2
  exit 1
fi
if [[ -n $model_provider_api_key_file ]]; then
  if ! provider_key_mode=$(stat -c '%a' "$model_provider_api_key_file" 2>/dev/null); then
    provider_key_mode=$(stat -f '%Lp' "$model_provider_api_key_file")
  fi
  if ((10#$provider_key_mode % 100 != 0)); then
    echo "Model provider API key file must not be group/world accessible" >&2
    exit 1
  fi
fi
if [[ -n $image_admission_keyring ]]; then
  if ! keyring_mode=$(stat -c '%a' "$image_admission_keyring" 2>/dev/null); then
    keyring_mode=$(stat -f '%Lp' "$image_admission_keyring")
  fi
  if ((10#$keyring_mode % 100 != 0)); then
    echo "Image admission keyring must not be group/world accessible" >&2
    exit 1
  fi
fi

normalized_provider_key=
token_file=
# shellcheck disable=SC2329  # invoked by the EXIT trap below
cleanup() {
  [[ -z $normalized_provider_key ]] || rm -f "$normalized_provider_key"
  [[ -z "$token_file" ]] || rm -f "$token_file"
}
trap cleanup EXIT
if [[ -n $model_provider_api_key_file ]]; then
  normalized_provider_key=$(mktemp)
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
else
  kubectl get secret -n loom loom-nebius-model-provider >/dev/null
  provider_key_sha256=$(kubectl get secret -n loom loom-nebius-model-provider -o json | python3 -c '
import base64
import hashlib
import json
import sys

payload = json.load(sys.stdin)
encoded = payload.get("data", {}).get("api-key")
if not isinstance(encoded, str):
    raise SystemExit("existing model-provider Secret is missing api-key")
print(hashlib.sha256(base64.b64decode(encoded, validate=True)).hexdigest())
')
fi
runtime_profile_sha256=$(python3 - "$service_execution_runtime_profile" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

print(sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)
if [[ -n $image_admission_keyring ]]; then
  image_admission_keyring_sha256=$(python3 - "$image_admission_keyring" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

print(sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
  )
else
  kubectl get secret -n loom loom-image-admission >/dev/null
  image_admission_keyring_sha256=$(kubectl get secret -n loom loom-image-admission -o json | python3 -c '
import base64
import hashlib
import json
import sys

payload = json.load(sys.stdin)
encoded = payload.get("data", {}).get("keyring-json")
if not isinstance(encoded, str):
    raise SystemExit("existing image-admission Secret is missing keyring-json")
print(hashlib.sha256(base64.b64decode(encoded, validate=True)).hexdigest())
')
fi
profile_task_image=$(python3 - "$service_execution_runtime_profile" <<'PY'
import json
from pathlib import Path
import sys

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["task_image_ref"])
PY
)

kubectl get namespace loom >/dev/null
kubectl get secret -n loom loom-admin-secret >/dev/null
kubectl get secret -n loom loom-image-admission >/dev/null
kubectl get secret -n loom-nebius-development loom-execution-actuator-db >/dev/null
deployed_service_image=$(kubectl get deployment -n loom loom-service \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="service")].image}')
if [[ $deployed_service_image != "$profile_task_image" ]]; then
  echo "Runtime profile task image does not match the deployed Loom service image" >&2
  exit 1
fi

if [[ -n $normalized_provider_key ]]; then
  kubectl create secret generic loom-nebius-model-provider \
    -n loom \
    --from-file=api-key="$normalized_provider_key" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

if [[ -n $image_admission_keyring ]]; then
  kubectl create secret generic loom-image-admission \
    -n loom \
    --from-file=keyring-json="$image_admission_keyring" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

kubectl create secret generic loom-service-execution-runtime-profile \
  -n loom \
  --from-file=profile-json="$service_execution_runtime_profile" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl patch deployment -n loom loom-llm-gateway --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-gateway-development-patch.yaml" >/dev/null
kubectl patch deployment -n loom loom-llm-gateway --type=merge \
  --patch "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"loom.ca/model-provider-secret-sha256\":\"$provider_key_sha256\"}}}}}" \
  >/dev/null
kubectl rollout status -n loom deployment/loom-llm-gateway --timeout=180s

kubectl patch deployment -n loom loom-control-plane --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-control-plane-development-patch.yaml" >/dev/null
kubectl patch deployment -n loom loom-control-plane --type=merge \
  --patch "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"loom.ca/image-admission-keyring-sha256\":\"$image_admission_keyring_sha256\"}}}}}" \
  >/dev/null
kubectl rollout status -n loom deployment/loom-control-plane --timeout=180s
kubectl patch deployment -n loom loom-service --type=strategic \
  --patch-file "$repo_root/deploy/k8s/nebius-service-development-patch.yaml" >/dev/null
kubectl patch deployment -n loom loom-service --type=merge \
  --patch "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"loom.ca/service-execution-runtime-profile-sha256\":\"$runtime_profile_sha256\"}}}}}" \
  >/dev/null
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
