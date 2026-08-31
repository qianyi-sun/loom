#!/usr/bin/env bash
# Publish the narrow staging credential consumed by an external Slurm controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTHORITY_MANIFEST="$SCRIPT_DIR/../k8s/external-slurm-autoscaler-authority.yaml"
MANAGER_BINDING_MANIFEST="$SCRIPT_DIR/../k8s/external-slurm-autoscaler-manager-export-binding.yaml"
KUBECTL="${KUBECTL:-/usr/local/bin/kubectl}"
PYTHON="${PYTHON:-/usr/bin/python3}"
NAMESPACE="loom-staging"
MANAGER_NAMESPACE="loom-dev"
MANAGER_POLICY="loom-external-slurm-autoscaler-manager-export"
MANAGER_DEPLOYMENT="loom-capacity-manager"
MANAGER_ROLE="loom-external-slurm-autoscaler-manager-export"
MANAGER_PRINCIPAL="system:serviceaccount:loom-staging:loom-external-slurm-autoscaler"
MANAGER_POD_SELECTOR="app.kubernetes.io/name=loom-capacity-manager,app.kubernetes.io/part-of=loom,loom.yylx.dev/capacity-component=manager"
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
for manifest in "$AUTHORITY_MANIFEST" "$MANAGER_BINDING_MANIFEST"; do
  if [ ! -f "$manifest" ] || [ -L "$manifest" ]; then
    echo "error: external autoscaler authority manifest is unavailable" >&2
    exit 1
  fi
done
if [ -L "$output_path" ]; then
  echo "error: output kubeconfig must not be a symlink" >&2
  exit 1
fi

umask 077
temporary_dir="$(mktemp -d)"
trap 'rm -rf -- "$temporary_dir"' EXIT

manager_pod_identity() {
  local deployment_uid pod_output pod_ref pod_name pod_state pod_uid
  local pod_owner owner_kind replica_set expected_replica_set owner_uid owner_controller
  local replica_set_owner replica_set_uid deployment_kind deployment_name
  local replica_set_deployment_uid deployment_controller

  if ! deployment_uid="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" get \
      "deployment/$MANAGER_DEPLOYMENT" -o 'jsonpath={.metadata.uid}'
  )" || [ -z "$deployment_uid" ]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  if ! pod_output="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" get pods \
      --selector "$MANAGER_POD_SELECTOR" --field-selector status.phase=Running \
      -o name
  )"; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  mapfile -t manager_pods <<<"$pod_output"
  if [ "${#manager_pods[@]}" -ne 1 ] || [ -z "${manager_pods[0]}" ]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  pod_ref="${manager_pods[0]}"
  pod_name="${pod_ref#pod/}"
  if [ "$pod_ref" != "pod/$pod_name" ] \
    || [[ ! "$pod_name" =~ ^loom-capacity-manager-[a-z0-9]{1,10}-[a-z0-9]{5}$ ]]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  if ! "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" wait \
    --for=condition=Ready "$pod_ref" --timeout=30s >/dev/null; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  if ! pod_state="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" get "$pod_ref" \
      -o 'jsonpath={.metadata.deletionTimestamp}{"|"}{.metadata.uid}'
  )" || [[ ! "$pod_state" =~ ^\|([a-zA-Z0-9-]+)$ ]]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  pod_uid="${BASH_REMATCH[1]}"
  if ! pod_owner="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" get "$pod_ref" \
      -o 'jsonpath={.metadata.ownerReferences[0].kind}{"|"}{.metadata.ownerReferences[0].name}{"|"}{.metadata.ownerReferences[0].uid}{"|"}{.metadata.ownerReferences[0].controller}'
  )"; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  IFS='|' read -r owner_kind replica_set owner_uid owner_controller <<<"$pod_owner"
  expected_replica_set="${pod_name%-?????}"
  if [ "$owner_kind" != "ReplicaSet" ] || [ -z "$replica_set" ] \
    || [[ ! "$replica_set" =~ ^loom-capacity-manager-[a-z0-9]{1,10}$ ]] \
    || [ "$replica_set" != "$expected_replica_set" ] \
    || [ -z "$owner_uid" ] || [ "$owner_controller" != "true" ]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  if ! replica_set_owner="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" -n "$MANAGER_NAMESPACE" get \
      "replicaset.apps/$replica_set" \
      -o 'jsonpath={.metadata.uid}{"|"}{.metadata.ownerReferences[0].kind}{"|"}{.metadata.ownerReferences[0].name}{"|"}{.metadata.ownerReferences[0].uid}{"|"}{.metadata.ownerReferences[0].controller}'
  )"; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  IFS='|' read -r replica_set_uid deployment_kind deployment_name \
    replica_set_deployment_uid deployment_controller <<<"$replica_set_owner"
  if [ "$replica_set_uid" != "$owner_uid" ] || [ "$deployment_kind" != "Deployment" ] \
    || [ "$deployment_name" != "$MANAGER_DEPLOYMENT" ] \
    || [ "$replica_set_deployment_uid" != "$deployment_uid" ] \
    || [ "$deployment_controller" != "true" ]; then
    echo "error: manager-export pod ownership is invalid" >&2
    return 1
  fi
  printf '%s|%s\n' "$pod_name" "$pod_uid"
}

manager_identity="$(manager_pod_identity)"
IFS='|' read -r manager_pod_name manager_pod_uid <<<"$manager_identity"

manager_exec_rule_state() {
  local manager_rules manager_rule_state
  if manager_rules="$(
    "$KUBECTL" --kubeconfig "$KUBECONFIG" create \
      --raw /apis/authorization.k8s.io/v1/selfsubjectrulesreviews \
      -f - --as "$MANAGER_PRINCIPAL" <<EOF
{"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectRulesReview","spec":{"namespace":"$MANAGER_NAMESPACE"}}
EOF
  )"; then
    if manager_rule_state="$(
      "$PYTHON" -c '
import json
import sys


def string_list(value):
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


try:
    document = json.load(sys.stdin)
    status = document.get("status")
    if not isinstance(status, dict) or status.get("incomplete") is not False:
        raise ValueError
    evaluation_error = status.get("evaluationError", "")
    if not isinstance(evaluation_error, str) or evaluation_error:
        raise ValueError
    rules = status.get("resourceRules")
    if not isinstance(rules, list):
        raise ValueError
    authority_present = False
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError
        groups = rule.get("apiGroups")
        resources = rule.get("resources")
        verbs = rule.get("verbs")
        if not string_list(groups) or not string_list(resources) or not string_list(verbs):
            raise ValueError
        if (
            any(group in {"", "*"} for group in groups)
            and any(resource in {"*", "*/exec", "pods/exec"} for resource in resources)
            and any(verb in {"create", "*"} for verb in verbs)
        ):
            authority_present = True
    print("present" if authority_present else "revoked")
except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(2) from None
' <<<"$manager_rules"
    )"; then
      printf '%s\n' "$manager_rule_state"
      return 0
    fi
  fi
  return 1
}

wait_for_manager_exec_revocation() {
  local manager_rule_state
  for _attempt in $(seq 1 30); do
    if manager_rule_state="$(manager_exec_rule_state)" \
      && [ "$manager_rule_state" = "revoked" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Revoke the permanent exec grant before changing its admission guard. A
# publisher retry may briefly stop witness export, but it can never create an
# unguarded exec window.
"$KUBECTL" --kubeconfig "$KUBECONFIG" delete \
  --ignore-not-found --wait=true -f "$MANAGER_BINDING_MANIFEST" >/dev/null
if ! wait_for_manager_exec_revocation; then
  echo "error: manager-export exec authority was not revoked" >&2
  exit 1
fi
"$KUBECTL" --kubeconfig "$KUBECONFIG" apply -f "$AUTHORITY_MANIFEST" >/dev/null

policy_generation="$(
  "$KUBECTL" --kubeconfig "$KUBECONFIG" get validatingadmissionpolicy \
    "$MANAGER_POLICY" -o 'jsonpath={.metadata.generation}'
)"
if [[ ! "$policy_generation" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: manager-export admission policy generation is invalid" >&2
  exit 1
fi
policy_ready=false
for _attempt in $(seq 1 30); do
  policy_observation="$(
      "$KUBECTL" --kubeconfig "$KUBECONFIG" get validatingadmissionpolicy \
      "$MANAGER_POLICY" \
      -o 'jsonpath={.status.observedGeneration}{"|"}{range .status.typeChecking.expressionWarnings[*]}{.warning}{"\n"}{end}'
  )"
  policy_observed="${policy_observation%%|*}"
  policy_warnings="${policy_observation#*|}"
  if [ -n "$policy_warnings" ]; then
    echo "error: manager-export admission policy has type-checking warnings" >&2
    exit 1
  fi
  if [ "$policy_observed" = "$policy_generation" ]; then
    policy_ready=true
    break
  fi
  sleep 1
done
if [ "$policy_ready" != true ]; then
  echo "error: manager-export admission policy was not observed" >&2
  exit 1
fi

# Replace the fail-closed sentinel Role with the one Ready Pod whose complete
# controller chain was verified above. Re-running this publisher is the only
# supported way to refresh authority after a Deployment rollout.
"$KUBECTL" --kubeconfig "$KUBECONFIG" apply -f - >/dev/null <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: $MANAGER_ROLE
  namespace: $MANAGER_NAMESPACE
  ownerReferences:
  - apiVersion: v1
    kind: Pod
    name: $manager_pod_name
    uid: $manager_pod_uid
    controller: false
    blockOwnerDeletion: false
rules:
  - apiGroups: ["apps"]
    resources: ["deployments"]
    resourceNames: ["$MANAGER_DEPLOYMENT"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    resourceNames: [$manager_pod_name]
    verbs: ["create"]
EOF

current_manager_identity="$(manager_pod_identity)"
if [ "$current_manager_identity" != "$manager_pod_name|$manager_pod_uid" ]; then
  echo "error: manager-export pod identity changed during publication" >&2
  exit 1
fi
"$KUBECTL" --kubeconfig "$KUBECONFIG" apply -f - >/dev/null <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: $MANAGER_ROLE
  namespace: $MANAGER_NAMESPACE
  ownerReferences:
  - apiVersion: v1
    kind: Pod
    name: $manager_pod_name
    uid: $manager_pod_uid
    controller: false
    blockOwnerDeletion: false
subjects:
  - kind: ServiceAccount
    name: loom-external-slurm-autoscaler
    namespace: loom-staging
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: $MANAGER_ROLE
EOF

post_binding_manager_identity="$(manager_pod_identity || true)"
if [ "$post_binding_manager_identity" != "$manager_pod_name|$manager_pod_uid" ]; then
  "$KUBECTL" --kubeconfig "$KUBECONFIG" delete \
    --ignore-not-found --wait=true -f "$MANAGER_BINDING_MANIFEST" >/dev/null
  "$KUBECTL" --kubeconfig "$KUBECONFIG" apply -f "$AUTHORITY_MANIFEST" >/dev/null
  if ! wait_for_manager_exec_revocation; then
    echo "error: manager-export post-binding authority could not be revoked" >&2
    exit 1
  fi
  echo "error: manager-export pod identity changed after binding" >&2
  exit 1
fi

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

# The same narrow credential exports the signed global-execution witness that
# fences both normal and builder scale-up. Refuse to publish a partial
# credential: it would authenticate successfully but fail every supervisor
# reconciliation on the controller.
assert_manager_export_permission() {
  local permission="$*"
  if ! "$KUBECTL" --kubeconfig "$temporary_dir/kubeconfig" \
    auth can-i "$@" --namespace "$MANAGER_NAMESPACE" >/dev/null
  then
    echo "error: external autoscaler lacks required manager-export authority: $permission" >&2
    exit 1
  fi
}
assert_manager_export_permission get deployment/loom-capacity-manager
assert_manager_export_permission list pods
assert_manager_export_permission create "pod/$manager_pod_name" --subresource=exec
install -m 0600 "$temporary_dir/kubeconfig" "$output_path"
printf 'published namespace-scoped external Slurm autoscaler kubeconfig: %s\n' \
  "$output_path"
