# Personal-development backup and isolated restore evidence

This procedure produces the owner-only backup/restore record required by the
personal-development acceptance and durable operational plans. Run it only
while the management plane is in exact shadow mode, lifecycle and builder are
disabled, activation replicas are zero, no `loom-dev-` or `loom-build-`
namespace exists, no personal worker exists, and the global manager executable
new-capacity ceiling is exactly zero.

The procedure is read-only against live Postgres and MinIO. It restores the
captured bytes into disposable local Docker containers with no published port,
compares exact logical state, then removes those containers and their private
network. It never copies a Kubernetes Secret value into evidence. The only
Secret-derived operations execute inside the existing Postgres and MinIO Pods;
their environment values are neither printed nor returned.

## 1. Bind exact inputs and an empty owner-only output root

```bash
set -euo pipefail
umask 077

repo="$(pwd -P)"
loom_cli="$repo/.venv/bin/loom"
python_cli="$repo/.venv/bin/python"
profile="$repo/deploy/dev-fleet/personal-dev-control-plane.toml"
trusted_release="<absolute-owner-only-trusted-release.json>"
trusted_release_sha256="<reviewed-trusted-release-sha256>"
kubeconfig="<absolute-reviewed-self-contained-mode-0600-kubeconfig>"
expected_kube_context="<reviewed-context>"
evidence_dir="<new-absolute-owner-only-issue-1571-evidence-directory>"

test -x "$loom_cli" && test -x "$python_cli"
test "$(git rev-parse --show-toplevel)" = "$repo"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test "$(git rev-parse HEAD)" = "$(jq -r .source_sha "$trusted_release")"
test "$(git rev-parse 'HEAD^{tree}')" = "$(jq -r .source_tree "$trusted_release")"
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = "$trusted_release_sha256"
test "$(stat -c %u "$kubeconfig")" = "$(id -u)"
test "$(stat -c %a "$kubeconfig")" = 600
test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
  "$expected_kube_context"
install -d -m 0700 "$evidence_dir"
test -z "$(find "$evidence_dir" -mindepth 1 -maxdepth 1 -print -quit)"

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
postgres_dump="$evidence_dir/postgres.dump"
postgres_source_state="$evidence_dir/postgres.source-state.tsv"
postgres_restored_state="$evidence_dir/postgres.restored-state.tsv"
minio_backup="$evidence_dir/minio"
minio_source_manifest="$evidence_dir/minio.source-manifest.json"
minio_restored_manifest="$evidence_dir/minio.restored-manifest.json"
secret_inventory="$evidence_dir/secret-key-inventory.json"
result="$evidence_dir/backup-restore-evidence.json"
install -d -m 0700 "$minio_backup" "$minio_backup/objects"
```

Do not continue if the output root was reused. Do not use another operator's
evidence directory or private files.

## 2. Prove the zero-write live boundary

```bash
"$loom_cli" admin personal-dev-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$evidence_dir/pre-backup.shadow-status.json"
chmod 0600 "$evidence_dir/pre-backup.shadow-status.json"
jq -e '
  .mode == "shadow" and .ready == true and .blockers == [] and
  .manager_ceiling == 0 and .worker_available == false and
  any(.components[]; .name == "personal-workers" and .observed == 0)
' "$evidence_dir/pre-backup.shadow-status.json" >/dev/null

kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
  > "$evidence_dir/pre-backup.namespaces.json"
chmod 0600 "$evidence_dir/pre-backup.namespaces.json"
jq -e '[.items[].metadata.name |
  select(startswith("loom-dev-") or startswith("loom-build-"))] |
  length == 0' "$evidence_dir/pre-backup.namespaces.json" >/dev/null

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  statefulset.apps/loom-dev-postgres statefulset.apps/loom-dev-minio \
  persistentvolumeclaim/data-loom-dev-postgres-0 \
  persistentvolumeclaim/data-loom-dev-minio-0 -o json \
  > "$evidence_dir/storage-inventory.json"
chmod 0600 "$evidence_dir/storage-inventory.json"
jq -e '
  ([.items[] | select(.kind == "PersistentVolumeClaim") |
    .spec.storageClassName] | sort) == ["longhorn","longhorn"]
' "$evidence_dir/storage-inventory.json" >/dev/null

postgres_image="$(jq -r .images.postgres "$trusted_release")"
minio_image="$(jq -r .images.minio "$trusted_release")"
minio_client_image="$(jq -r .images.minio_client "$trusted_release")"
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  statefulset.apps/loom-dev-postgres -o jsonpath='{.spec.template.spec.containers[?(@.name=="postgres")].image}')" = \
  "$postgres_image"
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  statefulset.apps/loom-dev-minio -o jsonpath='{.spec.template.spec.containers[?(@.name=="minio")].image}')" = \
  "$minio_image"
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  statefulset.apps/loom-dev-minio -o jsonpath='{.spec.template.spec.containers[?(@.name=="admin")].image}')" = \
  "$minio_client_image"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get secrets \
  loom-personal-dev-management loom-personal-dev-activation-public \
  loom-personal-dev-activation-agent -o json |
  jq -cS '{items:[.items[] | {name:.metadata.name,keys:(.data|keys|sort)}] |
    sort_by(.name)}' > "$secret_inventory"
chmod 0600 "$secret_inventory"
```

The Secret inventory contains names and key names only. It must never contain
`.data` values, decoded payloads, tokens, passwords, or private keys.

## 3. Dump and restore Postgres locally

The state fingerprint contains only the schema head and exact per-table row
counts. It deliberately excludes row values.

```bash
postgres_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-postgres -o json | jq -er '
    [.items[] | select(.status.phase == "Running") | .metadata.name] |
    if length == 1 then .[0] else error("postgres pod cardinality") end')"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
  -c postgres -- /bin/sh -euc \
  'exec pg_dump --format=custom --no-owner --no-acl --username "$POSTGRES_USER" "$POSTGRES_DB"' \
  > "$postgres_dump"
chmod 0600 "$postgres_dump"
test -s "$postgres_dump"

capture_live_postgres_state() {
  local destination="$1"
  local tables="$evidence_dir/postgres.tables.txt"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c \
      "SELECT format('"'"'%I.%I'"'"',schemaname,tablename) FROM pg_tables WHERE schemaname='"'"'public'"'"' ORDER BY 1"' \
    > "$tables"
  chmod 0600 "$tables"
  : > "$destination"
  while IFS= read -r table; do
    count="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
      "$postgres_pod" -c postgres -- /bin/sh -euc \
      'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT count(*) FROM $1"' \
      sh "$table")"
    printf '%s\t%s\n' "$table" "$count" >> "$destination"
  done < "$tables"
  chmod 0600 "$destination"
}
capture_live_postgres_state "$postgres_source_state"
source_schema_head="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
  "$postgres_pod" -c postgres -- /bin/sh -euc \
  'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT version_num FROM alembic_version"')"
test "$source_schema_head" = 0112

suffix="$(printf '%s' "$trusted_release_sha256" | cut -c1-12)"
postgres_restore="loom-personal-dev-pg-restore-$suffix"
restore_network="loom-personal-dev-restore-$suffix"
minio_restore="loom-personal-dev-minio-restore-$suffix"
restore_env="$evidence_dir/minio-restore.env"
cleanup_restore_resources() {
  docker rm --force "$postgres_restore" "$minio_restore" >/dev/null 2>&1 || true
  docker network rm "$restore_network" >/dev/null 2>&1 || true
  rm -f "$restore_env"
}
test -z "$(docker ps -a --format '{{.Names}}' |
  awk -v pg="$postgres_restore" -v minio="$minio_restore" '$0==pg || $0==minio')"
test -z "$(docker network ls --format '{{.Name}}' |
  awk -v name="$restore_network" '$0==name')"
trap cleanup_restore_resources EXIT
docker run --detach --network none --name "$postgres_restore" \
  --env POSTGRES_HOST_AUTH_METHOD=trust "$postgres_image" >/dev/null
for attempt in $(seq 1 60); do
  if docker exec "$postgres_restore" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    break
  fi
  test "$attempt" -lt 60
  sleep 1
done
docker exec -i "$postgres_restore" pg_restore -U postgres -d postgres \
  --clean --if-exists --no-owner --no-acl < "$postgres_dump"

docker exec "$postgres_restore" psql -AtX --set ON_ERROR_STOP=1 \
  -U postgres -d postgres -c \
  "SELECT format('%I.%I',schemaname,tablename) FROM pg_tables WHERE schemaname='public' ORDER BY 1" \
  > "$evidence_dir/postgres.restored-tables.txt"
: > "$postgres_restored_state"
while IFS= read -r table; do
  count="$(docker exec "$postgres_restore" psql -AtX --set ON_ERROR_STOP=1 \
    -U postgres -d postgres -c "SELECT count(*) FROM $table")"
  printf '%s\t%s\n' "$table" "$count" >> "$postgres_restored_state"
done < "$evidence_dir/postgres.restored-tables.txt"
chmod 0600 "$evidence_dir/postgres.restored-tables.txt" "$postgres_restored_state"
restored_schema_head="$(docker exec "$postgres_restore" psql -AtX \
  --set ON_ERROR_STOP=1 -U postgres -d postgres -c \
  'SELECT version_num FROM alembic_version')"
test "$restored_schema_head" = "$source_schema_head"
cmp -s "$postgres_source_state" "$postgres_restored_state"
```

## 4. Copy every MinIO object and restore it through the S3 API

The backup uses `mc ls` and `mc cat` inside the existing admin sidecar. Object
keys travel as separate argv values and payloads are stored under SHA-256 file
names, so a key cannot escape the owner-only directory. Empty buckets are
preserved separately.

```bash
minio_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-minio -o json | jq -er '
    [.items[] | select(.status.phase == "Running") | .metadata.name] |
    if length == 1 then .[0] else error("minio pod cardinality") end')"

mc_live() {
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$minio_pod" \
    -c admin -- /bin/sh -euc \
    'export MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"; exec mc "$@"' \
    sh "$@"
}

mc_live ls --json local | jq -r 'select(.type == "folder") | .key | rtrimstr("/")' |
  LC_ALL=C sort -u > "$minio_backup/buckets.txt"
chmod 0600 "$minio_backup/buckets.txt"
: > "$minio_backup/objects.jsonl"
while IFS= read -r bucket; do
  test -n "$bucket"
  mc_live ls --recursive --json "local/$bucket" > "$minio_backup/$bucket.list.jsonl"
  chmod 0600 "$minio_backup/$bucket.list.jsonl"
  while IFS= read -r object; do
    key="$(jq -er '.key' <<<"$object")"
    size="$(jq -er '.size' <<<"$object")"
    identity="$(printf '%s\0%s' "$bucket" "$key" | sha256sum | awk '{print $1}')"
    payload="$minio_backup/objects/$identity"
    mc_live cat "local/$bucket/$key" > "$payload"
    chmod 0600 "$payload"
    test "$(stat -c %s "$payload")" = "$size"
    payload_sha256="$(sha256sum "$payload" | awk '{print $1}')"
    jq -cnS --arg bucket "$bucket" --arg key "$key" \
      --arg sha256 "$payload_sha256" --argjson size "$size" \
      '{bucket:$bucket,key:$key,sha256:$sha256,size:$size}' \
      >> "$minio_backup/objects.jsonl"
  done < "$minio_backup/$bucket.list.jsonl"
done < "$minio_backup/buckets.txt"
chmod 0600 "$minio_backup/objects.jsonl"

"$python_cli" - "$minio_backup/buckets.txt" "$minio_backup/objects.jsonl" \
  "$minio_source_manifest" <<'PY'
import json
import sys
from pathlib import Path

buckets = sorted(filter(None, Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()))
objects = [json.loads(line) for line in Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()]
objects.sort(key=lambda item: (item["bucket"], item["key"]))
payload = json.dumps({"buckets": buckets, "objects": objects}, sort_keys=True,
                     separators=(",", ":"), ensure_ascii=True).encode("ascii")
Path(sys.argv[3]).write_bytes(payload)
PY
chmod 0600 "$minio_source_manifest"

printf 'MINIO_ROOT_USER=restore\nMINIO_ROOT_PASSWORD=%s\n' \
  "$(openssl rand -hex 24)" > "$restore_env"
chmod 0600 "$restore_env"
docker network create --internal "$restore_network" >/dev/null
docker run --detach --network "$restore_network" --network-alias minio-restore \
  --name "$minio_restore" \
  --env-file "$restore_env" "$minio_image" server /data >/dev/null

mc_restore() {
  docker run --rm --network "$restore_network" --env-file "$restore_env" \
    --entrypoint /bin/sh "$minio_client_image" -euc \
    'export MC_HOST_restore="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio-restore:9000"; exec mc "$@"' \
    sh "$@"
}
for attempt in $(seq 1 60); do
  if mc_restore ready restore >/dev/null 2>&1; then break; fi
  test "$attempt" -lt 60
  sleep 1
done
while IFS= read -r bucket; do
  mc_restore mb --ignore-existing "restore/$bucket" >/dev/null
done < "$minio_backup/buckets.txt"
while IFS= read -r object; do
  bucket="$(jq -er .bucket <<<"$object")"
  key="$(jq -er .key <<<"$object")"
  identity="$(printf '%s\0%s' "$bucket" "$key" | sha256sum | awk '{print $1}')"
  mc_restore pipe "restore/$bucket/$key" < "$minio_backup/objects/$identity" >/dev/null
done < "$minio_backup/objects.jsonl"

mc_restore ls --json restore | jq -r \
  'select(.type == "folder") | .key | rtrimstr("/")' |
  LC_ALL=C sort -u > "$evidence_dir/minio.restored-buckets.txt"
chmod 0600 "$evidence_dir/minio.restored-buckets.txt"
: > "$evidence_dir/minio.restored-objects.jsonl"
while IFS= read -r bucket; do
  test -n "$bucket"
  mc_restore ls --recursive --json "restore/$bucket" \
    > "$evidence_dir/minio.restored-$bucket.list.jsonl"
  chmod 0600 "$evidence_dir/minio.restored-$bucket.list.jsonl"
  while IFS= read -r object; do
    key="$(jq -er '.key' <<<"$object")"
    size="$(jq -er '.size' <<<"$object")"
    payload_sha256="$(mc_restore cat "restore/$bucket/$key" | sha256sum |
      awk '{print $1}')"
    jq -cnS --arg bucket "$bucket" --arg key "$key" \
      --arg sha256 "$payload_sha256" --argjson size "$size" \
      '{bucket:$bucket,key:$key,sha256:$sha256,size:$size}' \
      >> "$evidence_dir/minio.restored-objects.jsonl"
  done < "$evidence_dir/minio.restored-$bucket.list.jsonl"
done < "$evidence_dir/minio.restored-buckets.txt"
chmod 0600 "$evidence_dir/minio.restored-objects.jsonl"

"$python_cli" - "$evidence_dir/minio.restored-buckets.txt" \
  "$evidence_dir/minio.restored-objects.jsonl" \
  "$minio_restored_manifest" <<'PY'
import json
import sys
from pathlib import Path

buckets = sorted(filter(None, Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()))
objects = [json.loads(line) for line in Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()]
objects.sort(key=lambda item: (item["bucket"], item["key"]))
payload = json.dumps({"buckets": buckets, "objects": objects}, sort_keys=True,
                     separators=(",", ":"), ensure_ascii=True).encode("ascii")
Path(sys.argv[3]).write_bytes(payload)
PY
chmod 0600 "$minio_restored_manifest"
cmp -s "$minio_source_manifest" "$minio_restored_manifest"
```

The restored-manifest producer re-lists every bucket and object and re-reads
every payload through the isolated S3 API. MinIO readiness, a local file count,
or a manually edited manifest is not accepted as restore evidence.

## 5. Cleanup and emit the canonical record

```bash
cleanup_restore_resources
trap - EXIT
test ! -e "$restore_env"
test -z "$(docker ps -a --format '{{.Names}}' |
  awk -v pg="$postgres_restore" -v minio="$minio_restore" '$0==pg || $0==minio')"
test -z "$(docker network ls --format '{{.Name}}' | awk -v name="$restore_network" '$0==name')"

"$loom_cli" admin personal-dev-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$evidence_dir/post-restore.shadow-status.json"
chmod 0600 "$evidence_dir/post-restore.shadow-status.json"
jq -e '.mode == "shadow" and .ready == true and .manager_ceiling == 0 and
  .worker_available == false' "$evidence_dir/post-restore.shadow-status.json" >/dev/null

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
result_tmp="$(mktemp "$evidence_dir/backup-restore.XXXXXX.json")"
result_render_evidence="$evidence_dir/backup-restore-evidence.render.json"
"$loom_cli" admin personal-dev-control-plane render-backup-restore-evidence \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --started-at "$started_at" \
  --completed-at "$completed_at" \
  --postgres-dump-file "$postgres_dump" \
  --postgres-source-state-file "$postgres_source_state" \
  --postgres-restored-state-file "$postgres_restored_state" \
  --source-schema-head "$source_schema_head" \
  --restored-schema-head "$restored_schema_head" \
  --minio-source-manifest-file "$minio_source_manifest" \
  --minio-restored-manifest-file "$minio_restored_manifest" \
  --secret-key-inventory-file "$secret_inventory" \
  --pre-shadow-status-file "$evidence_dir/pre-backup.shadow-status.json" \
  --post-shadow-status-file "$evidence_dir/post-restore.shadow-status.json" \
  --storage-inventory-file "$evidence_dir/storage-inventory.json" \
  --isolated-postgres-name "$postgres_restore" \
  --isolated-minio-name "$minio_restore" \
  --isolated-network-name "$restore_network" \
  > "$result_tmp" 2> "$result_render_evidence"
chmod 0600 "$result_tmp" "$result_render_evidence"
mv "$result_tmp" "$result"
backup_restore_evidence_sha256="$(sha256sum "$result" | awk '{print $1}')"
printf '%s\n' "$backup_restore_evidence_sha256"
```

Bind the printed digest into `storage.backup_restore_evidence_sha256`. The
acceptance and operational renderer/status commands must receive this exact
file through `--backup-restore-evidence-file`; they reject a noncanonical file,
source/release/image/schema mismatch, unequal source/restored state, included
Secret values, nonzero capacity/worker observations, or incomplete cleanup.

Retain the dump, every MinIO payload, both source/restored manifests, the
Secret-key inventory, pre/post shadow status, storage inventory, and the
canonical result under the owner-only evidence root. They are the restorable
backup and its proof, not temporary QA output.
