# Personal-development incompatible-schema transition

This procedure moves the inert management plane in loom-dev from an old
Alembic head to a newer head when the old service cannot start on the new
schema. It first proves forward migration and full predecessor restore in
private Docker containers. It then quiesces the only enabled database writer,
applies only the exact reviewed migration Job, and either activates the new
inert shadow or restores the exact old database before restarting the old
image.

The executable-capacity ceiling remains zero. Never enable the builder,
activation agent, personal lifecycle mutations, executors, or workers; never
submit a task; and never operate Slurm. Secret values remain inside their
existing Pods and are neither printed nor copied to evidence.

Run every Bash block in the same shell.

The reviewed Loom and Python launchers are toolchain executables and may live
outside either clean checkout. They do not select application source: every
host-side Python process receives the intended checkout through `PYTHONPATH`,
ignores repository bytecode caches, and verifies its loaded module paths.

## 1. Bind and render exact inputs

```bash
set -euo pipefail
umask 077
repo="$(pwd -P)"
loom_cli="<absolute-reviewed-target-compatible-loom-cli>"
python_cli="<absolute-reviewed-python-cli>"
profile="<absolute-reviewed-owner-only-operational-profile.toml>"
profile_sha256="<reviewed-operational-profile-sha256>"
trusted_release="<absolute-target-trusted-release.json>"
trusted_release_sha256="<target-trusted-release-sha256>"
predecessor_repo="<absolute-clean-predecessor-checkout>"
predecessor_loom_cli="<absolute-reviewed-predecessor-compatible-loom-cli>"
predecessor_profile="$predecessor_repo/deploy/dev-fleet/personal-dev-control-plane.toml"
predecessor_release="<absolute-predecessor-trusted-release.json>"
predecessor_release_sha256="<predecessor-trusted-release-sha256>"
predecessor_shadow="<absolute-predecessor-shadow.yaml>"
predecessor_shadow_sha256="<predecessor-shadow-sha256>"
backup_root="<absolute-predecessor-backup-directory>"
backup_evidence="$backup_root/backup-restore-evidence.json"
backup_evidence_sha256="<backup-restore-evidence-sha256>"
postgres_dump="$backup_root/postgres.dump"
postgres_source_state="$backup_root/postgres.source-state.tsv"
kubeconfig="<absolute-reviewed-mode-0600-kubeconfig>"
kubeconfig_sha256="<reviewed-kubeconfig-sha256>"
expected_kube_context="<reviewed-context>"
expected_predecessor_schema_head=0112
expected_target_schema_head=0122
evidence_dir="<new-absolute-owner-only-transition-evidence-directory-outside-repositories>"

test -x "$loom_cli"
test -x "$python_cli"
test -x "$predecessor_loom_cli"
for reviewed_cli in "$loom_cli" "$python_cli" "$predecessor_loom_cli"; do
  case "$reviewed_cli" in /*) ;; *) false ;; esac
  test "$(/usr/bin/realpath -e -- "$reviewed_cli")" = "$reviewed_cli"
done
for reviewed in "$profile" "$trusted_release" "$predecessor_release" \
  "$predecessor_shadow" "$backup_evidence" "$postgres_dump" \
  "$postgres_source_state" "$kubeconfig"; do
  test -f "$reviewed" && test ! -L "$reviewed"
  test "$(realpath -e "$reviewed")" = "$reviewed"
  test "$(stat -c %u "$reviewed")" = "$(id -u)"
  test "$(stat -c %a "$reviewed")" = 600
  test "$(stat -c %h "$reviewed")" = 1
  test "$(stat -c %s "$reviewed")" -gt 0
done
test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test "$(sha256sum "$predecessor_release" | awk '{print $1}')" = \
  "$predecessor_release_sha256"
test "$(sha256sum "$predecessor_shadow" | awk '{print $1}')" = \
  "$predecessor_shadow_sha256"
test "$(sha256sum "$backup_evidence" | awk '{print $1}')" = \
  "$backup_evidence_sha256"
test "$(stat -c %s "$kubeconfig")" -le 1048576
test "$(sha256sum "$kubeconfig" | awk '{print $1}')" = "$kubeconfig_sha256"
test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
  "$expected_kube_context"
prepare_transition_evidence_dir() {
  local directory="$1"
  local contents
  case "$directory" in /*) ;; *) return 1 ;; esac
  case "$directory/" in
    "$repo/"*|"$predecessor_repo/"*) return 1 ;;
  esac
  test ! -e "$directory" && test ! -L "$directory" || return
  install -d -m 0700 "$directory" || return
  test "$(realpath -e "$directory")" = "$directory" || return
  test "$(stat -c %u "$directory")" = "$(id -u)" || return
  test "$(stat -c %a "$directory")" = 700 || return
  contents="$(find "$directory" -mindepth 1 -maxdepth 1 -print -quit)" || return
  test -z "$contents" || return
}
assert_owner_only_sha256() {
  local path="$1"
  local expected_sha256="$2"
  local maximum_bytes="$3"
  local before
  local observed_sha256
  test "${#expected_sha256}" -eq 64 || return 1
  case "$expected_sha256" in *[!0-9a-f]*) return 1 ;; esac
  test -f "$path" && test ! -L "$path" || return 1
  test "$(realpath -e -- "$path")" = "$path" || return 1
  test "$(stat -c %u -- "$path")" = "$(id -u)" || return 1
  test "$(stat -c %a -- "$path")" = 600 || return 1
  test "$(stat -c %h -- "$path")" = 1 || return 1
  test "$(stat -c %s -- "$path")" -gt 0 || return 1
  test "$(stat -c %s -- "$path")" -le "$maximum_bytes" || return 1
  before="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")" || return 1
  observed_sha256="$(sha256sum -- "$path" | awk '{print $1}')" || return 1
  test -f "$path" && test ! -L "$path" || return 1
  test "$(realpath -e -- "$path")" = "$path" || return 1
  test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")" = \
    "$before" || return 1
  test "$observed_sha256" = "$expected_sha256" || return 1
}
assert_pinned_owner_only_sha256() {
  local descriptor="$1"
  local expected_sha256="$2"
  local maximum_bytes="$3"
  local descriptor_path="/proc/self/fd/$descriptor"
  local before
  local observed_sha256
  test "${#expected_sha256}" -eq 64 || return 1
  case "$expected_sha256" in *[!0-9a-f]*) return 1 ;; esac
  test -r "$descriptor_path" || return 1
  test "$(stat -Lc %u -- "$descriptor_path")" = "$(id -u)" || return 1
  test "$(stat -Lc %a -- "$descriptor_path")" = 600 || return 1
  test "$(stat -Lc %h -- "$descriptor_path")" = 1 || return 1
  test "$(stat -Lc %s -- "$descriptor_path")" -gt 0 || return 1
  test "$(stat -Lc %s -- "$descriptor_path")" -le "$maximum_bytes" || return 1
  before="$(stat -Lc '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- \
    "$descriptor_path")" || return 1
  observed_sha256="$(sha256sum -- "$descriptor_path" | awk '{print $1}')" || return 1
  test "$(stat -Lc '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- \
    "$descriptor_path")" = "$before" || return 1
  test "$observed_sha256" = "$expected_sha256" || return 1
}
assert_open_owner_only_sha256() {
  local path="$1"
  local descriptor="$2"
  local expected_sha256="$3"
  local maximum_bytes="$4"
  local descriptor_path="/proc/self/fd/$descriptor"
  assert_owner_only_sha256 "$path" "$expected_sha256" "$maximum_bytes" || return 1
  test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")" = \
    "$(stat -Lc '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$descriptor_path")" || return 1
  assert_pinned_owner_only_sha256 \
    "$descriptor" "$expected_sha256" "$maximum_bytes" || return 1
}
prepare_transition_evidence_dir "$evidence_dir"

transition_job="$evidence_dir/reviewed-migration-job.json"
transition_plan="$evidence_dir/schema-transition-plan.json"
job_tmp="$(mktemp "$evidence_dir/migration-job.XXXXXX.json")"
plan_tmp="$(mktemp "$evidence_dir/transition-plan.XXXXXX.json")"
PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/dev/null \
  PYTHONPATH="$repo/src" "$loom_cli" admin personal-dev-control-plane \
  render-schema-transition \
  --file "$profile" \
  --profile-sha256 "$profile_sha256" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --source-root "$repo" \
  --predecessor-trusted-release-file "$predecessor_release" \
  --predecessor-trusted-release-sha256 "$predecessor_release_sha256" \
  --backup-restore-evidence-file "$backup_evidence" \
  --backup-restore-evidence-sha256 "$backup_evidence_sha256" \
  --postgres-dump-file "$postgres_dump" \
  --postgres-source-state-file "$postgres_source_state" \
  --predecessor-shadow-manifest-file "$predecessor_shadow" \
  --predecessor-shadow-manifest-sha256 "$predecessor_shadow_sha256" \
  --alembic-config-file "$repo/migrations/alembic.ini" \
  --expected-predecessor-schema-head "$expected_predecessor_schema_head" \
  --expected-target-schema-head "$expected_target_schema_head" \
  > "$job_tmp" 2> "$plan_tmp"
chmod 0600 "$job_tmp" "$plan_tmp"
jq -e '.kind == "Job" and .metadata.namespace == "loom-dev" and
  .metadata.labels.app == "loom-personal-dev-migration" and
  .spec.template.spec.containers[0].command[0] == "/bin/sh" and
  .spec.template.spec.containers[0].command[1] == "-euc"' "$job_tmp" >/dev/null
jq -e --arg predecessor_head "$expected_predecessor_schema_head" \
  --arg target_head "$expected_target_schema_head" \
  '.schema == "loom-personal-dev-schema-transition-plan-v1" and
  .namespace == "loom-dev" and
  .predecessor.schema_head == $predecessor_head and
  .target.schema_head == $target_head and
  .predecessor.migration_job_name != .migration.job_name and
  .capacity.executable_new_capacity_ceiling == 0 and
  .rollback.method == "full-predecessor-database-restore" and
  .rollback.requires_exact_state_match == true and
  (.rollback.delete_after_predecessor_apply == [] or
   .rollback.delete_after_predecessor_apply == [
     "deployment.apps/loom-personal-dev-web",
     "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress",
     "service/loom-personal-dev-web"
   ])' "$plan_tmp" >/dev/null
test "$(sha256sum "$job_tmp" | awk '{print $1}')" = \
  "$(jq -r .migration.job_sha256 "$plan_tmp")"
mv "$job_tmp" "$transition_job"
mv "$plan_tmp" "$transition_plan"
plan_sha256="$(sha256sum "$transition_plan" | awk '{print $1}')"
predecessor_head="$(jq -r .predecessor.schema_head "$transition_plan")"
target_head="$(jq -r .target.schema_head "$transition_plan")"
migration_job_name="$(jq -r .migration.job_name "$transition_plan")"
predecessor_migration_job_name="$(jq -er \
  .predecessor.migration_job_name "$transition_plan")"
migration_image="$(jq -r .migration.service_image "$transition_plan")"
target_source_commit="$(jq -er .target.source_commit "$transition_plan")"
target_source_tree="$(jq -er .target.source_tree "$transition_plan")"
predecessor_source_commit="$(jq -er .predecessor.source_commit "$transition_plan")"
predecessor_source_tree="$(jq -er .predecessor.source_tree "$transition_plan")"
migration_job_sha256="$(jq -er .migration.job_sha256 "$transition_plan")"
target_shadow_sha256="$(jq -er .target.shadow_manifest_sha256 "$transition_plan")"
postgres_dump_sha256="$(jq -er .backup.postgres_dump_sha256 "$transition_plan")"
postgres_state_sha256="$(jq -er .backup.postgres_state_sha256 "$transition_plan")"
test "$predecessor_head" = "$expected_predecessor_schema_head"
test "$target_head" = "$expected_target_schema_head"
test -n "$predecessor_migration_job_name"
test "$predecessor_migration_job_name" != "$migration_job_name"

trusted_release_source="$trusted_release"
predecessor_release_source="$predecessor_release"
predecessor_shadow_source="$predecessor_shadow"
backup_evidence_source="$backup_evidence"
postgres_dump_source="$postgres_dump"
postgres_source_state_source="$postgres_source_state"
kubeconfig_source="$kubeconfig"
transition_job_source="$transition_job"
transition_plan_source="$transition_plan"
exec 30< "$trusted_release_source"
exec 31< "$predecessor_release_source"
exec 32< "$predecessor_shadow_source"
exec 33< "$backup_evidence_source"
exec 34< "$postgres_dump_source"
exec 35< "$postgres_source_state_source"
exec 36< "$kubeconfig_source"
exec 37< "$transition_job_source"
exec 38< "$transition_plan_source"
assert_open_owner_only_sha256 \
  "$trusted_release_source" 30 "$trusted_release_sha256" 16777216
assert_open_owner_only_sha256 \
  "$predecessor_release_source" 31 "$predecessor_release_sha256" 16777216
assert_open_owner_only_sha256 \
  "$predecessor_shadow_source" 32 "$predecessor_shadow_sha256" 16777216
assert_open_owner_only_sha256 \
  "$backup_evidence_source" 33 "$backup_evidence_sha256" 16777216
assert_open_owner_only_sha256 \
  "$postgres_dump_source" 34 "$postgres_dump_sha256" 4294967296
assert_open_owner_only_sha256 \
  "$postgres_source_state_source" 35 "$postgres_state_sha256" 4294967296
assert_open_owner_only_sha256 \
  "$kubeconfig_source" 36 "$kubeconfig_sha256" 1048576
assert_open_owner_only_sha256 \
  "$transition_job_source" 37 "$migration_job_sha256" 16777216
assert_open_owner_only_sha256 \
  "$transition_plan_source" 38 "$plan_sha256" 16777216
trusted_release=/proc/self/fd/30
predecessor_release=/proc/self/fd/31
predecessor_shadow=/proc/self/fd/32
backup_evidence=/proc/self/fd/33
postgres_dump=/proc/self/fd/34
postgres_source_state=/proc/self/fd/35
kubeconfig=/proc/self/fd/36
transition_job=/proc/self/fd/37
transition_plan=/proc/self/fd/38
```

Byte-review the canonical plan and Job. The ordered migration.revisions array
must be the intended exclusive path after the predecessor through the target.

## 2. Rehearse migration and full restore in isolation

The rehearsal publishes no port and uses an internal Docker network. Trust
authentication exists only inside that network and is never written to disk.

```bash
suffix="$(printf '%s' "$plan_sha256" | cut -c1-12)"
rehearsal_postgres="loom-personal-dev-transition-pg-$suffix"
rehearsal_network="loom-personal-dev-transition-$suffix"
postgres_image="$(jq -r .postgres.image "$backup_evidence")"
test "$postgres_image" = "$(jq -r .images.postgres "$predecessor_release")"
test -z "$(docker ps -a --format '{{.Names}}' |
  awk -v name="$rehearsal_postgres" '$0 == name')"
test -z "$(docker network ls --format '{{.Name}}' |
  awk -v name="$rehearsal_network" '$0 == name')"

cleanup_rehearsal() {
  docker rm --force "$rehearsal_postgres" >/dev/null 2>&1 || true
  docker network rm "$rehearsal_network" >/dev/null 2>&1 || true
}
trap cleanup_rehearsal EXIT
# The image entrypoint briefly accepts connections through a bootstrap server
# before replacing PID 1 with the final PostgreSQL server.
wait_for_final_postgres() {
  local container="$1"
  for attempt in $(seq 1 60); do
    if docker exec "$container" /bin/sh -euc \
      'read -r comm </proc/1/comm; test "$comm" = postgres' \
      >/dev/null 2>&1 && \
      docker exec "$container" \
      pg_isready -U postgres -d postgres >/dev/null 2>&1; then
      return
    fi
    test "$attempt" -lt 60
    sleep 1
  done
}
start_rehearsal_postgres() {
  docker run --detach --network "$rehearsal_network" \
    --network-alias transition-postgres --name "$rehearsal_postgres" \
    --env POSTGRES_HOST_AUTH_METHOD=trust \
    --env POSTGRES_USER=postgres --env POSTGRES_DB=postgres \
    "$postgres_image" >/dev/null
  wait_for_final_postgres "$rehearsal_postgres"
}
capture_docker_postgres_state() {
  local container="$1"
  local destination="$2"
  local sequences="$destination.sequences"
  local tables="$destination.tables"
  docker exec "$container" psql -AtX --set ON_ERROR_STOP=1 \
    -U postgres -d postgres -c \
    "SELECT format('%I.%I',schemaname,sequencename) FROM pg_sequences WHERE schemaname='public' ORDER BY 1" \
    > "$sequences" || return
  docker exec "$container" psql -AtX --set ON_ERROR_STOP=1 \
    -U postgres -d postgres -c \
    "SELECT format('%I.%I',schemaname,tablename) FROM pg_tables WHERE schemaname='public' ORDER BY 1" \
    > "$tables" || return
  chmod 0600 "$sequences" "$tables" || return
  : > "$destination" || return
  while IFS= read -r sequence; do
    sequence_state="$(docker exec "$container" psql -AtX \
      --set ON_ERROR_STOP=1 -U postgres -d postgres \
      -c "SELECT last_value,is_called FROM $sequence")" || return
    IFS='|' read -r last_value is_called <<< "$sequence_state"
    [[ "$last_value" =~ ^-?[0-9]+$ ]] || return
    test "$is_called" = t || test "$is_called" = f || return
    printf 'sequence\t%s\t%s\t%s\n' \
      "$sequence" "$last_value" "$is_called" >> "$destination" || return
  done < "$sequences"
  while IFS= read -r table; do
    count="$(docker exec "$container" psql -AtX --set ON_ERROR_STOP=1 \
      -U postgres -d postgres -c "SELECT count(*) FROM $table")" || return
    row_sha256="$(docker exec "$container" psql -AtX \
      --set ON_ERROR_STOP=1 -U postgres -d postgres -c \
      "COPY (SELECT to_jsonb(loom_row)::text FROM $table AS loom_row ORDER BY to_jsonb(loom_row)::text COLLATE \"C\") TO STDOUT" |
      sha256sum | awk '{print $1}')" || return
    printf 'table\t%s\t%s\t%s\n' \
      "$table" "$count" "$row_sha256" >> "$destination" || return
  done < "$tables"
  chmod 0600 "$destination" || return
}

docker network create --internal "$rehearsal_network" >/dev/null
start_rehearsal_postgres
docker exec -i "$rehearsal_postgres" pg_restore \
  --exit-on-error --clean --if-exists --no-owner --no-acl \
  -U postgres -d postgres < "$postgres_dump"
test "$(docker exec "$rehearsal_postgres" psql -AtX --set ON_ERROR_STOP=1 \
  -U postgres -d postgres -c 'SELECT version_num FROM alembic_version')" = \
  "$predecessor_head"
rehearsal_pre_state="$evidence_dir/rehearsal.predecessor-before-forward.tsv"
capture_docker_postgres_state "$rehearsal_postgres" "$rehearsal_pre_state"
cmp -s "$postgres_source_state" "$rehearsal_pre_state"

restore_rehearsal_predecessor() {
  docker exec "$rehearsal_postgres" /bin/sh -euc '
    dropdb --force --if-exists --maintenance-db=template1 \
      --username "$POSTGRES_USER" -- "$POSTGRES_DB"
    exec createdb --maintenance-db=template1 --username "$POSTGRES_USER" \
      --owner "$POSTGRES_USER" -- "$POSTGRES_DB"' || return
  docker exec -i "$rehearsal_postgres" pg_restore \
    --exit-on-error --clean --if-exists --no-owner --no-acl \
    -U postgres -d postgres < "$postgres_dump" || return
}
docker run --rm --network "$rehearsal_network" --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --env PYTHONDONTWRITEBYTECODE=1 \
  --env LOOM_DB_URL=postgresql+psycopg://postgres@transition-postgres:5432/postgres \
  --env PGCONNECT_TIMEOUT=3 --entrypoint /bin/sh "$migration_image" \
  -euc "$(jq -er '.spec.template.spec.containers[0].command[2]' "$transition_job")"
test "$(docker exec "$rehearsal_postgres" psql -AtX --set ON_ERROR_STOP=1 \
  -U postgres -d postgres -c 'SELECT version_num FROM alembic_version')" = \
  "$target_head"
capture_docker_postgres_state "$rehearsal_postgres" \
  "$evidence_dir/rehearsal.target-after-forward.tsv"

restore_rehearsal_predecessor
test "$(docker exec "$rehearsal_postgres" psql -AtX --set ON_ERROR_STOP=1 \
  -U postgres -d postgres -c 'SELECT version_num FROM alembic_version')" = \
  "$predecessor_head"
rehearsal_restored_state="$evidence_dir/rehearsal.predecessor-after-restore.tsv"
capture_docker_postgres_state "$rehearsal_postgres" "$rehearsal_restored_state"
cmp -s "$postgres_source_state" "$rehearsal_restored_state"
cleanup_rehearsal
trap - EXIT
test -z "$(docker ps -a --format '{{.Names}}' |
  awk -v name="$rehearsal_postgres" '$0 == name')"
test -z "$(docker network ls --format '{{.Name}}' |
  awk -v name="$rehearsal_network" '$0 == name')"
```

The two predecessor fingerprint comparisons above are authoritative restore
proof. The target fingerprint is retained for diagnosis but is not compared
with the predecessor because the schema intentionally differs.

## 3. Prove live invariants and quiesce

The exact predecessor checkout may predate trusted-release and kubeconfig
descriptor loading. Its two status invocations therefore receive the original
regular release and kubeconfig pathnames. The target status command accepts its
trusted release on the pinned descriptor, but its hardened kubectl runner also
requires the original regular kubeconfig pathname at the public boundary before
it creates anonymous snapshots for child processes. Immediately before and
after every regular-path invocation, the runbook proves those pathnames are
still the same owner-only inodes pinned on descriptors 31 and 36 with the
reviewed digests and context. The loaders independently enforce no-follow open,
owner, mode, link count, size, stable metadata, canonical release JSON, and a
self-contained kubeconfig. Every direct kubectl call continues to consume only
the pinned kubeconfig descriptor.

```bash
target_shadow="$evidence_dir/target-shadow.yaml"
target_render_evidence="$evidence_dir/target-shadow.render.json"
PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/dev/null \
  PYTHONPATH="$repo/src" "$loom_cli" admin personal-dev-control-plane render \
  --file "$profile" --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$target_shadow" 2> "$target_render_evidence"
chmod 0600 "$target_shadow" "$target_render_evidence"
test "$(sha256sum "$target_shadow" | awk '{print $1}')" = \
  "$target_shadow_sha256"
target_shadow_source="$target_shadow"
exec 39< "$target_shadow_source"
assert_open_owner_only_sha256 \
  "$target_shadow_source" 39 "$target_shadow_sha256" 16777216
target_shadow=/proc/self/fd/39
assert_exact_source_repository() {
  local root="$1"
  local expected_commit="$2"
  local expected_tree="$3"
  case "$root" in /*) ;; *) return 1 ;; esac
  test -d "$root" && test ! -L "$root" || return 1
  test "$(/usr/bin/realpath -e -- "$root")" = "$root" || return 1
  /usr/bin/env -i \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_CONFIG_NOSYSTEM=1 GIT_OPTIONAL_LOCKS=0 GIT_TERMINAL_PROMPT=0 \
    LANG=C LC_ALL=C PATH=/usr/bin:/bin \
    /usr/bin/python3 -I -S - "$root" "$expected_commit" "$expected_tree" <<'PY'
import hashlib
import os
import stat
import subprocess
import sys

root_text, expected_commit, expected_tree = sys.argv[1:]
root = os.fsencode(root_text)
git = [
    "/usr/bin/git",
    "-c", "core.filemode=true",
    "-c", "core.fsmonitor=false",
    "-c", "core.untrackedCache=false",
    "-C", root_text,
]

def output(*arguments: str) -> bytes:
    return subprocess.run(
        [*git, *arguments],
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout

def identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev, metadata.st_ino, metadata.st_mode,
        metadata.st_uid, metadata.st_gid, metadata.st_nlink,
        metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns,
    )

try:
    if os.fsdecode(output("rev-parse", "--show-toplevel").strip()) != root_text:
        raise ValueError
    if output("rev-parse", "HEAD").strip() != expected_commit.encode("ascii"):
        raise ValueError
    if output("rev-parse", "HEAD^{tree}").strip() != expected_tree.encode("ascii"):
        raise ValueError
    if output("ls-files", "--others", "--exclude-standard", "-z", "--"):
        raise ValueError
    for entry in output("ls-files", "-v", "-z", "--").split(b"\0"):
        if entry and (entry[:1] == b"S" or entry[:1].islower()):
            raise ValueError
    object_format = output("rev-parse", "--show-object-format").strip()
    digest_factory = {b"sha1": hashlib.sha1, b"sha256": hashlib.sha256}.get(
        object_format
    )
    if digest_factory is None:
        raise ValueError
    seen: set[bytes] = set()
    for entry in output("ls-tree", "-r", "-z", expected_commit).split(b"\0"):
        if not entry:
            continue
        metadata, separator, relative = entry.partition(b"\t")
        parts = metadata.split(b" ")
        if (
            not separator or len(parts) != 3
            or parts[0] not in {b"100644", b"100755", b"120000"}
            or parts[1] != b"blob" or not relative or relative in seen
            or os.path.isabs(relative) or os.path.normpath(relative) != relative
        ):
            raise ValueError
        seen.add(relative)
        path = os.path.join(root, relative)
        before = os.lstat(path)
        digest = digest_factory()
        if parts[0] == b"120000":
            if (
                not stat.S_ISLNK(before.st_mode)
                or before.st_uid != os.getuid() or before.st_nlink != 1
            ):
                raise ValueError
            payload = os.readlink(path)
            digest.update(f"blob {len(payload)}\0".encode("ascii"))
            digest.update(payload)
            if identity(os.lstat(path)) != identity(before):
                raise ValueError
        else:
            if (
                not stat.S_ISREG(before.st_mode)
                or bool(before.st_mode & stat.S_IXUSR) != (parts[0] == b"100755")
                or before.st_uid != os.getuid() or before.st_nlink != 1
            ):
                raise ValueError
            descriptor = os.open(
                path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
            )
            try:
                opened = os.fstat(descriptor)
                if identity(opened) != identity(before):
                    raise ValueError
                digest.update(f"blob {opened.st_size}\0".encode("ascii"))
                while chunk := os.read(descriptor, 1024 * 1024):
                    digest.update(chunk)
                if identity(os.lstat(path)) != identity(opened):
                    raise ValueError
            finally:
                os.close(descriptor)
        if digest.hexdigest().encode("ascii") != parts[2]:
            raise ValueError
    if not seen:
        raise ValueError
    for relative_root in (b"migrations", b"src/loom", b"src/loom_cli"):
        candidate_root = os.path.join(root, relative_root)
        if not os.path.isdir(candidate_root):
            continue
        for directory, directories, files in os.walk(candidate_root):
            for name in [*directories, *files]:
                if os.path.islink(os.path.join(directory, name)):
                    raise ValueError
            for name in files:
                if name.endswith((b".py", b".pyi", b".so")):
                    relative = os.path.relpath(os.path.join(directory, name), root)
                    if relative not in seen:
                        raise ValueError
    if output("rev-parse", "HEAD").strip() != expected_commit.encode("ascii"):
        raise ValueError
    if output("rev-parse", "HEAD^{tree}").strip() != expected_tree.encode("ascii"):
        raise ValueError
except (OSError, subprocess.SubprocessError, UnicodeError, ValueError):
    raise SystemExit(1) from None
PY
}
assert_exact_source_repository \
  "$repo" "$target_source_commit" "$target_source_tree"
assert_exact_source_repository \
  "$predecessor_repo" "$predecessor_source_commit" "$predecessor_source_tree"
predecessor_checkout_schema_head="$(PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPYCACHEPREFIX=/dev/null PYTHONPATH="$predecessor_repo/src" \
  "$python_cli" - "$predecessor_repo" <<'PY'
import sys
from pathlib import Path

import loom
from loom.db import schema_startup

root = Path(sys.argv[1]).resolve(strict=True)
alembic_ini = root / "migrations" / "alembic.ini"
if (
    Path(loom.__file__).resolve(strict=True) != (root / "src" / "loom" / "__init__.py")
    or Path(schema_startup.__file__).resolve(strict=True)
    != (root / "src" / "loom" / "db" / "schema_startup.py")
    or alembic_ini.resolve(strict=True) != alembic_ini
    or alembic_ini.is_symlink()
):
    raise SystemExit("predecessor source binding failed")
heads = tuple(schema_startup.service_schema_heads(alembic_ini=alembic_ini))
if len(heads) != 1:
    raise SystemExit("predecessor schema head is not singular")
print(heads[0])
PY
)"
test "$predecessor_checkout_schema_head" = "$expected_predecessor_schema_head"
assert_predecessor_release_compat_path() {
  assert_open_owner_only_sha256 \
    "$predecessor_release_source" 31 "$predecessor_release_sha256" 16777216
}
assert_predecessor_kubeconfig_compat_path() {
  local observed_context
  assert_open_owner_only_sha256 \
    "$kubeconfig_source" 36 "$kubeconfig_sha256" 1048576 || return 1
  observed_context="$(kubectl --kubeconfig "$kubeconfig" \
    config current-context)" || return 1
  test "$observed_context" = "$expected_kube_context" || return 1
  assert_open_owner_only_sha256 \
    "$kubeconfig_source" 36 "$kubeconfig_sha256" 1048576
}
run_predecessor_shadow_status() {
  local destination="$1"
  local status=0
  local temporary
  case "$destination" in "$evidence_dir/"*) ;; *) return 1 ;; esac
  test ! -e "$destination" && test ! -L "$destination" || return 1
  assert_predecessor_release_compat_path || return 1
  assert_predecessor_kubeconfig_compat_path || return 1
  temporary="$(mktemp "$evidence_dir/predecessor-status.XXXXXX.json")" || return 1
  if PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/dev/null \
    PYTHONPATH="$predecessor_repo/src" "$predecessor_loom_cli" admin \
    personal-dev-control-plane status \
    --namespace loom-dev --kubeconfig "$kubeconfig_source" \
    --file "$predecessor_profile" \
    --trusted-release-file "$predecessor_release_source" \
    --trusted-release-sha256 "$predecessor_release_sha256" \
    > "$temporary"; then
    :
  else
    status=$?
    rm -f -- "$temporary"
    return "$status"
  fi
  chmod 0600 "$temporary" || {
    rm -f -- "$temporary"
    return 1
  }
  if ! assert_predecessor_release_compat_path ||
    ! assert_predecessor_kubeconfig_compat_path; then
    rm -f -- "$temporary"
    return 1
  fi
  if mv "$temporary" "$destination"; then return 0; fi
  rm -f -- "$temporary"
  return 1
}
assert_target_status_compat_paths() {
  assert_pinned_owner_only_sha256 \
    30 "$trusted_release_sha256" 16777216 || return 1
  assert_open_owner_only_sha256 \
    "$kubeconfig_source" 36 "$kubeconfig_sha256" 1048576 || return 1
  test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
    "$expected_kube_context" || return 1
  assert_open_owner_only_sha256 \
    "$kubeconfig_source" 36 "$kubeconfig_sha256" 1048576 || return 1
  assert_pinned_owner_only_sha256 \
    30 "$trusted_release_sha256" 16777216
}
run_target_shadow_status() {
  local destination="$1"
  local status=0
  local temporary
  case "$destination" in "$evidence_dir/"*) ;; *) return 1 ;; esac
  test ! -e "$destination" && test ! -L "$destination" || return 1
  assert_target_status_compat_paths || return 1
  temporary="$(mktemp "$evidence_dir/target-status.XXXXXX.json")" || return 1
  if PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/dev/null \
    PYTHONPATH="$repo/src" "$loom_cli" admin personal-dev-control-plane \
    status --namespace loom-dev --kubeconfig "$kubeconfig_source" \
    --file "$profile" --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" \
    > "$temporary"; then
    :
  else
    status=$?
    rm -f -- "$temporary"
    return "$status"
  fi
  chmod 0600 "$temporary" || {
    rm -f -- "$temporary"
    return 1
  }
  if ! assert_target_status_compat_paths; then
    rm -f -- "$temporary"
    return 1
  fi
  if mv "$temporary" "$destination"; then return 0; fi
  rm -f -- "$temporary"
  return 1
}
assert_transition_artifacts() {
  assert_exact_source_repository \
    "$repo" "$target_source_commit" "$target_source_tree" || return 1
  assert_exact_source_repository \
    "$predecessor_repo" "$predecessor_source_commit" \
    "$predecessor_source_tree" || return 1
  assert_pinned_owner_only_sha256 30 "$trusted_release_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 31 "$predecessor_release_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 32 "$predecessor_shadow_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 33 "$backup_evidence_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 38 "$plan_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 37 "$migration_job_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 39 "$target_shadow_sha256" 16777216 || return 1
}
assert_predecessor_restore_artifacts() {
  assert_pinned_owner_only_sha256 34 "$postgres_dump_sha256" 4294967296 || return 1
  assert_pinned_owner_only_sha256 35 "$postgres_state_sha256" 4294967296 || return 1
}
assert_predecessor_recovery_artifacts() {
  local observed_commit
  local observed_head
  local observed_tree
  assert_pinned_owner_only_sha256 31 "$predecessor_release_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 32 "$predecessor_shadow_sha256" 16777216 || return 1
  assert_pinned_owner_only_sha256 33 "$backup_evidence_sha256" 16777216 || return 1
  observed_commit="$(jq -er .source_sha /proc/self/fd/31)" || return 1
  observed_tree="$(jq -er .source_tree /proc/self/fd/31)" || return 1
  observed_head="$(jq -er .postgres.source_schema_head /proc/self/fd/33)" || return 1
  test "$observed_commit" = "$predecessor_source_commit" || return 1
  test "$observed_tree" = "$predecessor_source_tree" || return 1
  test "$observed_head" = "$predecessor_head" || return 1
  assert_exact_source_repository \
    "$predecessor_repo" "$observed_commit" "$observed_tree" || return 1
}

assert_no_dynamic_namespaces() {
  local dynamic_namespaces
  local namespaces
  namespaces="$(kubectl --kubeconfig "$kubeconfig" get namespaces -o json)" || return
  dynamic_namespaces="$(jq -r '.items[].metadata.name |
    select(startswith("loom-dev-") or startswith("loom-build-"))' \
    <<< "$namespaces")" || return
  test -z "$dynamic_namespaces"
}
assert_zero_capacity() {
  local source_root="${1:-$repo}"
  local selected_cli
  case "$source_root" in
    "$repo") selected_cli="$loom_cli" ;;
    "$predecessor_repo") selected_cli="$predecessor_loom_cli" ;;
    *) return 1 ;;
  esac
  test "$(PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/dev/null \
    PYTHONPATH="$source_root/src" "$selected_cli" \
    admin capacity-control-plane status \
    --namespace loom-dev --kubeconfig "$kubeconfig")" = \
    '{"executable_new_capacity_ceiling":0,"status":"ready"}'
}
assert_reviewed_kubeconfig() {
  assert_pinned_owner_only_sha256 36 "$kubeconfig_sha256" 1048576 || return
  test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
    "$expected_kube_context" || return
}
assert_transition_interlocks() {
  assert_reviewed_kubeconfig || return
  assert_transition_artifacts || return
  assert_no_dynamic_namespaces || return
  assert_zero_capacity || return
  test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    deployment/loom-personal-dev-activation-agent \
    -o jsonpath='{.spec.replicas}')" = 0 || return
}
assert_predecessor_recovery_interlocks() {
  assert_reviewed_kubeconfig || return
  assert_predecessor_recovery_artifacts || return
  assert_no_dynamic_namespaces || return
  assert_zero_capacity "$predecessor_repo" || return
  test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    deployment/loom-personal-dev-activation-agent \
    -o jsonpath='{.spec.replicas}')" = 0 || return
}

assert_reviewed_kubeconfig
predecessor_status="$evidence_dir/predecessor.status.json"
run_predecessor_shadow_status "$predecessor_status"
jq -e '.mode == "shadow" and .ready == true and .blockers == [] and
  .manager_ceiling == 0 and .worker_available == false and
  any(.components[]; .name == "personal-workers" and .observed == 0)' \
  "$predecessor_status" >/dev/null
assert_no_dynamic_namespaces
assert_zero_capacity
assert_reviewed_kubeconfig
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  deployment/loom-personal-dev-activation-agent \
  -o jsonpath='{.spec.replicas}')" = 0
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  deployment/loom-personal-dev-management \
  -o jsonpath='{.spec.replicas}')" = 1
```

The next block defines the exact live database fingerprint, head, and writer
quiescence operations. Database row values stream directly into `sha256sum` and
are not printed or retained.

```bash
postgres_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-postgres -o json | jq -er '
    [.items[] | select(.status.phase == "Running") | .metadata.name] |
    if length == 1 then .[0] else error("postgres pod cardinality") end')"
capture_live_postgres_state() {
  local destination="$1"
  local sequences="$destination.sequences"
  local tables="$destination.tables"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c \
      "SELECT format('"'"'%I.%I'"'"',schemaname,sequencename) FROM pg_sequences WHERE schemaname='"'"'public'"'"' ORDER BY 1"' \
    > "$sequences" || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c \
      "SELECT format('"'"'%I.%I'"'"',schemaname,tablename) FROM pg_tables WHERE schemaname='"'"'public'"'"' ORDER BY 1"' \
    > "$tables" || return
  chmod 0600 "$sequences" "$tables" || return
  : > "$destination" || return
  while IFS= read -r sequence; do
    sequence_state="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev \
      exec "$postgres_pod" -c postgres -- /bin/sh -euc \
      'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT last_value,is_called FROM $1"' \
      sh "$sequence")" || return
    IFS='|' read -r last_value is_called <<< "$sequence_state"
    [[ "$last_value" =~ ^-?[0-9]+$ ]] || return
    test "$is_called" = t || test "$is_called" = f || return
    printf 'sequence\t%s\t%s\t%s\n' \
      "$sequence" "$last_value" "$is_called" >> "$destination" || return
  done < "$sequences"
  while IFS= read -r table; do
    count="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
      "$postgres_pod" -c postgres -- /bin/sh -euc \
      'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT count(*) FROM $1"' \
      sh "$table")" || return
    row_sha256="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
      "$postgres_pod" -c postgres -- /bin/sh -euc \
      'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "COPY (SELECT to_jsonb(loom_row)::text FROM $1 AS loom_row ORDER BY to_jsonb(loom_row)::text COLLATE \"C\") TO STDOUT"' \
      sh "$table" | sha256sum | awk '{print $1}')" || return
    printf 'table\t%s\t%s\t%s\n' \
      "$table" "$count" "$row_sha256" >> "$destination" || return
  done < "$tables"
  chmod 0600 "$destination" || return
}
live_schema_head() {
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
    "$postgres_pod" -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT version_num FROM alembic_version"'
}
assert_live_predecessor_state() {
  local phase="$1"
  local observed="$evidence_dir/live.$phase.tsv"
  capture_live_postgres_state "$observed" || return
  cmp -s "$postgres_source_state" "$observed" || return
  test "$(live_schema_head)" = "$predecessor_head" || return
}
scale_management_to_zero() {
  local phase="$1"
  local replicas
  replicas="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    deployment/loom-personal-dev-management -o jsonpath='{.spec.replicas}')" || return
  case "$replicas" in
    0) printf '%s\n' already-zero > "$evidence_dir/$phase-management.txt" || return ;;
    1) kubectl --kubeconfig "$kubeconfig" --namespace loom-dev scale \
         deployment/loom-personal-dev-management \
         --current-replicas=1 --replicas=0 \
         > "$evidence_dir/$phase-management.txt" || return ;;
    *) return 1 ;;
  esac
  chmod 0600 "$evidence_dir/$phase-management.txt" || return
}
wait_for_no_management_pods() {
  local count
  for attempt in $(seq 1 120); do
    count="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pods \
      -l app=loom-personal-dev-management -o json |
      jq '[.items[] | select(.metadata.deletionTimestamp == null)] | length')" || return
    if test "$count" -eq 0; then return 0; fi
    test "$attempt" -lt 120 || return
    sleep 2
  done
}
quiesce_management() {
  assert_transition_interlocks || return
  assert_predecessor_restore_artifacts || return
  assert_live_predecessor_state pre-quiesce || return
  transition_quiesced=1
  scale_management_to_zero quiesce || return
  wait_for_no_management_pods || return
  assert_live_predecessor_state post-quiesce || return
}
```

`transition_quiesced` is set before the first scale attempt. Therefore even an
ambiguous scale response enters the full-restore recovery path; migration never
starts until the post-quiesce fingerprint and predecessor head both match.

## 4. Apply only the migration, then the full target

```bash
assert_migration_job_absent() {
  local observed
  observed="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    job "$migration_job_name" --ignore-not-found -o name)" || return
  test -z "$observed"
}
apply_reviewed_migration_job() {
  kubectl --kubeconfig "$kubeconfig" apply --server-side \
    --field-manager=loom-personal-dev-control-plane \
    -f "$transition_job" > "$evidence_dir/migration-job.apply.txt" || return
  chmod 0600 "$evidence_dir/migration-job.apply.txt" || return
}
wait_for_reviewed_migration_job() {
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev wait \
    --for=condition=complete --timeout=900s "job/$migration_job_name" || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    "job/$migration_job_name" -o json \
    > "$evidence_dir/migration-job.completed.json" || return
  chmod 0600 "$evidence_dir/migration-job.completed.json" || return
  jq -e '.status.succeeded == 1 and
    ([.status.conditions[]? | select(.type == "Complete" and .status == "True")] |
      length) == 1 and
    ([.status.conditions[]? | select(.type == "Failed" and .status == "True")] |
      length) == 0' "$evidence_dir/migration-job.completed.json" >/dev/null || return
}
assert_live_target_head() {
  test "$(live_schema_head)" = "$target_head"
}
apply_reviewed_target_shadow() {
  kubectl --kubeconfig "$kubeconfig" apply --server-side \
    --field-manager=loom-personal-dev-control-plane \
    -f "$target_shadow" > "$evidence_dir/target-shadow.apply.txt" || return
  chmod 0600 "$evidence_dir/target-shadow.apply.txt" || return
}
wait_for_target_shadow() {
  kubectl --kubeconfig "$kubeconfig" rollout status \
    deployment/loom-personal-dev-management \
    --namespace loom-dev --timeout=300s || return
  kubectl --kubeconfig "$kubeconfig" rollout status \
    deployment/loom-personal-dev-web \
    --namespace loom-dev --timeout=300s || return
}
assert_target_shadow_ready() {
  run_target_shadow_status "$evidence_dir/target-shadow.status.json" || return
  chmod 0600 "$evidence_dir/target-shadow.status.json" || return
  jq -e '.mode == "shadow" and .ready == true and .blockers == [] and
    .manager_ceiling == 0 and .worker_available == false and
    any(.components[]; .name == "personal-workers" and .observed == 0)' \
    "$evidence_dir/target-shadow.status.json" >/dev/null || return
}
apply_target_transition() {
  assert_transition_interlocks || return
  assert_migration_job_absent || return
  apply_reviewed_migration_job || return
  wait_for_reviewed_migration_job || return
  assert_live_target_head || return
  assert_transition_interlocks || return
  apply_reviewed_target_shadow || return
  wait_for_target_shadow || return
  assert_target_shadow_ready || return
  assert_transition_interlocks || return
}
```

After success, immediately run the backup/restore procedure from the target
checkout. Only that new target-head backup may enter target acceptance and
operational plans.

## 5. Mandatory recovery after any post-quiesce failure

Do not run this on the successful path. It replaces the live database with the
reviewed predecessor dump and is authorized only after section 3 has quiesced
the old management Deployment.

The exact succeeded predecessor migration Job must still exist. Recovery
preserves that Job and deletes only the target migration Job and its Pods, so
reapplying the predecessor shadow updates an already-complete Job rather than
starting a schema writer concurrently with predecessor management. After that
apply, delete the complete Web resource set only when the pinned transition
plan records it as target-only. An empty deletion set means the predecessor
already owns Web and must retain it; any partial or unrelated set fails closed.
Other retained migration Jobs and Pods are immutable historical evidence:
recovery preserves them only when every Job is complete, every Pod is succeeded
and owned by a complete Job, and none is being deleted. Any active, failed,
orphaned, or deleting historical entry fails closed before target deletion.

```bash
stop_target_migration() {
  local inventory="$evidence_dir/rollback-migration-inventory.json"
  local remaining="$evidence_dir/rollback-migration-remaining.json"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    jobs,pods -l app=loom-personal-dev-migration -o json \
    > "$inventory" || return
  chmod 0600 "$inventory" || return
  jq -e --arg predecessor "$predecessor_migration_job_name" \
    --arg target "$migration_job_name" '
      def completed_job:
        .kind == "Job" and
        (.metadata.deletionTimestamp // null) == null and
        (.status.succeeded // 0) == 1 and
        (.status.active // 0) == 0 and (.status.failed // 0) == 0;
      def pod_owner:
        (.metadata.labels["batch.kubernetes.io/job-name"] //
         .metadata.labels["job-name"] // "");
      (.items | type == "array") and
      (.items as $items |
        ([$items[] | select(
          completed_job and .metadata.name == $predecessor
        )] | length == 1) and
        all($items[];
          if .kind == "Job" then
            (.metadata.name == $target or completed_job)
          elif .kind == "Pod" then
            (pod_owner as $owner |
              if $owner == $target then true
              else
                (.metadata.deletionTimestamp // null) == null and
                .status.phase == "Succeeded" and
                any($items[];
                  completed_job and .metadata.name == $owner
                )
              end)
          else false end
        )
      )' "$inventory" >/dev/null || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev delete job \
    "$migration_job_name" --ignore-not-found \
    --cascade=foreground --wait=true --timeout=300s \
    > "$evidence_dir/rollback-delete-target-migration.txt" || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev delete pods \
    -l "batch.kubernetes.io/job-name=$migration_job_name" --ignore-not-found \
    --wait=true --timeout=300s \
    >> "$evidence_dir/rollback-delete-target-migration.txt" || return
  chmod 0600 "$evidence_dir/rollback-delete-target-migration.txt" || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    jobs,pods -l app=loom-personal-dev-migration -o json \
    > "$remaining" || return
  chmod 0600 "$remaining" || return
  jq -e --arg predecessor "$predecessor_migration_job_name" \
    --arg target "$migration_job_name" '
      def completed_job:
        .kind == "Job" and
        (.metadata.deletionTimestamp // null) == null and
        (.status.succeeded // 0) == 1 and
        (.status.active // 0) == 0 and (.status.failed // 0) == 0;
      def pod_owner:
        (.metadata.labels["batch.kubernetes.io/job-name"] //
         .metadata.labels["job-name"] // "");
      (.items | type == "array") and
      (.items as $items |
        ([$items[] | select(
          completed_job and .metadata.name == $predecessor
        )] | length == 1) and
        ([$items[] | select(
          .metadata.name == $target or pod_owner == $target
        )] | length == 0) and
        all($items[];
          if .kind == "Job" then completed_job
          elif .kind == "Pod" then
            ((.metadata.deletionTimestamp // null) == null and
             .status.phase == "Succeeded" and
             (pod_owner as $owner |
               any($items[];
                 completed_job and .metadata.name == $owner
               )))
          else false end
        )
      )' "$remaining" >/dev/null || return
}
assert_only_restore_connection() {
  local connections
  connections="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
    "$postgres_pod" -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"')" || return
  test "$connections" = 1
}
restore_predecessor_database() {
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec \
    "$postgres_pod" -c postgres -- /bin/sh -euc '
      dropdb --force --if-exists --maintenance-db=template1 \
        --username "$POSTGRES_USER" -- "$POSTGRES_DB"
      exec createdb --maintenance-db=template1 --username "$POSTGRES_USER" \
        --owner "$POSTGRES_USER" -- "$POSTGRES_DB"' || return
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec -i \
    "$postgres_pod" -c postgres -- /bin/sh -euc \
    'exec pg_restore --exit-on-error --clean --if-exists --no-owner --no-acl --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
    < "$postgres_dump" > "$evidence_dir/rollback-pg-restore.txt" || return
  chmod 0600 "$evidence_dir/rollback-pg-restore.txt" || return
}
apply_reviewed_predecessor_shadow() {
  kubectl --kubeconfig "$kubeconfig" apply --server-side \
    --field-manager=loom-personal-dev-control-plane \
    -f "$predecessor_shadow" > "$evidence_dir/rollback-predecessor.apply.txt" || return
  chmod 0600 "$evidence_dir/rollback-predecessor.apply.txt" || return
}
remove_forward_only_web() {
  local deletion_mode
  deletion_mode="$(jq -er '
    if .rollback.delete_after_predecessor_apply == [] then "none"
    elif .rollback.delete_after_predecessor_apply == [
      "deployment.apps/loom-personal-dev-web",
      "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress",
      "service/loom-personal-dev-web"
    ] then "web"
    else error("invalid forward-only rollback deletion set")
    end' "$transition_plan")" || return
  if test "$deletion_mode" = web; then
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev delete \
      deployment/loom-personal-dev-web \
      networkpolicy/loom-personal-dev-web-ingress \
      service/loom-personal-dev-web \
      --ignore-not-found --wait=true --timeout=300s \
      > "$evidence_dir/rollback-remove-forward-web.txt" || return
  else
    printf '%s\n' predecessor-owned-web-retained \
      > "$evidence_dir/rollback-remove-forward-web.txt" || return
  fi
  chmod 0600 "$evidence_dir/rollback-remove-forward-web.txt" || return
}
wait_for_predecessor_shadow() {
  kubectl --kubeconfig "$kubeconfig" rollout status \
    deployment/loom-personal-dev-management \
    --namespace loom-dev --timeout=300s || return
}
assert_predecessor_shadow_ready() {
  run_predecessor_shadow_status \
    "$evidence_dir/rollback-predecessor.status.json" || return
  jq -e '.mode == "shadow" and .ready == true and .blockers == [] and
    .manager_ceiling == 0 and .worker_available == false and
    any(.components[]; .name == "personal-workers" and .observed == 0)' \
    "$evidence_dir/rollback-predecessor.status.json" >/dev/null || return
}
restore_predecessor() {
  assert_predecessor_recovery_interlocks || return
  scale_management_to_zero rollback-quiesce || return
  wait_for_no_management_pods || return
  assert_predecessor_recovery_interlocks || return
  stop_target_migration || return
  assert_predecessor_recovery_interlocks || return
  assert_predecessor_restore_artifacts || return
  assert_only_restore_connection || return
  restore_predecessor_database || return
  assert_predecessor_restore_artifacts || return
  assert_live_predecessor_state rollback-restored || return
  assert_predecessor_recovery_interlocks || return
  apply_reviewed_predecessor_shadow || return
  remove_forward_only_web || return
  wait_for_predecessor_shadow || return
  assert_predecessor_shadow_ready || return
  assert_predecessor_recovery_interlocks || return
  transition_quiesced=0
}
run_schema_transition() {
  local forward_status
  transition_quiesced=0
  if quiesce_management; then
    :
  else
    forward_status=$?
    if test "$transition_quiesced" -eq 1; then
      restore_predecessor || return 125
    fi
    return "$forward_status"
  fi
  if apply_target_transition; then
    transition_quiesced=0
    printf '%s\n' target-ready > "$evidence_dir/schema-transition.result.txt"
    chmod 0600 "$evidence_dir/schema-transition.result.txt"
    return 0
  else
    forward_status=$?
  fi
  restore_predecessor || return 125
  printf 'forward-failed-%s-restored-predecessor\n' "$forward_status" \
    > "$evidence_dir/schema-transition.result.txt"
  chmod 0600 "$evidence_dir/schema-transition.result.txt"
  return "$forward_status"
}
run_schema_transition
```

Retain the complete evidence directory on either path. Never retry forward from
a partially migrated database. Recovery must first reproduce the exact
predecessor head and database-state fingerprint.
