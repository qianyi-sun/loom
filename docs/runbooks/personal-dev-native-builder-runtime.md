# Personal-development native builder runtime rollout

This runbook stages and activates the dedicated native `linux/arm64` personal
candidate builder on GB10 host `gx10-01c7`. It complements the measured OLDLAB
Kubernetes builder runtime; it does not replace it. The transaction creates no
Loom Task, Trial, Worker, Slurm job, reservation, or executable capacity. The
executable-new-capacity ceiling remains exactly `0`.

Repository presence is not operational authority. Use only an exact merged,
protected-CI-approved source commit, schema-4 trusted release, reviewed native
operational plan, owner-only evidence directory, and separately authorized
runtime window. There is no QEMU path and no runc fallback. Stop at the first
unexpected byte, identity, route, process, image, namespace, grant, or capacity
observation.

The ordering is intentional:

1. capture read-only before-state;
2. stage and verify all root-owned bytes while both dedicated services are
   inactive;
3. activate only the dedicated daemon, converge exact current and previous
   images, and run the disposable two-container conformance;
4. stop the daemon, stage the private key and agent unit while inactive, then
   reactivate the daemon and start the agent;
5. continue with the native acceptance runbook, which applies management only
   after it observes the agent service active and then requires fresh signed
   zero-grant readiness before any owner deployment.

Secret values are never printed, placed in command arguments, copied into the
repository, or included in evidence. Paths, ownership, modes, public-key
digests, CA digests, and bounded Secret key-name inventories may be recorded.

## 1. Bind the exact source, release, profile, and evidence authority

Run all blocks in one Bash session from the repository root. Replace every
placeholder. The evidence root must already exist outside the repository and
must be an owner-owned mode-`0700` directory.

```bash
set -euo pipefail
umask 077
export LC_ALL=C
test "$(id -u)" != 0

merged_source_sha='<merged-40-lowercase-hex>'
trusted_release_artifact='<absolute-trusted-release-artifact-directory>'
trusted_release="$trusted_release_artifact/trusted-release.json"
trusted_release_sha256='<trusted-release-64-lowercase-hex>'
previous_trusted_release='<absolute-previous-trusted-release.json-or-empty>'
runtime_window_id='<authorized-native-runtime-window-id>'
reviewed_kubeconfig='<absolute-owner-only-mode-0600-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
gb10_target='<ssh-user>@gx10-01c7'
slurm_observer='<read-only-slurm-observer-ssh-target>'

runtime_profile='deploy/personal-dev-native-builder/runtime-profile-v1.json'
runtime_profile_sha256='c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d'
prepared_control_profile='<absolute-owner-only-prepared-schema-3-profile.toml>'
prepared_control_profile_sha256='<prepared-profile-64-lowercase-hex>'
installer='scripts/ops/install_personal_dev_native_builder_runtime.py'
converger='scripts/ops/converge_personal_dev_native_builder_release.py'
archive_url='https://storage.googleapis.com/gvisor/releases/release/20260810/aarch64/gvisor.tar.bz2'
archive_sha512='dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7'
agent_private_key='<absolute-root-owned-mode-0400-ed25519-private-key>'
service_ca='<absolute-root-owned-mode-0444-service-ca>'
rollback_shadow_manifest='<absolute-byte-reviewed-schema-4-shadow-manifest>'
rollback_shadow_sha256='<rollback-shadow-64-lowercase-hex>'

repository_root="$(pwd -P)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="$evidence_root/${timestamp}-${merged_source_sha}"
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)

test "$merged_source_sha" != '<merged-40-lowercase-hex>'
test "$trusted_release_sha256" != '<trusted-release-64-lowercase-hex>'
test "$runtime_window_id" != '<authorized-native-runtime-window-id>'
test "$(git rev-parse --show-toplevel)" = "$repository_root"
test "$(git rev-parse HEAD)" = "$merged_source_sha"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test "$(jq -er .source_sha "$trusted_release")" = "$merged_source_sha"
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = \
  "$runtime_profile_sha256"
test "$(jq -er .schema_version "$trusted_release")" = 4

for path in "$trusted_release" "$reviewed_kubeconfig" "$prepared_control_profile" \
  "$rollback_shadow_manifest"; do
  test -f "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
done
test "$(sha256sum "$rollback_shadow_manifest" | awk '{print $1}')" = \
  "$rollback_shadow_sha256"
test "$(sha256sum "$prepared_control_profile" | awk '{print $1}')" = \
  "$prepared_control_profile_sha256"

test -d "$evidence_root"
test ! -L "$evidence_root"
test "$(realpath -e "$evidence_root")" = "$evidence_root"
test "$(stat -c %u "$evidence_root")" = "$(id -u)"
test "$(stat -c %a "$evidence_root")" = 700
case "$evidence_root/" in
  "$repository_root"/*) exit 1 ;;
esac
case "$repository_root/" in
  "$evidence_root"/*) exit 1 ;;
esac
test ! -e "$evidence_dir"
install -d -m 0700 "$evidence_dir"

prepared_binding="$(python3 - "$prepared_control_profile" \
  "$runtime_profile_sha256" <<'PY'
import ipaddress
import json
import re
import sys
import tomllib
import uuid
from pathlib import Path
from urllib.parse import urlsplit

value = tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
native = value.get("native_builder")
network = value.get("network")
identities = value.get("identities")
if (
    not isinstance(native, dict)
    or not isinstance(network, dict)
    or not isinstance(identities, dict)
):
    raise SystemExit(1)
management = urlsplit(network.get("public_origin", ""))
public_store = urlsplit(native.get("public_store_origin", ""))
cidrs = native.get("public_store_endpoint_cidrs")
if (
    value.get("schema_version") != 3
    or native.get("prepared") is not True
    or native.get("host_name") != "gx10-01c7"
    or native.get("runtime_profile_sha256") != sys.argv[2]
    or native.get("provider") != "gb10-gvisor-docker-v1"
    or native.get("platform") != "linux/arm64"
    or native.get("protocol_version") != 1
    or native.get("max_concurrency") != 2
    or identities.get("native_builder_public_secret")
    != "loom-personal-dev-native-builder-public"
    or not isinstance(native.get("agent_instance_id"), str)
    or str(uuid.UUID(native["agent_instance_id"])) != native["agent_instance_id"]
    or not isinstance(native.get("agent_key_id"), str)
    or re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", native["agent_key_id"]) is None
    or not isinstance(native.get("public_key_sha256"), str)
    or re.fullmatch(r"[0-9a-f]{64}", native["public_key_sha256"]) is None
    or native["public_key_sha256"] == "0" * 64
    or not isinstance(cidrs, list)
    or not cidrs
    or management.scheme != "https"
    or not management.hostname
    or management.path not in {"", "/"}
    or management.username
    or management.password
    or management.query
    or management.fragment
    or public_store.scheme != "https"
    or not public_store.hostname
    or public_store.path not in {"", "/"}
    or public_store.username
    or public_store.password
    or public_store.query
    or public_store.fragment
):
    raise SystemExit(1)
parsed_cidrs = [ipaddress.ip_network(item, strict=True) for item in cidrs]
canonical_cidrs = [
    str(item)
    for item in sorted(
        parsed_cidrs, key=lambda item: (item.version, int(item.network_address))
    )
]
if (
    cidrs != canonical_cidrs
    or len(set(cidrs)) != len(cidrs)
    or any(not item.is_global or item.prefixlen != item.max_prefixlen for item in parsed_cidrs)
):
    raise SystemExit(1)
print(json.dumps({
    "agent_instance_id": native["agent_instance_id"],
    "agent_key_id": native["agent_key_id"],
    "management_origin": network["public_origin"],
    "native_builder_public_secret": identities["native_builder_public_secret"],
    "public_key_sha256": native["public_key_sha256"],
    "public_store_endpoint_cidrs": cidrs,
    "public_store_origin": native["public_store_origin"],
}, sort_keys=True, separators=(",", ":")))
PY
)"
agent_instance_id="$(jq -er .agent_instance_id <<< "$prepared_binding")"
agent_key_id="$(jq -er .agent_key_id <<< "$prepared_binding")"
expected_public_key_sha256="$(jq -er .public_key_sha256 <<< "$prepared_binding")"
reviewed_management_origin="$(jq -er .management_origin <<< "$prepared_binding")"
native_builder_public_secret="$(jq -er .native_builder_public_secret <<< "$prepared_binding")"
reviewed_public_store_origin="$(jq -er .public_store_origin <<< "$prepared_binding")"
reviewed_public_store_cidrs="$(jq -r '.public_store_endpoint_cidrs[]' <<< "$prepared_binding")"
printf '%s\n' "$prepared_binding" > "$evidence_dir/prepared-profile-binding.json"
chmod 0600 "$evidence_dir/prepared-profile-binding.json"

current_agent="$(jq -er .images.personal_dev_native_builder_agent "$trusted_release")"
current_builder="$(jq -er .images.personal_dev_builder "$trusted_release")"
current_revision="$(jq -er .source_sha "$trusted_release")"
[[ "$current_agent" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$ ]]
[[ "$current_builder" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]

previous_args=()
if test -n "$previous_trusted_release"; then
  test -f "$previous_trusted_release"
  test ! -L "$previous_trusted_release"
  test "$(stat -c %u "$previous_trusted_release")" = "$(id -u)"
  test "$(stat -c %a "$previous_trusted_release")" = 600
  previous_args=(
    --previous-agent "$(jq -er .images.personal_dev_native_builder_agent "$previous_trusted_release")"
    --previous-builder "$(jq -er .images.personal_dev_builder "$previous_trusted_release")"
    --previous-revision "$(jq -er .source_sha "$previous_trusted_release")"
  )
fi

jq -cnS \
  --arg source "$merged_source_sha" \
  --arg tree "$(git rev-parse HEAD^{tree})" \
  --arg release "$trusted_release_sha256" \
  --arg profile "$runtime_profile_sha256" \
  --arg prepared_profile "$prepared_control_profile_sha256" \
  --arg archive "$archive_sha512" \
  --arg window "$runtime_window_id" \
  '{archive_sha512:$archive,prepared_profile_sha256:$prepared_profile,
    profile_sha256:$profile,
    source_sha:$source,source_tree:$tree,
    trusted_release_sha256:$release,window_id:$window}' \
  > "$evidence_dir/immutable-inputs.json"
chmod 0600 "$evidence_dir/immutable-inputs.json"
```

Validate the private material only on the protected operator host. Do not
record either file's bytes or private-key digest in issue comments.

```bash
test -f "$agent_private_key"
test ! -L "$agent_private_key"
test "$(realpath -e "$agent_private_key")" = "$agent_private_key"
test "$(stat -c %u "$agent_private_key")" = 0
test "$(stat -c %g "$agent_private_key")" = 0
test "$(stat -c %a "$agent_private_key")" = 400
test "$(stat -c %s "$agent_private_key")" = 32
test "$(stat -c %h "$agent_private_key")" = 1

sudo env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repository_root/src" \
  "$repository_root/.venv/bin/python" - "$agent_private_key" \
  "$agent_key_id" "$expected_public_key_sha256" <<'PY'
import hashlib
import sys
from pathlib import Path

from loom.personal_dev_native_builder_protocol import (
    load_personal_dev_native_builder_signer,
)

signer = load_personal_dev_native_builder_signer(
    Path(sys.argv[1]), key_id=sys.argv[2]
)
if hashlib.sha256(signer.public_key_bytes(sys.argv[2])).hexdigest() != sys.argv[3]:
    raise SystemExit(1)
PY

test -f "$service_ca"
test ! -L "$service_ca"
test "$(realpath -e "$service_ca")" = "$service_ca"
test "$(stat -c %u "$service_ca")" = 0
test "$(stat -c %g "$service_ca")" = 0
test "$(stat -c %a "$service_ca")" = 444
test "$(stat -c %h "$service_ca")" = 1
```

## 2. Capture read-only before-state

The database snapshot records counts only. PostgreSQL credentials remain inside
the existing Postgres container. The Slurm observer account must be read-only;
only `scontrol show` and `squeue` are permitted. This procedure contains no
Slurm mutation and no task submission.

```bash
kubeconfig="$evidence_dir/kubeconfig"
install -m 0600 "$reviewed_kubeconfig" "$kubeconfig"
test "$(sha256sum "$reviewed_kubeconfig" | awk '{print $1}')" = \
  "$(sha256sum "$kubeconfig" | awk '{print $1}')"

capture_host() {
  local output="$1"
  ssh "${ssh_options[@]}" "$gb10_target" -- /bin/sh -euc '
    jq -cnS \
      --arg architecture "$(uname -m)" \
      --arg boot_id "$(sed -n "1p" /proc/sys/kernel/random/boot_id)" \
      --arg hostname "$(hostname)" \
      --arg dedicated_daemon "$(systemctl is-active loom-personal-dev-builder-dockerd.service 2>/dev/null || true)" \
      --arg agent "$(systemctl is-active loom-personal-dev-native-builder-agent.service 2>/dev/null || true)" \
      --arg primary_docker "$(docker version --format "{{.Server.Version}}" 2>/dev/null || true)" \
      "{agent:\$agent,architecture:\$architecture,boot_id:\$boot_id,
        dedicated_daemon:\$dedicated_daemon,hostname:\$hostname,
        primary_docker:\$primary_docker}"
  ' > "$output"
  chmod 0600 "$output"
}

capture_slurm() {
  local output="$1"
  local temporary="$output.tmp"
  ssh "${ssh_options[@]}" "$slurm_observer" -- scontrol show nodes --json \
    | jq -cS . > "$output"
  ssh "${ssh_options[@]}" "$slurm_observer" -- squeue --json \
    | jq -cS . > "$temporary"
  chmod 0600 "$output" "$temporary"
  jq -cnS \
    --slurpfile nodes "$output" \
    --slurpfile queue "$temporary" \
    '{nodes:$nodes[0],queue:$queue[0]}' > "$output.merged"
  mv "$output.merged" "$output"
  rm -f "$temporary"
}

assert_no_loom_slurm_jobs() {
  jq -e '[.queue.jobs[]? |
    select(((.name // "") | ascii_downcase | startswith("loom")))] |
    length == 0' "$1" >/dev/null
}

postgres_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-postgres -o json | jq -er '
    [.items[] | select(.status.phase == "Running") | .metadata.name] |
    if length == 1 then .[0] else error("postgres cardinality") end')"

read_count() {
  local sql="$1"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "$1"' \
    sh "$sql"
}

capture_database_counts() {
  local output="$1" grants=null grant_table
  local tasks workers
  tasks="$(read_count 'SELECT count(*) FROM tasks')"
  workers="$(read_count 'SELECT count(*) FROM workers')"
  grant_table="$(read_count "SELECT to_regclass('public.personal_dev_native_build_grants') IS NOT NULL")"
  if test "$grant_table" = t; then
    grants="$(read_count "SELECT count(*) FROM personal_dev_native_build_grants WHERE state IN ('queued','running')")"
  fi
  jq -cnS --argjson grants "$grants" --argjson tasks "$tasks" \
    --argjson workers "$workers" \
    '{active_native_grants:$grants,tasks:$tasks,workers:$workers}' > "$output"
  chmod 0600 "$output"
}

capture_namespaces() {
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -cS '[.items[].metadata.name |
        select(startswith("loom-dev-") or startswith("loom-build-"))] | sort' \
    > "$1"
  chmod 0600 "$1"
}

capture_host "$evidence_dir/before-host.json"
capture_slurm "$evidence_dir/before-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/before-slurm.json"
capture_database_counts "$evidence_dir/before-database-counts.json"
capture_namespaces "$evidence_dir/before-namespaces.json"
"$repository_root/.venv/bin/loom" admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/before-capacity.status.json"
chmod 0600 "$evidence_dir/before-capacity.status.json"
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/before-capacity.status.json" >/dev/null
```

The personal control-plane status used later must include the exact canonical
fragments `"manager_ceiling":0` and `"worker_available":false`; neither
host activation nor agent registration may weaken them.

## 3. Prove the current public-store DNS/CIDR binding

The prepared profile and operational plan must contain the same exact HTTPS
origin and sorted public `/32` or `/128` endpoints. Resolve again from GB10 at
the start of the window; guessed, private, broad, stale, or extra addresses are
a stop condition.

```bash
public_store_host="$(python3 - "$reviewed_public_store_origin" <<'PY'
import sys
from urllib.parse import urlsplit

value = urlsplit(sys.argv[1])
if value.scheme != "https" or not value.hostname or value.path not in {"", "/"}:
    raise SystemExit(1)
if value.username or value.password or value.query or value.fragment:
    raise SystemExit(1)
print(value.hostname)
PY
)"

dns_raw="$evidence_dir/public-store-dns.raw"
{
  ssh "${ssh_options[@]}" "$gb10_target" -- getent ahostsv4 "$public_store_host" || true
  ssh "${ssh_options[@]}" "$gb10_target" -- getent ahostsv6 "$public_store_host" || true
} > "$dns_raw"
chmod 0600 "$dns_raw"

observed_public_store_cidrs="$(awk '{print $1}' "$dns_raw" | sort -u \
  | python3 -c 'import ipaddress,sys
values=[]
for line in sys.stdin:
    address=ipaddress.ip_address(line.strip())
    if not address.is_global:
        raise SystemExit(1)
    values.append(f"{address}/{address.max_prefixlen}")
if not values:
    raise SystemExit(1)
networks=[ipaddress.ip_network(value,strict=True) for value in set(values)]
print("\n".join(
    str(item) for item in sorted(
        networks,key=lambda item:(item.version,int(item.network_address))
    )
))')"
test "$observed_public_store_cidrs" = "$reviewed_public_store_cidrs"
jq -cnS --arg host "$public_store_host" \
  --arg origin "$reviewed_public_store_origin" \
  --arg cidrs "$observed_public_store_cidrs" \
  '{host:$host,origin:$origin,public_store_endpoint_cidrs:($cidrs|split("\n"))}' \
  > "$evidence_dir/public-store-dns.json"
chmod 0600 "$evidence_dir/public-store-dns.json"
rm -f "$dns_raw"
```

## 4. Download, root-stage, preflight, and install while inactive

Download without privilege. The installer independently validates the archive,
five members, host name, aarch64 identity, KVM, Docker 28.3.3, cgroup v2,
routes, identities, resources, and every generated byte.

```bash
archive="$evidence_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
archive_part="$archive.part"
curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  --connect-timeout 10 --max-time 1800 --max-filesize 1073741824 \
  --output "$archive_part" "$archive_url"
chmod 0600 "$archive_part"
test "$(sha512sum "$archive_part" | awk '{print $1}')" = "$archive_sha512"
mv -T "$archive_part" "$archive"

host_stage="/var/tmp/loom-native-builder-$merged_source_sha"
ssh "${ssh_options[@]}" "$gb10_target" -- /bin/sh -euc \
  'test ! -e "$1"; install -d -m 0700 "$1" "$1/scripts/ops" "$1/deploy/personal-dev-native-builder"' \
  sh "$host_stage"
scp "${ssh_options[@]}" \
  "$installer" "$converger" \
  scripts/ops/personal_dev_native_builder_runtime_profile.py \
  "$gb10_target:$host_stage/scripts/ops/"
scp "${ssh_options[@]}" deploy/personal-dev-native-builder/* \
  "$gb10_target:$host_stage/deploy/personal-dev-native-builder/"
scp "${ssh_options[@]}" "$archive" "$gb10_target:$host_stage/archive.tar.bz2"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  root_stage="$1"
  chown -R 0:0 "$root_stage"
  chmod 0700 "$root_stage" "$root_stage/scripts" "$root_stage/scripts/ops" "$root_stage/deploy" "$root_stage/deploy/personal-dev-native-builder"
  chmod 0600 "$root_stage/archive.tar.bz2"
  test "$(systemctl is-active loom-personal-dev-builder-dockerd.service 2>/dev/null || true)" != active
  test "$(systemctl is-active loom-personal-dev-native-builder-agent.service 2>/dev/null || true)" != active
' sh "$host_stage"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" preflight \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  --archive "$host_stage/archive.tar.bz2" \
  > "$evidence_dir/runtime-preflight.json"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" install \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  --archive "$host_stage/archive.tar.bz2" \
  > "$evidence_dir/runtime-install.json"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" verify-staged \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  > "$evidence_dir/runtime-verify-staged.json"

for service in loom-personal-dev-builder-dockerd.service \
  loom-personal-dev-native-builder-agent.service; do
  ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl is-active --quiet "$service" && exit 1
  test "$(ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl is-enabled "$service" 2>/dev/null || true)" = disabled
done
chmod 0600 "$evidence_dir"/runtime-*.json
```

## 5. Activate the daemon, converge images, and run two-container conformance

Loading the exact nftables table and starting the dedicated daemon are the
first live host mutations. They do not touch the primary daemon. Image
convergence retains only the exact current and previous trusted releases; it
uses repository, label, digest, platform, revision, and zero-container checks,
never daemon-wide garbage collection.

```bash
ssh "${ssh_options[@]}" "$gb10_target" -- sudo nft --file \
  /etc/loom/personal-dev-native-builder/provider-network.nft
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl start \
  loom-personal-dev-builder-dockerd.service

release_args=(
  --current-agent "$current_agent"
  --current-builder "$current_builder"
  --current-revision "$current_revision"
  "${previous_args[@]}"
)

ssh "${ssh_options[@]}" "$gb10_target" -- sudo python3 \
  "$host_stage/scripts/ops/converge_personal_dev_native_builder_release.py" plan \
  "${release_args[@]}" > "$evidence_dir/image-convergence.plan.json"
ssh "${ssh_options[@]}" "$gb10_target" -- sudo python3 \
  "$host_stage/scripts/ops/converge_personal_dev_native_builder_release.py" plan \
  "${release_args[@]}" > "$evidence_dir/image-convergence.recheck.json"
cmp -s "$evidence_dir/image-convergence.plan.json" \
  "$evidence_dir/image-convergence.recheck.json"
ssh "${ssh_options[@]}" "$gb10_target" -- sudo python3 \
  "$host_stage/scripts/ops/converge_personal_dev_native_builder_release.py" apply \
  "${release_args[@]}" > "$evidence_dir/image-convergence.apply.json"
ssh "${ssh_options[@]}" "$gb10_target" -- sudo python3 \
  "$host_stage/scripts/ops/converge_personal_dev_native_builder_release.py" verify \
  "${release_args[@]}" > "$evidence_dir/image-convergence.verify.json"
chmod 0600 "$evidence_dir"/image-convergence.*.json
```

The disposable probe uses two separate containers and therefore two separate
gVisor KVM sandboxes: a RootlessKit BuildKit server and a capability-free
client. It publishes no port, mounts no Docker socket, uses no host device in
either container, and proves native `linux/arm64` execution and private
cross-network denial. Candidate source is not used in this host-only probe;
the acceptance runbook proves actual source builds.

```bash
ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/bash -seu -- \
  "$current_builder" "$reviewed_public_store_origin" <<'REMOTE' \
  > "$evidence_dir/two-container-conformance.txt"
builder_image="$1"
public_https="$2"
docker_endpoint=unix:///run/loom-personal-dev-builder/docker.sock
network_name=loom-native-conformance
denied_network_name=loom-native-conformance-denied
buildkit_name=loom-native-conformance-buildkit
client_name=loom-native-conformance-client
denial_name=loom-native-conformance-denial-target
docker_native=(docker -H "$docker_endpoint")

cleanup_conformance() {
  "${docker_native[@]}" rm -f "$client_name" "$buildkit_name" "$denial_name" >/dev/null 2>&1 || true
  "${docker_native[@]}" network rm "$network_name" "$denied_network_name" >/dev/null 2>&1 || true
}
trap cleanup_conformance EXIT
cleanup_conformance

"${docker_native[@]}" network create --driver bridge --subnet 172.28.250.0/24 \
  --label loom.personal-dev-native-builder.managed=true "$network_name" >/dev/null
"${docker_native[@]}" network create --driver bridge --subnet 172.28.251.0/24 \
  --label loom.personal-dev-native-builder.managed=true "$denied_network_name" >/dev/null
"${docker_native[@]}" create --name "$buildkit_name" \
  --runtime runsc-personal-dev-native --network "$network_name" \
  --network-alias buildkit-0123456789ab \
  --hostname buildkit-0123456789ab --read-only --user 1000:1000 \
  --cgroup-parent loom-personal-dev-builder.slice \
  --cap-drop ALL --cap-add SETUID --cap-add SETGID \
  --security-opt seccomp=unconfined --cpus 3 --memory 17179869184 \
  --memory-swap 17179869184 --pids-limit 4096 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=2147483648,mode=1777 \
  --tmpfs /workspace/home:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000 \
  --entrypoint /usr/local/bin/loom-personal-dev-buildkitd \
  "$builder_image" --native-tcp-buildkit-child >/dev/null
"${docker_native[@]}" start "$buildkit_name" >/dev/null

for attempt in $(seq 1 60); do
  if "${docker_native[@]}" logs "$buildkit_name" 2>&1 \
      | grep -Fq 'loom-buildkitd-native-child-preflight nnp=1' &&
    "${docker_native[@]}" exec "$buildkit_name" \
      buildctl --addr tcp://127.0.0.1:1234 debug workers >/dev/null 2>&1; then
    break
  fi
  test "$attempt" != 60
  sleep 1
done

"${docker_native[@]}" create --name "$denial_name" \
  --runtime runsc-personal-dev-native --network "$denied_network_name" \
  --ip 172.28.251.10 --read-only --user 1000:1000 \
  --cgroup-parent loom-personal-dev-builder.slice \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --cpus 1 --memory 1073741824 --memory-swap 1073741824 --pids-limit 64 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000 \
  --entrypoint /usr/bin/python3 "$builder_image" \
  -m http.server 1234 --bind 0.0.0.0 >/dev/null
"${docker_native[@]}" start "$denial_name" >/dev/null

"${docker_native[@]}" create --name "$client_name" \
  --runtime runsc-personal-dev-native --network "$network_name" \
  --read-only --user 1000:1000 --cap-drop ALL \
  --cgroup-parent loom-personal-dev-builder.slice \
  --security-opt no-new-privileges:true --security-opt seccomp=default \
  --cpus 1 --memory 17179869184 --memory-swap 17179869184 --pids-limit 1024 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=1073741824,mode=1777 \
  --tmpfs /workspace:rw,nosuid,nodev,size=2147483648,mode=0700,uid=1000,gid=1000 \
  --env PUBLIC_HTTPS="$public_https" \
  --entrypoint /bin/sh "$builder_image" -euc \
  'test -r /proc/gvisor/kernel_is_gvisor
   test "$(uname -m)" = aarch64
   python3 -c "import os,urllib.error,urllib.request
try:
    response=urllib.request.urlopen(os.environ[\"PUBLIC_HTTPS\"],timeout=10)
except urllib.error.HTTPError as error:
    if not 400 <= error.code < 500:
        raise
    error.close()
else:
    response.close()"
   python3 -c "import socket
targets=((\"192.168.50.103\",6443),(\"172.28.251.10\",1234))
for target in targets:
    connection=socket.socket()
    connection.settimeout(2)
    try:
        connection.connect(target)
    except OSError:
        pass
    else:
        raise SystemExit(1)
    finally:
        connection.close()"
   exec buildctl --addr tcp://buildkit-0123456789ab:1234 debug workers' >/dev/null
"${docker_native[@]}" start -a "$client_name"
test "$("${docker_native[@]}" inspect --format '{{.State.ExitCode}}' "$client_name")" = 0

buildkit_container_id="$("${docker_native[@]}" inspect --format '{{.Id}}' "$buildkit_name")"
client_container_id="$("${docker_native[@]}" inspect --format '{{.Id}}' "$client_name")"
buildkit_sandbox_id="$buildkit_container_id"
client_sandbox_id="$client_container_id"
test "$buildkit_container_id" != "$client_container_id"
test "$buildkit_sandbox_id" != "$client_sandbox_id"
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Runtime}}' "$buildkit_name")" = runsc-personal-dev-native
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Runtime}}' "$client_name")" = runsc-personal-dev-native
test "$("${docker_native[@]}" image inspect --format '{{.Architecture}}' "$builder_image")" = arm64
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.CgroupParent}}' "$buildkit_name")" = loom-personal-dev-builder.slice
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.CgroupParent}}' "$client_name")" = loom-personal-dev-builder.slice
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.NanoCpus}}' "$buildkit_name")" = 3000000000
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.NanoCpus}}' "$client_name")" = 1000000000
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Memory}}' "$buildkit_name")" = 17179869184
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Memory}}' "$client_name")" = 17179869184
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Devices}}' "$buildkit_name")" = '[]'
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Devices}}' "$client_name")" = '[]'
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Binds}}' "$buildkit_name")" = null
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Binds}}' "$client_name")" = null
printf 'Runtime=runsc-personal-dev-native buildkit=%s client=%s architecture=arm64 platform=linux/arm64 kvm=/dev/kvm public_https=allowed private=denied cross_network=denied\n' \
  "$buildkit_container_id" "$client_container_id"
REMOTE
chmod 0600 "$evidence_dir/two-container-conformance.txt"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  endpoint=unix:///run/loom-personal-dev-builder/docker.sock
  test -z "$(docker -H "$endpoint" ps -aq --filter label=loom.personal-dev-native-builder.managed=true)"
  test -z "$(docker -H "$endpoint" network ls -q --filter label=loom.personal-dev-native-builder.managed=true)"
'
```

## 6. Return inactive, stage the agent, then activate it

The key staging operation refuses active services. Stop the dedicated daemon,
remove the exact nft table, and prove both units inactive before transferring
private material through SSH stdin. The path passed to the installer is a
root-owned host file; no secret value is an argument.

```bash
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl stop \
  loom-personal-dev-builder-dockerd.service
ssh "${ssh_options[@]}" "$gb10_target" -- sudo nft delete table inet \
  loom_personal_dev_builder
ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  systemctl is-active --quiet loom-personal-dev-builder-dockerd.service && exit 1
  systemctl is-active --quiet loom-personal-dev-native-builder-agent.service && exit 1
'

host_private_key="$host_stage/agent-ed25519"
host_service_ca="$host_stage/service-ca.pem"
sudo dd if="$agent_private_key" bs=32 count=1 status=none \
  | ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc \
  'umask 077; test ! -e "$1"; install -o 0 -g 0 -m 0400 /dev/stdin "$1"' \
  sh "$host_private_key"
sudo dd if="$service_ca" status=none \
  | ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc \
  'test ! -e "$1"; install -o 0 -g 0 -m 0444 /dev/stdin "$1"' \
  sh "$host_service_ca"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" stage-agent \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  --agent-image "$current_agent" \
  --builder-image "$current_builder" \
  --service-url "$reviewed_management_origin" \
  --agent-instance-id "$agent_instance_id" \
  --key-id "$agent_key_id" \
  --private-key "$host_private_key" \
  --ca-file "$host_service_ca" \
  > "$evidence_dir/agent-stage.json"
ssh "${ssh_options[@]}" "$gb10_target" -- sudo rm -f -- \
  "$host_private_key" "$host_service_ca"

emit_public_key() {
  sudo env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repository_root/src" \
    "$repository_root/.venv/bin/python" - "$agent_private_key" \
    "$agent_key_id" <<'PY'
import sys
from pathlib import Path

from loom.personal_dev_native_builder_protocol import (
    load_personal_dev_native_builder_signer,
)

signer = load_personal_dev_native_builder_signer(
    Path(sys.argv[1]), key_id=sys.argv[2]
)
sys.stdout.buffer.write(signer.public_key_bytes(sys.argv[2]))
PY
}

emit_public_key \
  | kubectl --kubeconfig "$kubeconfig" --namespace loom-dev create secret generic \
      "$native_builder_public_secret" \
      --from-file=public-key=/dev/stdin --dry-run=client -o yaml \
  | kubectl --kubeconfig "$kubeconfig" apply --server-side \
      --field-manager=loom-personal-dev-native-builder-public-key -f - \
      > "$evidence_dir/native-builder-public-key.apply.txt"
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get secret \
  "$native_builder_public_secret" \
  -o 'go-template={{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}')" = \
  public-key
chmod 0600 "$evidence_dir/native-builder-public-key.apply.txt"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" verify-staged \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  > "$evidence_dir/agent-verify-staged.json"
ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  systemctl is-active --quiet loom-personal-dev-builder-dockerd.service && exit 1
  systemctl is-active --quiet loom-personal-dev-native-builder-agent.service && exit 1
'

ssh "${ssh_options[@]}" "$gb10_target" -- sudo nft --file \
  /etc/loom/personal-dev-native-builder/provider-network.nft
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl start \
  loom-personal-dev-builder-dockerd.service
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl start \
  loom-personal-dev-native-builder-agent.service
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl is-active --quiet \
  loom-personal-dev-native-builder-agent.service

ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" verify-active \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  > "$evidence_dir/runtime-verify-active.json"
chmod 0600 "$evidence_dir/runtime-verify-active.json"

capture_host "$evidence_dir/after-host.json"
```

The agent is now active before management activation, but signed durable
readiness is not yet possible if the current management release has native mode
disabled. That is expected. Continue immediately with
`personal-dev-native-builder-acceptance.md`; its `signed-zero-grant-readiness`
gate occurs after management apply and before any owner request. The runtime
transaction is not accepted until that gate passes.

## 7. Capture after-state and seal evidence

Run this block after the acceptance runbook has reached signed zero-grant
readiness. The Slurm and Task/Worker counts must be unchanged, active native
grants and dynamic namespaces must be zero, the capacity manager remains at
ceiling zero, and both host units have their exact active identity.

```bash
capture_slurm "$evidence_dir/after-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/after-slurm.json"
capture_database_counts "$evidence_dir/after-database-counts.json"
capture_namespaces "$evidence_dir/after-namespaces.json"
"$repository_root/.venv/bin/loom" admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/after-capacity.status.json"
chmod 0600 "$evidence_dir/after-capacity.status.json"

jq -e --slurpfile before "$evidence_dir/before-database-counts.json" '
  .tasks == $before[0].tasks and .workers == $before[0].workers and
  .active_native_grants == 0' "$evidence_dir/after-database-counts.json" >/dev/null
jq -e '. == []' "$evidence_dir/after-namespaces.json" >/dev/null
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/after-capacity.status.json" >/dev/null

(
  cd "$evidence_dir"
  find . -maxdepth 1 -type f ! -name kubeconfig \
    ! -name evidence-index.sha256 -printf '%P\n' \
    | LC_ALL=C sort \
    | while IFS= read -r file; do sha256sum "$file"; done \
    > evidence-index.sha256
  chmod 0600 evidence-index.sha256
)
```

The evidence index is sanitized. Secret values are never sealed, uploaded, or
included in review evidence. Keep the kubeconfig and raw operator credentials
outside the index.

## Rollback to the exact inert shadow

Rollback is permitted only with zero active grants and no personal/build
namespace. First apply the exact reviewed shadow to disable new provider claims.
Then stop the agent before disabling the dedicated daemon, remove the exact nft
table, and ask the installer to remove only byte-identical managed runtime
files. It never restarts or alters the primary Docker daemon.

```bash
rollback_shadow_recheck="$evidence_dir/rollback-shadow.recheck.yaml"
install -m 0600 "$rollback_shadow_manifest" "$rollback_shadow_recheck"
cmp -s "$rollback_shadow_recheck" "$rollback_shadow_manifest"
test "$(sha256sum "$rollback_shadow_recheck" | awk '{print $1}')" = \
  "$rollback_shadow_sha256"

capture_database_counts "$evidence_dir/rollback-pre-counts.json"
jq -e '.active_native_grants == 0' \
  "$evidence_dir/rollback-pre-counts.json" >/dev/null
capture_namespaces "$evidence_dir/rollback-pre-namespaces.json"
jq -e '. == []' "$evidence_dir/rollback-pre-namespaces.json" >/dev/null

kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback-shadow.diff.txt" 2>&1 || rollback_diff_status=$?
test "${rollback_diff_status:-0}" -eq 0 || test "$rollback_diff_status" -eq 1
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback-shadow.apply.txt"

ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl stop \
  loom-personal-dev-native-builder-agent.service
ssh "${ssh_options[@]}" "$gb10_target" -- sudo systemctl stop \
  loom-personal-dev-builder-dockerd.service
ssh "${ssh_options[@]}" "$gb10_target" -- sudo nft delete table inet \
  loom_personal_dev_builder
ssh "${ssh_options[@]}" "$gb10_target" -- sudo env PYTHONPATH="$host_stage" \
  python3 "$host_stage/scripts/ops/install_personal_dev_native_builder_runtime.py" remove \
  --profile "$host_stage/deploy/personal-dev-native-builder/runtime-profile-v1.json" \
  > "$evidence_dir/runtime-remove.json"

"$repository_root/.venv/bin/loom" admin personal-dev-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  --file deploy/dev-fleet/personal-dev-control-plane.toml \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$evidence_dir/rollback-shadow.status.json"
chmod 0600 "$evidence_dir"/rollback-*
```

If a grant, managed container, network, namespace, changed byte, or unexpected
unit remains, stop. Do not broaden selectors or improvise cleanup.
