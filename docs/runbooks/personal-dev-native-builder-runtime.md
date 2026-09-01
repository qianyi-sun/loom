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
2. stream the measured archive to the fixed no-argument root authority, which
   stages the runtime, temporarily converges the exact current and previous
   images, runs the single disposable two-container conformance script, and
   returns both dedicated services to inactive state;
3. stream the private key and CA through that same bounded stdin protocol while
   both services remain inactive;
4. activate and verify only the dedicated daemon and agent through the same
   fixed authority;
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
previous_trusted_release_sha256='<previous-trusted-release-64-lowercase-hex-or-empty>'
runtime_window_id='<authorized-native-runtime-window-id>'
reviewed_kubeconfig='<absolute-owner-only-mode-0600-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
gb10_target='<ssh-user>@gx10-01c7'
slurm_observer='<read-only-slurm-observer-ssh-target>'

runtime_profile='deploy/personal-dev-native-builder/runtime-profile-v1.json'
runtime_profile_sha256='c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d'
prepared_control_profile='<absolute-owner-only-prepared-schema-3-profile.toml>'
prepared_control_profile_sha256='<prepared-profile-64-lowercase-hex>'
native_runtime_authority='/usr/local/libexec/loom-personal-dev-native-runtime-authority'
native_runtime_request_schema='loom.personal-dev-native-runtime-authority.request.v1'
archive_url='https://storage.googleapis.com/gvisor/releases/release/20260810/aarch64/gvisor.tar.bz2'
archive_sha512='dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7'
agent_private_key='<absolute-root-owned-mode-0400-ed25519-private-key>'
service_ca='<absolute-root-owned-mode-0444-service-ca>'
rollback_shadow_manifest='<absolute-byte-reviewed-schema-4-shadow-manifest>'
rollback_shadow_sha256='<rollback-shadow-64-lowercase-hex>'

repository_root="$(pwd -P)"
merged_source_tree="$(git rev-parse 'HEAD^{tree}')"
loom_python="$repository_root/.venv/bin/python"
loom_cli() {
  env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1 \
    PYTHONPATH="$repository_root/src" "$loom_python" -m loom_cli "$@"
}
verify_loom_cli_source() {
  env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1 \
    PYTHONPATH="$repository_root/src" "$loom_python" - "$repository_root" <<'PY'
import sys
from pathlib import Path

import loom
import loom_cli

root = Path(sys.argv[1]).resolve(strict=True)
expected_loom = root / "src" / "loom" / "__init__.py"
expected_loom_cli = root / "src" / "loom_cli" / "__init__.py"
observed_loom = Path(loom.__file__).resolve(strict=True)
observed_loom_cli = Path(loom_cli.__file__).resolve(strict=True)
if observed_loom != expected_loom or observed_loom_cli != expected_loom_cli:
    raise SystemExit(1)
PY
}
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="$evidence_root/${timestamp}-${merged_source_sha}"
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
ssh_run() {
  local target="$1"
  local remote_command
  shift
  remote_command="$(python3 - "$@" <<'PY'
import shlex
import sys

if len(sys.argv) < 2:
    raise SystemExit(1)
sys.stdout.write(shlex.join(sys.argv[1:]))
PY
)"
  test -n "$remote_command"
  ssh "${ssh_options[@]}" "$target" -- "$remote_command"
}
native_runtime_header() {
  local action="$1"
  local request_id="$2"
  local fields="$3"
  jq -cnS \
    --arg action "$action" \
    --arg request_id "$request_id" \
    --arg schema "$native_runtime_request_schema" \
    --arg source_sha "$merged_source_sha" \
    --arg source_tree_sha "$merged_source_tree" \
    --argjson fields "$fields" \
    '$fields + {action:$action,request_id:$request_id,schema:$schema,
      source_sha:$source_sha,source_tree_sha:$source_tree_sha}'
}

test "$merged_source_sha" != '<merged-40-lowercase-hex>'
test "$trusted_release_sha256" != '<trusted-release-64-lowercase-hex>'
test "$runtime_window_id" != '<authorized-native-runtime-window-id>'
test "$(git rev-parse --show-toplevel)" = "$repository_root"
test "$(git rev-parse HEAD)" = "$merged_source_sha"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test -x "$loom_python"
verify_loom_cli_source
test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = \
  "$runtime_profile_sha256"

for path in "$trusted_release" "$reviewed_kubeconfig" "$prepared_control_profile" \
  "$rollback_shadow_manifest"; do
  test -f "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
done
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test "$(jq -er .source_sha "$trusted_release")" = "$merged_source_sha"
test "$(jq -er .schema_version "$trusted_release")" = 4
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
    or public_store.hostname == management.hostname
    or public_store.port not in {None, 443}
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
  [[ "$previous_trusted_release_sha256" =~ ^[0-9a-f]{64}$ ]]
  test -f "$previous_trusted_release"
  test ! -L "$previous_trusted_release"
  test "$(realpath -e "$previous_trusted_release")" = "$previous_trusted_release"
  test "$(stat -c %u "$previous_trusted_release")" = "$(id -u)"
  test "$(stat -c %a "$previous_trusted_release")" = 600
  test "$(stat -c %h "$previous_trusted_release")" = 1
  test "$(sha256sum "$previous_trusted_release" | awk '{print $1}')" = \
    "$previous_trusted_release_sha256"
  test "$(jq -er .schema_version "$previous_trusted_release")" = 4
  previous_agent="$(jq -er .images.personal_dev_native_builder_agent "$previous_trusted_release")"
  previous_builder="$(jq -er .images.personal_dev_builder "$previous_trusted_release")"
  previous_revision="$(jq -er .source_sha "$previous_trusted_release")"
  [[ "$previous_agent" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$ ]]
  [[ "$previous_builder" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]
  [[ "$previous_revision" =~ ^[0-9a-f]{40}$ ]]
  test "$previous_revision" != "$current_revision"
  previous_args=(
    --previous-agent "$previous_agent"
    --previous-builder "$previous_builder"
    --previous-revision "$previous_revision"
  )
else
  test -z "$previous_trusted_release_sha256"
fi

jq -cnS \
  --arg source "$merged_source_sha" \
  --arg tree "$(git rev-parse HEAD^{tree})" \
  --arg release "$trusted_release_sha256" \
  --arg previous_release "$previous_trusted_release_sha256" \
  --arg profile "$runtime_profile_sha256" \
  --arg prepared_profile "$prepared_control_profile_sha256" \
  --arg archive "$archive_sha512" \
  --arg window "$runtime_window_id" \
  '{archive_sha512:$archive,prepared_profile_sha256:$prepared_profile,
    profile_sha256:$profile,
    source_sha:$source,source_tree:$tree,
    trusted_release_sha256:$release,
    previous_trusted_release_sha256:$previous_release,window_id:$window}' \
  > "$evidence_dir/immutable-inputs.json"
chmod 0600 "$evidence_dir/immutable-inputs.json"
```

Validate the private material only on the protected operator host. Do not
record either file's bytes or private-key digest in issue comments.

```bash
validate_protected_material_metadata() {
  sudo /bin/sh -euc '
    key="$1"
    ca="$2"
    test -f "$key"
    test ! -L "$key"
    test "$(realpath -e "$key")" = "$key"
    test "$(stat -c %u "$key")" = 0
    test "$(stat -c %g "$key")" = 0
    test "$(stat -c %a "$key")" = 400
    test "$(stat -c %s "$key")" = 32
    test "$(stat -c %h "$key")" = 1
    test -f "$ca"
    test ! -L "$ca"
    test "$(realpath -e "$ca")" = "$ca"
    test "$(stat -c %u "$ca")" = 0
    test "$(stat -c %g "$ca")" = 0
    test "$(stat -c %a "$ca")" = 444
    test "$(stat -c %h "$ca")" = 1
  ' sh "$agent_private_key" "$service_ca"
}

validate_protected_material_metadata

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
  ssh_run "$gb10_target" /bin/sh -euc '
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
  ssh_run "$slurm_observer" scontrol show nodes --json \
    | jq -cS . > "$output"
  ssh_run "$slurm_observer" squeue --json \
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
loom_cli admin capacity-control-plane status \
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
origin on external port 443 and sorted public `/32` or `/128` endpoints. The
prepared shadow must already own the TLS Ingress while routing it to the
selectorless disabled Service; only an acceptance or operational manifest may
route it to MinIO port 9000 and open the matching ingress rule. Port 9001 must
remain absent. Resolve again from GB10 at the start of the window; guessed,
private, broad, stale, or extra addresses are a stop condition.

```bash
public_store_host="$(python3 - "$reviewed_public_store_origin" <<'PY'
import sys
from urllib.parse import urlsplit

value = urlsplit(sys.argv[1])
if (
    value.scheme != "https"
    or not value.hostname
    or value.port not in {None, 443}
    or value.path not in {"", "/"}
):
    raise SystemExit(1)
if value.username or value.password or value.query or value.fragment:
    raise SystemExit(1)
print(value.hostname)
PY
)"

dns_raw="$evidence_dir/public-store-dns.raw"
{
  ssh_run "$gb10_target" getent ahostsv4 "$public_store_host" || true
  ssh_run "$gb10_target" getent ahostsv6 "$public_store_host" || true
} > "$dns_raw"
chmod 0600 "$dns_raw"

normalize_public_store_cidrs() {
  python3 -c 'import ipaddress,sys
values=[]
for line in sys.stdin:
    address=ipaddress.ip_address(line.strip())
    if address.version == 6 and address.ipv4_mapped is not None:
        address=address.ipv4_mapped
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
))'
}

observed_public_store_cidrs="$(awk '{print $1}' "$dns_raw" | sort -u \
  | normalize_public_store_cidrs)"
test "$observed_public_store_cidrs" = "$reviewed_public_store_cidrs"
jq -cnS --arg host "$public_store_host" \
  --arg origin "$reviewed_public_store_origin" \
  --arg cidrs "$observed_public_store_cidrs" \
  '{host:$host,origin:$origin,public_store_endpoint_cidrs:($cidrs|split("\n"))}' \
  > "$evidence_dir/public-store-dns.json"
chmod 0600 "$evidence_dir/public-store-dns.json"
rm -f "$dns_raw"
```

## 4. Bootstrap the sealed authority once, then prepare while inactive

The checked-in installer, converger, profile, and conformance script are the
only runtime implementation. The authority does not copy that logic. It
validates a root-owned sealed checkout, invokes those exact assets with a clean
environment, and accepts no command-line arguments through sudo.

Before the first request for a new authority source, an external administrator
must provision the exact clean checkout at
`/opt/loom-personal-dev-native-runtime-authority/source` as root-owned mode
`0700` and run the following from a **direct root login**, not through sudo.
This is the only broad root bootstrap. It installs sudoers last and is not
reachable through the operator rule.

```bash
cd /opt/loom-personal-dev-native-runtime-authority/source
test "$(id -u)" = 0
test -z "${SUDO_USER:-}${SUDO_UID:-}${SUDO_GID:-}${SUDO_COMMAND:-}"
test "$(stat -c %U:%G:%a .)" = root:root:700
test "$(git rev-parse HEAD)" = '<merged-40-lowercase-hex>'
test "$(git rev-parse 'HEAD^{tree}')" = '<merged-tree-40-lowercase-hex>'
test -z "$(git status --porcelain=v1 --untracked-files=all)"
python3 scripts/ops/personal_dev_native_runtime_authority.py bootstrap \
  --source-sha '<merged-40-lowercase-hex>' \
  --source-tree-sha '<merged-tree-40-lowercase-hex>'
/usr/sbin/visudo -cf \
  /etc/sudoers.d/loom-personal-dev-native-runtime-authority
```

Do not bootstrap from an operator-owned checkout, replace the fixed sudoers
line, or install a second copy of the installer. A future source upgrade repeats
this direct-root sealed-source transaction; it never edits the installed
policy or wrapper in place.

Download the measured gVisor archive without privilege. The `prepare` request
then streams the archive through bounded stdin. The fixed authority verifies
its SHA-512, runs `preflight`, `install`, and `verify-staged`, starts only the
dedicated daemon, runs the release plan twice, applies/verifies it, invokes the
single checked-in two-container conformance script, and returns the host to an
inactive state in a `finally` boundary.

```bash
archive="$evidence_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
archive_part="$archive.part"
curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  --connect-timeout 10 --max-time 1800 --max-filesize 1073741824 \
  --output "$archive_part" "$archive_url"
chmod 0600 "$archive_part"
test "$(sha512sum "$archive_part" | awk '{print $1}')" = "$archive_sha512"
mv -T "$archive_part" "$archive"
archive_size="$(stat -c %s "$archive")"
test "$archive_size" -gt 0
test "$archive_size" -le 1073741824

prepare_fields="$(jq -cnS \
  --arg archive_sha512 "$archive_sha512" \
  --argjson archive_size "$archive_size" \
  --arg current_agent "$current_agent" \
  --arg current_builder "$current_builder" \
  --arg current_revision "$current_revision" \
  --arg previous_agent "${previous_agent:-}" \
  --arg previous_builder "${previous_builder:-}" \
  --arg previous_revision "${previous_revision:-}" \
  --arg public_store_origin "$reviewed_public_store_origin" \
  '{archive_sha512:$archive_sha512,archive_size:$archive_size,
    current_agent:$current_agent,current_builder:$current_builder,
    current_revision:$current_revision,previous_agent:$previous_agent,
    previous_builder:$previous_builder,previous_revision:$previous_revision,
    public_store_origin:$public_store_origin}')"
prepare_header="$(native_runtime_header \
  prepare "$runtime_window_id-prepare" "$prepare_fields")"

{
  printf '%s\n' "$prepare_header"
  dd if="$archive" status=none
} | ssh_run "$gb10_target" sudo "$native_runtime_authority" \
  > "$evidence_dir/native-runtime-prepare.json"
chmod 0600 "$evidence_dir/native-runtime-prepare.json"
jq -e --arg source "$merged_source_sha" '
  .action=="prepare" and .status=="ok" and .source_sha==$source and
  (.receipts|keys)==[
    "release-apply","release-plan","release-verify",
    "runtime-install","runtime-preflight","runtime-verify-staged",
    "two-container-conformance"] and
  all(.receipts[];test("^[0-9a-f]{64}$"))' \
  "$evidence_dir/native-runtime-prepare.json" >/dev/null
```

## 5. Stream the agent secrets through the fixed authority while inactive

The prepare action leaves both dedicated units inactive and the exact nftables
table absent. The stage-agent action re-verifies that boundary, reads exactly
32 private-key bytes followed by the declared bounded CA bytes from stdin, and
passes only root-owned temporary paths to the existing installer. Secret bytes
never appear in argv, stdout, the receipt, or the journal.

```bash
test "$(sudo stat -c %s "$agent_private_key")" = 32
ca_size="$(sudo stat -c %s "$service_ca")"
test "$ca_size" -gt 0
test "$ca_size" -le 1048576

stage_fields="$(jq -cnS \
  --arg agent_instance_id "$agent_instance_id" \
  --arg current_agent "$current_agent" \
  --arg current_builder "$current_builder" \
  --arg key_id "$agent_key_id" \
  --arg service_url "$reviewed_management_origin" \
  --argjson ca_size "$ca_size" \
  '{agent_instance_id:$agent_instance_id,ca_size:$ca_size,
    current_agent:$current_agent,current_builder:$current_builder,
    key_id:$key_id,private_key_size:32,service_url:$service_url}')"
stage_header="$(native_runtime_header \
  stage-agent "$runtime_window_id-stage-agent" "$stage_fields")"

{
  printf '%s\n' "$stage_header"
  sudo dd if="$agent_private_key" bs=32 count=1 iflag=fullblock status=none
  sudo dd if="$service_ca" bs=1 count="$ca_size" iflag=fullblock status=none
} | ssh_run "$gb10_target" sudo "$native_runtime_authority" \
  > "$evidence_dir/native-runtime-stage-agent.json"
chmod 0600 "$evidence_dir/native-runtime-stage-agent.json"
jq -e --arg source "$merged_source_sha" '
  .action=="stage-agent" and .status=="ok" and .source_sha==$source and
  (.receipts|keys)==["agent-stage","runtime-verify-staged"] and
  all(.receipts[];test("^[0-9a-f]{64}$"))' \
  "$evidence_dir/native-runtime-stage-agent.json" >/dev/null

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
```

## 6. Activate through the same no-argument authority

Activation first repeats staged verification, then loads only the fixed nftables
file and starts only the dedicated daemon and agent. Any failure runs the fixed
deactivation path before returning a generic error. A separate check request
proves the final installed state without granting broader command authority.

```bash
activate_header="$(native_runtime_header \
  activate "$runtime_window_id-activate" '{}')"
printf '%s\n' "$activate_header" \
  | ssh_run "$gb10_target" sudo "$native_runtime_authority" \
  > "$evidence_dir/native-runtime-activate.json"
chmod 0600 "$evidence_dir/native-runtime-activate.json"
jq -e --arg source "$merged_source_sha" '
  .action=="activate" and .status=="ok" and .source_sha==$source and
  (.receipts|keys)==["runtime-verify-active","runtime-verify-staged"] and
  all(.receipts[];test("^[0-9a-f]{64}$"))' \
  "$evidence_dir/native-runtime-activate.json" >/dev/null

check_fields='{"expected_state":"active"}'
check_header="$(native_runtime_header \
  check "$runtime_window_id-check-active" "$check_fields")"
printf '%s\n' "$check_header" \
  | ssh_run "$gb10_target" sudo "$native_runtime_authority" \
  > "$evidence_dir/native-runtime-check-active.json"
chmod 0600 "$evidence_dir/native-runtime-check-active.json"
jq -e --arg source "$merged_source_sha" '
  .action=="check" and .status=="ok" and .source_sha==$source and
  (.receipts|keys)==["runtime-verify-active"] and
  all(.receipts[];test("^[0-9a-f]{64}$"))' \
  "$evidence_dir/native-runtime-check-active.json" >/dev/null

capture_host "$evidence_dir/after-host.json"
```

The agent is now active before management activation, but signed durable
readiness is not yet possible if the current management release has native mode
disabled. That is expected. Continue immediately with
personal-dev-native-builder-acceptance.md; its signed-zero-grant-readiness gate
occurs after management apply and before any owner request. The runtime
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
loom_cli admin capacity-control-plane status \
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
files. The dedicated image cache and system identities are retained as inert
state: removal never recursively deletes Docker data or accounts. The removal
receipt therefore reports `managed-files-absent`, not whole-host absence. It
never restarts or alters the primary Docker daemon.

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

remove_header="$(native_runtime_header \
  remove "$runtime_window_id-remove" '{}')"
printf '%s\n' "$remove_header" \
  | ssh_run "$gb10_target" sudo "$native_runtime_authority" \
  > "$evidence_dir/runtime-remove.json"
jq -e --arg source "$merged_source_sha" '
  .action=="remove" and .status=="ok" and .source_sha==$source and
  (.receipts|keys)==["runtime-remove"] and
  all(.receipts[];test("^[0-9a-f]{64}$"))' \
  "$evidence_dir/runtime-remove.json" >/dev/null

loom_cli admin personal-dev-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  --file deploy/dev-fleet/personal-dev-control-plane.toml \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$evidence_dir/rollback-shadow.status.json"
chmod 0600 "$evidence_dir"/rollback-*
```

If a grant, managed container, network, namespace, changed byte, or unexpected
unit remains, stop. Do not broaden selectors or improvise cleanup.
