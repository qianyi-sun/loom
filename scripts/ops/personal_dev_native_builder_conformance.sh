#!/bin/bash
set -euo pipefail
export LC_ALL=C

test "$#" -eq 3
builder_image="$1"
agent_image="$2"
public_https="$3"

[[ "$builder_image" =~ ^ghcr\.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]
[[ "$agent_image" =~ ^ghcr\.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$ ]]
[[ "$public_https" =~ ^https://[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:443)?/?$ ]]
[[ "$public_https" != "https://loom-service.dev.yylx.world" ]]

docker_endpoint=unix:///run/loom-personal-dev-builder/docker.sock
network_name=loom-native-conformance
denied_network_name=loom-native-conformance-denied
buildkit_name=loom-native-conformance-buildkit
client_name=loom-native-conformance-client
denial_name=loom-native-conformance-denial-target
foreign_client_name=loom-native-conformance-foreign-client
docker_native=(/usr/bin/docker -H "$docker_endpoint")
docker_primary=(/usr/bin/docker -H unix:///var/run/docker.sock)
provider_network_id=
denied_network_id=
buildkit_container_id=
client_container_id=
denial_container_id=
foreign_client_id=

cleanup_conformance() {
  test -z "$foreign_client_id" || \
    "${docker_primary[@]}" rm -f "$foreign_client_id" >/dev/null 2>&1 || true
  test -z "$client_container_id" || \
    "${docker_native[@]}" rm -f "$client_container_id" >/dev/null 2>&1 || true
  test -z "$denial_container_id" || \
    "${docker_native[@]}" rm -f "$denial_container_id" >/dev/null 2>&1 || true
  test -z "$buildkit_container_id" || \
    "${docker_native[@]}" rm -f "$buildkit_container_id" >/dev/null 2>&1 || true
  test -z "$provider_network_id" || \
    "${docker_native[@]}" network rm "$provider_network_id" >/dev/null 2>&1 || true
  test -z "$denied_network_id" || \
    "${docker_native[@]}" network rm "$denied_network_id" >/dev/null 2>&1 || true
}
trap cleanup_conformance EXIT

provider_network_id="$("${docker_native[@]}" network create --driver bridge \
  --subnet 172.28.0.0/24 \
  --label loom.personal-dev-native-builder.managed=true "$network_name")"
denied_network_id="$("${docker_native[@]}" network create --driver bridge \
  --subnet 172.28.1.0/24 \
  --label loom.personal-dev-native-builder.managed=true "$denied_network_name")"
buildkit_container_id="$("${docker_native[@]}" create --name "$buildkit_name" \
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
  "$builder_image" --native-tcp-buildkit-child)"
"${docker_native[@]}" start "$buildkit_container_id" >/dev/null

for attempt in $(seq 1 60); do
  if "${docker_native[@]}" logs "$buildkit_container_id" 2>&1 \
      | grep -Fq 'loom-buildkitd-native-child-preflight nnp=1' &&
    "${docker_native[@]}" exec "$buildkit_container_id" \
      buildctl --addr tcp://127.0.0.1:1234 debug workers >/dev/null 2>&1; then
    break
  fi
  test "$attempt" != 60
  sleep 1
done

buildkit_ip="$("${docker_native[@]}" inspect --format \
  '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  "$buildkit_container_id")"
case "$buildkit_ip" in
  172.28.*) ;;
  *) exit 1 ;;
esac
/usr/bin/python3 - "$buildkit_ip" <<'PY'
import socket
import sys

try:
    connection = socket.create_connection((sys.argv[1], 1234), timeout=2)
except OSError:
    pass
else:
    connection.close()
    raise SystemExit("host unexpectedly reached provider BuildKit")
PY

foreign_client_id="$("${docker_primary[@]}" create \
  --name "$foreign_client_name" --network bridge \
  --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  --cpus 1 --memory 268435456 --memory-swap 268435456 --pids-limit 64 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=16777216,mode=0700 \
  --entrypoint python "$agent_image" -c \
  'import socket,sys
try:
    connection=socket.create_connection((sys.argv[1],1234),timeout=2)
except OSError:
    raise SystemExit(0)
connection.close()
raise SystemExit(1)' "$buildkit_ip")"
"${docker_primary[@]}" start -a "$foreign_client_id"
test "$("${docker_primary[@]}" inspect --format '{{.State.ExitCode}}' \
  "$foreign_client_id")" = 0

denial_container_id="$("${docker_native[@]}" create --name "$denial_name" \
  --runtime runsc-personal-dev-native --network "$denied_network_name" \
  --ip 172.28.1.10 --read-only --user 1000:1000 \
  --cgroup-parent loom-personal-dev-builder.slice \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --cpus 1 --memory 1073741824 --memory-swap 1073741824 --pids-limit 64 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000 \
  --entrypoint /usr/bin/python3 "$builder_image" \
  -m http.server 1234 --bind 0.0.0.0)"
"${docker_native[@]}" start "$denial_container_id" >/dev/null

# The single-quoted payload expands only inside the disposable client container.
# shellcheck disable=SC2016
client_container_id="$("${docker_native[@]}" create --name "$client_name" \
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
targets=((\"192.168.50.103\",6443),(\"172.28.1.10\",1234))
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
   exec buildctl --addr tcp://buildkit-0123456789ab:1234 debug workers')"
"${docker_native[@]}" start -a "$client_container_id"
test "$("${docker_native[@]}" inspect --format '{{.State.ExitCode}}' \
  "$client_container_id")" = 0

test "$buildkit_container_id" != "$client_container_id"
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Runtime}}' \
  "$buildkit_container_id")" = runsc-personal-dev-native
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Runtime}}' \
  "$client_container_id")" = runsc-personal-dev-native
test "$("${docker_native[@]}" image inspect --format '{{.Architecture}}' \
  "$builder_image")" = arm64
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.CgroupParent}}' \
  "$buildkit_container_id")" = loom-personal-dev-builder.slice
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.CgroupParent}}' \
  "$client_container_id")" = loom-personal-dev-builder.slice
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.NanoCpus}}' \
  "$buildkit_container_id")" = 3000000000
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.NanoCpus}}' \
  "$client_container_id")" = 1000000000
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Memory}}' \
  "$buildkit_container_id")" = 17179869184
test "$("${docker_native[@]}" inspect --format '{{.HostConfig.Memory}}' \
  "$client_container_id")" = 17179869184
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Devices}}' \
  "$buildkit_container_id")" = '[]'
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Devices}}' \
  "$client_container_id")" = '[]'
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Binds}}' \
  "$buildkit_container_id")" = null
test "$("${docker_native[@]}" inspect --format '{{json .HostConfig.Binds}}' \
  "$client_container_id")" = null

printf 'Runtime=runsc-personal-dev-native architecture=arm64 platform=linux/arm64 kvm=/dev/kvm public_https=allowed private=denied host_to_provider=denied foreign_to_provider=denied cross_network=denied\n'
