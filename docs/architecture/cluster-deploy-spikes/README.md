# Cluster deployment regression probes

These executable probes verify the Docker, Kubernetes, CNI, DNS, and TLS
mechanisms used by the current
[`cluster-deploy.md`](../cluster-deploy.md) contract. Each script is a
standalone regression check: it creates disposable resources, reports each
claim as `PASS` or `FAIL`, cleans up through a shell `trap`, and exits non-zero
when a required mechanism is unavailable.

## Contracts

| Probe | Current contract |
|---|---|
| [`01-sandbox-bridge.sh`](01-sandbox-bridge.sh) | A gateway singleton can join a normal uplink bridge and a per-trial `--internal` bridge. A sandbox on only the internal bridge reaches the singleton's pinned IP but has no route to the internet or the host's uplink address. |
| [`02-preflight-hostpath.sh`](02-preflight-hostpath.sh) | A Kubernetes preflight Job mounts `/var/run` as a directory, tests for `docker.sock` without making the socket a scheduling prerequisite, uses the host network, and spreads one probe per worker node. |
| [`03-hostport-from-bridge.sh`](03-hostport-from-bridge.sh) | A container on the Docker uplink bridge can reach the gateway router's mapped host port. The route is CNI- and host-routing-dependent, so protected deployment preflight must test it on the target cluster. |
| [`04-jwt-fsnotify-rotation.sh`](04-jwt-fsnotify-rotation.sh) | Step-JWT refresh uses a host bind mount and atomic rename with no partial read. `docker cp` into an active tmpfs mount is explicitly rejected. |
| [`05-add-host-ssl-cert-file.sh`](05-add-host-ssl-cert-file.sh) | Docker `--add-host`, a bind-mounted Loom CA certificate, and `SSL_CERT_FILE`/`CURL_CA_BUNDLE` provide the sandbox-to-gateway DNS and TLS path without SDK changes. |

The corresponding architecture is described under
[Sandbox-to-Gateway flow](../cluster-deploy.md#sandbox-to-gateway-flow) and
[Preflight and diagnosis](../cluster-deploy.md#preflight-and-diagnosis).

## Run locally

All probes require Docker. Probes 02 and 03 also require `kind` and `kubectl`.

```bash
bash docs/architecture/cluster-deploy-spikes/01-sandbox-bridge.sh
bash docs/architecture/cluster-deploy-spikes/02-preflight-hostpath.sh
bash docs/architecture/cluster-deploy-spikes/03-hostport-from-bridge.sh
bash docs/architecture/cluster-deploy-spikes/04-jwt-fsnotify-rotation.sh
bash docs/architecture/cluster-deploy-spikes/05-add-host-ssl-cert-file.sh
```

Probe 03 is an environment check as well as a regression check. Failure means
the target's CNI or host routing does not provide the required bridge-to-host
path; do not deploy the singleton path until the operator configures a reviewed
alternative and the probe passes.

## Continuous integration

The
[`cluster-deploy-spikes.yml`](../../../.github/workflows/cluster-deploy-spikes.yml)
workflow runs probes 01, 04, and 05 in its Docker job and probes 02 and 03 in a
kind cluster. It runs for pull requests that change the cluster deployment
reference, these probes, or the workflow, and it also supports manual dispatch.

Changes to a load-bearing deployment mechanism must update or add the probe
that exercises that mechanism. Pure prose changes and behavior already covered
by existing repository tests do not require another probe.
