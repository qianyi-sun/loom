# cluster-deploy spikes

Executable proofs that the load-bearing mechanisms in
[`../cluster-deploy.md`](../cluster-deploy.md) actually compose with the
underlying primitives (Docker, k8s, CNI) they claim to use.

## Why

Spec review by humans (including the author) repeatedly missed mechanisms
that didn't work — Docker `--network host` + bridge attachments (rev 6),
`docker network connect` to a k8s pod (rev 5), `hostPath` socket mounts
that strand pods in `ContainerCreating` (rev 9). Each was a 5-minute test
away from being caught at design time. These spikes are the 5-minute tests.

Run any spike to empirically verify the corresponding spec claim against
your local Docker / k8s. If a spike fails, the spec is wrong.

## What's verified

| Spike | Spec section | Claim |
|---|---|---|
| [`01-sandbox-bridge.sh`](01-sandbox-bridge.sh) | §Sandbox→gateway (a) | A singleton attached to one `--internal` and one normal bridge serves the per-trial sandbox while remaining reachable on the uplink. Sandbox on `--internal` cannot reach the internet OR the host's IP on the uplink bridge. |
| [`02-preflight-hostpath.sh`](02-preflight-hostpath.sh) | §Prerequisites | A k8s Job with `hostPath: /var/run` (type Directory) + `test -S` schedules and reports docker.sock presence regardless of whether Docker is installed. `topologySpreadConstraints` lands one pod per worker without pre-existing role labels. |
| [`03-hostport-from-bridge.sh`](03-hostport-from-bridge.sh) | §Sandbox→gateway (a), §K8s manifest changes | A Docker bridge container can reach a k8s `hostPort: 30443` pod via the host's IP on the bridge. This is the load-bearing routing claim for the singleton→gateway path. |
| [`04-jwt-fsnotify-rotation.sh`](04-jwt-fsnotify-rotation.sh) | §Sandbox→gateway (b) JWT refresh | Bind-mount + host-side atomic-rename rotation is visible to the container without partial reads. **Caught a real spec bug**: revs 7–11's `docker cp` to tmpfs mount doesn't work; spec was updated. The spike's negative assertion verifies the broken mechanism stays broken (alerts if Docker ever fixes it). |
| [`05-add-host-ssl-cert-file.sh`](05-add-host-ssl-cert-file.sh) | §Sandbox→gateway (b) | The full sandbox TLS round-trip: `--add-host` writes `/etc/hosts`, `SSL_CERT_FILE` + bind-mounted loom-ca lets stock curl validate the loom-ca-signed server cert by hostname. End-to-end no SDK changes needed. |

## How to run

Each spike is a standalone bash script. They install nothing system-wide;
they clean up after themselves via `trap`.

```bash
# Prerequisites: Docker. Spikes 02+03 also need `kind` and `kubectl`.
# Install kind/kubectl to ~/.local/bin if not already present.

bash docs/architecture/cluster-deploy-spikes/01-sandbox-bridge.sh
bash docs/architecture/cluster-deploy-spikes/02-preflight-hostpath.sh
bash docs/architecture/cluster-deploy-spikes/03-hostport-from-bridge.sh
```

Each spike prints `PASS:` / `FAIL:` for each claim and exits 0 / non-0
accordingly. Expected runtime: spike 01 ~5 s, spikes 02–03 ~60 s each
(kind cluster creation dominates).

## CI integration

The workflow at [`.github/workflows/cluster-deploy-spikes.yml`](../../../.github/workflows/cluster-deploy-spikes.yml)
runs all three spikes on:
- PRs touching `docs/architecture/cluster-deploy*` (catches spec changes
  that propose a mechanism the spikes don't cover yet — operator must add
  a spike or accept review-only verification).
- PRs touching `docs/architecture/cluster-deploy-spikes/` itself.
- Manual `workflow_dispatch`.

If a spike fails on CI, the spec change cannot land. This is the
intended gate: the design must be empirically verifiable against the
primitives before review effort applies.

## When to add a new spike

When the spec introduces a new mechanism that depends on a specific
behavior of Docker / k8s / CNI / kernel that you're not 100% certain
about. Examples that would warrant a new spike:
- JWT rotation via fsnotify on a bind-mounted volume.
- `docker network connect --ip` deterministic IP assignment.
- `--add-host` injection into a non-Linux base image.
- Any new use of `hostPath`, `hostNetwork`, or `securityContext` privileges.

Examples that don't need a spike:
- Pure prose decisions (CLI verb names, schema column choices).
- Mechanisms with multiple existing in-tree consumers (the existing
  worker→docker.sock pattern; trajectory writer to MinIO).

## Philosophy

The spike + review loop catches a different class of bugs than the
review-only loop:

| Bug class | Caught by review | Caught by spike |
|---|---|---|
| Wrong CLI flag name | ✓ | — |
| Schema column type | ✓ | — |
| Missing migration `down_revision` | ✓ | — |
| Race condition logic | sometimes | sometimes |
| "X composes with Y" | rarely | ✓ |
| "Z works on platform W" | almost never | ✓ |

Use both. Neither alone is sufficient. The point of the spike isn't
to replace review — it's to catch the class of bug that review
demonstrably misses.

See the [PR #50 thread](https://github.com/carinrc/loom/pull/50) for
the history that motivated this directory (revs 1–11 of the design
caught many "doesn't compose" bugs in review; the spikes are how we
break that pattern going forward).
