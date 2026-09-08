# Nebius Actions Runner Controller bootstrap

This is the opt-in runner bootstrap for #1798/#1543. It installs standard GitHub
ARC Helm charts, without a routing broker or another required CI check. Existing
PR workflows keep their current runners until the acceptance below passes.
Rendering or successfully installing Helm charts does not prove a GitHub job ran.

| Purpose | Scale set (`runs-on`) | Kubernetes namespace | Node role | Idle / maximum |
| --- | --- | --- | --- | --- |
| Pull request checks/builds | `loom-nebius-ci` | `loom-nebius-arc-ci` | `ci` | 0 / 2 |
| Protected candidate publication | `loom-nebius-release` | `loom-nebius-arc-release` | `release` | 0 / 0 initially; 0 / 1 for acceptance |
| ARC controller and listeners | — | Controller: `loom-nebius-arc-system`; listeners: their scale set namespace | `system` | Always running |

The Terraform integration node groups supply these roles, the
`loom.nebius/platform=integration` label and matching `NoSchedule` taints.
Runner pods request 4 CPUs and 16 GiB in total; each fits one 8-CPU/32-GiB node
with room for Kubernetes daemons. Runner counts must remain within the matching
Terraform node-group limits. An idle scale set can reach zero runners and zero
runner nodes; its listener remains on the system node to receive new jobs.

## Pins and configuration

Copy `deploy/nebius/runners.example.json` into an ignored operator directory.
The example uses ARC chart/controller **0.14.2**, Actions runner **2.337.0**, and
Docker DinD **29.8.0** for CI and rootless BuildKit **0.33.0** for releases, with
registry digests verified on 2026-09-08. The renderer
rejects floating image tags. The controller tag must match the chart version.
Update the chart constant and image together when upgrading ARC; keep runner
versions current within GitHub's supported update window.

The example explicitly sets `allow_external_images_for_bootstrap=true` so it can
bootstrap before registry mirroring exists. For pure Nebius acceptance, mirror
the upstream images to the Terraform registry, preserve their manifest digests,
replace the repository prefixes, and set this flag to `false`. Also set the
explicit `release_runner_image` to the reviewed derivative built from
`deploy/Dockerfile.nebius-runner`, published in Nebius with its **own** immutable
digest. This derivative includes publication tools such as skopeo; it does not
need to retain the upstream base digest. CI's separate `runner_image` can remain
the original image until its actual test lane establishes tooling compatibility.
The renderer rejects non-Nebius references in pure mode; actual tool availability
and publication still require the release smoke. The plain upstream runner is
only a bootstrap placeholder and cannot pass publication acceptance.
Set optional `image_pull_secret` to a read-only registry Secret already present
in all three namespaces. No registry write identity belongs in a runner PodSpec.

`github_config_url` is the repository or organization where the scale sets
register. `github_secret_ci` and `github_secret_release` are **Secret names**, not
credentials. Create distinct GitHub Apps with only the ARC registration
permissions needed for the selected scope. Store `github_app_id`,
`github_app_installation_id`, and `github_app_private_key` in each scale set's
namespace through the operator's existing secret-management process. Do not
put their values in JSON, shell history, generated output, or Git. A supported
fine-grained PAT may instead be supplied through the Secret's `github_token`
field when GitHub App registration is unavailable for the account scope.

For organization-owned runners, optional `runner_group_ci` and
`runner_group_release` select existing GitHub runner groups. Restrict the release
group to the approved publication workflow when available. Organization migration
is not required by this bootstrap. Repository-level scale set names
alone do **not** authorize workflows: anyone who can change a workflow may ask
for that `runs-on` name. In either case, release credentials must be job-scoped
GitHub Environment secrets, with the environment restricted to the integration
branch and protected approval policy. PR jobs must never receive the signing key,
registry write credentials, or deployment credentials. A separate namespace
does not replace those GitHub restrictions. Release uses **rootless BuildKit**,
not privileged DinD, because this repository is public and a runner label cannot
be treated as a trust boundary. `release_max_runners` defaults to `0`; set it to
`1` explicitly for bootstrap verification, then run the acceptance below before
routing publication jobs. This capacity setting adds no required CI gate.

## Render and inspect

Prerequisites: the locked Python development environment, Helm 3 with OCI chart
support, and Kubernetes 1.30 or newer for native sidecars and the rootless
BuildKit container's AppArmor field.

```bash
uv run --no-sync python scripts/ops/render_nebius_runners.py \
  --config deploy/nebius/runners.example.json \
  --output .loom/generated/nebius-runners
.loom/generated/nebius-runners/install.sh --dry-run
```

The output directory must be new. Five files are produced: three Helm values
files, `namespaces.yaml`, and `install.sh`. The dry run uses `helm template`,
fetches only pinned public charts, and writes three rendered manifests. It does
not read kubeconfig or call Kubernetes. Review the namespaces, node placement,
Secret references, image digests, controller RBAC, and runner PodSpecs before
installation. ARC chart 0.14.2 generates a no-permission service account for each
runner; the renderer also disables its API-token automount.

## Install after infrastructure and secrets are ready

This is an explicit cluster mutation, separate from the local render:

```bash
kubectl --context "$NEBIUS_CONTEXT" apply -f .loom/generated/nebius-runners/namespaces.yaml
# Provision the named registration Secrets and optional image-pull Secrets
# using the existing operator secret-management process in these namespaces.
.loom/generated/nebius-runners/install.sh --apply --context "$NEBIUS_CONTEXT"
kubectl --context "$NEBIUS_CONTEXT" get pods -n loom-nebius-arc-system
kubectl --context "$NEBIUS_CONTEXT" get autoscalingrunnersets -A
kubectl --context "$NEBIUS_CONTEXT" get pods -n loom-nebius-arc-ci
kubectl --context "$NEBIUS_CONTEXT" get pods -n loom-nebius-arc-release
```

The script requires an explicit Kubernetes context and checks both registration
Secrets before installing Helm releases. Controller installation waits for
readiness; scale set installation still needs listener/GitHub registration
readback. Rerunning `--apply` converges the same three Helm releases. Retain
controller, listener, and ephemeral runner logs through the cluster's existing
log collector; disappearing runner Pods must not erase failure diagnostics.

## Isolation and acceptance before routing jobs

Each job gets a new ARC runner Pod. CI uses privileged DinD only on dedicated CI
nodes. Releases use a non-privileged rootless BuildKit sidecar, UID/GID 1000, on
separate release nodes. All workspaces, Docker/BuildKit data, sockets and copied
tools are Pod-lifetime `emptyDir` volumes. No host Docker socket, host directory,
shared PVC, or Kubernetes API token is mounted.

Only the rootless BuildKit sidecar sets seccomp and AppArmor to `Unconfined`, as
the official Kubernetes example requires for user namespaces and mount syscalls.
Its `--oci-worker-no-process-sandbox` flag allows build steps to affect processes
inside that job's build daemon container; it does not grant a host PID namespace.
The release runner retains default seccomp/AppArmor, drops capabilities and
disables privilege escalation. No node-wide sysctl or host mount is added. Verify
rootless user-namespace support on the actual Nebius node image; do not replace a
failed rootless setup with privileged release containers.

The CI NetworkPolicy selects runner Pods, permits cluster DNS and public IPv4
egress, and blocks ingress plus private/link-local destinations, including the
cloud metadata address. Controllers/listeners are outside that selector and
retain their required Kubernetes API access. Confirm the Nebius cluster's CNI
enforces NetworkPolicy and that DNS uses the `kube-system/k8s-app=kube-dns`
selector. Custom private mirrors or node-local DNS require an explicit narrow
policy adjustment; do not silently allow all private egress. Release jobs need
their own approved deployment access, so this CI-only policy does not select
release Pods. A separate deny-ingress policy selects release Pods. Nodes retain
the Terraform registry-read-only cloud identity.

Release client contract: `BUILDKIT_CLIENT=/opt/buildkit/buildctl` (also exposed as
`BUILDKIT_BIN`) and
`BUILDKIT_HOST=tcp://127.0.0.1:1234`. An init container copies the client from the
same pinned BuildKit image; the native sidecar startup probe blocks runner start
until the daemon answers. The daemon listens only on Pod loopback, with no
Service, host port, or host network. The client transfers local context via the
BuildKit session; the sidecar does not mount the runner workspace or credentials.
Publication must use buildctl/archive tooling, not Docker daemon commands.

Before switching any existing PR lane, run a disposable manually dispatched
workflow using the single `runs-on: loom-nebius-ci` string. Verify:

1. A zero-runner scale set creates a runner on the `ci` node group and completes
   checkout, `docker info`, `docker buildx version`, and a local image build/run.
2. The actual selected Loom test lane runs, including its locked Python/Node/Go
   tools and Docker requirements. The upstream ARC runner image is minimal; it
   is not a complete GitHub-hosted runner image. Bake missing tools into a pinned
   derivative only when the lane needs them, mirror it, then repeat the smoke.
3. Cancellation deletes the job Pod and its workspace/build data; a subsequent
   job cannot read the previous workspace. Idle nodes scale back down.
4. CI Pods cannot read registration Secrets, use a Kubernetes service-account
   token, reach metadata/private service addresses, or receive release secrets.
5. Temporarily set `release_max_runners=1`, apply, and execute
   `"$BUILDKIT_BIN" debug workers` and the actual candidate build/scan/publication
   flow on `loom-nebius-release`. It must land on the release node group without
   privileged containers. Unauthorized PR requests cannot receive protected
   GitHub Environment credentials. Record job URLs and namespace/node readback.

Promote runner routing through an ordinary reviewed change only after these
checks pass. Keep the existing hosted route available until the actual Loom
lane is proven; no custom failover broker is introduced. If rollback is needed,
restore the workflow's prior `runs-on`, wait for running jobs to finish, then
uninstall the two scale set Helm releases. Remove the controller only after its
scale sets are gone. Preserve credential Secrets and audit logs for the operator.

Official references:
[ARC deployment and custom DinD configuration](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/deploy-runner-scale-sets),
[pinned chart values](https://github.com/actions/actions-runner-controller/blob/gha-runner-scale-set-0.14.2/charts/gha-runner-scale-set/values.yaml),
[runner scale set workflow routing](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/use-arc-in-a-workflow),
[ARC authentication](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/authenticate-to-the-api),
[BuildKit rootless Kubernetes example](https://github.com/moby/buildkit/blob/v0.33.0/examples/kubernetes/pod.rootless.yaml),
[rootless limitations](https://github.com/moby/buildkit/blob/v0.33.0/docs/rootless.md).
