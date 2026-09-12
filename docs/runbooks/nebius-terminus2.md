# Terminus-2 on Nebius: first Terminal-Bench task

The initial task is `file-archive-manifest` from the old staging catalog
`terminal-bench-2-harbor-90`, using `terminus-2` and Gateway model `glm-5.2`.
This prepares one task, not a claim that all 90 tasks support Nebius.

## Prepare Dockerfile upload files

The existing GitHub-hosted `nebius-candidate` publication builds the trusted
`images.harbor_runtime` controller with the platform components. It does not
build or admit this task image. The ordinary task-image queue prepares the
submitted Dockerfile and binds the ready image to the Trial's frozen task
revision. The compiler uses that image for task and isolated verifier, and the
published Harbor runtime for the controller. Direct-completion continues using
the existing service image. No separate Nebius runner or CI gate is required.

For a local environment check, build from the repository root:

```sh
docker build --platform linux/amd64 \
  -f deploy/catalog/nebius-terminal-bench/file-archive-manifest/Dockerfile \
  -t <registry>/loom-terminal-bench-file-archive-manifest:<version> \
  deploy/catalog/nebius-terminal-bench/file-archive-manifest
```

Prepare the ordinary upload directory without a prebuilt image:

```sh
PYTHONPATH=src:. python scripts/ops/prepare_nebius_terminal_bench.py \
  --output /tmp/nebius-terminal-bench-taskset
loom tasksets submit /tmp/nebius-terminal-bench-taskset --format json
loom tasksets status '<returned TaskSet ID>' --format json
```

The helper writes `manifest.yaml`, `bundle.tar.gz` and `taskset-build.json`.
It does not build, publish, submit, create resources or call a model. Loom's
existing TaskSet upload contract uses TAR archives; ZIP is not accepted.
The output directory must be absent or empty to preserve existing operator work.
The default manifest uses a distinct `-dockerfile` name. Its task environment
declares `dockerfile="environment/Dockerfile"`,
`docker_build_context="environment"` and no `docker_image`. The context contains
only the reviewed prepared Dockerfile. Unchanged `instruction.md` and
`tests/test_outputs.py`, the offline `verifier/run.sh`, and source provenance
remain outside that context in the uploaded task directory. No oracle is uploaded.

The optional `--image '<registry>/image@sha256:<digest>'` retains preparation
for a prebuilt image already admitted by the target runtime profile. It does
not grant admission to arbitrary images. Historical prebuilt TaskSets remain
historical inputs; submitting them against a new profile without their image
admission is unsupported. Preserve old candidates and image references needed
by existing frozen Batches.

Import the directory through the ordinary TaskSet path after the target runtime
supports Terminus-2. In **Task Sets → Submit Task Set**, select `manifest.yaml`
and `bundle.tar.gz`; the archive already includes its verifier, so leave the
separate verifier and transform fields empty. Select `terminus-2` + `glm-5.2` when submitting a Trial;
model/provider configuration and credentials stay in the Gateway. Catalog/model
availability must be verified on the target environment. The task configuration
starts with 2 vCPU, 4 GiB memory and 8 GiB storage for the task container. The
controller and verifier each request another 2 vCPU and 4 GiB: the complete Trial
therefore requests 6 vCPU and 12 GiB, with about 33 GiB conservatively accounted
storage including workspaces and runtime volumes. The current 16-vCPU/64-GiB
node template can fit this Trial. This is an initial allocation, not a promise
for all 90 tasks; placement must account for all containers and live regional
quota. Agent and verifier each retain the original 900-second timeout.

## Dockerfile task prerequisites

Ordinary Nebius Batches can also queue a compatible Terminus-2 TaskSet whose
primary environment specifies a Dockerfile. Import readiness means the task
files and input manifest have been published; it does not mean the image has
been built. The Trial waits for its linked x86_64 task-image materialization
before reserving execution, consuming an attempt, or calling a model. An
architecture-independent task may also have an arm64 prerequisite; that row
does not block the x86_64 Nebius execution path. A failed x86_64 prerequisite
finishes the waiting Trial with `task_image_build_failed` and no consumed attempt.

When the platform's native task-image builder is enabled, its existing actuator
prepares and publishes the queued Dockerfile image. Import alone does not
activate a builder or permit arbitrary prebuilt images. The trusted controller and
runtime still come from the published platform profile; only the task and its
private verifier sandbox use the image associated with the frozen task revision.
Direct-completion cannot use this Dockerfile path. The other existing CPU,
workspace, networking, verifier and single-step restrictions still apply.

Resubmitting an existing Trial preserves its original task-image links even
when the TaskSet has since been rebuilt. Submit a new Trial to use the new
revision. Cancelling a waiting Trial does not cancel a shared image build needed
by other Trials. Generated inputs remain retained while a current task revision,
a live Trial, an unexpired build lease, or a ready image cache references them.
A retired cache with no remaining consumer does not retain historical inputs.
Preparing the same content again after cache retirement uses the current upload
location; ready cache reuse preserves its existing frozen source.

Native preparation reads ordinary TaskSet file modes from the frozen
`service_execution_input` manifest already published by TaskSet materialization.
It verifies that binding and the transferred bundle revision; ordinary uploads
do not need the benchmark publisher's `.loom-bundle-files.v1.json` sidecar.
Benchmark sources without an input-manifest binding retain the sidecar path.
A missing or corrupt bound manifest fails preparation; it does not fall back to
unbound modes or a different source revision.

## What changes from old staging

The original instruction and `tests/test_outputs.py` are copied byte for byte.
The original seven source files, registered checksum and object location remain
under `deploy/catalog/nebius-terminal-bench/file-archive-manifest/` for provenance.
The TaskSet uploads the assertion file only as a private verifier input; the
oracle solution and original package installer are not uploaded. Source identity
also travels in the TaskSet's `source-provenance.json`; the materializer remains
responsible for its existing database input-manifest provenance.

This is an explicitly adapted AMD64/non-root/offline execution profile. The
old staging `task.toml` explicitly declares ARM64 and a root verifier; that
original execution profile remains unsupported on this Nebius path. The saved
objects do not establish whether those declarations originated upstream or
in the old importer. The instruction, fixture commands and assertions contain
no architecture or UID dependency. The derived image preinstalls Python 3.13,
pytest 8.4.1, pytest-json-ctrf 0.3.5, bash, tmux and asciinema. Trials use non-root
UID/GID 65532, `/app`, and gateway-only networking. The original runtime apt/curl/
uv bootstrap is retained as source history and is not executed. The offline
wrapper replaces `tests/test.sh` execution with the same pytest invocation and
unchanged test assertions; this is a verifier bootstrap adaptation, not a claim
that the original verifier script runs unchanged.
`archive_manifest.json` and `build_manifest.py` are collected when present; the
original task declares no required artifact. Missing or incorrect answers must
reach the original verifier and yield numeric reward zero, not turn into an
artifact-collection platform failure. Successful acceptance requires reward one
and its corresponding manifest output.

Use the existing pinned Harbor Terminus-2 runtime and trajectory capture. The
controller talks to task and verifier over two different Unix sockets on
separate private volumes; the task cannot connect to the verifier socket. These
containers do not share a PID namespace. Keep
the controller in its image-owned `/app` directory, with Python isolated mode
and an explicit `--workspace /workspace` input path. This prevents task modules,
startup hooks and `.env` files from affecting trusted imports or configuration.
Keep
tests and verifier files out of the task image and agent workspace; the trusted
executor transfers them only to a distinct verifier container after the agent
finishes. That container uses the same image and receives the completed public
workspace. Preserve the baked `/app/archive_src` files when initializing it.
Both typed Terminus events and original Harbor trajectory/recording artifacts
are retained. The Control Plane uses the existing Terminus mapper for complete
ATIF export. Model calls join the execution ledger by the exact lease and
generation so another attempt's usage cannot be attributed to this Trial.

Terminus uses the Gateway OpenAI facade with a team-owned provider connection.
The Trial must bind `provider_connection_id` for that team and select its model;
selecting a model alone is insufficient. Configure the connection through the
normal provider API using a secret reference. A platform provider entry used by
direct-completion is not automatically a Terminus BYO connection.

The Harbor runtime applies the [Harbor current-user probe patch](../../deploy/patches/README.md)
at image build time. Read-only tool checks use the sandbox default user; only
actual installation requests root. Nebius images must have tools preinstalled,
and unsupported root execution still fails normally. Remove the #1550 patch
when the Harbor pin includes the corresponding upstream fix.

## Verification and acceptance

A local image check should run with `--network none`, the image's non-root user,
and the private assertion/bridge files available only in the verifier container.
An empty answer must yield reward zero; running the original oracle in a separate
local task container and copying its resulting workspace to a fresh verifier
must yield reward one. This is a no-model environment/verifier check, not live
Terminus acceptance. A Mac AMD64 container uses emulation and does not establish
native Nebius execution.

Live acceptance additionally requires the ordinary member submit/monitor/download
flow, actual Terminus turns through `glm-5.2`, attributed Gateway usage, original
verifier reward, successful `archive_manifest.json` output, full Trial artifacts, and
resource release/scale-zero. Do not infer any of these from a successful image
build or local oracle. This preparation does not submit a paid Trial or activate
cross-region capacity.

To establish the generic path, inspect the Trial's task-image materialization
link and native build attempt, its ready image, and the runtime plan's non-empty
`task_image_materialization_id`. The task/verifier image must match that ready
record, independently of the controller image; a different image digest alone
is insufficient. Image preparation must not consume a Trial attempt or call a
model. Then verify the real Trial's usage, cost, trajectory and original reward
assertions. A single-attempt acceptance request should retain
`retry={"max_attempts":1,"retry_on":[]}` through the ordinary Batch API; the
current `loom eval batch create` command does not expose retry configuration.
