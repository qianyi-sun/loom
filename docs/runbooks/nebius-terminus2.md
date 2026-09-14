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
verifier requests the same task resources. Newly published Nebius profiles set
`controller_resources: {"cpu_millis": 1000, "memory_mib": 2048}` for the trusted
Harbor controller, independently of the task's compute. This example therefore
requests 5 vCPU and 10 GiB across the complete Pod. Controller storage remains
task-derived (8 GiB here), as do the task/verifier allocations, workspace and
output bounds; conservative storage accounting remains about 33 GiB. The current
16-vCPU/64-GiB node template can fit this Trial. CPU/RAM savings alone do not
establish better packing when storage is the limiting resource. This is an
initial allocation, not a promise for all 90 tasks; placement accounts for all
containers and live regional quota. Agent and verifier each retain the original
900-second timeout.

The deployment runtime profile's optional `controller_resources` contains only
CPU and memory. Automatic native Terminus plans freeze it with task-derived
storage. An absent setting preserves legacy task-sized controller behavior;
existing frozen plans retain their original allocation and remain readable.
Direct-completion plans do not use this setting. Explicit published harness
versions retain their selected controller image. The runtime image supplies the
Go plan reader; the selected Harbor image receives unchanged task/trial inputs
and phase arguments, not the execution plan or deployment profile.
The Python compiler, actuator,
capacity admission and Go execution runtime must be deployed together before
publishing plans with the new field. The 1-vCPU/2-GiB controller baseline retains
the allocation exercised by the ordinary-task acceptance; it is not derived
from a low point-in-time usage sample. Further reductions require evidence from
startup, Harbor processing, workspace handoff and output publication, including
memory peaks and CPU throttling. Both resource requests and limits use the
configured values.

## Dockerfile task prerequisites

Ordinary Nebius Batches can also queue a compatible Terminus-2 TaskSet whose
primary environment specifies a Dockerfile. Import readiness means the task
files and input manifest have been published; it does not mean the image has
been built. The Trial waits for its linked x86_64 task-image materialization
before reserving execution, consuming an attempt, or calling a model. An
architecture-independent task may also have an arm64 prerequisite; that row
does not block the x86_64 Nebius execution path. A failed x86_64 prerequisite
finishes the waiting Trial with `task_image_build_failed` and no consumed attempt.

`GET /api/v1/trials/{id}` exposes `task_environment_preparation` to the Trial's
authorized readers, including before execution starts. Each entry reports the
linked architecture's current shared preparation state, build-attempt count,
known failure reason, safe explanation, prepare/build/publish phase timestamps
and exit code, and whether native build resources have been released. It uses
the current materialization epoch; this is not historical evidence of which
build a completed Trial used. A later cache rebuild can change this view.
Arbitrary Dockerfile output, source locations, registry references and frozen
build configuration are not exposed. Operators retain the bounded build log
for deeper diagnosis. Cancelling the last waiting Trial can leave preparation
queued with `build_cancelled` and no active demand; it does not mean another
build is running. A pre-execution failure or cancellation has no execution
bundle, so `/bundle/download` continues returning HTTP 409 rather than creating
a synthetic successful trajectory.

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

Native Terminus execution keeps a declared dedicated Docker build-context
directory and its Dockerfile in the controller's frozen inputs. They are used
for image construction and are not uploaded again into the agent workspace.
This prevents setup scripts removed by the Dockerfile, and duplicate private
tests under the build context, from reappearing during evaluation. Files baked
into the image, including task data and Git history, are preserved; the private
verifier still receives its original `tests/` and `verifier/` inputs.

For a root (`.`) or unspecified build context, only the declared Dockerfile is
excluded in addition to the existing private paths. Other root runtime assets
remain available because their purpose cannot be inferred from context alone.
This does not guarantee isolation of arbitrary setup sources or nested tests in
an ambiguous root context; use a dedicated context directory for build-only
inputs. Image-only tasks retain their normal runtime inputs, without guessing
that a directory named `environment/` must be private. The native entrypoint
reads the original frozen `task.toml`, even though admission resolves the built
image separately. No shared legacy workspace policy is changed.

A task-image build failure or timeout happens before model execution. Public
diagnosis keeps that preparation failure and treats zero model calls as expected;
it does not recommend retrying the model to fix a Dockerfile. Cancellation before
execution has the same expected zero-call treatment. A genuine agent failure
without calls still retains the missing-usage diagnostic.

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

## Sandbox completion diagnostics

The trusted agent writes trajectory-derived usage before asking its task sandbox
to stop descendants. A cleanup failure keeps this accounting and fails the
phase; it does not export an unstable workspace or start the verifier. Cleanup
still runs if local usage parsing fails.

Native phase stderr reports a fixed sandbox operation, HTTP status and one of
`pid_namespace_invalid`, `process_owner_mismatch`, `process_inspection_failed`,
`cleanup_timeout`, `cleanup_cancelled` or `cleanup_failed` when that server
boundary fails. Unknown response reasons become `http_error`; connection and
timeout failures use fixed transport categories. Request bodies, commands,
environments, response bodies and endpoint URLs are excluded. These codes
identify the failed boundary; a completed model conversation alone does not
prove workspace handoff or verifier success.

Process cleanup checks the effective UID and state from the same `/proc/<pid>/status`
read. It tolerates process disappearance (`ENOENT`/`ESRCH`), while foreign effective
UIDs, malformed status and other read failures stop cleanup. Proc directory
ownership alone is not authoritative: Linux can return a successful root-owned
stat after a process has been reaped. The runtime must still be PID 1 of its own
sandbox, and workspace export still requires completed cleanup.
