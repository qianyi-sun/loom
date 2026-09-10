# Terminus-2 on Nebius: first Terminal-Bench task

The initial task is `file-archive-manifest` from the old staging catalog
`terminal-bench-2-harbor-90`, using `terminus-2` and Gateway model `glm-5.2`.
This prepares one task, not a claim that all 90 tasks support Nebius.

## Prepare the image and upload files

The existing GitHub-hosted `nebius-candidate` publication builds the worker and
this task image alongside the platform components, using the same image scan
and admission mechanism. Its candidate has `images.worker` and `images.tb90_task`;
the runtime profile uses the worker as `agent_image_ref`. The compiler chooses
the task's admitted image independently. Direct-completion continues using the
existing service image. No separate Nebius runner or CI gate is required.
Batch admission, Control Plane Trial creation and the execution compiler share
the runtime profile's image compatibility check: a Terminus task image may
differ from the default task image, but both it and the controller must be
present in the published profile's admitted images.

For a local environment check, build from the repository root:

```sh
docker build --platform linux/amd64 \
  -f deploy/catalog/nebius-terminal-bench/file-archive-manifest/Dockerfile \
  -t <registry>/loom-terminal-bench-file-archive-manifest:<version> \
  deploy/catalog/nebius-terminal-bench/file-archive-manifest
```

For the upload, use the `images.tb90_task.image_ref` digest returned by the
existing candidate publication for both task and verifier. Prepare the directory:

```sh
PYTHONPATH=src:. python scripts/ops/prepare_nebius_terminal_bench.py \
  --image '<registry>/loom-terminal-bench-file-archive-manifest@sha256:<digest>' \
  --output /tmp/nebius-terminal-bench-taskset
```

The helper writes `manifest.yaml`, `bundle.tar.gz` and `taskset-build.json`.
It does not build, publish, submit, create resources or call a model. Loom's
existing TaskSet upload contract uses TAR archives; ZIP is not accepted.
The output directory must be absent or empty to preserve existing operator work.

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

## What changes from old staging

The original instruction and `tests/test_outputs.py` are copied byte for byte.
The original seven source files, registered checksum and object location remain
under `deploy/catalog/nebius-terminal-bench/file-archive-manifest/` for provenance.
The TaskSet uploads the assertion file only as a private verifier input; the
oracle solution and original package installer are not uploaded. Source identity
also travels in the TaskSet's `source-provenance.json`; the materializer remains
responsible for its existing database input-manifest provenance.

Only architecture and environment preparation change. Ubuntu fixtures are
architecture independent, and the derived image preinstalls Python 3.13,
pytest 8.4.1, pytest-json-ctrf 0.3.5, bash, tmux and asciinema. Trials use non-root
UID/GID 65532, `/app`, and gateway-only networking. The original runtime apt/curl/
uv bootstrap is retained as source history and is not executed.
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

The worker applies the [Harbor current-user probe patch](../../deploy/patches/README.md)
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
