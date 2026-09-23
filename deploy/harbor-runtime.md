# Native Harbor controller image

`Dockerfile.harbor-runtime` packages the trusted Terminus-2 controller for native
task and verifier sandboxes. It keeps Harbor 0.18.0 at compatibility commit
`527d50deb63a5d279e8c20593c18a2cbc7f61f9e` and the current-user tool-probe patch.
Task images must already contain bash, tmux and asciinema; missing tools fail
before a model request with an actionable, fixed diagnostic.

The pinned Harbor patch also handles two legacy task-image contracts:

- **UTF-8 recording locale (#2079):** if installed asciinema rejects its version
  probe, enumerate the image's installed UTF-8 locales and probe with one before
  attempting package installation. Reuse the successful locale for recording.
  Ubuntu 18.04 offers `C.UTF-8`; CentOS 7 may offer only `en_US.utf8`. An absent
  or still-broken tool continues to fail; sandbox identity is never widened.
- **tmux 1.x buffers (#2080):** these versions use an indexed buffer stack and
  reject modern named buffers. Serialize load/paste/delete within the session,
  using stack index zero for 1.x and unique names for newer versions. Preserve
  older buffers, remove staged files, and never retry a failed paste: the shell
  may already have received the input. Missing-server errors remain failures.

These are bounded patches to the pinned upstream dependency. Remove each hunk
when a Harbor upgrade provides equivalent behavior and the regression below
passes. The legacy path assumes the existing one-controller-per-task tmux
server ownership; unrelated writers to that server's buffer stack are not
supported. No task OS/toolchain replacement is required.

Sandbox exec rejection responses expose fixed reason codes for invalid requests,
user mismatch, timeout, and environment. The client accepts only the operation's
allowlisted codes; it never publishes raw response bodies or submitted values.

The image installs Harbor's base dependencies and the Loom library in Python
3.12's trusted site-packages. It does not install the full Loom platform,
launcher or worker. Harbor retains its own upstream dependencies. Start the
controller with `python -I -B` from `/app`; mount task inputs separately and
never replace the packaged source. Dependencies are installed at image build
time, not when a Trial starts.

LiteLLM reads only packaged model metadata (`LITELLM_LOCAL_MODEL_COST_MAP=True`)
from process startup, including imports. The image build first uses the dependency's
valid bundled price/context table. If missing or corrupt, it downloads the official
snapshot at the installed LiteLLM version's `v<version>` tag and packages it at the
same local path. If that exact snapshot is unavailable or invalid, the build fails
with a diagnostic; it never substitutes `main`, an empty table, or a runtime fetch.
The final import smoke runs as the nonroot runtime user with networking disabled.
Models absent from the table remain absent; this does not synthesize zero prices
or replace the Gateway's authoritative usage accounting.

Build and exercise the actual image with the existing local task fixture:

```sh
docker build --platform linux/amd64 -f deploy/Dockerfile.harbor-runtime \
  -t loom-harbor-runtime:local .
LOOM_TERMINUS_SMOKE_IMAGE=loom-nebius-file-archive-manifest:local \
LOOM_TERMINUS_CONTROLLER_IMAGE=loom-harbor-runtime:local \
PYTHONPATH=src:. python -m pytest tests/integration/test_nebius_terminus_e2e.py -q
```

The opt-in test requires Docker, Go and the local prepared task image. It uses
isolated local containers and a loopback model stub; no external model calls.
It covers reward and trajectory, private verifier inputs, task-file import
poisoning, current-user tool probes, and missing tools with zero recorded usage.
It also expires a real agent phase while a loopback model request is outstanding,
then checks bounded finalization, native/usage artifacts and private evaluation
of the retained workspace.

To reproduce legacy-image startup and dispatch without any model, prepare the
original task images locally and run:

```sh
LOOM_TERMINUS_COMPAT_IMAGES="$QUALITY_IMAGE,$WORLD_IMAGE,$CENTOS_IMAGE" \
LOOM_TERMINUS_CONTROLLER_IMAGE=loom-harbor-runtime:local \
PYTHONPATH=src:. python -m pytest tests/integration/test_nebius_terminus_compat.py \
  --noconftest -q -s
```

This opt-in test uses the installed controller and actual Go sandbox RPC as UID
65532, with network disabled. It checks recording startup, multiline Unicode
payloads above the paste threshold, tmux's smaller legacy fallback threshold,
concurrent paste serialization, exactly-once delivery after an injected response
failure, missing-server propagation, retained recording, existing buffers and
temporary-file cleanup. Recording is tested with a short multiline payload;
the >16 KiB transport test runs after recording stops. Asciinema 2.0 on the
Ubuntu 18.04 images independently loses input on a single large paste while
recording; this patch does not repair that separate forwarding limitation.
It does not prove model-backed trajectory-generation acceptance on Nebius; use
the ordinary team TaskSet path for that separately authorized verification.

Only the fixture and evidence are mounted into the controller. Native platform
candidates use this image as their default controller. Independent publication
and per-Trial selection are described in
[Harbor runtime versions](../docs/runbooks/harbor-runtime-versions.md).

Shared Trial scalars stay in `loom.models.types`; reading a Trial inside the
controller must not import publication admission, database or worker packages.
