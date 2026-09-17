# Native Harbor controller image

`Dockerfile.harbor-runtime` packages the trusted Terminus-2 controller for native
task and verifier sandboxes. It keeps Harbor 0.18.0 at compatibility commit
`527d50deb63a5d279e8c20593c18a2cbc7f61f9e` and the current-user tool-probe patch.
Task images must already contain bash, tmux and asciinema; missing tools fail
before a model request with an actionable, fixed diagnostic.

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
Only the fixture and evidence are mounted into the controller. Native platform
candidates use this image as their default controller. Independent publication
and per-Trial selection are described in
[Harbor runtime versions](../docs/runbooks/harbor-runtime-versions.md).

Shared Trial scalars stay in `loom.models.types`; reading a Trial inside the
controller must not import publication admission, database or worker packages.
