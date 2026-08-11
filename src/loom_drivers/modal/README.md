# loom_drivers.modal — Modal Cloud Driver for Loom

Implements `loom.driver.base.Driver` against Modal's
[`Sandbox`](https://modal.com/docs/reference/modal.Sandbox) primitive.

## Install

```bash
pip install 'loom[modal]'
```

## Configure

```bash
export MODAL_TOKEN_ID=ak-...
export MODAL_TOKEN_SECRET=as-...
# optional: workspace selector
export MODAL_WORKSPACE=my-workspace
```

Or run `modal token new` to populate `~/.modal.toml` and source the env
vars from there.

## Use via CLI

```bash
loom run --backend modal \
         --dataset humaneval \
         --agent oracle \
         --concurrency 100

# GPU trial
loom run --backend modal \
         --gpu A10 \
         --agent claude-code \
         --task swe-bench-verified/django__django-11848
```

## GPU support

Valid `--gpu` values: `T4`, `L4`, `A10`, `L40S`, `A100`, `A100-40GB`,
`A100-80GB`, `RTX-PRO-6000`, `H100`, `H100!`, `H200`, `B200`, `B200+`.
Multi-GPU: append `:N` (e.g. `H100:8`).

## Cost reporting

Each sandbox emits one `cloud_compute_records` row tagged
`cloud_provider='modal'`. The `/api/v1/usage` endpoint sums these into
the per-bucket `modal_compute_seconds` / `modal_cost_usd` fields (and
the cross-provider `cloud_compute_seconds` / `cloud_cost_usd` totals).

Per-SKU rates live in `src/loom/cost/cloud.py` and must match
[modal.com/pricing](https://modal.com/pricing).

## Quirks vs. DockerDriver

- **Network policy is create-time only.** Calling `set_network_policy()`
  with a different policy after `start()` raises `DriverError`. Pass the
  desired policy as `ModalDriver(network_policy_baseline=...)` before
  `start()`, or `stop()` and restart with the new policy.
- **Hostname-based `Allowlist` is unsupported** (Modal accepts CIDR
  ranges only). Use `Public` / `NoNetwork`, or run under
  `--backend docker` for hostname allowlists.
- **`upload()` / `download()` are base64-over-exec.** Not suitable for
  files larger than ~5 MB. Use a Modal `NetworkFileSystem` for larger
  payloads.
- **No host-side PID exposed** via `ExecHandle.pid` (always 0).

## Lifecycle cleanup

The driver registers each running sandbox in a process-wide WeakSet and
installs `atexit` + `SIGINT` / `SIGTERM` handlers on first `start()` to
synchronously terminate any live sandboxes when the process exits. This
mirrors `loom_drivers.daytona.registry.LiveSandboxRegistry` but uses a
simpler weak-reference model since Modal's `Sandbox.terminate(wait=False)`
is a cheap network call.
