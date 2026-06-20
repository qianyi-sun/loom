# Benchmark adapter

A `BenchmarkAdapter` is a class that knows how to fetch an upstream
dataset, walk its instances, and convert each one into the Loom
canonical task layout (`task.toml` + `instruction.md` + assets).

Lives at `packages/loom-benchmarks/loom_benchmarks/base.py`. Per-adapter
metadata (display_name, series, upstream, license, splits, params) is
declarative — `packages/loom-benchmarks/loom_benchmarks/benchmarks.json`.

## Contract (first-party, catalog-backed)

```python
from collections.abc import Iterator
from pathlib import Path

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)


class MyAdapter(CatalogBackedAdapter):
    # Just the catalog key. The mixin installs display_name, series,
    # upstream_source, license_spdx, license_url, splits, and _params
    # from `benchmarks.json` at class-creation time.
    name = "my-benchmark"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Walk the cached upstream tree, yield one BenchmarkInstance
        per task. `source_dir` is what fetch_upstream produced."""

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        """Write the Loom canonical task layout under out_dir/.
        Returns ConvertedTask(task_id, checksum, license_spdx, warnings)."""
```

### benchmarks.json entry

```json
{
  "name": "my-benchmark",
  "display_name": "My Benchmark",
  "series": "code",
  "upstream": {"kind": "huggingface", "locator": "org/repo"},
  "license": {"spdx": "MIT", "url": "https://github.com/org/repo/blob/main/LICENSE"},
  "splits": ["test"],
  "params": {}
}
```

`params` is an arbitrary string→string dict the adapter can read via
`self._params`. Per-year AIME adapters share `_AIMEYearBase` and read
`self._params["year"]` to filter the upstream — adding `aime-21` is one
JSON entry + a 2-line subclass.

### Third-party / external adapters

`CatalogBackedAdapter` is optional. External adapter packages that
declare metadata as plain class attributes still work — the mixin
falls back silently when the catalog has no entry for `cls.name`. The
declarative pattern is for first-party benchmarks shipped inside
`loom_benchmarks/`; external plugins keep using the legacy shape:

```python
class MyAdapter:
    name = "third-party-bench"
    display_name = "Third-Party Bench"
    upstream_source = UpstreamSource(kind="huggingface", locator="org/repo")
    license_spdx = "MIT"
    license_url = "..."
    splits = ("test",)
    # list_instances / convert_instance as above
```

### Canonical task layout (what `convert_instance` writes)

```
out_dir/
  task.toml                # validated against loom.models.task.TaskConfig
  instruction.md           # agent-visible prompt
  tests/                   # verifier test files (per-adapter convention)
  environment/             # bundled assets the prepare phase mounts
  verifier/                # optional shim scripts (some adapters)
```

### `task.toml` schema

```toml
schema_version = "1"

[task]
id = "<benchmark>/<instance>"   # e.g. "humaneval/HumanEval/0"
name = "<display>"

[environment]
os = "linux"
docker_image = "python:3.12-slim"
workdir = "/workspace"   # default; override for upstreams that expect /app
gpu_vendor = "none"
network_policies_supported = ["public", "allowlist"]
baseline_network_policy = { kind = "public" }

[agent]
name = "oracle"
timeout_sec = 1800

[verifier]
name = "pytest"
timeout_sec = 300

[verifier.args]      # per-verifier-kind kwargs
# script_path = "/workspace/verifier/run.sh"   # ScriptVerifier
# test_dir = "tests/"                      # PytestVerifier (default)

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["result.json"]
```

The full pydantic models live in `src/loom/models/task.py`. `extra =
"forbid"` is enforced — `convert_instance` output that adds fields
not in the schema will fail to load.

`environment.workdir` is the sandbox root for the materialized task bundle,
agent working directory, artifact collection root, and verifier artifact
directory. Most Loom-authored tasks use the default `/workspace`; adapters for
upstreams with their own convention should declare it explicitly. For example,
Terminal-Bench-2 tasks use `/app`, so their script verifier path is
`/app/verifier/run.sh` and their test tree lands under
`/app/environment/tb2-tests`.

## Publish/Register Boundary

Adapters convert upstream data into local bundles. Publication turns
those bundles into a durable dataset repo, and registration turns the
published manifest into catalog rows:

```bash
loom datasets publish humaneval --hf-org PRHW
loom datasets register humaneval --hf-org PRHW --db-url "$LOOM_DB_URL"
```

The publish command validates every generated `task.toml` against
`TaskConfig` before upload. Schema v3 manifests include the validated
raw `task_config` for each task, alongside the bundle checksum,
`hf_path`, split, tags, and license metadata. The register command
validates that payload again, verifies `task_config.task.id` matches
the manifest `task_id`, and writes it to `tasks.config`.

That stored config is the runnable boundary used by the service,
batch runner, and SPA. Legacy manifests that lack `task_config` are
still registered for metadata and provenance, but their rows keep
`config = {}` and are counted as `legacy_placeholders` in CLI output.
They are not runnable until the benchmark is republished with a v3
manifest or explicitly backfilled.

This same boundary is the intended scaling path for user-owned
benchmarks: validate a folder of Loom task bundles, publish the bundle
tree to a supported object store or dataset repo, register the manifest,
then smoke a small sample. Browser upload and admin APIs should wrap
these primitives instead of inventing separate ingestion behavior.

If a generated task config uses `agent.name = "oracle"`, the bundle
must include an executable `solution/solve.sh`. Code-completion
benchmarks that already ship a canonical `solution/solution.py` may
emit a no-op `solve.sh`; the verifier tests remain the correctness
source of truth. This keeps service-mode smoke tests on the same agent
contract as hand-authored tasks without adding one-off benchmark
wrappers.

## Discovery

Each adapter package declares:

```toml
[project.entry-points."loom.benchmarks"]
<slug> = "<module_path>.adapter:<ClassName>"
```

**Point at the class, not a pre-created instance.** The loader in
`loom_cli.builtin` does `ep.load()()`; an instance isn't callable.

`loom datasets list` enumerates `importlib.metadata.entry_points(
group="loom.benchmarks")`, instantiates each adapter class, and
returns `DatasetEntry` records. The union with the registry JSON +
remote service surface is described in
[../user-guide.md](../user-guide.md).

## Shipped adapters

### `packages/loom-benchmarks/` — 16 adapters across 7 series

Source of truth: `packages/loom-benchmarks/loom_benchmarks/benchmarks.json`.
Use `loom datasets list` to enumerate at runtime.

| Series | Adapters |
|---|---|
| `aime` | aime-22, aime-23, aime-24, aime-25 |
| `swe-bench` | swe-bench, swe-bench-verified, swe-bench-multimodal |
| `code` | humaneval, mbpp, livecodebench |
| `tool-use` | bfcl |
| `ui-agent` | osworld, webarena |
| `research-agent` | gaia |
| `skill` | skillflow, skilllearnbench |

### `packages/loom-benchmark-terminal-bench-2/` — 1 adapter

| Slug | License | Upstream |
|---|---|---|
| terminal-bench-2 | Apache-2.0 | git `laude-institute/terminal-bench` @ SHA `91e10457` (v0.1.1) |

The TB-2 adapter is broken out into its own package because it
pins a specific upstream SHA + ships a verifier shim that
translates pytest exit codes into the ScriptVerifier JSON contract.

## License enforcement

`team_quotas.license_allowlist` defaults to `[MIT, Apache-2.0,
BSD-3-Clause, CC-BY-4.0]`. `POST /trials` returns 403 if the task's
adapter declares a license outside the allowlist and its execution policy is
the default `allowlist`. Public benchmark mirrors can keep their source
license metadata while declaring `license.execution_policy = "notice"` in
`benchmarks.json`; import/publish writes this as a task tag and submission
allows it without mutating the team's hard-license allowlist. AIME 2022-2025
uses this notice policy. Truly restricted, private, NDA, or non-commercial
datasets should stay on the default hard allowlist path until an operator
extends the team's allowlist via the rate-cards admin API.

## Upstream fetching

`loom_benchmarks.fetch.fetch_upstream(src, cache_root=...)` is the
single fetcher. Dispatches on `src.kind`:

- **huggingface** — `datasets.load_dataset(src.locator, src.subset,
  revision=src.revision, cache_dir=str(target))`. `locator` MUST be
  namespaced (`namespace/name`) — newer `huggingface_hub` releases
  reject unnamespaced IDs.
- **git** — `git init && git remote add origin <locator> && git
  fetch --depth=1 origin <revision>` when `revision` is a SHA;
  otherwise `git clone --depth=1 --branch <revision>`.
- **https-tarball** — `httpx` download + `tarfile.extract`. Path
  traversal guarded.

Caches content-addressed under
`<cache_root>/<kind>/<sha256_of_(locator,revision,subset)>/`. A
sentinel `.fetch_complete` marks the cache as usable; partial fetches
get cleaned up via `rmtree` on the next call.

## Adding a new adapter

**First-party (inside `loom_benchmarks/adapters/`):**

1. Add a JSON entry to `packages/loom-benchmarks/loom_benchmarks/benchmarks.json`:
   ```json
   {
     "name": "my-bench",
     "display_name": "My Bench",
     "series": "code",
     "upstream": {"kind": "huggingface", "locator": "org/repo"},
     "license": {"spdx": "MIT", "url": "https://..."},
     "splits": ["test"]
   }
   ```
2. Write the Python adapter — inherit `CatalogBackedAdapter`, declare
   `name = "my-bench"`, implement `list_instances` + `convert_instance`.
3. Register the entry-point in `packages/loom-benchmarks/pyproject.toml`:
   ```toml
   my-bench = "loom_benchmarks.adapters.my_bench:MyBenchAdapter"
   ```
4. Run `uv pip install -e packages/loom-benchmarks --no-deps` so the
   entry-point dist-info regenerates.
5. Verify: `loom datasets list --installed` shows the new slug;
   `python -m pytest packages/loom-benchmarks/tests/test_catalog.py`
   passes (catalog ↔ adapter consistency).

**Third-party (sibling package, e.g. `packages/loom-benchmark-<name>/`):**

1. New PyPI-style sibling with `pyproject.toml`,
   `loom_benchmark_<name>/`, `tests/` (no `__init__.py` in `tests/`
   to avoid collision with the main repo's `tests/` root).
2. Declare metadata on the adapter class (the legacy attribute shape).
   No benchmarks.json needed — `CatalogBackedAdapter` is optional.
3. Add the entry-point in your `pyproject.toml`:
   ```toml
   [project.entry-points."loom.benchmarks"]
   <slug> = "loom_benchmark_<name>.adapter:<YourAdapterClass>"
   ```
4. License must be in the default allowlist (MIT, Apache-2.0,
   BSD-3-Clause, CC-BY-4.0), use an explicit `notice` execution policy for
   public benchmark mirrors, or operators extend their team's allowlist before
   trials run.
5. `convert_instance` must produce a deterministic checksum —
   `loom_benchmarks.util.sha256_of_dir` hashes `out_dir`'s relpaths +
   bytes in sorted order. Avoid timestamp-based content.
6. Smoke-test with `loom datasets list --installed` + `loom run
   --task <slug>/<one-instance-id> --agent oracle --backend fake`.

## Reusable conversion helpers

`packages/loom-benchmarks/loom_benchmarks/util.py`:

- `pytest_from_test_strings(tests, out_dir, prefix)` — write
  per-test files for the PytestVerifier
- `pytest_from_unittest(unittest_class_source, out_dir)` — wrap a
  unittest TestCase as a pytest file
- `structured_verifier_script(script_body, out_dir)` — write a
  script-verifier shim. The body must emit a `VerifierResult` JSON object to
  `LOOM_VERIFIER_OUTPUT`; derive task/artifact paths from the script location
  or explicit paths such as `/workspace`, because `ScriptVerifier` only
  guarantees the output env var.
- `embed_base64_image(image_bytes, alt_text)` — for multimodal
  benchmarks (SWE-Bench Multimodal)
- `download_files_from_record(...)` — fetch per-instance assets
  during conversion
- `sha256_of_dir(directory)` — deterministic content hash
- `toml_string(value)` — TOML-escape strings safely (handles
  embedded quotes, newlines, control chars)

## Common pitfalls

- **Adapter declares unnamespaced HuggingFace IDs**. `openai_humaneval`,
  `mbpp` (without `google-research-datasets/`) — newer `datasets`
  releases raise `HfUriError`. Always use `namespace/name`.
- **`list_instances` hardcodes a literal where the dataclass field
  exists.** Use `self.upstream_source.locator` (+ `.subset`) so the
  field stays the single source of truth.
- **TaskConfig schema is `extra="forbid"`.** `convert_instance` output
  that adds unknown fields under `[verifier]`, `[agent]`, etc. won't
  load. Put per-verifier args under `[verifier.args]`.
- **`tests/__init__.py` in a sibling package**. Sibling
  `packages/<name>/tests/` must omit `__init__.py` so pytest doesn't
  collide it with the main repo's `tests/` root.

## Operator-facing TOML registry: `config/benchmarks.toml`

Most adapter work goes through Python (entry-points). Two operator
scenarios don't need new Python and live in `config/benchmarks.toml`
instead (issue #234):

### Local task collections (`[[local]]`)

When you already have a folder of `task.toml` bundles, register the
folder rather than writing an adapter:

```toml
schema_version = 1

[[local]]
id = "team-evals"                          # kebab-case, unique
display_name = "Internal team evaluations"
series = "internal"
license_spdx = "proprietary"
source_subdir = "tasks"                    # optional
```

The source directory is *derived*: `<worker.fixtures_root>/<id>/`, plus
`source_subdir` when it is set. The example above expects bundles at
`<worker.fixtures_root>/team-evals/tasks/<task>/task.toml`; without
`source_subdir`, it expects
`<worker.fixtures_root>/team-evals/<task>/task.toml`. Operators
provision the directory out-of-band — host bind-mount in dev compose;
PV / hostPath in k8s. The TOML carries the `id` and optional relative
subdir only, not an absolute path, to avoid drift between the configured
path and the worker materializer's resolution.

For user-authored folders, prefer this root layout:

```text
team-evals/
  benchmark.toml
  tasks/
    alpha/
      task.toml
      instruction.md
      solution/
      tests/
```

Validate it before syncing:

```bash
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"
```

`validate-local` checks `benchmark.toml`, validates every discovered
`task.toml` against `TaskConfig`, and prints the `[[local]]` snippet to
copy into `config/benchmarks.toml`. With `source_subdir = "tasks"`, the
DB task id stays `team-evals/alpha` while the materializer source points
at `fixture://team-evals/tasks/alpha`.

For production, use `loom datasets publish-local <folder>` instead of
`sync-config` when workers should materialize from object storage rather than a
shared fixture mount. It uploads bundle files under
`s3://<bucket>/<benchmark-id>/<task-id>/` and upserts DB rows with those
sources, reusing the worker's existing `s3://` materializer. Tasks may declare
either `environment.docker_image` for a prebuilt sandbox image or
`environment.dockerfile` for a Dockerfile inside the task bundle. Service-mode
workers build Dockerfile tasks from the materialized bundle and cache the image
under a deterministic `loom-task:<hash>` tag. Worker operators bound cache-miss
build contexts with `LOOM_TASK_IMAGE_BUILD_MAX_FILES` (default 2000) and
`LOOM_TASK_IMAGE_BUILD_MAX_BYTES` (default 536870912).

Sync (UPSERT into the `benchmarks` + `tasks` tables) runs:

- Automatically on `loom service up` after seed, when
  `config/benchmarks.toml` is present AND `LOOM_WORKER_FIXTURES_ROOT`
  is set.
- Manually via `loom datasets sync-config [--dry-run]`.

In k8s, sync is operator-driven — there is no automatic CP-lifespan
sync because the control-plane pod does not have the worker's data PV
mounted (workload separation). Run sync from a one-shot Job that
mounts the same PV.

Sync-time failure modes:

| Condition                              | Behavior          | Exit |
|----------------------------------------|-------------------|------|
| TOML file missing                      | no-op             | 0    |
| TOML malformed / Pydantic validation   | error, abort      | 1    |
| ID collides with REGISTRY entry-point  | error, abort      | 1    |
| `[[local]]` source dir missing/empty   | WARN, skip entry  | 0    |
| `task.toml` inside source dir invalid  | error, abort      | 1    |
| unsafe `source_subdir`                 | error, abort      | 1    |

Entries removed from the TOML do **not** delete their DB rows —
trials may still reference them. Operators clean up manually.

### Adapter remaps (`[[remap]]`)

Reuse an existing adapter's parsing logic against a different
upstream — e.g., a fork of HumanEval:

```toml
[[remap]]
id = "humaneval-internal-fork"
inherit = "humaneval"                  # must be in REGISTRY
display_name = "HumanEval (internal fork)"
upstream_kind = "huggingface"          # huggingface | git | https-tarball
upstream_locator = "myorg/humaneval-fork"
license_spdx = "Apache-2.0"
license_url = "https://github.com/myorg/humaneval-fork/blob/main/LICENSE"
series = "code"                        # optional; defaults to base adapter's
splits = ["test"]                      # optional; defaults to base adapter's
```

Sync (UPSERT into `benchmarks`) runs the same way as `[[local]]`.
The row is keyed on `remap.id`, with `upstream_kind` /
`upstream_locator` / license fields from the remap and any unset
optionals inherited from the base adapter via `REGISTRY[inherit]`.

Importing tasks is a separate step: `loom datasets import
<remap.id>`. The importer resolves `inherit` against `REGISTRY`,
overrides `name` + `upstream_source` on a shallow-copied adapter
instance, and writes `tasks.benchmark_id = remap.id`. So the
fork's tasks live under the remap's id — not the inherit's —
across the S3 prefix, the per-task `task.toml`'s `task.id`, and the
DB.

Pre-flight (at sync) fails if:
- `remap.id` collides with a name in `REGISTRY`.
- `remap.inherit` does NOT resolve in `REGISTRY`.

## See also

- [overview.md](overview.md)
- [trajectory-and-atif.md](trajectory-and-atif.md) — what the
  trajectory looks like for a converted task
- `packages/loom-benchmark-terminal-bench-2/` — slim reference impl
- `packages/loom-benchmarks/loom_benchmarks/adapters/humaneval.py` —
  typical HuggingFace-backed code adapter
- `packages/loom-benchmarks/loom_benchmarks/adapters/swe_bench_verified.py`
  — typical git-backed adapter
