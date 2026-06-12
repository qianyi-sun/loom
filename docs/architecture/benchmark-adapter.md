# Benchmark adapter

A `BenchmarkAdapter` is a class that knows how to fetch an upstream
dataset, walk its instances, and convert each one into the Loom
canonical task layout (`task.toml` + `instruction.md` + assets).

Lives at `packages/loom-benchmarks/loom_benchmarks/base.py`.

## Contract

```python
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol

from loom_benchmarks.base import (
    BenchmarkInstance,
    ConvertedTask,
    UpstreamSource,
)

class BenchmarkAdapter(Protocol):
    name: str                          # slug, e.g. "humaneval"
    display_name: str                  # "HumanEval"
    upstream_source: UpstreamSource    # kind ∈ {huggingface, git, https-tarball}
    license_spdx: str                  # e.g. "MIT"
    license_url: str                   # canonical license URL
    splits: tuple[str, ...]            # ("test",), or ("train", "test"), ...

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Walk the cached upstream tree, yield one BenchmarkInstance
        per task. `source_dir` is the path fetch_upstream cached the
        dataset to."""

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        """Write the Loom canonical task layout under out_dir/.
        Returns ConvertedTask(task_id, checksum, license_spdx, warnings)."""
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
# script_path = "/loom/verifier/run.sh"   # ScriptVerifier
# test_dir = "tests/"                      # PytestVerifier (default)

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["tb2-verifier.json"]
```

The full pydantic models live in `src/loom/models/task.py`. `extra =
"forbid"` is enforced — `convert_instance` output that adds fields
not in the schema will fail to load.

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

### `packages/loom-benchmarks/` — 13 adapters

| Slug | License | Upstream |
|---|---|---|
| aime | proprietary-MAA | HF `AI-MO/aimo-validation-aime` |
| bfcl | Apache-2.0 | git `gorilla` |
| gaia | Apache-2.0 | HF `gaia-benchmark/GAIA` |
| humaneval | MIT | HF `openai/openai_humaneval` |
| livecodebench | CC-BY-NC-4.0 | HF `livecodebench/code_generation_lite` |
| mbpp | CC-BY-4.0 | HF `google-research-datasets/mbpp` (subset `sanitized`) |
| osworld | Apache-2.0 | git `OSWorld` |
| skillflow | NOASSERTION | git `ZhangZi-a/SkillFlow` |
| skilllearnbench | Apache-2.0 | git `cxcscmu/SkillLearnBench` |
| swe-bench | MIT | HF `princeton-nlp/SWE-bench` |
| swe-bench-multimodal | MIT | HF `princeton-nlp/SWE-bench_Multimodal` |
| swe-bench-verified | MIT | HF `princeton-nlp/SWE-bench_Verified` |
| webarena | Apache-2.0 | git `webarena` |

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
adapter declares a license outside the allowlist. Operators extend
their team's allowlist via the rate-cards admin API.

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

1. New PyPI-style sibling under `packages/loom-benchmark-<name>/`
   with `pyproject.toml`, `loom_benchmark_<name>/`, `tests/` (no
   `__init__.py` in `tests/` to avoid collision with the main repo's
   `tests/` root).
2. Implement the `BenchmarkAdapter` Protocol. The `terminal-bench-2`
   adapter is the slimmest reference.
3. `pyproject.toml`:
   ```toml
   [project]
   name = "loom-benchmark-<name>"
   dependencies = ["loom-benchmarks>=0.1.0,<0.3", "loom>=0.0.0"]

   [project.entry-points."loom.benchmarks"]
   <slug> = "loom_benchmark_<name>.adapter:<YourAdapterClass>"
   ```
4. License must be in the default allowlist (MIT, Apache-2.0,
   BSD-3-Clause, CC-BY-4.0) or operators will need to extend their
   team's allowlist before trials run.
5. `convert_instance` must produce a deterministic checksum —
   `loom_benchmarks.util.sha256_of_dir` walks `out_dir` in sorted
   order and hashes relpath + bytes. Avoid timestamp-based or
   iteration-order-dependent content.
6. Test with `loom datasets list --installed` (should surface your
   slug) and `loom run --task <slug>/<one-instance-id> --agent
   oracle --backend fake` (should round-trip).

## Reusable conversion helpers

`packages/loom-benchmarks/loom_benchmarks/util.py`:

- `pytest_from_test_strings(tests, out_dir, prefix)` — write
  per-test files for the PytestVerifier
- `pytest_from_unittest(unittest_class_source, out_dir)` — wrap a
  unittest TestCase as a pytest file
- `structured_verifier_script(script_body, out_dir)` — write a
  script-verifier shim that emits the JSON `LOOM_VERIFIER_OUTPUT`
  shape ScriptVerifier consumes
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

## See also

- [overview.md](overview.md)
- [trajectory-and-atif.md](trajectory-and-atif.md) — what the
  trajectory looks like for a converted task
- `packages/loom-benchmark-terminal-bench-2/` — slim reference impl
- `packages/loom-benchmarks/loom_benchmarks/adapters/humaneval.py` —
  typical HuggingFace-backed adapter
- `packages/loom-benchmarks/loom_benchmarks/adapters/swe_bench_verified.py`
  — typical git-backed adapter
