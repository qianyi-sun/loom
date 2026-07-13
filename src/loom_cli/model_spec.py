"""Parse `--model` CLI values into `ModelSpec` without benchmark dependencies.

Kept separate from `run_cmd` so parser-only tests and helpers do not
eagerly import `task_loader` / `loom_benchmarks.fetch` (which pulls in
optional HuggingFace `datasets`).
"""

from __future__ import annotations

from loom.models.types import ModelSpec


def parse_model(spec: str) -> ModelSpec:
    """Parse `--model VALUE` into a `ModelSpec`.

    Recognized shapes:

    - `<provider>/<name>` — cloud provider or registered local server.
      Examples: `anthropic/claude-opus-4-7`,
      `local/vllm/Llama-3.1-8B-Instruct`.
    - `hf:<org>/<name>` — HuggingFace model id; Loom will launch vLLM
      on this model for the duration of the run.
      Example: `hf:meta-llama/Llama-3.1-8B-Instruct`.
    - `<absolute-or-relative-path-to-weights-dir>` — local weights
      directory; Loom launches vLLM on it. Detected by leading `/`,
      `~`, `./`, or `../`. Example: `/data/checkpoints/my-model/`.
    """
    # Path detection — a leading filesystem marker is unambiguous and
    # avoids forcing the user to type a `file:` prefix.
    if spec.startswith(("/", "~", "./", "../")):
        return ModelSpec(provider="file", name=spec)
    if spec.startswith("hf:"):
        body = spec[len("hf:"):]
        if "/" not in body:
            raise SystemExit(
                f"hf:<id> must be `<org>/<name>` (got hf:{body!r}). "
                "Example: hf:meta-llama/Llama-3.1-8B-Instruct",
            )
        return ModelSpec(provider="hf", name=body)
    if "/" not in spec:
        raise SystemExit(
            f"--model must be 'provider/name', 'hf:<id>', or an "
            f"absolute / relative path to weights (got {spec!r}); "
            f"e.g. anthropic/claude-opus-4-7, "
            f"hf:meta-llama/Llama-3.1-8B-Instruct, or "
            f"/data/checkpoints/my-model/",
        )
    provider, name = spec.split("/", 1)
    return ModelSpec(provider=provider, name=name)
