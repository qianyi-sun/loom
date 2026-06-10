"""`loom run` orchestrator — fans out trials over `loom_benchmarks` tasks.

Flow:
  1. Parse `--dataset` or `--task` -> list of LoadedTask
  2. Resolve `--agent` + `--model` -> patched TaskConfig per task
  3. Construct UpstreamDirectGatewayClient (or skip if agent is oracle)
  4. Bound by `--concurrency` asyncio.Semaphore, run each task through
     LocalRunner.run() in `asyncio.gather`
  5. Print result line per task (text or JSON)
  6. If `--server-url` (or `LOOM_SERVER_URL`) is set, POST each result
     to `<url>/api/v1/cli/results` (best-effort; never fails the CLI)
  7. Exit 0 if every trial state == succeeded, else 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import httpx

from loom.driver.base import Driver
from loom.driver.docker import DockerDriver
from loom.driver.fake import FakeDriver
from loom.models.result import TrialResult, TrialState
from loom.models.task import TaskConfig
from loom.models.types import ModelSpec
from loom_cli.config import LocalProvider, load_config
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner
from loom_cli.output import format_json_line, format_text_line
from loom_cli.task_loader import LoadedTask, load_tasks
from loom_cli.vllm_runner import (
    MissingVLLMDependencyError,
    VLLMLaunchSpec,
    launch_vllm,
    stop_all,
)


def run(args: argparse.Namespace) -> int:
    return asyncio.run(_run_async(args))


async def _run_async(args: argparse.Namespace) -> int:
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    workdir = output_dir / "_tasks"
    workdir.mkdir(parents=True, exist_ok=True)

    if args.dataset:
        dataset = args.dataset
        task_filter = None
    else:
        assert args.task is not None
        dataset = args.task.split("/", 1)[0]
        task_filter = args.task

    try:
        tasks = list(load_tasks(
            dataset=dataset, split=args.split,
            task_filter=task_filter, workdir=workdir,
        ))
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not tasks:
        print(
            f"no tasks selected (dataset={dataset!r} task={task_filter!r})",
            file=sys.stderr,
        )
        return 2

    cfg = load_config()
    tokens = dict(cfg.tokens)
    for env_key, provider in (
        ("ANTHROPIC_API_KEY", "anthropic"),
        ("OPENAI_API_KEY", "openai"),
        ("GOOGLE_API_KEY", "google"),
    ):
        val = os.environ.get(env_key)
        if val:
            tokens[provider] = val

    model = _parse_model(args.model) if args.model else None

    # --local-server: ad-hoc dispatch against an already-running
    # OpenAI-compatible server. We register a transient `_inline`
    # local provider and rewrite the model spec to address it. No
    # vLLM subprocess; the server is the user's responsibility.
    inline_local_name: str | None = None
    if getattr(args, "local_server", None):
        if model is not None and model.provider in ("hf", "file"):
            raise SystemExit(
                "--local-server is mutually exclusive with "
                "--model hf:<id> or --model /path/ — those "
                "launch their own vLLM; --local-server targets a "
                "server you already started.",
            )
        if model is None:
            raise SystemExit(
                "--local-server requires --model <model_id>",
            )
        inline_local_name = "_inline"
        cfg.local_providers[inline_local_name] = LocalProvider(
            base_url=args.local_server,
            api_key=(
                getattr(args, "local_api_key", None)
                or os.environ.get("LOOM_LOCAL_API_KEY")
            ),
        )
        # The whole `--model` value is the upstream model id; rewrite
        # to local/_inline/<full-id> so dispatch hits _call_local.
        model = ModelSpec(
            provider="local",
            name=f"{inline_local_name}/{args.model}",
        )

    # If the user passed `--model hf:<id>` or a path, we launch a
    # vLLM subprocess on those weights, register it as a transient
    # local provider, and rewrite `model` to point at it. The
    # trial(s) then dispatch through the existing local-provider
    # path; vLLM is torn down at end-of-process via atexit.
    auto_local_name: str | None = None
    _maybe_warn_unused_vllm_flags(args, model)
    if model is not None and model.provider in ("hf", "file"):
        try:
            info = launch_vllm(VLLMLaunchSpec(
                model=model.name,
                port=getattr(args, "vllm_port", 0),
                host=getattr(args, "vllm_host", "127.0.0.1"),
                gpu_memory_utilization=getattr(args, "gpu_memory_utilization", 0.90),
                tensor_parallel_size=getattr(args, "tensor_parallel_size", 1),
                max_model_len=getattr(args, "max_model_len", None),
                enforce_eager=getattr(args, "enforce_eager", False),
                keep_alive=getattr(args, "keep_alive", False),
            ))
        except MissingVLLMDependencyError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        except RuntimeError as exc:
            # Launch failure: missing path, no free port, vLLM startup
            # crash, or 300s health-check timeout. Surface it as a clean
            # CLI error rather than a stacktrace.
            print(f"vLLM launch failed: {exc}", file=sys.stderr)
            return 2
        # Register under a fixed name so `loom config show` doesn't pick
        # this up. The dispatch contract reads `cfg.local_providers`;
        # we mutate cfg in-process only.
        auto_local_name = "_auto_vllm"
        cfg.local_providers[auto_local_name] = LocalProvider(
            base_url=info.base_url, api_key=None,
        )
        # Rewrite the spec so `_call_local` resolves the right server +
        # the right model id (vLLM may have shortened HF org/name into
        # something different in served_model_name).
        model = ModelSpec(
            provider="local",
            name=f"{auto_local_name}/{info.served_model_name}",
        )
    store = LocalDiskObjectStore(root=output_dir / "_store")

    a_client, o_client, g_client = _build_sdk_clients(tokens)

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))
    completed: list[TrialResult] = []

    async def _one(loaded: LoadedTask) -> int:
        async with sem:
            patched = _patch_agent(loaded.task_config, args.agent, model)
            runner = LocalRunner(
                trial_id=uuid4(),
                team_id=uuid4(),
                task_config=patched,
                task_checksum=loaded.checksum,
                task_dir=loaded.task_dir,
                driver_factory=_driver_factory(
                    args.backend, patched, gpu=getattr(args, "gpu", None),
                ),
                output_dir=output_dir,
                object_store=store,
                upstream_gateway_tokens=tokens,
                anthropic_client=a_client,
                openai_client=o_client,
                google_client=g_client,
                local_providers=cfg.local_providers,
            )
            try:
                result = await runner.run()
            except Exception as exc:
                print(
                    f"{loaded.task_config.task.id} [ERROR] "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                return 1
            line = (
                format_json_line(result) if args.json_output
                else format_text_line(result)
            )
            completed.append(result)
            print(line, flush=True)
            await _maybe_post_result(args.server_url, result)
            return 0 if result.state == TrialState.SUCCEEDED else 1

    try:
        exit_codes = await asyncio.gather(*[_one(t) for t in tasks])
        _maybe_write_tb2_report(args, completed)
    finally:
        # vLLM teardown happens on EVERY exit path (happy or exception)
        # to close the GPU-orphan window. atexit catches Python exit
        # but not OOM / SIGKILL — explicit cleanup is the safer default.
        if auto_local_name is not None and not getattr(args, "keep_alive", False):
            stop_all()

    return 0 if all(c == 0 for c in exit_codes) else 1


def _maybe_write_tb2_report(
    args: argparse.Namespace, trials: list[TrialResult],
) -> None:
    """Emit a TB-2 canonical BenchmarkResults JSON when --tb2-report is set.

    Imported lazily so the loom-benchmark-terminal-bench-2 package isn't
    a hard dependency unless the flag is actually used.
    """
    target: Path | None = getattr(args, "tb2_report", None)
    if target is None:
        return
    try:
        from loom_benchmark_terminal_bench_2.report import to_tb2_report
    except ImportError as exc:
        print(
            f"warning: --tb2-report set but loom-benchmark-terminal-bench-2 "
            f"is not installed: {exc}",
            file=sys.stderr,
        )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(to_tb2_report(trials), indent=2, sort_keys=True))


def _parse_model(spec: str) -> ModelSpec:
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


def _patch_agent(
    cfg: TaskConfig, agent_name: str, model: ModelSpec | None,
) -> TaskConfig:
    """Return a TaskConfig with agent.name swapped to `agent_name` and
    agent.model swapped to `model` if provided."""
    update_kwargs: dict[str, object] = {"name": agent_name}
    if model is not None:
        update_kwargs["model"] = model
    new_agent = cfg.agent.model_copy(update=update_kwargs)
    return cfg.model_copy(update={"agent": new_agent})


class UnsupportedFlagError(ValueError):
    """Raised when a CLI flag is incompatible with the chosen backend."""


_VALID_BACKENDS = {"docker", "fake", "daytona", "modal"}


def build_driver(*, backend: str, image: str, gpu: str | None = None) -> Driver:
    """Construct a Driver for ``backend``.

    Raises:
        UnsupportedFlagError: when ``--gpu`` is set with a backend that does
            not support GPU passthrough (docker / fake / daytona today).
        ValueError: for an unknown backend value.
        ModalConfigError: when ``--backend modal`` is chosen but Modal
            credentials env vars are not set.
    """
    if backend not in _VALID_BACKENDS:
        raise ValueError(
            f"Unknown --backend {backend!r}. "
            f"Valid: {sorted(_VALID_BACKENDS)}",
        )
    if backend == "fake":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend fake. "
                "Use --backend modal for GPU trials.",
            )
        return FakeDriver()
    if backend == "docker":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend docker. "
                "Use --backend modal for GPU trials.",
            )
        return DockerDriver(image=image)
    if backend == "daytona":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend daytona. "
                "Use --backend modal for GPU trials.",
            )
        from loom_drivers.daytona.config import DaytonaConfig
        from loom_drivers.daytona.driver import DaytonaDriver
        return DaytonaDriver(image=image, config=DaytonaConfig.from_env())
    # backend == "modal"
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver
    return ModalDriver(image=image, gpu=gpu, config=ModalConfig.from_env())


def _driver_factory(
    backend: str,
    cfg: TaskConfig,
    *,
    gpu: str | None = None,
) -> Callable[[], Driver]:
    """Return a zero-arg factory for the chosen backend.

    Each call to the returned factory constructs a fresh Driver instance.
    Heavy SDK config (e.g. ``DaytonaConfig`` / ``ModalConfig``) is captured
    in the closure so env vars are read once per ``loom run`` invocation.
    """
    image = cfg.environment.docker_image or "alpine"
    if backend not in _VALID_BACKENDS:
        raise SystemExit(f"unknown backend: {backend!r}")
    if backend in {"docker", "fake"}:
        if gpu is not None:
            raise UnsupportedFlagError(
                f"--gpu is not supported by --backend {backend}. "
                "Use --backend modal for GPU trials.",
            )
        if backend == "fake":
            return FakeDriver
        return lambda: DockerDriver(image=image)
    if backend == "daytona":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend daytona. "
                "Use --backend modal for GPU trials.",
            )
        from loom_drivers.daytona.config import DaytonaConfig
        from loom_drivers.daytona.driver import DaytonaDriver
        daytona_cfg = DaytonaConfig.from_env()
        return lambda: DaytonaDriver(image=image, config=daytona_cfg)
    # backend == "modal"
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver
    modal_cfg = ModalConfig.from_env()
    return lambda: ModalDriver(image=image, gpu=gpu, config=modal_cfg)


def _build_sdk_clients(
    tokens: dict[str, str],
) -> tuple[object, object, object]:
    """Construct provider SDK clients lazily. Each may be None if no
    token for that provider is set — UpstreamDirectGatewayClient raises
    a clear ValueError when the call tries to use a missing client."""
    a_client: object = None
    o_client: object = None
    g_client: object = None
    if tokens.get("anthropic"):
        try:
            import anthropic
            a_client = anthropic.AsyncAnthropic(api_key=tokens["anthropic"])
        except ImportError:
            pass
    if tokens.get("openai"):
        try:
            import openai
            o_client = openai.AsyncOpenAI(api_key=tokens["openai"])
        except ImportError:
            pass
    if tokens.get("google"):
        try:
            import google.generativeai as genai
            genai.configure(api_key=tokens["google"])  # type: ignore[attr-defined]
            g_client = genai
        except ImportError:
            pass
    return a_client, o_client, g_client


async def _maybe_post_result(
    server_url: str | None, result: TrialResult,
) -> None:
    url = server_url or os.environ.get("LOOM_SERVER_URL")
    if not url:
        return
    payload = format_json_line(result)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{url.rstrip('/')}/api/v1/cli/results",
                content=payload,
                headers={"content-type": "application/json"},
            )
    except Exception:
        pass


def _maybe_warn_unused_vllm_flags(
    args: argparse.Namespace, model: ModelSpec | None,
) -> None:
    """If the user set vLLM-launcher flags but the model spec doesn't
    trigger a launch (e.g. they passed `--model anthropic/...
    --tensor-parallel-size 2`), the flags are silently ignored. Warn
    once on stderr so users notice the typo."""
    if model is not None and model.provider in ("hf", "file"):
        return
    flagged: list[str] = []
    if getattr(args, "vllm_port", 0):
        flagged.append("--vllm-port")
    if getattr(args, "vllm_host", "127.0.0.1") != "127.0.0.1":
        flagged.append("--vllm-host")
    if getattr(args, "tensor_parallel_size", 1) != 1:
        flagged.append("--tensor-parallel-size")
    if getattr(args, "max_model_len", None) is not None:
        flagged.append("--max-model-len")
    if abs(getattr(args, "gpu_memory_utilization", 0.90) - 0.90) > 1e-9:
        flagged.append("--gpu-memory-utilization")
    if getattr(args, "enforce_eager", False):
        flagged.append("--enforce-eager")
    if getattr(args, "keep_alive", False):
        flagged.append("--keep-alive")
    if flagged:
        sys.stderr.write(
            "warning: ignoring vLLM-launcher flags "
            f"({', '.join(flagged)}) because --model is not "
            "`hf:<id>` or a local weights path. These flags only apply when "
            "Loom manages the vLLM lifecycle.\n",
        )
