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
from loom_cli.config import load_config
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner
from loom_cli.output import format_json_line, format_text_line
from loom_cli.task_loader import LoadedTask, load_tasks


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
                driver_factory=_driver_factory(args.backend, patched),
                output_dir=output_dir,
                object_store=store,
                upstream_gateway_tokens=tokens,
                anthropic_client=a_client,
                openai_client=o_client,
                google_client=g_client,
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

    exit_codes = await asyncio.gather(*[_one(t) for t in tasks])
    _maybe_write_tb2_report(args, completed)
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
    if "/" not in spec:
        raise SystemExit(
            f"--model must be 'provider/name' (got {spec!r}); "
            f"e.g. anthropic/claude-opus-4-7",
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


def _driver_factory(backend: str, cfg: TaskConfig) -> Callable[[], Driver]:
    image = cfg.environment.docker_image or "alpine"
    if backend == "fake":
        return FakeDriver
    if backend == "docker":
        return lambda: DockerDriver(image=image)
    raise SystemExit(f"unknown backend: {backend!r}")


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
