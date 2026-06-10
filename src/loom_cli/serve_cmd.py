"""`loom serve <spec> --name X` — foreground vLLM launcher.

Launches a vLLM subprocess (HF id or local weights path), writes a
transient `[local_providers.<name>]` block to
`~/.config/loom/config.toml`, blocks until Ctrl-C / SIGTERM, then
removes the config block on shutdown.

Reuses `vllm_runner.launch_vllm` for the subprocess lifecycle. The
server is the user's responsibility to share across `loom run`
invocations (they reference it via `--model local/<name>/...`).
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from loom_cli.config import set_local_provider, unset_local_provider
from loom_cli.vllm_runner import (
    _LIVE_PROCESSES,  # deliberate seam: find the Popen by pid after launch
    MissingVLLMDependencyError,
    VLLMLaunchSpec,
    launch_vllm,
    model_slug,
    stop_one,
)


async def serve(args: argparse.Namespace) -> int:
    """Launch a vLLM server, register it as local.<name>, block
    until shutdown, then deregister + stop the server."""
    _validate_serve_spec(args.model_spec)
    name = args.name or model_slug(args.model_spec)

    from loom_cli.config import load_config
    existing_cfg = load_config()
    if name in existing_cfg.local_providers:
        print(
            f"local/{name} is already registered. "
            f"Run `loom config unset local.{name}.base_url` first, "
            "or pass a different `--name`.",
            file=sys.stderr,
        )
        return 2

    try:
        info = launch_vllm(VLLMLaunchSpec(
            model=args.model_spec.removeprefix("hf:"),
            port=args.vllm_port,
            host=args.vllm_host,
            gpu_memory_utilization=args.gpu_memory_utilization,
            tensor_parallel_size=args.tensor_parallel_size,
            max_model_len=args.max_model_len,
            enforce_eager=args.enforce_eager,
        ))
    except MissingVLLMDependencyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"vLLM launch failed: {exc}", file=sys.stderr)
        return 2

    # Find the Popen we just registered so cleanup hits this specific
    # subprocess (rather than `stop_all`).
    proc = next(
        (p for p in _LIVE_PROCESSES if p.pid == info.pid),
        None,
    )

    set_local_provider(
        name,
        base_url=info.base_url,
        api_key=None,
        served_model_name=info.served_model_name,
    )
    sys.stderr.write(f"→ registered as local/{name}\n")
    sys.stderr.write("→ keeping process alive; Ctrl-C to stop\n")

    try:
        await _block_until_shutdown()
    finally:
        unset_local_provider(name)
        if proc is not None:
            stop_one(proc)

    return 0


async def _block_until_shutdown() -> None:
    """Block forever until SIGINT / SIGTERM. Tests override this."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:  # pragma: no cover — Windows
            pass
    await stop_event.wait()


def _validate_serve_spec(spec: str) -> None:
    """Raise SystemExit if `spec` isn't a serveable model id."""
    if spec.startswith(("/", "~", "./", "../")):
        return
    if spec.startswith("hf:"):
        body = spec[len("hf:"):]
        if "/" not in body:
            raise SystemExit(
                f"hf:<id> must be `<org>/<name>` (got hf:{body!r}). "
                "Example: hf:meta-llama/Llama-3.1-8B-Instruct",
            )
        return
    raise SystemExit(
        f"`loom serve` takes hf:<org>/<name> or a path to weights "
        f"(starting with /, ~, ./, ../); got {spec!r}.",
    )
