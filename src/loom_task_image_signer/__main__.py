"""Run only an explicitly configured, operator-provisioned dedicated signer."""

from __future__ import annotations

import argparse
import asyncio
import signal
from pathlib import Path

from loom_task_image_authority.config import read_owner_only_bytes
from loom_task_image_signer.config import SignerSettings, decode_signer_settings
from loom_task_image_signer.runtime import running_signer


async def _run(settings: SignerSettings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    try:
        async with running_signer(settings) as server:
            stopping = asyncio.create_task(stop.wait())
            serving = asyncio.create_task(server.wait())
            try:
                done, _ = await asyncio.wait((stopping, serving), return_when=asyncio.FIRST_COMPLETED)
                if serving in done:
                    await serving
                    raise RuntimeError("dedicated signer listener stopped unexpectedly")
            finally:
                stopping.cancel()
                serving.cancel()
                await asyncio.gather(stopping, serving, return_exceptions=True)
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the operator-provisioned dedicated task-image signer")
    parser.add_argument("--config", type=Path, required=True)
    options = parser.parse_args()
    try:
        settings = decode_signer_settings(read_owner_only_bytes(options.config, max_bytes=64 * 1024))
        asyncio.run(_run(settings))
    except KeyboardInterrupt:
        return
    except Exception:
        # DB URLs, key metadata, provider errors and configuration values are not
        # safe diagnostic output. Operators inspect protected configuration.
        raise SystemExit("dedicated signer failed startup or operation") from None


if __name__ == "__main__":
    main()
