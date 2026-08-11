"""Fixed Stage 1 VLA server launcher.

The pinned GPU image provides the policy implementation.  This module owns the
closed CLI, one-time seed authority, checkpoint root, and loopback-only bind.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

from loom.integrations.behavior.errors import BehaviorContractError, BehaviorExitCode
from loom.integrations.behavior.stages.pipeline_mode import SeedAuthority


class VlaServerBackend(Protocol):
    def load_policy(
        self,
        *,
        task_id: int,
        checkpoint: Path,
        policy_config: str,
        seed: int,
    ) -> None: ...

    def serve(self, *, host: str, port: int) -> None: ...


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m loom.integrations.behavior.vla.server")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("policy_mode", choices=("policy:checkpoint",))
    parser.add_argument("--policy.config", dest="policy_config", required=True)
    parser.add_argument("--policy.dir", dest="policy_dir", type=Path, required=True)
    return parser


def _production_backend() -> VlaServerBackend:
    try:
        module = import_module("loom.integrations.behavior.vla.policy_backend")
        factory = cast(Callable[[], VlaServerBackend], module.create_server_backend)
    except ImportError as exc:  # pragma: no cover - pinned GPU image boundary
        raise BehaviorContractError("pinned image is missing the Loom VLA policy backend") from exc
    return factory()


def run_server(
    *,
    task_id: int,
    seed: int,
    port: int,
    policy_config: str,
    policy_dir: Path,
    backend: VlaServerBackend,
    seed_authority: SeedAuthority | None = None,
) -> None:
    if isinstance(task_id, bool) or not 0 <= task_id <= 9_999:
        raise BehaviorContractError("task-id is outside 0..9999")
    if isinstance(seed, bool) or not 0 <= seed <= 4_294_967_295:
        raise BehaviorContractError("seed is outside uint32")
    if port != 8000:
        raise BehaviorContractError("Pipeline VLA port must be exactly 8000")
    if policy_config != "pi_behavior_b1k_fast":
        raise BehaviorContractError("Pipeline VLA policy config drift")
    if policy_dir != Path("/inputs/policy/payload/checkpoint"):
        raise BehaviorContractError("Pipeline VLA checkpoint root drift")
    authority = seed_authority or SeedAuthority(allow_same_seed_replay=False)
    authority.apply(seed)
    backend.load_policy(
        task_id=task_id,
        checkpoint=policy_dir,
        policy_config=policy_config,
        seed=seed,
    )
    backend.serve(host="127.0.0.1", port=port)


def main(
    argv: Sequence[str] | None = None,
    *,
    backend_factory: Callable[[], VlaServerBackend] = _production_backend,
) -> int:
    args = _parser().parse_args(argv)
    try:
        run_server(
            task_id=args.task_id,
            seed=args.seed,
            port=args.port,
            policy_config=args.policy_config,
            policy_dir=args.policy_dir,
            backend=backend_factory(),
        )
        return int(BehaviorExitCode.SUCCESS)
    except (BehaviorContractError, OSError, ValueError) as exc:
        print(f"contract error: {exc}")
        return int(BehaviorExitCode.CONTRACT_ERROR)


if __name__ == "__main__":
    raise SystemExit(main())
