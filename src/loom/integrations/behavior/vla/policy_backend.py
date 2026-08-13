"""Pinned OpenPI / BEHAVIOR-1K policy backend for the Stage 1 image.

The public launcher intentionally does not import this module's implementation
dependencies.  They are present only in the immutable GPU image.  This adapter
keeps the image boundary narrow and rejects version or API drift before loading
the signed checkpoint.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.integrations.behavior.errors import BehaviorContractError

if TYPE_CHECKING:
    from loom.integrations.behavior.vla.server import VlaServerBackend

_B1K_VERSION = "0.1.0"
# Image builds must pin these source identities in the image/SBOM lock.  A
# Python process cannot prove its Git origin from package metadata alone, so
# the runtime gate below additionally checks every consumed API before use.
PINNED_B1K_SOURCE_COMMIT = "ca556f74a455cef7987a2be4537b5ac85cc56dd7"
PINNED_OPENPI_SOURCE_COMMIT = "01177e0242a1c7e8fad2547caa0e987def614cda"
_POLICY_CONFIG = "pi_behavior_b1k_fast"
_CHECKPOINT_ROOT = Path("/inputs/policy/payload/checkpoint")
_HOST = "127.0.0.1"
_PORT = 8000


class _LoadedPolicy(Protocol):
    _rng: object

    @property
    def metadata(self) -> dict[str, Any]: ...


class _TrainingConfig(Protocol):
    name: str


class _ServingRuntime(Protocol):
    """The exact image-owned symbols used by the Stage 1 VLA process."""

    @property
    def version(self) -> str: ...

    def get_config(self, name: str) -> _TrainingConfig: ...

    def create_policy(
        self,
        config: _TrainingConfig,
        checkpoint: Path,
        *,
        sample_kwargs: dict[str, int],
        default_prompt: str,
    ) -> _LoadedPolicy: ...

    def seed_policy(self, policy: _LoadedPolicy, seed: int) -> None: ...

    def wrap_policy(self, policy: _LoadedPolicy, *, task_id: int) -> object: ...

    def make_server(
        self,
        *,
        policy: object,
        host: str,
        port: int,
        metadata: dict[str, Any],
    ) -> object: ...

    def serve_forever(self, server: object) -> None: ...


def _require_signature(
    value: object,
    *,
    label: str,
    parameters: tuple[str, ...],
) -> None:
    if not callable(value):
        raise BehaviorContractError(f"pinned VLA API is missing callable {label}")
    try:
        actual = inspect.signature(value).parameters
    except (TypeError, ValueError) as exc:
        raise BehaviorContractError(f"pinned VLA API has no inspectable {label}") from exc
    missing = [name for name in parameters if name not in actual]
    if missing:
        raise BehaviorContractError(
            f"pinned VLA API drift at {label}: missing {','.join(missing)}"
        )


@dataclass(frozen=True)
class _PinnedServingRuntime:
    version: str
    _get_config: Callable[[str], _TrainingConfig]
    _create_policy: Callable[..., _LoadedPolicy]
    _jax_key: Callable[[int], object]
    _wrapper_config: Callable[..., object]
    _wrapper: Callable[..., object]
    _server: Callable[..., object]

    def get_config(self, name: str) -> _TrainingConfig:
        return self._get_config(name)

    def create_policy(
        self,
        config: _TrainingConfig,
        checkpoint: Path,
        *,
        sample_kwargs: dict[str, int],
        default_prompt: str,
    ) -> _LoadedPolicy:
        return self._create_policy(
            config,
            checkpoint,
            sample_kwargs=sample_kwargs,
            default_prompt=default_prompt,
        )

    def seed_policy(self, policy: _LoadedPolicy, seed: int) -> None:
        # The pinned OpenPI Policy currently defaults to key(0).  Stage 1 owns
        # the signed seed, so accepting that default would make the request lie.
        if not hasattr(policy, "_rng"):
            raise BehaviorContractError("pinned OpenPI policy has no JAX RNG authority")
        policy._rng = self._jax_key(seed)

    def wrap_policy(self, policy: _LoadedPolicy, *, task_id: int) -> object:
        wrapper_config = self._wrapper_config(
            actions_to_execute=26,
            actions_to_keep=4,
            execute_in_n_steps=20,
            history_len=3,
            votes_to_promote=2,
            time_threshold_inpaint=0.3,
            num_steps=20,
            apply_eval_tricks=True,
        )
        return self._wrapper(
            policy,
            text_prompt="PI_BEHAVIOR model (task-conditioned)",
            task_id=task_id,
            config=wrapper_config,
            checkpoint_switcher=None,
        )

    def make_server(
        self,
        *,
        policy: object,
        host: str,
        port: int,
        metadata: dict[str, Any],
    ) -> object:
        return self._server(policy=policy, host=host, port=port, metadata=metadata)

    def serve_forever(self, server: object) -> None:
        serve = getattr(server, "serve_forever", None)
        if not callable(serve):
            raise BehaviorContractError(
                "pinned VLA API drift at WebsocketPolicyServer.serve_forever"
            )
        serve()


def _load_runtime() -> _ServingRuntime:
    """Resolve only symbols verified in the pinned Stage 1 image stack."""

    try:
        b1k = import_module("b1k")
        policy_config = import_module("b1k.policies.policy_config")
        wrapper = import_module("b1k.shared.eval_b1k_wrapper")
        training_config = import_module("b1k.training.config")
        jax_random = import_module("jax.random")
        network = import_module("omnigibson.learning.utils.network_utils")
    except ImportError as exc:  # pragma: no cover - GPU image boundary
        unresolved = exc.name or "unknown"
        raise BehaviorContractError(
            f"pinned VLA runtime import is unavailable: {unresolved}"
        ) from exc

    version = getattr(b1k, "__version__", None)
    if version != _B1K_VERSION:
        raise BehaviorContractError(
            f"pinned VLA runtime requires b1k {_B1K_VERSION}, found {version!r}"
        )
    try:
        get_config = training_config.get_config
        create_policy = policy_config.create_trained_policy
        wrapper_config = wrapper.B1KWrapperConfig
        policy_wrapper = wrapper.B1KPolicyWrapper
        jax_key = jax_random.key
        server = network.WebsocketPolicyServer
    except AttributeError as exc:
        raise BehaviorContractError(
            f"pinned VLA API is missing symbol {exc.name or 'unknown'}"
        ) from exc

    _require_signature(get_config, label="b1k.training.config.get_config", parameters=("config_name",))
    _require_signature(
        create_policy,
        label="b1k.policies.policy_config.create_trained_policy",
        parameters=("train_config", "checkpoint_dir", "sample_kwargs", "default_prompt"),
    )
    _require_signature(jax_key, label="jax.random.key", parameters=("seed",))
    _require_signature(
        wrapper_config,
        label="b1k.shared.eval_b1k_wrapper.B1KWrapperConfig",
        parameters=(
            "actions_to_execute",
            "actions_to_keep",
            "execute_in_n_steps",
            "history_len",
            "votes_to_promote",
            "time_threshold_inpaint",
            "num_steps",
            "apply_eval_tricks",
        ),
    )
    _require_signature(
        policy_wrapper,
        label="b1k.shared.eval_b1k_wrapper.B1KPolicyWrapper",
        parameters=("policy", "task_id", "config", "checkpoint_switcher"),
    )
    _require_signature(
        server,
        label="omnigibson.learning.utils.network_utils.WebsocketPolicyServer",
        parameters=("policy", "host", "port", "metadata"),
    )
    return _PinnedServingRuntime(
        version=version,
        _get_config=cast(Callable[[str], _TrainingConfig], get_config),
        _create_policy=cast(Callable[..., _LoadedPolicy], create_policy),
        _jax_key=cast(Callable[[int], object], jax_key),
        _wrapper_config=cast(Callable[..., object], wrapper_config),
        _wrapper=cast(Callable[..., object], policy_wrapper),
        _server=cast(Callable[..., object], server),
    )


class OpenPiStage1ServerBackend:
    """Load one exact checkpoint and serve it on the private loopback socket."""

    def __init__(self, runtime: _ServingRuntime | None = None) -> None:
        self._runtime = runtime
        self._policy: object | None = None
        self._metadata: dict[str, Any] | None = None

    def load_policy(
        self,
        *,
        task_id: int,
        checkpoint: Path,
        policy_config: str,
        seed: int,
    ) -> None:
        if self._policy is not None:
            raise BehaviorContractError("Stage 1 VLA policy may be loaded only once")
        if policy_config != _POLICY_CONFIG:
            raise BehaviorContractError("Stage 1 VLA policy config drift")
        if checkpoint != _CHECKPOINT_ROOT:
            raise BehaviorContractError("Stage 1 VLA checkpoint root drift")
        if checkpoint.is_symlink() or not checkpoint.is_dir():
            raise BehaviorContractError("signed VLA checkpoint root is not a real directory")
        if isinstance(task_id, bool) or not 0 <= task_id <= 9_999:
            raise BehaviorContractError("Stage 1 VLA task id is outside 0..9999")
        if isinstance(seed, bool) or not 0 <= seed <= 4_294_967_295:
            raise BehaviorContractError("Stage 1 VLA seed is outside uint32")

        runtime = self._runtime or _load_runtime()
        if runtime.version != _B1K_VERSION:
            raise BehaviorContractError(
                f"pinned VLA runtime requires b1k {_B1K_VERSION}, found {runtime.version!r}"
            )
        try:
            config = runtime.get_config(policy_config)
            if config.name != policy_config:
                raise BehaviorContractError("resolved VLA training config identity drift")
            base_policy = runtime.create_policy(
                config,
                checkpoint,
                sample_kwargs={"num_steps": 20},
                default_prompt="PI_BEHAVIOR model (task-conditioned)",
            )
            runtime.seed_policy(base_policy, seed)
            self._metadata = dict(base_policy.metadata)
            self._policy = runtime.wrap_policy(base_policy, task_id=task_id)
        except BehaviorContractError:
            raise
        except Exception as exc:
            raise BehaviorContractError("signed Stage 1 VLA checkpoint load failed") from exc

    def serve(self, *, host: str, port: int) -> None:
        if host != _HOST or port != _PORT:
            raise BehaviorContractError("Stage 1 VLA server must bind only 127.0.0.1:8000")
        if self._policy is None or self._metadata is None:
            raise BehaviorContractError("Stage 1 VLA server cannot start before checkpoint load")
        runtime = self._runtime or _load_runtime()
        server = runtime.make_server(
            policy=self._policy,
            host=host,
            port=port,
            metadata=self._metadata,
        )
        runtime.serve_forever(server)


def create_server_backend() -> OpenPiStage1ServerBackend:
    """Production image factory loaded by :mod:`loom.integrations.behavior.vla.server`."""

    return OpenPiStage1ServerBackend()


if TYPE_CHECKING:

    def _protocol_check(backend: OpenPiStage1ServerBackend) -> VlaServerBackend:
        return backend


__all__ = [
    "PINNED_B1K_SOURCE_COMMIT",
    "PINNED_OPENPI_SOURCE_COMMIT",
    "OpenPiStage1ServerBackend",
    "create_server_backend",
]
