"""Fixed unprivileged TLS/receiver composition from protected local policy.

This process does not install its policy, modify Slurm/cgroups or authorize
execution. Its service unit must contain all descendants on abrupt process loss.
"""

from __future__ import annotations

import asyncio
import ipaddress
import os
import signal
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_capacity_executor.native_bootstrap_delivery import BootstrapDeliveryError, _canonical
from loom_capacity_executor.native_bootstrap_process_adapter import (
    NativeBootstrapProcessAdapter,
    NativeBootstrapReceiverProcessPolicy,
)
from loom_capacity_executor.native_bootstrap_receiver import (
    _load_fixed_receiver,
    _protected_destination,
)
from loom_capacity_executor.native_bootstrap_transport import (
    NativeBootstrapPeer,
    NativeBootstrapTLSIdentity,
    NativeBootstrapTLSServer,
    NativeBootstrapTransportLimits,
    _assert_private_process,
)
from loom_capacity_executor.native_worker_bootstrap import _disable_bootstrap_dumps
from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2
from loom_capacity_executor.trusted_launcher import _read_verified_file
from loom_capacity_manager.contracts import Digest, Identifier

_FAILURE = "native bootstrap supervisor unavailable or refused"


class NativeBootstrapSupervisorPeerV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    certificate_sha256: Digest
    pool_id: Identifier
    executor_id: Identifier
    executor_incarnation: UUID
    operations: tuple[Literal["deliver", "status"], ...]
    expires_at: datetime

    @model_validator(mode="after")
    def _valid_peer(self) -> NativeBootstrapSupervisorPeerV1:
        if tuple(sorted(set(self.operations))) != self.operations:
            raise ValueError("native bootstrap peer operations are not canonical")
        self.peer()
        return self

    def peer(self) -> NativeBootstrapPeer:
        return NativeBootstrapPeer(pool_id=self.pool_id, executor_id=self.executor_id,
            executor_incarnation=self.executor_incarnation, operations=frozenset(self.operations),
            expires_at=self.expires_at)


class NativeBootstrapSupervisorConfigV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_name: Literal["loom.native-bootstrap-supervisor-config/v1"] = Field(default="loom.native-bootstrap-supervisor-config/v1", alias="schema")
    listen_address: Annotated[str, Field(max_length=64)]
    listen_port: Annotated[int, Field(ge=1024, le=65535)]
    target_node: Identifier
    pool_id: Identifier
    trusted_release_sha256: Digest
    identity: NativeBootstrapTLSIdentity
    receiver: NativeBootstrapReceiverProcessPolicy
    limits: NativeBootstrapTransportLimits = NativeBootstrapTransportLimits()
    peers: Annotated[tuple[NativeBootstrapSupervisorPeerV1, ...], Field(min_length=1, max_length=32)]

    @field_validator("listen_address")
    @classmethod
    def _exact_address(cls, value: str) -> str:
        address = ipaddress.ip_address(value)
        if str(address) != value or address.is_unspecified or address.is_multicast or "%" in value:
            raise ValueError("native bootstrap listener requires one exact numeric address")
        return value

    @model_validator(mode="after")
    def _coherent(self) -> NativeBootstrapSupervisorConfigV1:
        pins = tuple(peer.certificate_sha256 for peer in self.peers)
        if (pins != tuple(sorted(set(pins))) or any(peer.pool_id != self.pool_id for peer in self.peers)
            or self.receiver.maximum_processes > self.limits.maximum_operations
            or self.receiver.timeout_seconds + self.receiver.cleanup_wait_seconds > self.limits.total_seconds):
            raise ValueError("native bootstrap supervisor policy is inconsistent")
        return self


def load_native_bootstrap_supervisor_config(identity: SlurmFileIdentityV2) -> NativeBootstrapSupervisorConfigV1:
    """Read pinned local files before any listener or receiver process exists."""
    try:
        with _protected_destination(Path(identity.path).parent):
            raw = _read_verified_file(identity, label="native bootstrap supervisor configuration", executable=False)
        if len(raw) > 64 * 1024:
            raise ValueError
        config = NativeBootstrapSupervisorConfigV1.model_validate_json(raw)
        if _canonical(config) != raw:
            raise ValueError
        receiver = _load_fixed_receiver(config.receiver.configuration)
        with _protected_destination(receiver.directory):
            pass
        if (receiver.target_node != config.target_node or receiver.pool_id != config.pool_id
            or receiver.trusted_release_sha256 != config.trusted_release_sha256):
            raise ValueError
        for pin in (config.identity.ca, config.identity.certificate, config.identity.private_key):
            with _protected_destination(Path(pin.path).parent):
                pass
        return config
    except Exception:
        raise BootstrapDeliveryError(_FAILURE) from None


async def _join_shutdown(task: asyncio.Task[None]) -> None:
    cancelled = False
    while True:
        try:
            await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


async def run_native_bootstrap_supervisor(config: NativeBootstrapSupervisorConfigV1, stop: asyncio.Event) -> None:
    """Observe both listener and cleanup failure; join every constructed owner."""
    adapter: NativeBootstrapProcessAdapter | None = None
    server: NativeBootstrapTLSServer | None = None
    watchers: list[asyncio.Task[None] | asyncio.Task[bool]] = []

    async def shutdown() -> None:
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)
        try:
            if server is not None:
                await server.aclose()
        finally:
            if adapter is not None:
                await adapter.aclose()

    try:
        try:
            _assert_private_process()
            config = NativeBootstrapSupervisorConfigV1.model_validate_json(config.model_dump_json(by_alias=True))
            adapter = NativeBootstrapProcessAdapter(config.receiver)
            server = NativeBootstrapTLSServer(adapter, identity=config.identity, target_node=config.target_node,
                pool_id=config.pool_id, trusted_release_sha256=config.trusted_release_sha256,
                peers={peer.certificate_sha256: peer.peer() for peer in config.peers}, limits=config.limits)
            await server.start(host=config.listen_address, port=config.listen_port)
            stopped = asyncio.create_task(stop.wait())
            listener = asyncio.create_task(server.wait())
            cleanup_failure = asyncio.create_task(adapter.wait())
            watchers.extend((stopped, listener, cleanup_failure))
            done, _pending = await asyncio.wait(watchers, return_when=asyncio.FIRST_COMPLETED)
            # An error coincident with a stop request must still be reported.
            if listener in done or cleanup_failure in done:
                raise BootstrapDeliveryError(_FAILURE)
        finally:
            await _join_shutdown(asyncio.create_task(shutdown()))
    except asyncio.CancelledError:
        raise
    except Exception:
        raise BootstrapDeliveryError(_FAILURE) from None


async def _run_service(config: NativeBootstrapSupervisorConfigV1) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    try:
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            loop.add_signal_handler(number, stop.set)
            installed.append(number)
        await run_native_bootstrap_supervisor(config, stop)
    finally:
        for number in installed:
            loop.remove_signal_handler(number)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        _disable_bootstrap_dumps()
        arguments = tuple(sys.argv[1:] if argv is None else argv)
        if len(arguments) != 6 or arguments[::2] != ("--configuration", "--configuration-sha256", "--configuration-owner-uid"):
            raise ValueError
        owner = int(arguments[5])
        if str(owner) != arguments[5]:
            raise ValueError
        config = load_native_bootstrap_supervisor_config(SlurmFileIdentityV2(
            path=arguments[1], sha256=arguments[3], owner_uid=owner))
        asyncio.run(_run_service(config))
        return 0
    except BaseException:
        try:
            os.set_blocking(2, False)
            os.write(2, (_FAILURE + "\n").encode("ascii"))
        except OSError:
            pass
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
