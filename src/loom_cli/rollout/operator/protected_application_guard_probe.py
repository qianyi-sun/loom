"""Fresh fixed epoch reads through the retained guard's existing connection.

The private runtime mailbox carries a nonce and original guard digest, never SQL,
credentials or a database target. A reply is usable only by its waiting challenge
and while the same supervised guard remains ready. It is not handoff completion.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from .protected_application_guard_retention import _read, _read_pending_retention

if TYPE_CHECKING:
    from loom_cli.rollout.readonly_database_authority import DatabaseQuery

    from .config import OperatorConfig
    from .staging_mutation_guard import MutationGuardEvidence


_SECONDS = 35.0


def _paths(config: OperatorConfig, guard: MutationGuardEvidence, uid: int) -> tuple[Path, Path]:
    from .staging_mutation_guard import _ensure_evidence_directory

    root = _ensure_evidence_directory(config, service_uid=uid)
    identity = hashlib.sha256(f"{guard.request_id}:{guard.generation}".encode()).hexdigest()
    return root / f"probe-{identity}.request.json", root / f"probe-{identity}.response.json"


def _publish(path: Path, value: dict[str, object]) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    temporary = path.with_name(f".{path.name}.{uuid4().hex}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _is_retained(config: OperatorConfig, guard: MutationGuardEvidence, uid: int) -> bool:
    pending = _read_pending_retention(
        config.state_root,
        request_id=guard.request_id,
        service_uid=uid,
        guard=guard,
        require_record=True,
    )
    if pending is None:
        return False
    if not pending.acknowledged:
        raise RuntimeError("application guard epoch probe requires acknowledged retention")
    return True


def _require_retained(config: OperatorConfig, guard: MutationGuardEvidence, uid: int) -> None:
    if not _is_retained(config, guard, uid):
        raise RuntimeError("application guard epoch probe retention already completed")


def _check_challenge(value: dict[str, object], guard: MutationGuardEvidence) -> str:
    nonce = value.get("nonce")
    if (
        set(value) != {"schema_version", "guard_digest", "nonce"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["guard_digest"] != guard.evidence_digest
        or not isinstance(nonce, str)
        or re.fullmatch(r"[0-9a-f]{32}", nonce) is None
    ):
        raise ValueError("application guard epoch challenge binding changed")
    return nonce


def answer_retained_epoch_probe(
    config: OperatorConfig,
    *,
    guard: MutationGuardEvidence,
    service_uid: int,
    query: DatabaseQuery,
    assert_healthy: Callable[[], object],
) -> None:
    """Called only by the original guard loop, on its original database channel."""
    from .staging_mutation_guard import _READ_EPOCH_SQL, _parse_scalar

    if guard.guard_pid != os.getpid():
        raise RuntimeError("application guard epoch probe process changed")
    if not _is_retained(config, guard, service_uid):
        return
    request_path, response_path = _paths(config, guard, service_uid)
    try:
        request = _read(request_path, service_uid)
    except FileNotFoundError:
        return
    nonce = _check_challenge(request, guard)
    try:
        prior = _read(response_path, service_uid)
    except FileNotFoundError:
        prior = None
    if prior is not None and prior.get("nonce") == nonce:
        return
    assert_healthy()
    epoch: int | None = None
    try:
        observed = _parse_scalar(query(_READ_EPOCH_SQL), key="mutation_epoch", expected_type=int)
        if observed >= 0:
            epoch = observed
    except Exception:
        # A bounded SELECT failure is not permission to destroy a healthy guard.
        # Return only a sanitized refusal after independently checking the lock.
        pass
    assert_healthy()
    if not _is_retained(config, guard, service_uid):
        return
    _publish(response_path, {**request, "epoch": epoch})


def probe_retained_epoch(
    config: OperatorConfig,
    *,
    guard: MutationGuardEvidence,
    service_uid: int,
    assert_ready: Callable[[], MutationGuardEvidence],
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Challenge the same guard; never connect, reopen admission or reacquire."""
    _require_retained(config, guard, service_uid)
    if assert_ready() != guard:
        raise RuntimeError("application guard epoch probe original identity changed")
    request_path, response_path = _paths(config, guard, service_uid)
    challenge: dict[str, object] = {
        "schema_version": 1,
        "guard_digest": guard.evidence_digest,
        "nonce": uuid4().hex,
    }
    started = monotonic()
    if not math.isfinite(started):
        raise RuntimeError("application guard epoch probe clock is invalid")
    _publish(request_path, challenge)
    for _ in range(350):
        now = monotonic()
        if not math.isfinite(now) or now < started or now - started >= _SECONDS:
            break
        if assert_ready() != guard:
            raise RuntimeError("application guard epoch probe original identity changed")
        try:
            response = _read(response_path, service_uid)
        except FileNotFoundError:
            response = None
        if response is not None and response.get("nonce") == challenge["nonce"]:
            if (
                set(response) != {*challenge, "epoch"}
                or any(response.get(key) != value for key, value in challenge.items())
                or type(response["schema_version"]) is not int
                or type(response["epoch"]) is not int
                or response["epoch"] < 0
            ):
                raise RuntimeError("application guard epoch probe refused or changed")
            _require_retained(config, guard, service_uid)
            if assert_ready() != guard:
                raise RuntimeError("application guard epoch probe original identity changed")
            return response["epoch"]
        sleep(0.1)
    raise RuntimeError("application guard epoch probe timed out")
