"""Protected, execute-only staging rollout operator primitives."""

from .config import ConfigError, OperatorConfig
from .model import (
    ActivePointer,
    AttemptIdentity,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    RequestEvent,
    RolloutRequest,
)
from .store import RequestStore, RequestStoreError

__all__ = [
    "ActivePointer",
    "AttemptIdentity",
    "CallerIdentity",
    "CandidateBinding",
    "ConfigError",
    "DriverEnvelope",
    "OperatorConfig",
    "RequestEvent",
    "RequestStore",
    "RequestStoreError",
    "RolloutRequest",
]
