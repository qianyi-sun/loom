"""Code-backed, immutable official Recipe registry for runnable v1 graphs."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.pipeline.spec import (
    ContainerNodeV1,
    Digest,
    OutcomeGateNodeV1,
    RecipeIdentityV1,
    RequestRendererLockV1,
    RunGraphSpecV1,
    StageOutputBindingV1,
    TerminalOutputsBindingV1,
)

SubmissionPolicy = Literal["ordinary", "acceptance_authorization_only"]
RecipeFactory = Callable[[RecipeIdentityV1, Mapping[str, Any]], RunGraphSpecV1]

_SECRET_KEY = re.compile(r"(?:^|_)(?:api_?key|password|passwd|secret|token|credential)(?:$|_)", re.I)
_SECRET_VALUE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/=-]{12,}|"
    r"\b(?:sk|rk|ghp|gho|github_pat)-?[A-Za-z0-9_\-]{16,})"
)
_OPAQUE_REFERENCE = re.compile(r"^(?:loom|k8s-secret)://[A-Za-z0-9._/@:-]+$")


def reject_secret_literals(value: Any, *, reference_field: bool = False) -> None:
    """Reject secret material before any graph or registry metadata persists."""

    if isinstance(value, BaseModel):
        reject_secret_literals(value.model_dump(mode="json", exclude_none=False))
    elif isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            is_reference = key.endswith("_ref") or key.endswith("_reference_id")
            if _SECRET_KEY.search(key) and not is_reference:
                raise ValueError("secret-looking field name is forbidden")
            reject_secret_literals(item, reference_field=is_reference)
    elif isinstance(value, list | tuple):
        for item in value:
            reject_secret_literals(item, reference_field=reference_field)
    elif isinstance(value, str):
        if reference_field:
            if _OPAQUE_REFERENCE.fullmatch(value):
                return
            raise ValueError("secret reference must be an opaque provider reference ID")
        if _SECRET_VALUE.search(value):
            raise ValueError("secret-looking literal is forbidden")


@dataclass(frozen=True, slots=True)
class ConditionalOutputContract:
    """Code-backed outcome-to-optional-output contract for one Recipe node."""

    stage_key: str
    outcomes: tuple[tuple[str, tuple[str, ...]], ...]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.stage_key):
            raise ValueError("invalid conditional-output stage key")
        names = [outcome for outcome, _outputs in self.outcomes]
        if len(names) != len(set(names)):
            raise ValueError("conditional-output outcomes must be unique")
        for outcome, outputs in self.outcomes:
            if not outcome or len(outcome.encode("utf-8")) > 128:
                raise ValueError("conditional-output outcome is invalid")
            if len(outputs) != len(set(outputs)) or any(
                re.fullmatch(r"[a-z][a-z0-9_]{0,62}", output) is None
                for output in outputs
            ):
                raise ValueError("conditional-output names must be valid and unique")


@dataclass(frozen=True, slots=True)
class OfficialRecipeRegistration:
    """One repo-owned, versioned, code-backed Recipe registration."""

    name: str
    version: int
    submission_policy: SubmissionPolicy
    factory: RecipeFactory
    parameter_contract_digest: Digest
    source_lock_digest: Digest
    renderer_locks: tuple[RequestRendererLockV1, ...] = ()
    conditional_output_contracts: tuple[ConditionalOutputContract, ...] = ()

    def __post_init__(self) -> None:
        RecipeIdentityV1(
            name=self.name,
            version=self.version,
            digest="sha256:" + "0" * 64,
        )
        if self.submission_policy not in {"ordinary", "acceptance_authorization_only"}:
            raise ValueError("invalid Recipe submission policy")
        is_fixed_preflight = (self.name, self.version) == (
            "behavior-recovery-acceptance-preflight",
            1,
        )
        if (self.submission_policy == "acceptance_authorization_only") != is_fixed_preflight:
            raise ValueError(
                "only the fixed acceptance preflight Recipe may use, and must use, this policy"
            )
        for label, digest in (
            ("parameter contract", self.parameter_contract_digest),
            ("source lock", self.source_lock_digest),
        ):
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError(f"invalid {label} digest")
        lock_names = [(lock.name, lock.version) for lock in self.renderer_locks]
        if len(lock_names) != len(set(lock_names)):
            raise ValueError("renderer locks must be unique")
        contract_keys = [contract.stage_key for contract in self.conditional_output_contracts]
        if len(contract_keys) != len(set(contract_keys)):
            raise ValueError("conditional-output contracts must have unique stage keys")

    @property
    def digest(self) -> str:
        lock_digests = [canonical_digest(lock) for lock in self.renderer_locks]
        return canonical_digest(
            {
                "name": self.name,
                "conditional_output_contracts": [
                    {
                        "outcomes": [
                            {"name": outcome, "required_outputs": list(outputs)}
                            for outcome, outputs in contract.outcomes
                        ],
                        "stage_key": contract.stage_key,
                    }
                    for contract in self.conditional_output_contracts
                ],
                "parameter_contract_digest": self.parameter_contract_digest,
                "renderer_lock_digests": lock_digests,
                "source_lock_digest": self.source_lock_digest,
                "submission_policy": self.submission_policy,
                "version": self.version,
            }
        )

    @property
    def identity(self) -> RecipeIdentityV1:
        return RecipeIdentityV1(name=self.name, version=self.version, digest=self.digest)

    def resolve(
        self, parameters: Mapping[str, Any], *, repo_root: Path | None = None
    ) -> RunGraphSpecV1:
        reject_secret_literals(parameters)
        if self.renderer_locks and repo_root is None:
            raise ValueError("renderer locks require a repository root for drift verification")
        lock_digests: dict[tuple[str, int], str] = {}
        for lock in self.renderer_locks:
            assert repo_root is not None
            lock_digests[(lock.name, lock.version)] = verify_renderer_lock(lock, repo_root)
        graph = self.factory(self.identity, parameters)
        if graph.recipe != self.identity:
            raise ValueError("Recipe factory returned the wrong immutable identity")
        if graph.parameters != dict(parameters):
            raise ValueError("Recipe factory did not freeze the declared parameters exactly")
        for node in graph.nodes:
            if not isinstance(node, ContainerNodeV1) or node.request_renderer is None:
                continue
            renderer = node.request_renderer
            if lock_digests.get((renderer.name, renderer.version)) != renderer.digest:
                raise ValueError("Recipe graph references a missing or drifted renderer lock")
        self._validate_conditional_outputs(graph)
        reject_secret_literals(graph)
        return graph

    def _validate_conditional_outputs(self, graph: RunGraphSpecV1) -> None:
        contracts = {
            contract.stage_key: {
                outcome: frozenset(outputs) for outcome, outputs in contract.outcomes
            }
            for contract in self.conditional_output_contracts
        }
        for node in graph.nodes:
            if isinstance(node, OutcomeGateNodeV1):
                subject = contracts.get(node.subject_stage_key, {})
                if any(outcome not in subject for outcome in node.match_outcomes):
                    raise ValueError("gate outcome is absent from the official Recipe contract")
            if not isinstance(node, ContainerNodeV1):
                continue
            for binding in node.inputs:
                if isinstance(binding, StageOutputBindingV1) and binding.match_outcomes:
                    subject = contracts.get(binding.stage_key, {})
                    if any(
                        binding.output_name not in subject.get(outcome, frozenset())
                        for outcome in binding.match_outcomes
                    ):
                        raise ValueError(
                            "conditional binding is absent from the official Recipe contract"
                        )
                elif isinstance(binding, TerminalOutputsBindingV1):
                    for stage_key in binding.stage_keys:
                        subject = contracts.get(stage_key, {})
                        if any(
                            binding.output_name not in subject.get(outcome, frozenset())
                            for outcome in binding.match_outcomes
                        ):
                            raise ValueError(
                                "terminal binding is absent from the official Recipe contract"
                            )


class OfficialRecipeRegistry:
    """In-memory code registry; no editable table or raw-graph path exists."""

    def __init__(
        self,
        registrations: tuple[OfficialRecipeRegistration, ...] = (),
        *,
        repo_root: Path | None = None,
    ) -> None:
        self._registrations: dict[tuple[str, int], OfficialRecipeRegistration] = {}
        self._repo_root = repo_root
        for registration in registrations:
            self.register(registration)

    def register(self, registration: OfficialRecipeRegistration) -> None:
        key = (registration.name, registration.version)
        if key in self._registrations:
            raise ValueError(f"Recipe already registered: {registration.name}@{registration.version}")
        if registration.renderer_locks:
            if self._repo_root is None:
                raise ValueError("renderer locks require a repository root at registry startup")
            for lock in registration.renderer_locks:
                verify_renderer_lock(lock, self._repo_root)
        self._registrations[key] = registration

    def get(self, name: str, version: int) -> OfficialRecipeRegistration:
        try:
            return self._registrations[(name, version)]
        except KeyError as exc:
            raise KeyError(f"unknown official Recipe: {name}@{version}") from exc

    def resolve_ordinary(
        self, name: str, version: int, parameters: Mapping[str, Any]
    ) -> RunGraphSpecV1:
        registration = self.get(name, version)
        if registration.submission_policy != "ordinary":
            raise PermissionError("acceptance-only Recipe is not available to ordinary submission")
        return registration.resolve(parameters, repo_root=self._repo_root)

    def resolve_acceptance_preflight(
        self,
        *,
        name: str,
        version: int,
        parameters: Mapping[str, Any],
        active_same_team_matrix_authorization: bool,
    ) -> RunGraphSpecV1:
        registration = self.get(name, version)
        if (
            registration.submission_policy != "acceptance_authorization_only"
            or (name, version) != ("behavior-recovery-acceptance-preflight", 1)
        ):
            raise PermissionError("Recipe is not the fixed acceptance preflight")
        if not active_same_team_matrix_authorization:
            raise PermissionError("active same-team matrix authorization is required")
        return registration.resolve(parameters, repo_root=self._repo_root)

    def list_identities(self) -> tuple[RecipeIdentityV1, ...]:
        return tuple(
            self._registrations[key].identity
            for key in sorted(self._registrations, key=lambda item: (item[0].encode(), item[1]))
        )


def verify_renderer_lock(lock: RequestRendererLockV1, repo_root: Path) -> str:
    """Recompute every pinned file and return the persisted lock digest."""

    root = repo_root.resolve()
    for item in lock.files:
        path = (root / item.repo_path).resolve()
        if root not in path.parents or not path.is_file():
            raise ValueError(f"renderer lock file is missing or outside the repository: {item.repo_path}")
        if digest_bytes(path.read_bytes()) != item.sha256:
            raise ValueError(f"renderer lock file drift: {item.repo_path}")
    return canonical_digest(lock)
