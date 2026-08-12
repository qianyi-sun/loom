"""Strict immutable RunGraph v1 schemas and graph-level admission checks."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Annotated, Any, Literal, TypeAlias
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_document

MAX_GRAPH_BYTES = 1_048_576
MAX_NODES = 128
MAX_GRAPH_INPUTS = 128
MAX_BINDINGS = 128
MAX_OUTPUTS = 64
MAX_FANOUT_ITEMS = 5_000
MAX_FANOUT_MANIFEST_BYTES = 16_777_216
MAX_ARGV_ITEMS = 256
MAX_ARGV_BYTES = 65_536
MAX_PARAMETERS_BYTES = 16_384

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
NAME_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
RECIPE_NAME_PATTERN = r"^[a-z][a-z0-9-]{0,127}$"
ARTIFACT_TYPE_PATTERN = r"^[a-z][a-z0-9_.-]{0,126}\.v[1-9][0-9]*$"
RESOURCE_PROFILE_PATTERN = r"^[a-z][a-z0-9_-]{0,62}@[1-9][0-9]*$"
EXECUTION_VARIANT_PATTERN = r"^[a-z][a-z0-9_-]{0,62}$"
IMAGE_PATTERN = r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
REASON_PATTERN = r"^[a-z][a-z0-9_]{0,127}$"
_SECRET_KEY_PATTERN = re.compile(
    r"(?:^|_)(?:api_?key|password|passwd|secret|token|credential)(?:$|_)", re.I
)
_SECRET_VALUE_PATTERN = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/=-]{12,}|"
    r"\b(?:sk|rk|ghp|gho|github_pat)-?[A-Za-z0-9_\-]{16,})"
)
_OPAQUE_REFERENCE_PATTERN = re.compile(r"^(?:loom|k8s-secret)://[A-Za-z0-9._/@:-]+$")

Digest = Annotated[str, StringConstraints(pattern=SHA256_PATTERN)]
BindingName = Annotated[str, StringConstraints(pattern=NAME_PATTERN)]
ExecutionVariantId = Annotated[
    str, StringConstraints(pattern=EXECUTION_VARIANT_PATTERN)
]
NodeKey = Annotated[str, StringConstraints(pattern=NAME_PATTERN)]
ArtifactType = Annotated[str, StringConstraints(pattern=ARTIFACT_TYPE_PATTERN)]
PositiveSafeInt = Annotated[int, Field(strict=True, ge=1, le=MAX_SAFE_INTEGER)]
NonNegativeSafeInt = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
PositiveVersion = Annotated[int, Field(strict=True, ge=1, le=2_147_483_647)]


class PipelineModel(BaseModel):
    """Base for every closed Pipeline v1 JSON object."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, validate_default=True)


def _nfc(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    normalized.encode("utf-8", errors="strict")
    return normalized


def _unique(values: list[str], label: str) -> list[str]:
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be ordered and unique")
    return values


def _bytewise_sorted(values: list[str], label: str) -> list[str]:
    if values != sorted(values, key=lambda item: item.encode("utf-8")):
        raise ValueError(f"{label} must be bytewise sorted")
    return _unique(values, label)


def reject_secret_literals(value: Any, *, reference_field: bool = False) -> None:
    if isinstance(value, BaseModel):
        reject_secret_literals(value.model_dump(mode="json", exclude_none=False))
    elif isinstance(value, dict):
        for key, item in value.items():
            is_reference = key.endswith("_ref") or key.endswith("_reference_id")
            if _SECRET_KEY_PATTERN.search(key) and not is_reference:
                raise ValueError("secret-looking field name is forbidden")
            reject_secret_literals(item, reference_field=is_reference)
    elif isinstance(value, list | tuple):
        for item in value:
            reject_secret_literals(item, reference_field=reference_field)
    elif isinstance(value, str):
        if reference_field:
            if _OPAQUE_REFERENCE_PATTERN.fullmatch(value):
                return
            raise ValueError("secret reference must be an opaque provider reference ID")
        if _SECRET_VALUE_PATTERN.search(value):
            raise ValueError("secret-looking literal is forbidden")


class RecipeIdentityV1(PipelineModel):
    name: Annotated[str, StringConstraints(pattern=RECIPE_NAME_PATTERN)]
    version: PositiveVersion
    digest: Digest

    _normalize_name = field_validator("name")(_nfc)


class GraphInputV1(PipelineModel):
    name: BindingName
    artifact_type: ArtifactType
    required: Literal[True]

    _normalize_name = field_validator("name")(_nfc)

    @field_validator("required", mode="before")
    @classmethod
    def required_is_strict_true(cls, value: Any) -> Any:
        if value is not True:
            raise ValueError("required must be the boolean true")
        return value


class RunBudgetV1(PipelineModel):
    max_provider_cost_usd: Annotated[
        str, StringConstraints(pattern=r"^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,6})?$")
    ]
    max_gpu_seconds: NonNegativeSafeInt
    max_wall_seconds: PositiveSafeInt
    max_artifact_bytes: PositiveSafeInt
    max_stage_runs: PositiveSafeInt
    max_attempts_total: PositiveSafeInt


class OutputDeclV1(PipelineModel):
    name: BindingName
    artifact_type: ArtifactType
    required: bool
    role: Literal["artifact", "fanout_manifest"]
    producer: Literal["container", "platform"]
    max_bytes: PositiveSafeInt

    _normalize_name = field_validator("name")(_nfc)

    @model_validator(mode="after")
    def validate_platform_output(self) -> OutputDeclV1:
        if self.producer == "platform":
            if (
                self.role != "fanout_manifest"
                or self.artifact_type != "loom.fanout-manifest.v1"
                or not self.required
                or self.max_bytes > MAX_FANOUT_MANIFEST_BYTES
            ):
                raise ValueError("platform outputs must be bounded required fanout manifests")
        elif self.role == "fanout_manifest":
            raise ValueError("fanout manifests must be produced by the platform")
        return self


class RequestRendererRefV1(PipelineModel):
    name: BindingName
    version: PositiveVersion
    digest: Digest
    max_bytes: Annotated[int, Field(strict=True, ge=1, le=MAX_FANOUT_MANIFEST_BYTES)]
    terminal_stage_keys: list[NodeKey] = Field(max_length=MAX_NODES)

    _normalize_name = field_validator("name")(_nfc)

    @field_validator("terminal_stage_keys")
    @classmethod
    def terminal_keys_unique(cls, values: list[str]) -> list[str]:
        return _unique(values, "terminal_stage_keys")


class RequestRendererLockFileV1(PipelineModel):
    repo_path: str
    sha256: Digest

    @field_validator("repo_path")
    @classmethod
    def validate_repo_path(cls, value: str) -> str:
        value = _nfc(value)
        if not value or value.startswith("/") or "\\" in value or "\x00" in value:
            raise ValueError("repo_path must be a relative forward-slash path")
        parts = value.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("repo_path contains an invalid component")
        return value


class RequestRendererLockV1(PipelineModel):
    name: BindingName
    version: PositiveVersion
    entrypoint: str
    files: Annotated[list[RequestRendererLockFileV1], Field(min_length=1)]

    _normalize_name = field_validator("name")(_nfc)

    @field_validator("entrypoint")
    @classmethod
    def validate_entrypoint(cls, value: str) -> str:
        value = _nfc(value)
        if not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*",
            value,
        ):
            raise ValueError("entrypoint must be a Python module:function symbol")
        return value

    @model_validator(mode="after")
    def files_are_bytewise_sorted(self) -> RequestRendererLockV1:
        paths = [item.repo_path for item in self.files]
        _bytewise_sorted(paths, "renderer lock files")
        return self


class ProviderAttemptLimitsV1(PipelineModel):
    provider_request_limit_per_attempt: PositiveSafeInt
    provider_cost_limit_microusd_per_attempt: PositiveSafeInt
    per_call_timeout_seconds: PositiveSafeInt


class StageBudgetV1(PipelineModel):
    provider: ProviderAttemptLimitsV1 | None
    gpu_seconds_limit: NonNegativeSafeInt
    final_output_bytes_limit: NonNegativeSafeInt
    checkpoint_bytes_limit: NonNegativeSafeInt
    timeout_seconds: PositiveSafeInt
    max_attempts: Annotated[int, Field(strict=True, ge=1, le=3)]

    @classmethod
    def for_node(
        cls,
        node: ContainerNodeV1,
        *,
        gpu_count_exact: int,
        provider: ProviderAttemptLimitsV1 | None = None,
    ) -> StageBudgetV1:
        if gpu_count_exact < 0:
            raise ValueError("gpu_count_exact cannot be negative")
        final_output_bytes = sum(output.max_bytes for output in node.outputs)
        if node.fanout_commit is not None:
            item = next(
                output
                for output in node.outputs
                if output.name == node.fanout_commit.item_binding_name
            )
            final_output_bytes += item.max_bytes * (node.fanout_commit.max_items - 1)
        checkpoint_bytes = node.checkpoint.max_bytes if node.checkpoint else 0
        return cls(
            provider=provider,
            gpu_seconds_limit=gpu_count_exact * (node.timeout_seconds + 35),
            final_output_bytes_limit=final_output_bytes,
            checkpoint_bytes_limit=checkpoint_bytes,
            timeout_seconds=node.timeout_seconds,
            max_attempts=node.max_attempts,
        )


class CheckpointPolicyV1(PipelineModel):
    max_bytes: PositiveSafeInt
    min_interval_seconds: Annotated[int, Field(strict=True, ge=5, le=MAX_SAFE_INTEGER)]
    max_committed_per_attempt: Annotated[int, Field(strict=True, ge=1, le=64)]


class RunInputBindingV1(PipelineModel):
    source: Literal["run_input"]
    binding_name: BindingName
    artifact_type: ArtifactType
    input_name: BindingName


class StageOutputBindingV1(PipelineModel):
    source: Literal["stage_output"]
    binding_name: BindingName
    artifact_type: ArtifactType
    stage_key: NodeKey
    output_name: BindingName
    shard_selection: Literal["singleton", "same_shard", "fanout_source_shard", "all_shards"]
    match_outcomes: list[str] | None

    @field_validator("match_outcomes")
    @classmethod
    def validate_match_outcomes(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        if not values:
            raise ValueError("match_outcomes cannot be empty")
        normalized = [_nfc(value) for value in values]
        if any(not value or len(value.encode("utf-8")) > 128 for value in normalized):
            raise ValueError("domain outcomes must be 1..128 UTF-8 bytes")
        return _unique(normalized, "match_outcomes")


class FanoutItemBindingV1(PipelineModel):
    source: Literal["fanout_item"]
    binding_name: BindingName
    artifact_type: ArtifactType


class TerminalOutputsBindingV1(PipelineModel):
    source: Literal["terminal_outputs"]
    binding_name: BindingName
    artifact_type: ArtifactType
    stage_keys: Annotated[list[NodeKey], Field(min_length=1, max_length=MAX_NODES)]
    output_name: BindingName
    match_outcomes: Annotated[list[str], Field(min_length=1)]

    @field_validator("stage_keys")
    @classmethod
    def validate_stage_keys(cls, values: list[str]) -> list[str]:
        return _unique(values, "stage_keys")

    @field_validator("match_outcomes")
    @classmethod
    def validate_match_outcomes(cls, values: list[str]) -> list[str]:
        normalized = [_nfc(value) for value in values]
        if any(not value or len(value.encode("utf-8")) > 128 for value in normalized):
            raise ValueError("domain outcomes must be 1..128 UTF-8 bytes")
        return _unique(normalized, "match_outcomes")


InputBindingV1: TypeAlias = Annotated[
    RunInputBindingV1
    | StageOutputBindingV1
    | FanoutItemBindingV1
    | TerminalOutputsBindingV1,
    Field(discriminator="source"),
]


class FanoutParametersContractRefV1(PipelineModel):
    name: BindingName
    version: PositiveVersion
    digest: Digest


class RunInputFanoutV1(PipelineModel):
    source: Literal["run_input"]
    manifest_input_name: BindingName
    items_pointer: Literal["/items"]
    shard_key_pointer: Literal["/shard_key"]
    item_binding_name: BindingName
    item_artifact_type: ArtifactType
    parameters_contract: FanoutParametersContractRefV1
    max_items: Annotated[int, Field(strict=True, ge=1, le=MAX_FANOUT_ITEMS)]


class StageOutputFanoutV1(PipelineModel):
    source: Literal["stage_output"]
    manifest_stage_key: NodeKey
    manifest_output_name: BindingName
    items_pointer: Literal["/items"]
    shard_key_pointer: Literal["/shard_key"]
    item_binding_name: BindingName
    item_artifact_type: ArtifactType
    max_items: Annotated[int, Field(strict=True, ge=1, le=MAX_FANOUT_ITEMS)]


FanoutV1: TypeAlias = Annotated[
    RunInputFanoutV1 | StageOutputFanoutV1, Field(discriminator="source")
]


class PlatformFanoutCommitV1(PipelineModel):
    index_output_name: BindingName
    manifest_output_name: BindingName
    items_pointer: Literal["/items"]
    item_binding_name: BindingName
    max_items: Annotated[int, Field(strict=True, ge=1, le=MAX_FANOUT_ITEMS)]

    @model_validator(mode="after")
    def distinct_names(self) -> PlatformFanoutCommitV1:
        names = {self.index_output_name, self.manifest_output_name, self.item_binding_name}
        if len(names) != 3:
            raise ValueError("fanout commit output names must be distinct")
        return self


class ContainerNodeV1(PipelineModel):
    node_kind: Literal["container"]
    node_key: NodeKey
    image: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    argv: Annotated[list[str], Field(min_length=1, max_length=MAX_ARGV_ITEMS)]
    workdir: str
    resource_profile: Annotated[str, StringConstraints(pattern=RESOURCE_PROFILE_PATTERN)]
    network_profile: Literal["none", "gateway"]
    needs: list[NodeKey] = Field(max_length=MAX_NODES)
    inputs: list[InputBindingV1] = Field(max_length=MAX_BINDINGS)
    outputs: list[OutputDeclV1] = Field(max_length=MAX_OUTPUTS)
    request_renderer: RequestRendererRefV1 | None
    checkpoint: CheckpointPolicyV1 | None
    fanout: FanoutV1 | None
    fanout_commit: PlatformFanoutCommitV1 | None
    timeout_seconds: PositiveSafeInt
    max_attempts: Annotated[int, Field(strict=True, ge=1, le=3)]
    failure_policy: Literal["fail_run", "continue"]

    @field_validator("argv")
    @classmethod
    def validate_argv(cls, values: list[str]) -> list[str]:
        if sum(len(value.encode("utf-8", errors="strict")) for value in values) > MAX_ARGV_BYTES:
            raise ValueError("argv exceeds 64 KiB")
        return values

    @field_validator("workdir")
    @classmethod
    def validate_workdir(cls, value: str) -> str:
        value = _nfc(value)
        if not value.startswith("/") or "\x00" in value or "\\" in value:
            raise ValueError("workdir must be an absolute container path")
        if any(part in {".", ".."} for part in value.split("/")):
            raise ValueError("workdir cannot contain dot components")
        return value

    @field_validator("needs")
    @classmethod
    def needs_unique(cls, values: list[str]) -> list[str]:
        return _unique(values, "needs")

    @model_validator(mode="after")
    def validate_local_contract(self) -> ContainerNodeV1:
        if len({item.binding_name for item in self.inputs}) != len(self.inputs):
            raise ValueError("input binding names must be unique")
        if len({item.name for item in self.outputs}) != len(self.outputs):
            raise ValueError("output names must be unique")
        if self.node_key in self.needs:
            raise ValueError("node cannot depend on itself")

        fanout_items = [item for item in self.inputs if item.source == "fanout_item"]
        if self.fanout is None and fanout_items:
            raise ValueError("fanout_item requires a fanout node")
        if self.fanout is not None:
            if len(fanout_items) != 1:
                raise ValueError("fanout node requires exactly one fanout_item binding")
            fanout_binding = fanout_items[0]
            if fanout_binding.artifact_type != self.fanout.item_artifact_type:
                raise ValueError("fanout_item type does not match fanout")
            if self.fanout.source == "stage_output" and self.fanout.manifest_stage_key not in self.needs:
                raise ValueError("stage-output fanout source must be in needs")

        platform_outputs = [output for output in self.outputs if output.producer == "platform"]
        if self.fanout_commit is None:
            if platform_outputs:
                raise ValueError("platform output requires fanout_commit")
        else:
            by_name = {output.name: output for output in self.outputs}
            commit = self.fanout_commit
            try:
                index = by_name[commit.index_output_name]
                manifest = by_name[commit.manifest_output_name]
                item_template = by_name[commit.item_binding_name]
            except KeyError as exc:
                raise ValueError("fanout_commit names undeclared output") from exc
            if (
                index.artifact_type != "loom.platform-fanout-index.v1"
                or index.producer != "container"
                or index.role != "artifact"
                or not index.required
            ):
                raise ValueError("fanout index output contract is invalid")
            if manifest.producer != "platform" or manifest.role != "fanout_manifest":
                raise ValueError("fanout manifest output contract is invalid")
            if (
                item_template.producer != "container"
                or item_template.role != "artifact"
                or item_template.required
            ):
                raise ValueError("fanout item template must be optional container artifact")
            if len(platform_outputs) != 1 or platform_outputs[0].name != manifest.name:
                raise ValueError("fanout_commit requires exactly one platform output")

        terminal_bindings = [item for item in self.inputs if item.source == "terminal_outputs"]
        if self.request_renderer is None and terminal_bindings:
            raise ValueError("terminal_outputs requires a request renderer")
        if self.request_renderer and self.request_renderer.terminal_stage_keys:
            if self.fanout is not None:
                raise ValueError("terminal renderer must be singleton")
            declared = set(self.request_renderer.terminal_stage_keys)
            if not declared.issubset(self.needs):
                raise ValueError("terminal renderer keys must be in needs")
            if any(not set(binding.stage_keys).issubset(declared) for binding in terminal_bindings):
                raise ValueError("terminal_outputs stage_keys must be renderer terminal keys")
        elif terminal_bindings:
            raise ValueError("terminal_outputs requires non-empty terminal_stage_keys")
        return self


class OutcomeGateNodeV1(PipelineModel):
    node_kind: Literal["gate"]
    gate_kind: Literal["outcome"]
    node_key: NodeKey
    shard_mode: Literal["subject"]
    needs: list[NodeKey] = Field(max_length=MAX_NODES)
    subject_stage_key: NodeKey
    match_outcomes: Annotated[list[str], Field(min_length=1)]
    matched_targets: list[NodeKey] = Field(max_length=MAX_NODES)
    unmatched_targets: list[NodeKey] = Field(max_length=MAX_NODES)

    @field_validator("needs", "matched_targets", "unmatched_targets")
    @classmethod
    def unique_keys(cls, values: list[str]) -> list[str]:
        return _unique(values, "gate key list")

    @field_validator("match_outcomes")
    @classmethod
    def unique_outcomes(cls, values: list[str]) -> list[str]:
        normalized = [_nfc(value) for value in values]
        if any(not value or len(value.encode("utf-8")) > 128 for value in normalized):
            raise ValueError("domain outcomes must be 1..128 UTF-8 bytes")
        return _unique(normalized, "match_outcomes")

    @model_validator(mode="after")
    def validate_gate(self) -> OutcomeGateNodeV1:
        if self.subject_stage_key not in self.needs:
            raise ValueError("gate subject must be in needs")
        if set(self.matched_targets) & set(self.unmatched_targets):
            raise ValueError("gate target sets must be disjoint")
        if self.node_key in self.needs or self.node_key in self.matched_targets + self.unmatched_targets:
            raise ValueError("gate cannot depend on or target itself")
        return self


NodeV1: TypeAlias = Annotated[ContainerNodeV1 | OutcomeGateNodeV1, Field(discriminator="node_kind")]


class RunGraphSpecV1(PipelineModel):
    schema_version: Literal["loom.run-graph.v1"]
    recipe: RecipeIdentityV1
    inputs: list[GraphInputV1] = Field(max_length=MAX_GRAPH_INPUTS)
    parameters: dict[str, Any]
    budget: RunBudgetV1
    nodes: Annotated[list[NodeV1], Field(min_length=1, max_length=MAX_NODES)]

    @model_validator(mode="after")
    def validate_graph(self) -> RunGraphSpecV1:
        reject_secret_literals(self.parameters)
        if len({item.name for item in self.inputs}) != len(self.inputs):
            raise ValueError("graph input names must be unique")
        if len({node.node_key for node in self.nodes}) != len(self.nodes):
            raise ValueError("node keys must be unique")
        input_by_name = {item.name: item for item in self.inputs}
        node_by_key = {node.node_key: node for node in self.nodes}

        for node in self.nodes:
            unknown_needs = set(node.needs) - node_by_key.keys()
            if unknown_needs:
                raise ValueError(f"unknown dependencies for {node.node_key}: {sorted(unknown_needs)}")
            if isinstance(node, OutcomeGateNodeV1):
                subject = node_by_key.get(node.subject_stage_key)
                if not isinstance(subject, ContainerNodeV1):
                    raise ValueError("gate subject must be a container node")
                for target_key in node.matched_targets + node.unmatched_targets:
                    target = node_by_key.get(target_key)
                    if not isinstance(target, ContainerNodeV1):
                        raise ValueError("gate targets must be container nodes")
                    if node.node_key not in target.needs:
                        raise ValueError("every gate target must list the gate in needs")
                    if (
                        target.request_renderer is not None
                        and target.request_renderer.terminal_stage_keys
                        and subject.fanout is not None
                    ):
                        raise ValueError("a sharded gate cannot target a terminal renderer")
                continue

            for binding in node.inputs:
                if isinstance(binding, RunInputBindingV1):
                    declared = input_by_name.get(binding.input_name)
                    if declared is None or declared.artifact_type != binding.artifact_type:
                        raise ValueError("run_input binding must match a declared graph input")
                elif isinstance(binding, FanoutItemBindingV1):
                    continue
                elif isinstance(binding, TerminalOutputsBindingV1):
                    for stage_key in binding.stage_keys:
                        producer = node_by_key.get(stage_key)
                        if not isinstance(producer, ContainerNodeV1):
                            raise ValueError("terminal_outputs producer must be a container")
                        output = next((item for item in producer.outputs if item.name == binding.output_name), None)
                        if output is None or output.artifact_type != binding.artifact_type:
                            raise ValueError("terminal_outputs must match a declared producer output")
                        if stage_key not in node.needs:
                            raise ValueError("terminal_outputs producers must be in needs")
                else:
                    producer = node_by_key.get(binding.stage_key)
                    if not isinstance(producer, ContainerNodeV1):
                        raise ValueError("stage_output producer must be a container")
                    output = next((item for item in producer.outputs if item.name == binding.output_name), None)
                    if output is None or output.artifact_type != binding.artifact_type:
                        raise ValueError("stage_output must match a declared producer output")
                    if binding.stage_key not in node.needs:
                        raise ValueError("stage_output producer must be in needs")
                    if output.producer != "container":
                        raise ValueError("platform outputs are consumed only through fanout")
                    producer_sharded = producer.fanout is not None
                    consumer_sharded = node.fanout is not None
                    if binding.shard_selection == "singleton" and producer_sharded:
                        raise ValueError("singleton selection requires a singleton producer")
                    if binding.shard_selection == "all_shards" and (
                        not producer_sharded or consumer_sharded
                    ):
                        raise ValueError("all_shards requires a sharded producer and singleton consumer")
                    if binding.shard_selection == "same_shard" and (
                        producer_sharded != consumer_sharded
                    ):
                        raise ValueError("same_shard requires both nodes singleton or both sharded")
                    if binding.shard_selection == "fanout_source_shard" and not isinstance(
                        node.fanout, StageOutputFanoutV1
                    ):
                        raise ValueError(
                            "fanout_source_shard requires a stage-output-expanded consumer"
                        )
                    if output.required and binding.match_outcomes is not None:
                        raise ValueError("required output binding must have match_outcomes=null")
                    if not output.required:
                        if (
                            binding.match_outcomes is None
                            or binding.shard_selection not in {"singleton", "same_shard"}
                        ):
                            raise ValueError("optional outputs require conditional scalar binding")
                        matching_gates = [
                            gate
                            for gate in self.nodes
                            if isinstance(gate, OutcomeGateNodeV1)
                            and gate.subject_stage_key == binding.stage_key
                            and gate.match_outcomes == binding.match_outcomes
                            and node.node_key in gate.matched_targets
                            and gate.node_key in node.needs
                        ]
                        if not matching_gates:
                            raise ValueError("conditional output requires a matching outcome gate")

            if node.fanout is not None:
                if isinstance(node.fanout, RunInputFanoutV1):
                    declared = input_by_name.get(node.fanout.manifest_input_name)
                    if declared is None or declared.artifact_type != "loom.fanout-manifest.v1":
                        raise ValueError("run-input fanout requires a manifest graph input")
                else:
                    source = node_by_key.get(node.fanout.manifest_stage_key)
                    if not isinstance(source, ContainerNodeV1) or source.fanout_commit is None:
                        raise ValueError("stage-output fanout requires an upstream fanout commit")
                    commit = source.fanout_commit
                    if commit.manifest_output_name != node.fanout.manifest_output_name:
                        raise ValueError("stage-output fanout manifest name drift")
                    template = next(
                        item for item in source.outputs if item.name == commit.item_binding_name
                    )
                    if (
                        template.artifact_type != node.fanout.item_artifact_type
                        or commit.item_binding_name != node.fanout.item_binding_name
                    ):
                        raise ValueError("stage-output fanout item contract drift")

        self._validate_acyclic(node_by_key)
        if declared_stage_run_upper_bound(self) > 50_000:
            raise ValueError("expanded StageRun bound exceeds 50,000")
        if len(canonical_document(self)) > MAX_GRAPH_BYTES:
            raise ValueError("canonical graph exceeds 1 MiB")
        return self

    def _validate_acyclic(self, node_by_key: dict[str, NodeV1]) -> None:
        temporary: set[str] = set()
        permanent: set[str] = set()

        def visit(key: str) -> None:
            if key in permanent:
                return
            if key in temporary:
                raise ValueError("run graph contains a cycle")
            temporary.add(key)
            for dependency in node_by_key[key].needs:
                visit(dependency)
            temporary.remove(key)
            permanent.add(key)

        for key in node_by_key:
            visit(key)


class ControlBindingSnapshotRefV1(PipelineModel):
    logical_name: BindingName
    kind: Literal["judge_profile", "provider"]
    object_id: UUID
    version: PositiveVersion
    snapshot_sha256: Digest


class ExecutionSpecSnapshotV1(PipelineModel):
    schema_version: Literal["loom.execution-spec.v1"]
    recipe_digest: Digest
    run_graph_digest: Digest
    node_key: NodeKey
    shard_key: str
    container_node: ContainerNodeV1
    image_runtime_contract_digest: Digest
    resource_profile_digest: Digest
    execution_variant_id: ExecutionVariantId
    gpu_backend_selection_sha256: Digest | None
    resolved_image_manifest_digest: Digest
    network_profile: Literal["none", "gateway"]
    resolved_input_bindings_digest: Digest
    fanout_source_manifest_digest: Digest | None
    fanout_item_digest: Digest | None
    fanout_parameters_digest: Digest | None
    request_renderer_lock_digest: Digest | None
    control_binding_snapshots: list[ControlBindingSnapshotRefV1]

    @field_validator("shard_key")
    @classmethod
    def valid_shard_key(cls, value: str) -> str:
        return validate_shard_key(value)

    @model_validator(mode="after")
    def validate_snapshot(self) -> ExecutionSpecSnapshotV1:
        if self.node_key != self.container_node.node_key:
            raise ValueError("snapshot node_key must match container_node")
        if self.network_profile != self.container_node.network_profile:
            raise ValueError("snapshot network_profile must match container_node")
        if (self.execution_variant_id == "cpu-data-x86_64") != (
            self.gpu_backend_selection_sha256 is None
        ):
            raise ValueError("only the CPU variant may omit GPU backend selection evidence")
        if self.container_node.request_renderer is None:
            if self.request_renderer_lock_digest is not None:
                raise ValueError("renderer lock digest must be null without renderer")
        elif self.request_renderer_lock_digest != self.container_node.request_renderer.digest:
            raise ValueError("renderer lock digest must match renderer reference")
        logical_names = [item.logical_name for item in self.control_binding_snapshots]
        _bytewise_sorted(logical_names, "control binding snapshots")
        fanout_values = (
            self.fanout_source_manifest_digest,
            self.fanout_item_digest,
            self.fanout_parameters_digest,
        )
        if self.shard_key == "singleton" and any(item is not None for item in fanout_values):
            raise ValueError("singleton snapshot cannot carry fanout digests")
        if self.shard_key != "singleton" and any(item is None for item in fanout_values):
            raise ValueError("expanded snapshot requires all fanout digests")
        return self


class PlatformFanoutIndexItemV1(PipelineModel):
    shard_key: str
    output_name: BindingName

    @field_validator("shard_key")
    @classmethod
    def valid_shard_key(cls, value: str) -> str:
        return validate_shard_key(value, allow_singleton=False)


class PlatformFanoutIndexV1(PipelineModel):
    items: list[PlatformFanoutIndexItemV1] = Field(max_length=MAX_FANOUT_ITEMS)
    schema_version: Literal["loom.platform-fanout-index.v1"]

    @model_validator(mode="after")
    def validate_items(self) -> PlatformFanoutIndexV1:
        keys = [item.shard_key for item in self.items]
        _bytewise_sorted(keys, "fanout index shard keys")
        if len({item.output_name for item in self.items}) != len(self.items):
            raise ValueError("fanout index output names must be unique")
        return self


class FanoutArtifactBindingV1(PipelineModel):
    artifact_id: UUID
    artifact_type: ArtifactType
    name: BindingName


class FanoutManifestItemV1(PipelineModel):
    artifact_bindings: Annotated[list[FanoutArtifactBindingV1], Field(min_length=1, max_length=1)]
    parameters: dict[str, Any]
    shard_key: str

    @field_validator("shard_key")
    @classmethod
    def valid_shard_key(cls, value: str) -> str:
        return validate_shard_key(value, allow_singleton=False)

    @model_validator(mode="after")
    def validate_parameters_size(self) -> FanoutManifestItemV1:
        if len(canonical_document(self.parameters)) - 1 > MAX_PARAMETERS_BYTES:
            raise ValueError("fanout parameters exceed 16,384 bytes")
        return self


class FanoutManifestV1(PipelineModel):
    schema_version: Literal["loom.fanout-manifest.v1"]
    items: list[FanoutManifestItemV1] = Field(max_length=MAX_FANOUT_ITEMS)

    @model_validator(mode="after")
    def validate_items(self) -> FanoutManifestV1:
        keys = [item.shard_key for item in self.items]
        _bytewise_sorted(keys, "fanout manifest shard keys")
        if len(canonical_document(self)) > MAX_FANOUT_MANIFEST_BYTES:
            raise ValueError("fanout manifest exceeds 16 MiB")
        return self


class BindingItemV1(PipelineModel):
    artifact_id: UUID
    content_sha256: Digest
    file_count: NonNegativeSafeInt
    item_key: str
    manifest_sha256: Digest
    stored_size_bytes: NonNegativeSafeInt
    unpacked_size_bytes: NonNegativeSafeInt

    @field_validator("item_key")
    @classmethod
    def validate_item_key(cls, value: str) -> str:
        value = _nfc(value)
        if value == "singleton":
            return value
        if not value or len(value.encode("utf-8")) > 128 or "\x00" in value:
            raise ValueError("item_key is invalid")
        return value


class BindingSetV1(PipelineModel):
    binding_name: BindingName
    artifact_type: ArtifactType
    cardinality: Literal["one", "many"]
    items: list[BindingItemV1]

    @model_validator(mode="after")
    def validate_cardinality(self) -> BindingSetV1:
        keys = [item.item_key for item in self.items]
        if len(keys) != len(set(keys)):
            raise ValueError("binding item keys must be unique")
        if self.cardinality == "one":
            if len(self.items) != 1 or keys != ["singleton"]:
                raise ValueError("scalar binding requires one singleton item")
        elif "singleton" in keys:
            raise ValueError("many binding cannot use singleton item_key")
        return self


class CommittedOutputDescriptorV1(PipelineModel):
    name: BindingName
    producer: Literal["container", "platform"]
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest


class TerminalStageDescriptorV1(PipelineModel):
    node_key: NodeKey
    shard_key: str
    stage_run_id: UUID
    terminal_state: Literal["succeeded", "failed", "skipped"]
    execution_attempt_id: UUID | None
    stage_result_sha256: Digest | None
    domain_outcome: str | None
    reason_code: Annotated[str, StringConstraints(pattern=REASON_PATTERN)]
    committed_outputs: list[CommittedOutputDescriptorV1]

    @field_validator("shard_key")
    @classmethod
    def valid_shard_key(cls, value: str) -> str:
        return validate_shard_key(value)

    @field_validator("domain_outcome")
    @classmethod
    def validate_domain_outcome(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = _nfc(value)
        if not value or len(value.encode("utf-8")) > 128:
            raise ValueError("domain_outcome must be 1..128 UTF-8 bytes")
        return value

    @model_validator(mode="after")
    def validate_terminal_state(self) -> TerminalStageDescriptorV1:
        names = [item.name for item in self.committed_outputs]
        _bytewise_sorted(names, "committed outputs")
        if self.terminal_state == "succeeded":
            if (
                self.execution_attempt_id is None
                or self.stage_result_sha256 is None
                or self.domain_outcome is None
            ):
                raise ValueError("succeeded descriptor requires attempt, result, and outcome")
        elif self.terminal_state == "failed":
            if (
                self.domain_outcome is not None
                or self.committed_outputs
                or (self.execution_attempt_id is None and self.stage_result_sha256 is not None)
            ):
                raise ValueError("failed descriptor cannot carry outcome or committed outputs")
        elif any(
            item is not None
            for item in (
                self.execution_attempt_id,
                self.stage_result_sha256,
                self.domain_outcome,
            )
        ) or self.committed_outputs:
            raise ValueError("skipped descriptor cannot carry attempt, result, outcome, or outputs")
        return self


class PipelineTerminalSnapshotDocumentV1(PipelineModel):
    schema_version: Literal["loom.pipeline-terminal-snapshot.v1"]
    pipeline_run_id: UUID
    run_graph_digest: Digest
    snapshot_id: UUID
    terminal_stage_keys: Annotated[list[NodeKey], Field(min_length=1, max_length=MAX_NODES)]
    stages: list[TerminalStageDescriptorV1]

    @field_validator("terminal_stage_keys")
    @classmethod
    def unique_terminal_keys(cls, values: list[str]) -> list[str]:
        return _unique(values, "terminal_stage_keys")

    @model_validator(mode="after")
    def validate_snapshot(self) -> PipelineTerminalSnapshotDocumentV1:
        key_order = {key: index for index, key in enumerate(self.terminal_stage_keys)}
        if any(stage.node_key not in key_order for stage in self.stages):
            raise ValueError("snapshot contains undeclared stage key")
        identities = [(stage.node_key, stage.shard_key) for stage in self.stages]
        if len(identities) != len(set(identities)):
            raise ValueError("snapshot stage identities must be unique")
        if len({stage.stage_run_id for stage in self.stages}) != len(self.stages):
            raise ValueError("snapshot stage_run_id values must be unique")
        expected = sorted(
            self.stages,
            key=lambda item: (key_order[item.node_key], item.shard_key.encode("utf-8")),
        )
        if self.stages != expected:
            raise ValueError("snapshot stages are not in terminal-key/shard order")
        if len(canonical_document(self)) > MAX_FANOUT_MANIFEST_BYTES:
            raise ValueError("terminal snapshot exceeds 16 MiB")
        return self


def validate_shard_key(value: str, *, allow_singleton: bool = True) -> str:
    value = _nfc(value)
    if allow_singleton and value == "singleton":
        return value
    if (
        not value
        or value in {".", "..", "singleton"}
        or len(value.encode("utf-8")) > 128
        or any(char in value for char in ("/", "\\", "\x00"))
    ):
        raise ValueError("expanded shard_key is invalid")
    return value


def validate_fanout_manifest(
    manifest: FanoutManifestV1,
    fanout: FanoutV1,
) -> FanoutManifestV1:
    """Validate a consumed manifest against its frozen fanout contract."""

    if len(manifest.items) > fanout.max_items:
        raise ValueError("fanout manifest exceeds node max_items")
    for item in manifest.items:
        binding = item.artifact_bindings[0]
        if (
            binding.name != fanout.item_binding_name
            or binding.artifact_type != fanout.item_artifact_type
        ):
            raise ValueError("fanout manifest item binding drift")
        if isinstance(fanout, StageOutputFanoutV1) and item.parameters != {}:
            raise ValueError("stage-output fanout parameters must be empty")
    return manifest


def declared_stage_run_upper_bound(graph: RunGraphSpecV1) -> int:
    """Return a conservative v1 admission bound for StageRun cardinality."""

    by_key = {node.node_key: node for node in graph.nodes}
    memo: dict[str, int] = {}

    def cardinality(node: NodeV1) -> int:
        if node.node_key in memo:
            return memo[node.node_key]
        if isinstance(node, OutcomeGateNodeV1):
            value = cardinality(by_key[node.subject_stage_key])
        elif isinstance(node.fanout, RunInputFanoutV1):
            value = node.fanout.max_items
        elif isinstance(node.fanout, StageOutputFanoutV1):
            value = cardinality(by_key[node.fanout.manifest_stage_key]) * node.fanout.max_items
        else:
            value = 1
        memo[node.node_key] = value
        return value

    return sum(cardinality(node) for node in graph.nodes)


def duplicate_values(values: list[str]) -> set[str]:
    """Small public helper used by registry and golden tests."""

    return {value for value, count in Counter(values).items() if count > 1}


def binding_cardinality(binding: InputBindingV1) -> Literal["one", "many"]:
    if isinstance(binding, TerminalOutputsBindingV1):
        return "many"
    if isinstance(binding, StageOutputBindingV1) and binding.shard_selection == "all_shards":
        return "many"
    return "one"


def terminal_binding_item_key(*, node_key: str, output_name: str, shard_key: str) -> str:
    from loom.pipeline.keys import canonical_digest

    digest = canonical_digest(
        {"node_key": node_key, "output_name": output_name, "shard_key": shard_key},
        persisted=False,
    )
    return f"terminal-{digest.removeprefix('sha256:')}"
