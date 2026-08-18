"""Unregistered complete graph factory for TerminalGen authoring.

The factory is deliberately not added to the official recipe registry.  It
becomes registrable only after every renderer/runtime lock and the external
source authority have been supplied and the ordinary Pipeline execution path
is accepted.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.integrations.terminalgen.contracts import (
    EXPECTED_CARD_COUNT,
    AuthoringImageLockV1,
    AuthoringParametersV1,
)
from loom.pipeline.spec import Digest, PipelineModel, RecipeIdentityV1, RunGraphSpecV1


class TerminalGenRendererLocksV1(PipelineModel):
    plan_audit: Digest
    card_finalize: Digest
    global_finalize: Digest
    authoring_package: Digest
    runtime_package: Digest


def _output(
    name: str,
    artifact_type: str,
    *,
    required: bool = True,
    producer: str = "container",
    role: str = "artifact",
    max_bytes: int = 67_108_864,
) -> dict[str, object]:
    return {
        "name": name,
        "artifact_type": artifact_type,
        "required": required,
        "role": role,
        "producer": producer,
        "max_bytes": max_bytes,
    }


def _container(
    node_key: str,
    *,
    image: str,
    profile: str,
    network: str,
    needs: list[str],
    inputs: list[dict[str, object]],
    outputs: list[dict[str, object]],
    timeout_seconds: int,
    request_renderer: dict[str, object] | None = None,
    fanout: dict[str, object] | None = None,
    fanout_commit: dict[str, object] | None = None,
    failure_policy: str = "fail_run",
) -> dict[str, object]:
    return {
        "node_kind": "container",
        "node_key": node_key,
        "image": image,
        "argv": ["python", "-m", "loom.integrations.terminalgen.cli", "run", node_key],
        "workdir": "/workspace",
        "resource_profile": profile,
        "network_profile": network,
        "needs": needs,
        "inputs": inputs,
        "outputs": outputs,
        "request_renderer": request_renderer,
        "checkpoint": None,
        "fanout": fanout,
        "fanout_commit": fanout_commit,
        "timeout_seconds": timeout_seconds,
        "max_attempts": 3,
        "failure_policy": failure_policy,
    }


def _stage_input(
    binding_name: str,
    artifact_type: str,
    stage_key: str,
    output_name: str,
    *,
    shard_selection: str,
    match_outcomes: list[str] | None = None,
) -> dict[str, object]:
    return {
        "source": "stage_output",
        "binding_name": binding_name,
        "artifact_type": artifact_type,
        "stage_key": stage_key,
        "output_name": output_name,
        "shard_selection": shard_selection,
        "match_outcomes": match_outcomes,
    }


def _terminal_input(
    binding_name: str,
    artifact_type: str,
    stage_keys: list[str],
    output_name: str,
    match_outcomes: list[str],
) -> dict[str, object]:
    return {
        "source": "terminal_outputs",
        "binding_name": binding_name,
        "artifact_type": artifact_type,
        "stage_keys": stage_keys,
        "output_name": output_name,
        "match_outcomes": match_outcomes,
    }


def _renderer(name: str, digest: str, terminal_keys: list[str]) -> dict[str, object]:
    return {
        "name": name,
        "version": 1,
        "digest": digest,
        "max_bytes": 16_777_216,
        "terminal_stage_keys": terminal_keys,
    }


def _gate(
    node_key: str,
    *,
    subject: str,
    match: str,
    matched_targets: list[str],
) -> dict[str, object]:
    return {
        "node_kind": "gate",
        "gate_kind": "outcome",
        "node_key": node_key,
        "shard_mode": "subject",
        "needs": [subject],
        "subject_stage_key": subject,
        "match_outcomes": [match],
        "matched_targets": matched_targets,
        "unmatched_targets": [],
    }


def _card_key(prefix: str, ordinal: int) -> str:
    return f"{prefix}_{ordinal:02d}"


def build_terminalgen_authoring_graph(
    identity: RecipeIdentityV1,
    parameters: Mapping[str, Any],
    *,
    images: AuthoringImageLockV1,
    renderers: TerminalGenRendererLocksV1,
) -> RunGraphSpecV1:
    """Build the complete 18-partition graph without registering it for submission."""

    parsed_parameters = AuthoringParametersV1.model_validate(dict(parameters))
    plan_nodes = [_card_key("plan_card", item) for item in range(EXPECTED_CARD_COUNT)]
    generate_nodes = [_card_key("generate_card", item) for item in range(EXPECTED_CARD_COUNT)]
    generate_gates = [_card_key("generate_gate", item) for item in range(EXPECTED_CARD_COUNT)]
    validate_nodes = [_card_key("validate_card", item) for item in range(EXPECTED_CARD_COUNT)]
    finalize_nodes = [_card_key("finalize_card", item) for item in range(EXPECTED_CARD_COUNT)]
    finalize_gates = [_card_key("finalize_gate", item) for item in range(EXPECTED_CARD_COUNT)]

    nodes: list[dict[str, object]] = [
        _container(
            "plan_batch",
            image=images.planner,
            profile="terminalgen-plan-none@1",
            network="none",
            needs=[],
            inputs=[
                {
                    "source": "run_input",
                    "binding_name": "catalog",
                    "artifact_type": "terminalgen.authoring-catalog.v1",
                    "input_name": "catalog",
                }
            ],
            outputs=[_output("admission", "terminalgen.plan-admission.v1")],
            timeout_seconds=300,
        )
    ]

    for plan_node in plan_nodes:
        nodes.append(
            _container(
                plan_node,
                image=images.planner,
                profile="terminalgen-plan-none@1",
                network="none",
                needs=["plan_batch"],
                inputs=[
                    {
                        "source": "run_input",
                        "binding_name": "catalog",
                        "artifact_type": "terminalgen.authoring-catalog.v1",
                        "input_name": "catalog",
                    },
                    _stage_input(
                        "admission",
                        "terminalgen.plan-admission.v1",
                        "plan_batch",
                        "admission",
                        shard_selection="singleton",
                    ),
                ],
                outputs=[
                    _output("partition", "terminalgen.partition-plan.v1", max_bytes=16_777_216),
                    _output("slot_index", "loom.platform-fanout-index.v1"),
                    _output("slot", "terminalgen.slot.v1", required=False, max_bytes=65_536),
                    _output(
                        "slot_manifest",
                        "loom.fanout-manifest.v1",
                        producer="platform",
                        role="fanout_manifest",
                        max_bytes=16_777_216,
                    ),
                ],
                fanout_commit={
                    "index_output_name": "slot_index",
                    "manifest_output_name": "slot_manifest",
                    "items_pointer": "/items",
                    "item_binding_name": "slot",
                    "max_items": parsed_parameters.slots_per_card,
                },
                timeout_seconds=600,
            )
        )

    nodes.extend(
        [
            _container(
                "plan_audit",
                image=images.planner,
                profile="terminalgen-plan-none@1",
                network="none",
                needs=plan_nodes,
                inputs=[
                    _terminal_input(
                        "partitions",
                        "terminalgen.partition-plan.v1",
                        plan_nodes,
                        "partition",
                        ["planned"],
                    )
                ],
                outputs=[_output("plan_audit", "terminalgen.plan-audit.v1", required=False)],
                request_renderer=_renderer(
                    "terminalgen_plan_audit", renderers.plan_audit, plan_nodes
                ),
                timeout_seconds=600,
            ),
            _gate(
                "plan_gate",
                subject="plan_audit",
                match="complete",
                matched_targets=generate_nodes,
            ),
        ]
    )

    for ordinal in range(EXPECTED_CARD_COUNT):
        plan_node = plan_nodes[ordinal]
        generate_node = generate_nodes[ordinal]
        generate_gate = generate_gates[ordinal]
        validate_node = validate_nodes[ordinal]
        finalize_node = finalize_nodes[ordinal]
        finalize_gate = finalize_gates[ordinal]
        fanout = {
            "source": "stage_output",
            "manifest_stage_key": plan_node,
            "manifest_output_name": "slot_manifest",
            "items_pointer": "/items",
            "shard_key_pointer": "/shard_key",
            "item_binding_name": "slot",
            "item_artifact_type": "terminalgen.slot.v1",
            "max_items": parsed_parameters.slots_per_card,
        }
        nodes.extend(
            [
                _container(
                    generate_node,
                    image=images.generator,
                    profile="terminalgen-generate-gateway@1",
                    network="gateway",
                    needs=[plan_node, "plan_gate"],
                    inputs=[
                        {
                            "source": "fanout_item",
                            "binding_name": "slot",
                            "artifact_type": "terminalgen.slot.v1",
                        }
                    ],
                    outputs=[
                        _output(
                            "task_bundle",
                            "terminalgen_task_bundle.v1",
                            required=False,
                            max_bytes=268_435_456,
                        ),
                        _output("slot_terminal", "terminalgen.slot-terminal.v1"),
                    ],
                    fanout=fanout,
                    timeout_seconds=3_600,
                    failure_policy="continue",
                ),
                _gate(
                    generate_gate,
                    subject=generate_node,
                    match="accepted",
                    matched_targets=[validate_node],
                ),
                _container(
                    validate_node,
                    image=images.dynamic_validator,
                    profile="terminalgen-validate-none@1",
                    network="none",
                    needs=[plan_node, generate_node, generate_gate],
                    inputs=[
                        {
                            "source": "fanout_item",
                            "binding_name": "slot",
                            "artifact_type": "terminalgen.slot.v1",
                        },
                        _stage_input(
                            "task_bundle",
                            "terminalgen_task_bundle.v1",
                            generate_node,
                            "task_bundle",
                            shard_selection="same_shard",
                            match_outcomes=["accepted"],
                        ),
                    ],
                    outputs=[
                        _output(
                            "validation",
                            "terminalgen_task_validation.v1",
                            required=False,
                            max_bytes=134_217_728,
                        ),
                        _output("validation_terminal", "terminalgen.validation-terminal.v1"),
                    ],
                    fanout=fanout,
                    timeout_seconds=7_200,
                    failure_policy="continue",
                ),
                _container(
                    finalize_node,
                    image=images.planner,
                    profile="terminalgen-plan-none@1",
                    network="none",
                    needs=[generate_node, validate_node],
                    inputs=[
                        _terminal_input(
                            "slot_terminals",
                            "terminalgen.slot-terminal.v1",
                            [generate_node],
                            "slot_terminal",
                            ["accepted", "rejected", "exhausted"],
                        ),
                        _terminal_input(
                            "validations",
                            "terminalgen_task_validation.v1",
                            [validate_node],
                            "validation",
                            ["validated"],
                        ),
                    ],
                    outputs=[_output("card_audit", "terminalgen.card-audit.v1", required=False)],
                    request_renderer=_renderer(
                        "terminalgen_card_finalize",
                        renderers.card_finalize,
                        [generate_node, validate_node],
                    ),
                    timeout_seconds=900,
                ),
                _gate(
                    finalize_gate,
                    subject=finalize_node,
                    match="complete",
                    matched_targets=["global_finalize"],
                ),
            ]
        )

    global_needs = [*finalize_nodes, *finalize_gates]
    nodes.extend(
        [
            _container(
                "global_finalize",
                image=images.planner,
                profile="terminalgen-plan-none@1",
                network="none",
                needs=global_needs,
                inputs=[
                    _terminal_input(
                        "card_audits",
                        "terminalgen.card-audit.v1",
                        finalize_nodes,
                        "card_audit",
                        ["complete"],
                    )
                ],
                outputs=[_output("final_audit", "terminalgen_final_audit.v1", required=False)],
                request_renderer=_renderer(
                    "terminalgen_global_finalize",
                    renderers.global_finalize,
                    finalize_nodes,
                ),
                timeout_seconds=900,
            ),
            _gate(
                "global_gate",
                subject="global_finalize",
                match="complete",
                matched_targets=["package_authoring", "package_runtime"],
            ),
        ]
    )

    terminal_producers = [*generate_nodes, *validate_nodes]
    for kind, image, renderer_digest in (
        ("authoring", images.packager, renderers.authoring_package),
        ("runtime", images.packager, renderers.runtime_package),
    ):
        node_key = f"package_{kind}"
        nodes.append(
            _container(
                node_key,
                image=image,
                profile="terminalgen-package-none@1",
                network="none",
                needs=["global_finalize", "global_gate", *terminal_producers],
                inputs=[
                    _stage_input(
                        "final_audit",
                        "terminalgen_final_audit.v1",
                        "global_finalize",
                        "final_audit",
                        shard_selection="singleton",
                        match_outcomes=["complete"],
                    ),
                    _terminal_input(
                        "task_bundles",
                        "terminalgen_task_bundle.v1",
                        generate_nodes,
                        "task_bundle",
                        ["accepted"],
                    ),
                    _terminal_input(
                        "validations",
                        "terminalgen_task_validation.v1",
                        validate_nodes,
                        "validation",
                        ["validated"],
                    ),
                ],
                outputs=[
                    _output(
                        "corpus",
                        "terminalgen_corpus.v1",
                        max_bytes=1_099_511_627_776,
                    )
                ],
                request_renderer=_renderer(
                    f"terminalgen_{kind}_package",
                    renderer_digest,
                    terminal_producers,
                ),
                timeout_seconds=7_200,
            )
        )

    nodes.append(
        _container(
            "publish_boundary",
            image=images.packager,
            profile="terminalgen-package-none@1",
            network="none",
            needs=["package_authoring", "package_runtime"],
            inputs=[
                _stage_input(
                    "authoring_corpus",
                    "terminalgen_corpus.v1",
                    "package_authoring",
                    "corpus",
                    shard_selection="singleton",
                ),
                _stage_input(
                    "runtime_corpus",
                    "terminalgen_corpus.v1",
                    "package_runtime",
                    "corpus",
                    shard_selection="singleton",
                ),
            ],
            outputs=[_output("publication_request", "terminalgen.publication-request.v1")],
            timeout_seconds=900,
        )
    )

    return RunGraphSpecV1.model_validate(
        {
            "schema_version": "loom.run-graph.v1",
            "recipe": identity.model_dump(mode="json"),
            "inputs": [
                {
                    "name": "catalog",
                    "artifact_type": "terminalgen.authoring-catalog.v1",
                    "required": True,
                }
            ],
            "parameters": parsed_parameters.model_dump(mode="json"),
            "budget": {
                "max_provider_cost_usd": "100000",
                "max_gpu_seconds": 0,
                "max_wall_seconds": 604_800,
                "max_artifact_bytes": 9_895_604_649_984,
                "max_stage_runs": 50_000,
                "max_attempts_total": 150_000,
            },
            "nodes": nodes,
        }
    )


__all__ = ["TerminalGenRendererLocksV1", "build_terminalgen_authoring_graph"]
