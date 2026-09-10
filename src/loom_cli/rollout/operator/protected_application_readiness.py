"""Bounded, read-only serving-generation checks for attested Deployments."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Protocol

import yaml  # type: ignore[import-untyped]

_CORE = frozenset({"loom-control-plane", "loom-service", "loom-llm-gateway", "loom-web"})
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
_MAX_OUTPUT_BYTES = 1024 * 1024
_QUANTITY = re.compile(
    r"([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|[numkMGTPE]|[KMGTPE]i)?"
)


def _quantity(value: object) -> Fraction | None:
    if type(value) not in (str, int, float):
        return None
    text = str(value)
    if len(text) > 128 or (match := _QUANTITY.fullmatch(text)) is None:
        return None
    number, suffix = match.groups()
    scale = Fraction(1)
    if suffix:
        if suffix.endswith("i"):
            scale = Fraction(1024 ** ("KMGTPE".index(suffix[0]) + 1))
        elif suffix[0] in "eE" and len(suffix) > 1:
            exponent = int(suffix[1:])
            if abs(exponent) > 30:
                return None
            scale = Fraction(10) ** exponent
        else:
            exponent = {
                "n": -9,
                "u": -6,
                "m": -3,
                "k": 3,
                "M": 6,
                "G": 9,
                "T": 12,
                "P": 15,
                "E": 18,
            }[suffix]
            scale = Fraction(10) ** exponent
    return Fraction(number) * scale


def _quantity_path(path: tuple[str, ...]) -> bool:
    return (
        len(path) == 6
        and path[0] == "spec"
        and path[1] in {"containers", "initContainers"}
        and path[2:4] == ("*", "resources")
        and path[4] in {"limits", "requests"}
    ) or path == ("spec", "volumes", "*", "emptyDir", "sizeLimit")


class ApplicationReadinessRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True)
class ApplicationDeployment:
    name: str
    namespace: str
    replicas: int
    template: dict[str, object]


def application_deployments(payload: bytes, namespace: str) -> tuple[ApplicationDeployment, ...]:
    """Select only identities from the already hash-verified manifest."""
    documents = list(yaml.safe_load_all(payload))
    deployments = []
    names: set[str] = set()
    for document in documents:
        if not isinstance(document, dict) or document.get("kind") != "Deployment":
            continue
        metadata = document.get("metadata")
        spec = document.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise ValueError("application readiness manifest is invalid")
        name = metadata.get("name")
        replicas = spec.get("replicas", 1)
        template = spec.get("template")
        if (
            document.get("apiVersion") != "apps/v1"
            or metadata.get("namespace", namespace) != namespace
            or not isinstance(name, str)
            or _NAME.fullmatch(name) is None
            or name in names
            or type(replicas) is not int
            or replicas < 0
            or (name in _CORE and replicas == 0)
            or not isinstance(template, dict)
            or not isinstance(template.get("spec"), dict)
            or not isinstance(template["spec"].get("containers"), list)
            or not template["spec"]["containers"]
        ):
            raise ValueError("application readiness manifest is invalid")
        names.add(name)
        deployments.append(ApplicationDeployment(name, namespace, replicas, template))
    if not _CORE <= names:
        raise ValueError("application readiness core Deployment set is incomplete")
    return tuple(sorted(deployments, key=lambda deployment: deployment.name))


def _submitted_fields_match(expected: object, actual: object, path: tuple[str, ...] = ()) -> bool:
    """Allow server-added defaults, but never change or omit a submitted field.

    Lists remain exact in length/order: extra containers, env or volumes are
    not scalar defaulting. A fresh server-side diff additionally checks the
    full applied manifest before this observation can become terminal evidence.
    """
    if _quantity_path(path):
        quantity = _quantity(expected)
        return quantity is not None and quantity == _quantity(actual)
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            isinstance(key, str)
            and key in actual
            and _submitted_fields_match(value, actual[key], (*path, key))
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(expected) == len(actual)
            and all(
                _submitted_fields_match(a, b, (*path, "*"))
                for a, b in zip(expected, actual, strict=True)
            )
        )
    return type(expected) is type(actual) and expected == actual


def deployment_is_ready(
    desired: ApplicationDeployment,
    *,
    runner: ApplicationReadinessRunner,
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> bool:
    payload = runner.capture_stdout(
        (
            "kubectl",
            "--namespace",
            desired.namespace,
            "get",
            "deployment",
            desired.name,
            "--output=json",
        ),
        env=environment,
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(payload, bytes) or not payload or len(payload) > _MAX_OUTPUT_BYTES:
        raise RuntimeError("application readiness response is invalid")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("application readiness response is invalid") from exc
    if not isinstance(document, dict):
        raise RuntimeError("application readiness response is invalid")
    metadata, spec = (document.get(key) for key in ("metadata", "spec"))
    status = document.get("status", {})
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or not isinstance(status, dict):
        raise RuntimeError("application readiness response is invalid")
    if (
        document.get("apiVersion") != "apps/v1"
        or document.get("kind") != "Deployment"
        or metadata.get("name") != desired.name
        or metadata.get("namespace") != desired.namespace
        or not isinstance(metadata.get("uid"), str)
        or not metadata["uid"]
        or metadata.get("deletionTimestamp") is not None
        or type(spec.get("replicas")) is not int
        or spec["replicas"] != desired.replicas
        or not _submitted_fields_match(desired.template, spec.get("template"))
    ):
        raise RuntimeError("application readiness Deployment identity drifted")
    generation = metadata.get("generation")
    observed = status.get("observedGeneration")
    if type(generation) is not int or generation <= 0:
        raise RuntimeError("application readiness generation is invalid")
    if observed is not None and type(observed) is not int:
        raise RuntimeError("application readiness observed generation is invalid")
    # Conditions from an old observation cannot condemn a new generation.
    conditions = status.get("conditions", [])
    if not isinstance(conditions, list) or any(not isinstance(item, dict) for item in conditions):
        raise RuntimeError("application readiness conditions are invalid")
    if observed == generation and any(
        item.get("type") == "Progressing"
        and item.get("status") == "False"
        and item.get("reason") == "ProgressDeadlineExceeded"
        for item in conditions
    ):
        raise RuntimeError("application readiness progress deadline exceeded")
    ready = observed == generation
    for field in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas"):
        value = status.get(field, 0)
        if type(value) is not int or value < 0:
            raise RuntimeError("application readiness replica count is invalid")
        ready = ready and value == desired.replicas
    for field in ("unavailableReplicas", "terminatingReplicas"):
        value = status.get(field, 0)
        if type(value) is not int or value < 0:
            raise RuntimeError("application readiness replica count is invalid")
        ready = ready and value == 0
    return ready
