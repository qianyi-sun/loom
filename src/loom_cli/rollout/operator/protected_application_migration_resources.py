"""Exact Kubernetes effects beneath the protected application migration journal.

The enclosing component must admit the original immutable migration artifact,
journal its generation and dispatch intent before creation, and retain the same
guard. This adapter does not grant SQL authority, admit an artifact, journal a
dispatch, or certify database cleanup. Role sealing precedes Job deletion; role
and session retirement precede Secret deletion and database reopening.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from loom_cli.rollout.application_migration_contract import (
    APPLICATION_OWNER_ROLE,
    application_migration_secret_name,
    require_application_migration_job,
)

from .protected_cnpg_writer_configuration import _json, _mapping
from .staging_mutation_guard import MutationGuardEvidence


class ApplicationMigrationResourceRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...

    def run_checked(self, argv: Sequence[str], *, env: Mapping[str, str], input_payload: bytes | None,
                    timeout_seconds: float) -> None: ...


@dataclass(frozen=True, slots=True)
class ApplicationMigrationResourceIdentity:
    uid: str
    resource_version: str

    def __post_init__(self) -> None:
        if (re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", self.uid) is None
                or re.fullmatch(r"[1-9][0-9]{0,31}", self.resource_version) is None):
            raise ValueError("application migration resource identity is invalid")


@dataclass(frozen=True, slots=True)
class ProtectedApplicationMigrationResources:
    runner: ApplicationMigrationResourceRunner
    job: Mapping[str, object] = field(repr=False)
    secret: Mapping[str, object] = field(repr=False)
    guard: MutationGuardEvidence
    assert_guard: Callable[[], MutationGuardEvidence]
    _job_digest: str = field(init=False)
    _secret_digest: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        require_application_migration_job(self.job, owner_role=APPLICATION_OWNER_ROLE)
        job_metadata, secret_metadata = _mapping(self.job.get("metadata")), _mapping(self.secret.get("metadata"))
        job_name = job_metadata["name"]
        assert isinstance(job_name, str)
        annotations = _mapping(job_metadata.get("annotations"))
        if (type(self.guard) is not MutationGuardEvidence or self.guard.state != "ready" or not callable(self.assert_guard)
                or set(annotations) != {"loom.carin.dev/migration-generation", "loom.carin.dev/request-id",
                                       "loom.carin.dev/candidate-sha", "loom.carin.dev/candidate-tree"}
                or annotations.get("loom.carin.dev/request-id") != self.guard.request_id
                or annotations.get("loom.carin.dev/candidate-sha") != self.guard.candidate_sha
                or annotations.get("loom.carin.dev/candidate-tree") != self.guard.candidate_tree
                or re.fullmatch(r"[0-9a-f]{64}", str(annotations.get("loom.carin.dev/migration-generation"))) is None
                or self.secret.get("apiVersion") != "v1" or self.secret.get("kind") != "Secret"
                or set(self.secret) != {"apiVersion", "kind", "metadata", "type", "immutable", "data"}
                or self.secret.get("immutable") is not True or self.secret.get("type") != "Opaque"
                or secret_metadata.get("namespace") != "loom-staging"
                or secret_metadata.get("name") != application_migration_secret_name(job_name)
                or secret_metadata.get("annotations") != annotations
                or set(_mapping(self.secret.get("data"))) != {"db-url", "ca.crt"}):
            raise ValueError("application migration resource binding is invalid")
        object.__setattr__(self, "job", copy.deepcopy(dict(self.job)))
        object.__setattr__(self, "secret", copy.deepcopy(dict(self.secret)))
        object.__setattr__(self, "_job_digest", _digest(self.job))
        object.__setattr__(self, "_secret_digest", _digest(self.secret))

    def _check(self) -> None:
        if (self.assert_guard() != self.guard or _digest(self.job) != self._job_digest
                or _digest(self.secret) != self._secret_digest):
            raise RuntimeError("application migration guard or desired resource changed")

    def _read(self, kind: str, *, expected_uid: str | None = None, deleting: bool = False,
              ) -> tuple[ApplicationMigrationResourceIdentity, dict[str, object]] | None:
        desired = self.job if kind == "Job" else self.secret
        name = str(_mapping(desired["metadata"])["name"])
        self._check()
        payload = self.runner.capture_stdout(("kubectl", "--namespace", "loom-staging", "get", kind.lower(), name,
            "--ignore-not-found", "-o", "json", "--request-timeout=30s"),
            env=self.runner.environment, timeout_seconds=35)
        self._check()
        if not payload.strip():
            return None
        document = _json(payload)
        metadata = _mapping(document.get("metadata"))
        identity = ApplicationMigrationResourceIdentity(str(metadata.get("uid")), str(metadata.get("resourceVersion")))
        if ((expected_uid is not None and identity.uid != expected_uid)
                or _projection(document, deleting=deleting) != desired):
            raise RuntimeError("application migration observed resource drifted")
        return identity, document

    def ensure_secret(self, *, creation_dispatched: bool, expected_uid: str | None = None,
                      ) -> ApplicationMigrationResourceIdentity:
        return self._ensure("Secret", creation_dispatched=creation_dispatched, expected_uid=expected_uid)

    def ensure_job(self, *, creation_dispatched: bool, expected_uid: str | None = None,
                   ) -> ApplicationMigrationResourceIdentity:
        if self._read("Secret") is None:
            raise RuntimeError("application migration credential Secret is absent")
        return self._ensure("Job", creation_dispatched=creation_dispatched, expected_uid=expected_uid)

    def _ensure(self, kind: str, *, creation_dispatched: bool, expected_uid: str | None,
                ) -> ApplicationMigrationResourceIdentity:
        self._check()
        if creation_dispatched is not True:
            raise RuntimeError("application migration creation lacks recorded dispatch")
        observed = self._read(kind, expected_uid=expected_uid)
        if observed is not None:
            return observed[0]
        if expected_uid is not None:
            raise RuntimeError("application migration recorded resource disappeared")
        desired = self.job if kind == "Job" else self.secret
        self._check()
        self.runner.run_checked(("kubectl", "--namespace", "loom-staging", "create", "--validate=strict",
            "--request-timeout=30s", "-f", "-"), env=self.runner.environment,
            input_payload=_bytes(desired), timeout_seconds=35)
        self._check()
        observed = self._read(kind)
        if observed is None:
            raise RuntimeError("application migration creation has no observed resource")
        return observed[0]

    def job_complete(self, *, expected_uid: str) -> bool:
        observed = self._read("Job", expected_uid=expected_uid)
        if observed is None:
            raise RuntimeError("application migration Job disappeared before completion")
        status = _mapping(observed[1].get("status", {}))
        conditions = status.get("conditions", [])
        if not isinstance(conditions, list) or any(not isinstance(item, dict) for item in conditions):
            raise RuntimeError("application migration Job status is invalid")
        if any(item.get("status") == "True" and item.get("type") in {"Failed", "FailureTarget"} for item in conditions):
            raise RuntimeError("application migration Job failed")
        return (any(item.get("type") == "Complete" and item.get("status") == "True" for item in conditions)
                and status.get("succeeded") == 1 and status.get("active", 0) == 0)

    def delete_job(self, *, expected_uid: str) -> None:
        self._delete("Job", expected_uid=expected_uid)
        self._require_no_consumers(job_uid=expected_uid)

    def delete_secret(self, *, expected_uid: str) -> None:
        if self._read("Job", deleting=True) is not None:
            raise RuntimeError("application migration Job must retire before its Secret")
        self._require_no_consumers()
        self._delete("Secret", expected_uid=expected_uid)
        self._require_no_consumers()

    def _delete(self, kind: str, *, expected_uid: str) -> None:
        observed = self._read(kind, expected_uid=expected_uid, deleting=True)
        if observed is None:
            return
        identity, document = observed
        name = str(_mapping(document["metadata"])["name"])
        prefix = "/apis/batch/v1" if kind == "Job" else "/api/v1"
        path = f"{prefix}/namespaces/loom-staging/{kind.lower()}s/{name}"
        options = {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
            "preconditions": {"uid": identity.uid, "resourceVersion": identity.resource_version}}
        self._check()
        self.runner.run_checked(("kubectl", "delete", "--request-timeout=30s", "-f", "-", "--raw", path),
            env=self.runner.environment, input_payload=_bytes(options), timeout_seconds=35)
        self._check()
        deadline = time.monotonic() + 60
        while self._read(kind, expected_uid=expected_uid, deleting=True) is not None:
            if time.monotonic() >= deadline:
                raise RuntimeError("application migration resource deletion is still pending")
            time.sleep(0.25)

    def _require_no_consumers(self, *, job_uid: str | None = None) -> None:
        self._check()
        payload = self.runner.capture_stdout(("kubectl", "--namespace", "loom-staging", "get", "pods",
            "-o", "json", "--request-timeout=30s"), env=self.runner.environment, timeout_seconds=35)
        self._check()
        items = _json(payload).get("items")
        if not isinstance(items, list):
            raise RuntimeError("application migration Pod inventory is invalid")
        name = _mapping(self.secret["metadata"])["name"]
        for item in items:
            pod = _mapping(item)
            metadata = _mapping(pod.get("metadata"))
            owners = metadata.get("ownerReferences", [])
            if (not isinstance(owners, list) or any(not isinstance(owner, dict) for owner in owners)
                    or _references_secret(_mapping(pod.get("spec")), name)
                    or (job_uid is not None and any(owner.get("uid") == job_uid for owner in owners))):
                raise RuntimeError("application migration credential or Job still has a Pod consumer")


def _references_secret(value: object, name: object) -> bool:
    if isinstance(value, dict):
        if value.get("secretName") == name:
            return True
        for key in ("secretKeyRef", "secretRef", "secret", "nodePublishSecretRef"):
            reference = value.get(key)
            if isinstance(reference, dict) and reference.get("name") == name:
                return True
        return any(_references_secret(item, name) for item in value.values())
    return isinstance(value, list) and any(_references_secret(item, name) for item in value)


def _projection(document: Mapping[str, object], *, deleting: bool) -> dict[str, object]:
    value = copy.deepcopy(dict(document))
    value.pop("status", None)
    metadata = _mapping(value.get("metadata"))
    uid, name = metadata.get("uid"), metadata.get("name")
    for key in ("uid", "resourceVersion", "managedFields", "creationTimestamp", "generation"):
        metadata.pop(key, None)
    if deleting and metadata.get("deletionTimestamp"):
        metadata.pop("deletionTimestamp")
        metadata.pop("deletionGracePeriodSeconds", None)
        if metadata.get("finalizers") == ["foregroundDeletion"]:
            metadata.pop("finalizers")
    if value.get("kind") == "Job":
        spec = _mapping(value.get("spec"))
        default: object
        for key, default in (("completionMode", "NonIndexed"), ("manualSelector", False),
            ("podReplacementPolicy", "TerminatingOrFailed"), ("suspend", False), ("parallelism", 1), ("completions", 1),
            ("selector", {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}})):
            if spec.get(key) == default:
                spec.pop(key)
        template = _mapping(spec.get("template"))
        labels = _mapping(_mapping(template.get("metadata")).get("labels"))
        for key, default in (("batch.kubernetes.io/controller-uid", uid), ("controller-uid", uid),
                             ("batch.kubernetes.io/job-name", name), ("job-name", name)):
            if labels.get(key) == default:
                labels.pop(key)
        pod = _mapping(template.get("spec"))
        for key, default in (("dnsPolicy", "ClusterFirst"), ("schedulerName", "default-scheduler"), ("terminationGracePeriodSeconds", 30)):
            if pod.get(key) == default:
                pod.pop(key)
        containers = pod.get("containers")
        if not isinstance(containers, list):
            raise ValueError("application migration containers are invalid")
        for item in containers:
            container = _mapping(item)
            for key, default in (("terminationMessagePath", "/dev/termination-log"), ("terminationMessagePolicy", "File")):
                if container.get(key) == default:
                    container.pop(key)
    return value


def _bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()
