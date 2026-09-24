#!/usr/bin/env python3
"""Owner-fenced public selector/configuration cutover; never retry unknown writes.

The connected operation supplies fresh identity/capacity/controller qualification
and authenticated public probes. Two API mutations are NOT an atomic operation.
Any failure after guard acquisition leaves its pause and private recovery journal.
"""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_gateway import KubectlControllerAPI, TLSBinding

MARKER = "loom.nebius/ingress-cutover-id"


class CutoverError(RuntimeError):
    """Fixed failure; never publish configuration, credentials or API output."""


class CutoverAPI(Protocol):
    def read(self) -> tuple[dict[str, Any], dict[str, Any]]: ...
    def qualify(self) -> None:
        """Fresh identity, eligible capacity, exact staged controller and TLS."""
        ...

    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]: ...
    def patch(self, before: dict[str, Any], after: dict[str, Any]) -> None:
        """One UID/RV-conditioned patch of selector or configuration + marker."""
        ...

    def public_probe(self) -> None:
        """Verify exact-IP public management TLS and legacy HTTPS passthrough."""
        ...


class RecoveryAPI(Protocol):
    def read(self) -> tuple[dict[str, Any], dict[str, Any]]: ...
    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]: ...
    def restore(self, before: dict[str, Any], after: dict[str, Any], owner: str) -> None: ...
    def probe_original_backend(self) -> None: ...
    def probe_original_public(self) -> None: ...


class KubectlCutoverAPI(KubectlControllerAPI):
    """Fixed transport only; the orchestrator supplies readiness/public probes."""

    def __init__(self, kubeconfig: Path, *, binding: TLSBinding, executable: Path, candidate: str):
        super().__init__(kubeconfig, binding=binding, executable=executable)
        if not re.fullmatch(r"[0-9a-f]{40}", candidate):
            raise CutoverError("invalid cutover candidate")
        self.candidate = candidate

    def patch(self, before: dict[str, Any], after: dict[str, Any]) -> None:
        try:
            kind = before["kind"]
            if kind not in {"Service", "ConfigMap"}:
                raise CutoverError("resource outside cutover authority")
            name = "loom-web" if kind == "Service" else "loom-platform-config"
            _identity(before, kind=kind, name=name, namespace=self.binding.namespace)
            desired = copy.deepcopy(before)
            owner = after["metadata"]["annotations"][MARKER]
            if str(UUID(owner)) != owner or UUID(owner).int == 0:
                raise CutoverError("invalid cutover operation identity")
            desired["metadata"].setdefault("annotations", {})[MARKER] = owner
            if kind == "Service":
                if before["spec"]["type"] != "LoadBalancer" or before["spec"]["selector"] != {"app": "loom-web"}:
                    raise CutoverError("public selector is not standalone")
                desired["spec"]["selector"] = {"app": "loom-shared-ingress"}
                field, value = "/spec/selector", desired["spec"]["selector"]
            else:
                if json.loads(before["data"]["profile.json"])["candidate_sha"] != self.candidate:
                    raise CutoverError("configuration candidate differs")
                environment = json.loads(before["data"]["environment.json"])
                if environment.get("shared_ingress_enabled", False) is not False:
                    raise CutoverError("public configuration is not standalone")
                wanted = {**environment, "shared_ingress_enabled": True}
                value = after["data"]["environment.json"]
                if json.dumps(json.loads(value), sort_keys=True) != json.dumps(wanted, sort_keys=True):
                    raise CutoverError("cutover may change only the ingress mode")
                desired["data"]["environment.json"] = value
                field = "/data/environment.json"
            if after != desired:
                raise CutoverError("cutover contains an unauthorized field change")
            meta = before["metadata"]
            patch = [
                {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
                {"op": "test", "path": "/spec" if kind == "Service" else "/data",
                 "value": before["spec" if kind == "Service" else "data"]},
                {"op": "replace", "path": field, "value": value},
                {"op": "add", "path": "/metadata/annotations", "value": desired["metadata"]["annotations"]},
            ]
            self.verify_identity(self.binding)
            self._run(["patch", kind, name, "-n", self.binding.namespace,
                       "--type=json", "--patch-file=/dev/stdin", "-o", "name"], payload=json.dumps(patch).encode())
        except CutoverError:
            raise
        except Exception:
            raise CutoverError("cutover patch unavailable; reconcile before any further write") from None

    def restore(self, before: dict[str, Any], after: dict[str, Any], owner: str) -> None:
        """Single reverse CAS; the private rollback journal supplies original values."""
        try:
            kind = before["kind"]
            if kind not in {"Service", "ConfigMap"}:
                raise CutoverError("resource outside rollback authority")
            name = "loom-web" if kind == "Service" else "loom-platform-config"
            resource_key = "spec" if kind == "Service" else "data"
            _identity(before, kind=kind, name=name, namespace=self.binding.namespace)
            if (str(UUID(owner)) != owner or UUID(owner).int == 0
                    or before["metadata"].get("annotations", {}).get(MARKER) != owner):
                raise CutoverError("rollback resource is not owned by this cutover")
            annotations = {k: v for k, v in before["metadata"]["annotations"].items() if k != MARKER}
            restored_annotations = after["metadata"].get("annotations", {})
            if {k: v for k, v in restored_annotations.items() if k != MARKER} != annotations:
                raise CutoverError("rollback cannot change unrelated annotations")
            desired = copy.deepcopy(before)
            if "annotations" in after["metadata"]:
                desired["metadata"]["annotations"] = restored_annotations
            else:
                desired["metadata"].pop("annotations")
            if kind == "Service":
                if (before["spec"]["type"] != "LoadBalancer"
                        or before["spec"]["selector"] != {"app": "loom-shared-ingress"}):
                    raise CutoverError("rollback selector is outside cutover state")
                field, value = "/spec/selector", {"app": "loom-web"}
                desired["spec"]["selector"] = value
            else:
                if json.loads(before["data"]["profile.json"])["candidate_sha"] != self.candidate:
                    raise CutoverError("rollback candidate differs")
                current = json.loads(before["data"]["environment.json"])
                original = json.loads(after["data"]["environment.json"])
                if current.get("shared_ingress_enabled") is not True or original.get("shared_ingress_enabled", False) is not False:
                    raise CutoverError("rollback ingress flags are invalid")
                current.pop("shared_ingress_enabled")
                original.pop("shared_ingress_enabled", None)
                if json.dumps(current, sort_keys=True) != json.dumps(original, sort_keys=True):
                    raise CutoverError("rollback cannot change unrelated environment settings")
                field, value = "/data/environment.json", after["data"]["environment.json"]
                desired["data"]["environment.json"] = value
            if after != desired:
                raise CutoverError("rollback contains an unauthorized field change")
            patch = [
                {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
                {"op": "test", "path": "/" + resource_key, "value": before[resource_key]},
                {"op": "replace", "path": field, "value": value},
                ({"op": "add", "path": "/metadata/annotations", "value": restored_annotations}
                 if "annotations" in after["metadata"] else {"op": "remove", "path": "/metadata/annotations"}),
            ]
            self.verify_identity(self.binding)
            self._run(["patch", kind, name, "-n", self.binding.namespace, "--type=json", "--patch-file=/dev/stdin", "-o", "name"],
                      payload=json.dumps(patch).encode())
        except CutoverError:
            raise
        except Exception:
            raise CutoverError("rollback patch outcome unavailable; reconcile before further writes") from None

    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]:
        allowed = {"acquire": {"acquired", "skipped_busy", "skipped_locked"},
                   "observe": {"open", "held", "skipped_locked"}, "release": {"released"}}
        try:
            if (action not in allowed or candidate != self.candidate
                    or str(UUID(owner)) != owner or UUID(owner).int == 0):
                raise CutoverError("guard command outside cutover authority")
            self.verify_identity(self.binding)
            result = json.loads(self._run([
                "exec", "-n", self.binding.namespace, "deployment/loom-control-plane", "--",
                "python", "-m", "loom.nebius_rollout_guard", action, "--owner", owner, "--candidate", candidate,
            ]))
            if result.get("status") not in allowed[action]:
                raise CutoverError("guard operation did not produce an authorized receipt")
            return {"status": result["status"]}
        except CutoverError:
            raise
        except Exception:
            raise CutoverError("guard outcome unavailable; preserve operation journal") from None


def _stable(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    for key in ("resourceVersion", "managedFields"):
        result["metadata"].pop(key, None)
    return result


def _identity(value: dict[str, Any], *, kind: str, name: str, namespace: str) -> None:
    meta = value["metadata"]
    if (value["kind"] != kind or value["apiVersion"] != "v1" or meta["name"] != name
            or meta["namespace"] != namespace or meta.get("deletionTimestamp") is not None
            or str(UUID(meta["uid"])) != meta["uid"] or UUID(meta["uid"]).int == 0
            or not isinstance(meta["resourceVersion"], str) or not meta["resourceVersion"]):
        raise CutoverError("public resource identity differs")


def cutover(*, api: CutoverAPI, state_dir: Path, installation_id: str, candidate: str,
            namespace: str) -> dict[str, Any]:
    """Start or reconcile the same operation; ambiguous writes stay paused.

    A skipped idle attempt is terminal for this journal. A new explicit attempt
    uses a new private operation directory, never erases an unresolved journal.
    """
    try:
        if (str(UUID(installation_id)) != installation_id or UUID(installation_id).int == 0
                or not re.fullmatch(r"[0-9a-f]{40}", candidate)
                or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace)):
            raise CutoverError("invalid protected cutover binding")
        identity = {"installation_id": installation_id, "candidate": candidate, "namespace": namespace}
        with private_state._locked_state(state_dir):
            path = state_dir / "cutover.json"
            api.qualify()
            service, config = api.read()
            _identity(service, kind="Service", name="loom-web", namespace=namespace)
            _identity(config, kind="ConfigMap", name="loom-platform-config", namespace=namespace)
            if json.loads(config["data"]["profile.json"])["candidate_sha"] != candidate:
                raise CutoverError("installed candidate differs")
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=2 * 1024 * 1024))
                if record["binding"] != identity or str(UUID(record["owner"])) != record["owner"]:
                    raise CutoverError("cutover journal binding differs")
            else:
                environment = json.loads(config["data"]["environment.json"])
                if (service["spec"]["type"] != "LoadBalancer" or service["spec"]["selector"] != {"app": "loom-web"}
                        or environment.get("shared_ingress_enabled", False) is not False
                        or not service["status"]["loadBalancer"]["ingress"]):
                    raise CutoverError("public entrypoint is not in standalone mode")
                owner = str(uuid4())
                desired_service, desired_config = copy.deepcopy(service), copy.deepcopy(config)
                for desired in (desired_service, desired_config):
                    desired["metadata"].setdefault("annotations", {})[MARKER] = owner
                desired_service["spec"]["selector"] = {"app": "loom-shared-ingress"}
                desired_config["data"]["environment.json"] = json.dumps({**environment, "shared_ingress_enabled": True}, sort_keys=True)
                record = {"binding": identity, "owner": owner, "phase": "prepared", "service_before": service,
                          "service_after": desired_service, "config_before": config, "config_after": desired_config}
                private_state._atomic_json(path, record)

            def save(phase: str) -> None:
                record["phase"] = phase
                private_state._atomic_json(path, record)

            def read_matches(service_key: str, config_key: str) -> tuple[dict[str, Any], dict[str, Any]]:
                current_service, current_config = api.read()
                if (_stable(current_service) != _stable(record[service_key])
                        or _stable(current_config) != _stable(record[config_key])):
                    raise CutoverError("public resources changed; preserve paused recovery state")
                return current_service, current_config

            def observe() -> str:
                result = api.guard("observe", record["owner"], candidate)
                if result.get("status") not in {"open", "held", "skipped_locked"}:
                    raise CutoverError("rollout guard observation unavailable")
                return str(result["status"])

            def held() -> None:
                if observe() != "held":
                    raise CutoverError("cutover no longer owns the rollout pause")

            def write(which: str, other: str) -> None:
                held()
                api.qualify()
                keys = (which + "_before", other) if which == "service" else (other, which + "_before")
                current = read_matches(*keys)[0 if which == "service" else 1]
                desired = copy.deepcopy(record[which + "_after"])
                desired["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
                save("selector_intent" if which == "service" else "config_intent")
                try:
                    api.patch(current, desired)
                except Exception:
                    pass  # Readback is the only resolution; there is no second PATCH.

            phase = record["phase"]
            if phase in {"skipped_busy", "skipped_locked"}:
                return {"status": phase, **identity}
            if phase == "prepared":
                read_matches("service_before", "config_before")
                # Prove the installed candidate supports recovery observation
                # before acquiring a pause an older CLI cannot reconcile.
                if observe() != "open":
                    save("skipped_locked")
                    return {"status": "skipped_locked", **identity}
                save("acquire_intent")
                try:
                    result = api.guard("acquire", record["owner"], candidate)
                except Exception:
                    result = {}
                if result.get("status") in {"skipped_busy", "skipped_locked"}:
                    save(result["status"])
                    return {"status": result["status"], **identity}
            if record["phase"] == "acquire_intent":
                held()
                save("acquired")
            if record["phase"] == "acquired":
                write("service", "config_before")
            if record["phase"] == "selector_intent":
                held()
                read_matches("service_after", "config_before")
                save("selector_switched")
            if record["phase"] == "selector_switched":
                held()
                read_matches("service_after", "config_before")
                api.public_probe()
                write("config", "service_after")
            if record["phase"] == "config_intent":
                held()
                read_matches("service_after", "config_after")
                save("configuration_switched")
            if record["phase"] == "configuration_switched":
                held()
                api.qualify()
                api.public_probe()
                read_matches("service_after", "config_after")
                save("release_intent")
                try:
                    api.guard("release", record["owner"], candidate)
                except Exception:
                    pass
            if record["phase"] == "release_intent":
                read_matches("service_after", "config_after")
                if observe() != "open":
                    raise CutoverError("guard release unresolved; reconcile without retry")
                save("complete")
            if record["phase"] != "complete":
                raise CutoverError("unknown cutover phase")
            read_matches("service_after", "config_after")
            api.public_probe()
            return {"status": "complete", **identity, "service_uid": record["service_before"]["metadata"]["uid"]}
    except CutoverError:
        raise
    except Exception:
        raise CutoverError("ingress cutover incomplete; preserve private journal and rollout pause") from None


def rollback(*, api: RecoveryAPI, state_dir: Path, installation_id: str, candidate: str,
             namespace: str) -> dict[str, Any]:
    """Explicitly restore an interrupted, still-owned cutover; retain ingress state.

    Not a reversal of a completed deployment. An unresolved guard-release intent
    cannot authorize recovery: that release might still arrive after a read.
    Never erase an uncertain forward write or automatically retry a restore.
    """
    try:
        identity = {"installation_id": installation_id, "candidate": candidate, "namespace": namespace}
        with private_state._locked_state(state_dir):
            path = state_dir / "cutover.json"
            record = json.loads(private_state._private_read(path, limit=2 * 1024 * 1024))
            if (record["binding"] != identity or str(UUID(record["owner"])) != record["owner"]
                    or UUID(record["owner"]).int == 0):
                raise CutoverError("rollback binding differs from paused operation")

            def save(phase: str) -> None:
                record["phase"] = phase
                private_state._atomic_json(path, record)

            def read_matches(service_key: str, config_key: str) -> tuple[dict[str, Any], dict[str, Any]]:
                observed = api.read()
                if (_stable(observed[0]) != _stable(record[service_key])
                        or _stable(observed[1]) != _stable(record[config_key])):
                    raise CutoverError("rollback cannot overwrite drift or an unresolved write")
                return observed

            def observe() -> str:
                result = api.guard("observe", record["owner"], candidate)
                if result.get("status") not in {"open", "held", "skipped_locked"}:
                    raise CutoverError("rollback guard observation unavailable")
                return str(result["status"])

            def held() -> None:
                if observe() != "held":
                    raise CutoverError("rollback requires this operation's exact pause")

            def restore(which: str, other: str) -> None:
                held()
                api.probe_original_backend()
                keys = ("service_after", other) if which == "service" else (other, "config_after")
                current = read_matches(*keys)[0 if which == "service" else 1]
                desired = copy.deepcopy(record[which + "_before"])
                desired["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
                save("rollback_" + which + "_intent")
                try:
                    api.restore(current, desired, record["owner"])
                except Exception:
                    pass  # Only a matching readback can resolve this single write.

            phase = record["phase"]
            if phase == "rolled_back":
                read_matches("service_before", "config_before")
                api.probe_original_public()
                return {"status": "rolled_back", **identity}
            if phase in {"complete", "release_intent", "prepared", "skipped_busy", "skipped_locked"}:
                raise CutoverError("operation has no unambiguous owned pause for rollback")
            if phase != "rollback_release_intent":
                held()
            if phase in {"acquire_intent", "acquired"}:
                read_matches("service_before", "config_before")
                save("rollback_verify")
            elif phase in {"selector_intent", "selector_switched"}:
                read_matches("service_after", "config_before")
                save("rollback_service_pending")
            elif phase in {"config_intent", "configuration_switched"}:
                read_matches("service_after", "config_after")
                restore("config", "service_after")
            if record["phase"] == "rollback_config_intent":
                held()
                read_matches("service_after", "config_before")
                save("rollback_service_pending")
            if record["phase"] == "rollback_service_pending":
                restore("service", "config_before")
            if record["phase"] == "rollback_service_intent":
                held()
                read_matches("service_before", "config_before")
                save("rollback_verify")
            if record["phase"] == "rollback_verify":
                held()
                read_matches("service_before", "config_before")
                api.probe_original_public()
                read_matches("service_before", "config_before")
                save("rollback_release_intent")
                try:
                    api.guard("release", record["owner"], candidate)
                except Exception:
                    pass
            if record["phase"] == "rollback_release_intent":
                read_matches("service_before", "config_before")
                if observe() != "open":
                    raise CutoverError("rollback guard release unresolved; do not retry")
                save("rolled_back")
            if record["phase"] != "rolled_back":
                raise CutoverError("unknown paused rollback phase")
            return {"status": "rolled_back", **identity}
    except CutoverError:
        raise
    except Exception:
        raise CutoverError("ingress rollback incomplete; preserve journal and any owned pause") from None
