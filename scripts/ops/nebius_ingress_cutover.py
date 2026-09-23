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
