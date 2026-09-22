#!/usr/bin/env python3
"""Plan or perform bounded, idle-only registry maintenance for Nebius Loom."""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.component_ownership import NEBIUS_PLATFORM_IMAGES  # noqa: E402
from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    Kubectl,
    rollout_guard,
    verify_cluster_identity,
)
from scripts.ops.nebius_image_retention_db import image_refs  # noqa: E402

CANDIDATE = re.compile(r"candidate-[a-f0-9]{40}\Z")
TASK_TAG = re.compile(r"[a-f0-9]{64}-[0-9]+-[0-9]+\Z")


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.utcoffset() is None:
        raise ValueError("image timestamp has no timezone")
    return result


def ref(image: dict, host: str) -> str:
    return f"{host}/{image['name']}@{image['digest']}"


def protected_images(images: list[dict], refs: set[str], host: str) -> set[str]:
    # Also honor explicit tags found in live workload templates/catalog config.
    return {ref(image, host) for image in images if ref(image, host) in refs or any(
        f"{host}/{image['name']}:{tag}" in refs for tag in image.get("tags", [])
    )}


def native_rows(database: dict, task_repository: str) -> list[dict]:
    return [row for row in database["materializations"] if not row["legacy_publication"]
            and row["references"] and all(value.startswith(task_repository + "@sha256:")
                                          for value in row["references"])]


def owned_task_image(image: dict) -> bool:
    return (image.get("type") == "MANIFEST" and image.get("status") == "ACTIVE"
            and bool(image.get("tags")) and all(TASK_TAG.fullmatch(tag) for tag in image["tags"]))


def plan(images: list[dict], database: dict, live_refs: set[str], *, prefix: str,
         task_repository: str, now: datetime, days: int = 30, keep: int = 3) -> dict:
    if days < 1 or keep < 1:
        raise ValueError("retention days and rollback versions must be positive")
    host, registry = prefix.split("/", 1)
    cutoff = now - timedelta(days=days)
    refs = live_refs | set(database["protected"])
    # No release GC may remove an image tracked by the task-image lifecycle.
    refs.update(value for row in database["materializations"] for value in row["references"])
    protected = protected_images(images, refs, host)
    release_names = {registry + "/" + name for name in NEBIUS_PLATFORM_IMAGES.values()}
    newest: set[str] = set()
    for name in release_names:
        candidates = [image for image in images if image["name"] == name and
                      any(CANDIDATE.fullmatch(tag) for tag in image.get("tags", []))]
        newest.update(image["id"] for image in sorted(
            candidates, key=lambda row: timestamp(row["created_at"]), reverse=True,
        )[:keep])
    eligible_rows = [row for row in native_rows(database, task_repository)
                     if not row["referenced"] and row["unreferenced_at"]
                     and timestamp(row["unreferenced_at"]) <= cutoff
                     and (row["state"] in {"ready", "failed", "retired"} or
                          (row["state"] == "retiring" and row["lease_expires_at"]
                          and timestamp(row["lease_expires_at"]) <= now))]
    eligible_refs = {value for row in eligible_rows for value in row["references"]}
    decisions = []
    for image in images:
        value = ref(image, host)
        tags = image.get("tags", [])
        reason = "unmanaged"
        if image["name"] in release_names and tags and all(CANDIDATE.fullmatch(tag) for tag in tags):
            reason = "release_expired"
            if value in protected:
                reason = "referenced"
            elif image["id"] in newest:
                reason = "rollback_window"
            elif timestamp(image["created_at"]) > cutoff or timestamp(image["updated_at"]) > cutoff:
                reason = "retention_window"
        elif host + "/" + image["name"] == task_repository:
            reason = "task_untracked_review"
            if value in refs:
                reason = "task_tracked"
            if value in eligible_refs and owned_task_image(image):
                reason = "task_retirement_claim_required"
        if image.get("type") != "MANIFEST" or image.get("status") != "ACTIVE":
            reason = "unsupported_artifact"
        decisions.append({"id": image["id"], "image": value, "tags": tags,
                          "size_bytes": int(image["size"]), "reason": reason})
    return {"observed_at": now.isoformat(), "retention_days": days, "keep_versions": keep,
            "counts": dict(Counter(row["reason"] for row in decisions)), "images": decisions,
            "task_materializations": len(native_rows(database, task_repository)),
            "task_retirement_candidates": [row["id"] for row in eligible_rows],
            "size_note": "Image sizes include shared layers; this is not reclaimable or billed storage."}


class Registry:
    """Native provider API in CI; the installed CLI supplies local operator auth."""
    def __init__(self, registry_id: str, credentials: Path | None = None):
        self.registry_id, self.credentials = registry_id, credentials

    def call(self, action: str, image_id: str | None = None) -> dict:
        if action not in {"list", "delete"} or (action == "delete" and not image_id):
            raise ValueError("unsupported registry maintenance operation")
        if self.credentials is None:
            command = ["nebius", "--no-browser", "--no-check-update", "--timeout", "60s",
                       "--format", "json", "registry", "image", action]
            command += ["--parent-id", self.registry_id, "--all"] if action == "list" else ["--id", str(image_id)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise RuntimeError("native registry operation failed")
            return json.loads(result.stdout)

        async def request() -> dict:
            from nebius.api.nebius.registry.v1 import (
                ArtifactServiceClient,
                DeleteArtifactRequest,
                ListArtifactsRequest,
            )
            from nebius.sdk import SDK

            async with SDK(credentials_file_name=str(self.credentials), user_agent_prefix="loom-image-retention/1.0") as sdk:
                client = ArtifactServiceClient(sdk)

                def encode(item):
                    return {"id": item.id, "name": item.name, "digest": item.digest, "size": item.size,
                            "tags": list(item.tags), "type": item.type.name, "status": item.status.name,
                            "created_at": item.created_at.isoformat(), "updated_at": item.updated_at.isoformat()}

                if action == "list":
                    items, token, seen = [], "", set()
                    while True:
                        page = await client.list(ListArtifactsRequest(parent_id=self.registry_id, page_token=token, page_size=100), timeout=60)
                        items.extend(encode(item) for item in page.items)
                        token = page.next_page_token
                        if not token:
                            return {"items": items}
                        if token in seen:
                            raise ValueError("registry pagination did not advance")
                        seen.add(token)
                operation = await client.delete(DeleteArtifactRequest(id=image_id), timeout=30)
                await operation.wait(timeout=60)
                if not operation.successful():
                    raise RuntimeError("native registry deletion did not succeed")
                return {"deleted": image_id}

        return asyncio.run(request())


def db(kube: Kubectl, namespace: str, payload: dict) -> dict:
    bridge = Path(__file__).with_name("nebius_image_retention_db.py").read_text()
    return json.loads(kube.run("exec", "-n", namespace, "deployment/loom-control-plane", "--",
                               "python", "-B", "-c", bridge, json.dumps(payload), timeout=60))


def read_live(kube: Kubectl, namespace: str) -> tuple[dict, set[str]]:
    data = kube.get("configmap", "loom-platform-config", namespace)["data"]
    config = json.loads(data["environment.json"])
    if config["namespace"] != namespace or config.get("regional_execution_targets"):
        raise ValueError("retention supports the single-primary environment only")
    refs = set().union(*(image_refs(json.loads(data[key])) for key in ("environment.json", "profile.json")))
    # ReplicaSets retain Kubernetes rollback templates; Pods cover current pulls.
    for ns in (namespace, config["execution_namespace"], config["execution_namespace"] + "-build"):
        resources = json.loads(kube.run("get", "deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs,pods", "-n", ns, "-o", "json"))
        refs.update(image_refs(resources))
    return config, refs


def maintain(args, kube: Kubectl, registry: Registry) -> dict:
    config, live_refs = read_live(kube, args.namespace)
    verify_cluster_identity(kube, config, args.expected_cluster_id)
    task_repository = config["task_image_builder"]["registry_repository"]
    if not task_repository.startswith(args.registry_prefix + "/"):
        raise ValueError("configured task repository differs from maintenance registry")
    host, registry_path = args.registry_prefix.split("/")

    def inventory():
        rows = registry.call("list")["items"]
        if any(not row["name"].startswith(registry_path + "/") for row in rows):
            raise ValueError("registry inventory escaped the selected registry")
        return rows

    images = inventory()
    database = db(kube, args.namespace, {"action": "snapshot"})
    report = plan(images, database, live_refs, prefix=args.registry_prefix, task_repository=task_repository,
                  now=datetime.now(UTC), days=args.days, keep=args.keep)
    report.update(status="preview", deleted=[], retired=[])

    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")

    save()
    if not args.apply:
        return report
    owner = "image-retention-" + uuid4().hex
    candidate = json.loads(kube.get("configmap", "loom-platform-config", args.namespace)["data"]["profile.json"])["candidate_sha"]
    guard = rollout_guard(kube, args.namespace, "acquire", owner, candidate)
    if guard["status"] != "acquired":
        report["status"] = guard["status"]
        save()
        return report
    try:
        # A preview is never a deletion authority: recompute after excluding
        # deployment/execution/build reservations with the existing idle guard.
        _, live_refs = read_live(kube, args.namespace)
        images = inventory()
        database = db(kube, args.namespace, {"action": "snapshot"})
        current = plan(images, database, live_refs, prefix=args.registry_prefix, task_repository=task_repository,
                       now=datetime.now(UTC), days=args.days, keep=args.keep)
        report.update(current)
        by_ref = {ref(image, host): image for image in images}

        def delete_image(image) -> bool:
            if len(report["deleted"]) >= args.max_delete:
                return False
            # Nebius GetArtifact omits tags; ListArtifacts is the authoritative
            # tag inventory. A fresh list catches an added keep/foreign tag.
            fresh = next((row for row in inventory() if row["id"] == image["id"]), None)
            if fresh is None:
                return True
            if any(fresh.get(key) != image.get(key) for key in ("name", "digest", "tags", "updated_at")):
                raise ValueError("image changed during maintenance")
            registry.call("delete", image["id"])
            if any(row["id"] == image["id"] for row in inventory()):
                raise ValueError("deleted registry artifact remains visible")
            report["deleted"].append(image["id"])
            save()
            return True

        for decision in current["images"]:
            if decision["reason"] == "release_expired" and len(report["deleted"]) < args.max_delete:
                delete_image(by_ref[decision["image"]])

        ids = [row["id"] for row in native_rows(database, task_repository)
               if all(value not in by_ref or owned_task_image(by_ref[value]) for value in row["references"])]
        for _ in range(args.max_delete):
            if len(report["deleted"]) >= args.max_delete:
                break
            claim = db(kube, args.namespace, {"action": "claim", "owner": owner, "days": args.days, "ids": ids})["claim"]
            if claim is None:
                break
            fresh = db(kube, args.namespace, {"action": "snapshot"})
            protected = live_refs | set(fresh["protected"])
            protected.update(value for row in fresh["materializations"] if row["id"] != claim["id"]
                             for value in row["references"])
            # Catalog re-admission can race retirement. It does not make the
            # retiring cache ready, and it must not cause an unnecessary delete.
            protected.update(value for row in fresh["materializations"]
                             if row["id"] == claim["id"] and row["referenced"]
                             for value in row["references"])
            protected = protected_images(images, protected, host)
            for value in claim["references"]:
                if not value.startswith(task_repository + "@sha256:"):
                    raise ValueError("retirement claim escaped the task repository")
                image = by_ref.get(value)
                if image is None or value in protected or image["id"] in report["deleted"]:
                    continue
                if not owned_task_image(image):
                    raise ValueError("retirement claim includes an unmanaged image")
                if not delete_image(image):
                    report.update(status="bounded", pending_retirement=claim["id"])
                    return report
            result = db(kube, args.namespace, {"action": "complete", "owner": owner, **claim})
            report["retired"].append({"id": claim["id"], "state": result["state"]})
            save()
        report["status"] = "complete"
    except Exception:
        report["status"] = "failed"
        raise
    finally:
        try:
            rollout_guard(kube, args.namespace, "release", owner, candidate)
        except Exception:
            report["status"] = "guard_release_failed"
            raise
        finally:
            save()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", default="loom-nebius-platform")
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--registry-prefix", required=True)
    parser.add_argument("--credentials", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--keep", type=int, default=3)
    parser.add_argument("--max-delete", type=int, default=20)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+", args.registry_prefix) or not 1 <= args.max_delete <= 100:
        parser.error("invalid registry or deletion bound")
    try:
        registry = Registry("registry-" + args.registry_prefix.split("/")[1], args.credentials)
        report = maintain(args, Kubectl(args.kubeconfig), registry)
        print(json.dumps({key: report[key] for key in ("status", "counts", "deleted", "retired")}))
        return 0
    except Exception:
        print("Nebius image retention failed; inspect the bounded maintenance report", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
