from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from scripts.ops import verify_nebius_restore as restore
from tests.ops.test_deploy_nebius_platform import FakeKubectl, rendered  # noqa: F401

from loom.nebius_restore import RestoreError, download_backup, verify_restored_records


def baseline() -> dict:
    return {
        "schema_revision": "0133",
        "trials": [
            {
                "id": str(uuid4()),
                "state": "succeeded",
                "aggregate_reward": 0,
                "llm_call_count": 1,
                "input_tokens": 3,
                "output_tokens": 2,
                "artifact_count": 1,
            }
        ],
    }


def request_data() -> dict:
    return {
        "namespace": "loom-nebius-platform",
        "buckets": {"backup": "backup", "artifacts": "artifacts", "trajectories": "trajectories"},
        "backup_key": "loom-nebius-platform/test.dump",
        "max_backup_bytes": 100,
        "baseline": baseline(),
    }


class Storage:
    def __init__(self, *, corrupt=False, size=4):
        self.calls = []
        self.corrupt, self.size = corrupt, size

    def head_object(self, **target):
        self.calls.append(("head", target))
        return {
            "ContentLength": self.size,
            "VersionId": "native-version",
            "Metadata": {"Sha256": hashlib.sha256(b"dump").hexdigest()},
        }

    def get_object(self, **target):
        self.calls.append(("get", target))
        return {"Body": io.BytesIO(b"oops" if self.corrupt else b"dump")}


def test_download_native_version_metadata_and_corruption(tmp_path):
    client = Storage()
    result = download_backup(request_data(), client, tmp_path)
    assert result["bytes"] == 4
    assert client.calls[-1][1]["VersionId"] == "native-version"
    (tmp_path / "loom.dump").unlink()
    with pytest.raises(RestoreError, match="backup-content-mismatch"):
        download_backup(request_data(), Storage(corrupt=True), tmp_path)
    assert not (tmp_path / "loom.dump").exists()


@pytest.mark.parametrize(
    "change,error",
    [
        ({"backup_key": "another-platform/test.dump"}, "target"),
        ({"backup_key": "loom-nebius-platform/../test.dump"}, "key"),
        ({"max_backup_bytes": 2}, "size-limit"),
    ],
)
def test_reject_wrong_target_or_oversized_before_download(tmp_path, change, error):
    request = request_data() | change
    client = Storage()
    with pytest.raises(RestoreError, match=error):
        download_backup(request, client, tmp_path)
    assert not any(call[0] == "get" for call in client.calls)


def test_canonical_references_cannot_redirect_endpoint_or_bucket():
    request = request_data()
    snapshot = copy.deepcopy(request["baseline"])
    snapshot["trials"][0]["objects"] = [
        {"files": [{"bucket": "foreign", "key": "secret", "size_bytes": 4}]}
    ]
    client = Storage()
    with pytest.raises(RestoreError, match="canonical-reference-target"):
        verify_restored_records(request, snapshot, client)
    assert client.calls == []


class RestoreKube(FakeKubectl):
    fail = False

    def run(self, *args, **kwargs):
        if args[:2] == ("create", "-f"):
            self.commands.append(args)
            for obj in yaml.safe_load_all(Path(args[2]).read_text()):
                if obj["kind"] == "Job":
                    obj["status"] = {
                        "conditions": [
                            {"type": "Failed" if self.fail else "Complete", "status": "True"}
                        ]
                    }
                self.objects[obj["kind"].lower(), obj["metadata"]["name"]] = obj
            return ""
        if args[0] == "logs":
            if self.fail:
                return 'private secret row must not persist\n{"status":"failed","phase":"pg_restore","exit_code":1}\n'
            return json.dumps(
                {"status": "verified", "local_database_stopped": True, "trial_count": 1}
            )
        return super().run(*args, **kwargs)


def args_for(fixture, tmp_path):
    args, config, _, files = fixture
    args.baseline = tmp_path / "baseline.json"
    args.baseline.write_text(json.dumps(baseline()))
    args.backup_key = config["namespace"] + "/test.dump"
    args.backup_version_id = None
    args.max_backup_bytes = 100
    return args, RestoreKube(config, files)


def test_plan_is_read_only_and_job_has_no_production_database(request, tmp_path):
    args, kube = args_for(request.getfixturevalue("rendered"), tmp_path)
    result = restore.run(args, kube)
    assert result["status"] == "planned"
    assert all(call[0] in {"get", "config"} for call in kube.commands)
    docs = list(yaml.safe_load_all((args.evidence_dir / "restore.yaml").read_text()))
    spec = docs[1]["spec"]["template"]["spec"]
    assert not any("persistentVolumeClaim" in volume for volume in spec["volumes"])
    assert spec["nodeSelector"]["loom.nebius/node-role"] == "system"
    assert spec["nodeSelector"]["loom.nebius/platform"] == "integration"
    db = spec["initContainers"][1]
    assert "env" not in db
    assert "listen_addresses=''" in docs[0]["data"]["restore.sh"]
    assert "--no-privileges" in docs[0]["data"]["restore.sh"]
    assert "loom-platform-database" not in json.dumps(docs)


def test_wrong_cluster_prevents_create(request, tmp_path):
    args, kube = args_for(request.getfixturevalue("rendered"), tmp_path)
    args.apply = True
    args.expected_cluster_id = "mk8scluster-foreign"
    with pytest.raises(restore.DeploymentError, match="expected cluster"):
        restore.run(args, kube)
    assert not any(call[0] == "create" for call in kube.commands)


@pytest.mark.parametrize("fail", [False, True])
def test_evidence_precedes_owned_cleanup_and_failed_job_retained(request, tmp_path, fail):
    args, kube = args_for(request.getfixturevalue("rendered"), tmp_path)
    args.apply, kube.fail = True, fail
    if fail:
        with pytest.raises(restore.DeploymentError):
            restore.run(args, kube)
    else:
        result = restore.run(args, kube)
        assert result["cleanup"] == "complete"
    evidence = (args.evidence_dir / "restore.json").read_text()
    assert "private secret" not in evidence
    if fail:
        assert "pg_restore" in evidence
        assert not any(call[0] == "delete" for call in kube.commands)
    else:
        assert any(call[0] == "delete" for call in kube.commands)


@pytest.mark.parametrize(
    "field,value", [("schema_revision", "0132"), ("input_tokens", 999), ("aggregate_reward", 1)]
)
def test_restored_baseline_mismatch_precedes_any_object_access(field, value):
    request = request_data()
    snapshot = copy.deepcopy(request["baseline"])
    if field == "schema_revision":
        snapshot[field] = value
    else:
        snapshot["trials"][0][field] = value
    client = Storage()
    with pytest.raises(RestoreError, match=r"restored-.*-mismatch"):
        verify_restored_records(request, snapshot, client)
    assert client.calls == []
