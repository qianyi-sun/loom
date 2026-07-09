"""Evidence directory tests (#340)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.evidence import (
    EvidenceDirectory,
    new_rollout_id,
)


class TestNewRolloutId:
    def test_deterministic_from_timestamp(self) -> None:
        now = datetime(2026, 7, 2, 23, 59, 59, tzinfo=UTC)
        rid = new_rollout_id(image_tag="staging-abc123", now=now)
        assert rid == "20260702t235959z-staging-abc123"

    def test_normalises_image_tag_to_dns(self) -> None:
        now = datetime(2026, 7, 2, 0, 0, 0, tzinfo=UTC)
        rid = new_rollout_id(image_tag="Staging.05ab776", now=now)
        assert rid == "20260702t000000z-staging-05ab776"  # DNS-normalization stress


class TestEvidenceDirectory:
    def test_ensure_creates_tree(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "20260702t000000z-x")
        assert not ev.exists()
        ev.ensure()
        assert ev.exists()
        assert (ev.path / "logs").is_dir()

    def test_step_dir_creates_zero_padded_prefix(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        s4 = ev.step_dir(4, "kind-load")
        assert s4.path.name == "04-kind-load"
        assert s4.path.is_dir()
        s12 = ev.step_dir(10, "cluster-up")
        assert s12.path.name == "10-cluster-up"

    def test_step_dir_slugifies_names(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        s = ev.step_dir(1, "GB10 SSH prep!")
        assert s.path.name == "01-gb10-ssh-prep"

    def test_write_and_read_inputs(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        payload = {
            "image_tag": "staging-abc",
            "resolved_sha": "abc123",
            "cluster_name": "loom-staging",
        }
        ev.write_inputs(payload)
        assert ev.read_inputs() == payload
        raw = ev.inputs_path().read_text()
        assert raw.index("cluster_name") < raw.index("image_tag")

    def test_write_and_read_step_result(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        s = ev.step_dir(4, "kind-load")
        assert ev.read_step_result(s) is None
        ev.write_step_result(
            s,
            {
                "state": "done",
                "inputs_hash": "h",
                "started_at": "t0",
                "finished_at": "t1",
                "exit_code": 0,
            },
        )
        assert ev.read_step_result(s) == {
            "state": "done",
            "inputs_hash": "h",
            "started_at": "t0",
            "finished_at": "t1",
            "exit_code": 0,
        }

    def test_existing_step_dir_returns_none_if_missing(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        assert ev.existing_step_dir(4, "kind-load") is None
        ev.step_dir(4, "kind-load")
        got = ev.existing_step_dir(4, "kind-load")
        assert got is not None
        assert got.path.name == "04-kind-load"


class TestFindInProgress:
    def test_finds_running_rollout_matching_tag(self, tmp_path: Path) -> None:
        old_dir = tmp_path / "rollouts" / "20260701t000000z-staging-abc"
        old_dir.mkdir(parents=True)
        (old_dir / "state.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": "20260701t000000z-staging-abc",
                    "status": "done",
                    "current_step": None,
                    "steps": [],
                }
            )
        )
        new_dir = tmp_path / "rollouts" / "20260702t000000z-staging-abc"
        new_dir.mkdir(parents=True)
        (new_dir / "state.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": "20260702t000000z-staging-abc",
                    "status": "running",
                    "current_step": 3,
                    "steps": [],
                }
            )
        )
        found = EvidenceDirectory.find_in_progress(
            tmp_path,
            image_tag="staging-abc",
        )
        assert found is not None
        assert found.rollout_id == "20260702t000000z-staging-abc"

    def test_returns_none_when_no_matching_dir(self, tmp_path: Path) -> None:
        (tmp_path / "rollouts").mkdir()
        assert (
            EvidenceDirectory.find_in_progress(
                tmp_path,
                image_tag="staging-abc",
            )
            is None
        )

    def test_returns_none_when_root_doesnt_exist(self, tmp_path: Path) -> None:
        assert (
            EvidenceDirectory.find_in_progress(
                tmp_path / "missing",
                image_tag="staging-abc",
            )
            is None
        )

    def test_skips_matching_but_done_rollouts(self, tmp_path: Path) -> None:
        d = tmp_path / "rollouts" / "20260702t000000z-staging-abc"
        d.mkdir(parents=True)
        (d / "state.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": d.name,
                    "status": "done",
                    "current_step": None,
                    "steps": [],
                }
            )
        )
        assert (
            EvidenceDirectory.find_in_progress(
                tmp_path,
                image_tag="staging-abc",
            )
            is None
        )

    def test_skips_matching_but_corrupted_state(self, tmp_path: Path) -> None:
        d = tmp_path / "rollouts" / "20260702t000000z-staging-abc"
        d.mkdir(parents=True)
        (d / "state.json").write_text("{ not json")
        assert (
            EvidenceDirectory.find_in_progress(
                tmp_path,
                image_tag="staging-abc",
            )
            is None
        )
