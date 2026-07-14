"""Evidence directory tests (#340)."""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.evidence import (
    EvidenceDirectory,
    new_rollout_id,
)
from loom_cli.rollout.operator.redaction import rollout_redaction_scope


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

    @pytest.mark.parametrize("swapped_component", ["root", "rollouts"])
    def test_ensure_refuses_a_parent_replaced_by_a_symlink(
        self,
        tmp_path: Path,
        swapped_component: str,
    ) -> None:
        root = tmp_path / "evidence-root"
        root.mkdir()
        ev = EvidenceDirectory(root, "rid")
        outside = tmp_path / "outside"
        outside.mkdir()

        if swapped_component == "root":
            root.rename(tmp_path / "original-root")
            root.symlink_to(outside, target_is_directory=True)
        else:
            rollouts = root / "rollouts"
            rollouts.mkdir()
            rollouts.rename(root / "original-rollouts")
            rollouts.symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            ev.ensure()

        assert list(outside.iterdir()) == []

    @pytest.mark.parametrize(
        "operation",
        ["write-inputs", "write-state", "append-driver-log", "create-step-dir"],
    )
    def test_core_writes_refuse_a_rollout_directory_replaced_by_a_symlink(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        root = tmp_path / "evidence-root"
        root.mkdir()
        ev = EvidenceDirectory(root, "rid")
        ev.ensure()
        original = root / "rollouts" / "original-rid"
        ev.path.rename(original)
        outside = tmp_path / "outside"
        outside.mkdir()
        ev.path.symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            if operation == "write-inputs":
                ev.write_inputs({"image_tag": "staging-abc"})
            elif operation == "write-state":
                ev.write_state({"version": 2, "status": "running"})
            elif operation == "append-driver-log":
                ev.append_driver_log("safe diagnostic\n")
            else:
                ev.step_dir(4, "kind-load")

        assert list(outside.iterdir()) == []

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
        assert stat.S_IMODE(ev.inputs_path().stat().st_mode) == 0o600

    def test_write_state_is_atomic_private_and_redacted(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()

        ev.write_state(
            {
                "version": 2,
                "status": "failed",
                "password": "never-persist-this",
            }
        )

        persisted = json.loads(ev.state_path().read_text(encoding="utf-8"))
        assert persisted == {
            "password": "[REDACTED:password]",
            "status": "failed",
            "version": 2,
        }
        assert stat.S_IMODE(ev.state_path().stat().st_mode) == 0o600

    def test_append_driver_log_is_private_and_refuses_a_log_symlink(
        self,
        tmp_path: Path,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        ev.append_driver_log("first\n")
        assert ev.driver_log_path().read_text(encoding="utf-8") == "first\n"
        assert stat.S_IMODE(ev.driver_log_path().stat().st_mode) == 0o600

        ev.driver_log_path().unlink()
        outside = tmp_path / "outside-driver.log"
        outside.write_text("outside\n", encoding="utf-8")
        ev.driver_log_path().symlink_to(outside)

        with pytest.raises(OSError):
            ev.append_driver_log("must-not-escape\n")

        assert outside.read_text(encoding="utf-8") == "outside\n"

    def test_append_driver_log_refuses_a_logs_directory_symlink(
        self,
        tmp_path: Path,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        logs = ev.path / "logs"
        logs.rmdir()
        outside = tmp_path / "outside-logs"
        outside.mkdir()
        logs.symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            ev.append_driver_log("must-not-escape\n")

        assert list(outside.iterdir()) == []

    def test_read_inputs_refuses_a_symlink(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        outside = tmp_path / "outside-inputs.json"
        outside.write_text('{"image_tag": "staging-abc"}\n', encoding="utf-8")
        ev.inputs_path().symlink_to(outside)

        with pytest.raises(OSError):
            ev.read_inputs()

    @pytest.mark.parametrize("flag_name", ["O_NOFOLLOW", "O_DIRECTORY"])
    def test_read_inputs_fails_closed_when_nofollow_traversal_is_unavailable(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        flag_name: str,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        ev.write_inputs({"image_tag": "staging-abc"})
        monkeypatch.setattr(f"loom_cli.rollout.evidence.os.{flag_name}", 0)

        with pytest.raises(OSError, match="no-follow"):
            ev.read_inputs()

    def test_write_inputs_keeps_prior_file_and_cleans_temp_on_replace_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        original = {"image_tag": "staging-original", "resolved_sha": "a" * 40}
        ev.write_inputs(original)
        original_bytes = ev.inputs_path().read_bytes()

        def fail_replace(
            _source: str | Path,
            _target: str | Path,
            **_kwargs: int,
        ) -> None:
            raise OSError("simulated interrupted publication")

        monkeypatch.setattr("loom_cli.rollout.evidence.os.replace", fail_replace)

        with pytest.raises(OSError, match="interrupted publication"):
            ev.write_inputs({"image_tag": "staging-new", "resolved_sha": "b" * 40})

        assert ev.inputs_path().read_bytes() == original_bytes
        assert sorted(path.name for path in ev.path.iterdir()) == ["inputs.json", "logs"]

    def test_write_inputs_fsyncs_file_then_parent_directory(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        real_fsync = os.fsync
        fsync_kinds: list[str] = []

        def recording_fsync(fd: int) -> None:
            metadata = os.fstat(fd)
            fsync_kinds.append("directory" if stat.S_ISDIR(metadata.st_mode) else "file")
            real_fsync(fd)

        monkeypatch.setattr("loom_cli.rollout.evidence.os.fsync", recording_fsync)

        ev.write_inputs({"image_tag": "staging-new", "resolved_sha": "b" * 40})

        assert fsync_kinds == ["file", "directory"]

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
        assert stat.S_IMODE(s.result_path().stat().st_mode) == 0o600

    def test_step_result_refuses_a_step_directory_symlink(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step = ev.step_dir(4, "kind-load")
        step.path.rmdir()
        outside = tmp_path / "outside-step"
        outside.mkdir()
        step.path.symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            ev.write_step_result(step, {"state": "done"})

        assert list(outside.iterdir()) == []

    def test_step_result_write_redacts_exact_and_sensitive_diagnostics(
        self,
        tmp_path: Path,
    ) -> None:
        secret = "opaque-evidence-secret"
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(4, "kind-load")

        with rollout_redaction_scope((secret,)):
            ev.write_step_result(
                step_dir,
                {
                    "state": "failed",
                    "error": f"failure {secret}",
                    "details": {"password": "plain-password"},
                },
            )

        raw = step_dir.result_path().read_text(encoding="utf-8")
        assert secret not in raw
        assert "plain-password" not in raw
        assert "[REDACTED:" in raw

    def test_existing_step_dir_returns_none_if_missing(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        assert ev.existing_step_dir(4, "kind-load") is None
        ev.step_dir(4, "kind-load")
        got = ev.existing_step_dir(4, "kind-load")
        assert got is not None
        assert got.path.name == "04-kind-load"

    def test_existing_step_dir_refuses_a_symlink(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        outside = tmp_path / "outside-step"
        outside.mkdir()
        (ev.path / "04-kind-load").symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            ev.existing_step_dir(4, "kind-load")


class TestResumeDocuments:
    def test_if_present_returns_none_only_when_rollout_directory_is_absent(
        self,
        tmp_path: Path,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")

        assert ev.read_resume_documents_if_present() is None

    def test_if_present_raises_when_existing_rollout_is_incomplete(
        self,
        tmp_path: Path,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        ev.write_inputs({"image_tag": "staging-abc"})

        with pytest.raises(FileNotFoundError):
            ev.read_resume_documents_if_present()

    def test_if_present_reads_both_documents_from_the_same_rollout_descriptor(
        self,
        tmp_path: Path,
    ) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        inputs = {"image_tag": "staging-abc"}
        state = {"version": 2, "status": "running"}
        ev.write_inputs(inputs)
        ev.write_state(state)

        assert ev.read_resume_documents_if_present() == (inputs, state)

    def test_if_present_refuses_an_unsafe_rollout_symlink(self, tmp_path: Path) -> None:
        ev = EvidenceDirectory(tmp_path, "rid")
        outside = tmp_path / "outside"
        outside.mkdir()
        (tmp_path / "rollouts").mkdir()
        ev.path.symlink_to(outside, target_is_directory=True)

        with pytest.raises(OSError):
            ev.read_resume_documents_if_present()


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

    @pytest.mark.parametrize("symlink_kind", ["rollout-directory", "state-file"])
    def test_never_follows_symlinks_during_discovery(
        self,
        tmp_path: Path,
        symlink_kind: str,
    ) -> None:
        rollouts = tmp_path / "rollouts"
        rollouts.mkdir()
        candidate_name = "20260702t000000z-staging-abc"
        outside = tmp_path / "outside" / candidate_name
        outside.mkdir(parents=True)
        outside_state = outside / "state.json"
        outside_state.write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": candidate_name,
                    "status": "running",
                    "current_step": 3,
                    "steps": [],
                }
            ),
            encoding="utf-8",
        )

        candidate = rollouts / candidate_name
        if symlink_kind == "rollout-directory":
            candidate.symlink_to(outside, target_is_directory=True)
        else:
            candidate.mkdir()
            (candidate / "state.json").symlink_to(outside_state)

        assert EvidenceDirectory.find_in_progress(tmp_path, "staging-abc") is None
