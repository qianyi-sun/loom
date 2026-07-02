"""`loom cluster load-images` unit tests (#96).

Loads local docker images into a kind cluster's node runtime so that
`kubectl apply` doesn't hit ErrImagePull on locally-built rollout images.
Also supports `--check-only` for use as a preflight smoke: verify each
image is already present in the kind node's image cache without pushing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_load_images import (
    ImageStatus,
    LoadResult,
    check_kind_image_loaded,
    load_images_into_kind,
    parse_images_from_manifest_text,
    resolve_images,
    run_kind_load,
)


class TestParseImagesFromManifestText:
    """Extracts `image: TAG` lines from k8s YAML."""

    def test_extracts_single_image(self) -> None:
        yaml = """
        apiVersion: apps/v1
        kind: Deployment
        spec:
          template:
            spec:
              containers:
              - name: main
                image: loom-worker:public-beta-1bbc323
        """
        assert parse_images_from_manifest_text(yaml) == [
            "loom-worker:public-beta-1bbc323"
        ]

    def test_extracts_images_from_multi_doc(self) -> None:
        yaml = (
            "---\n"
            "apiVersion: apps/v1\n"
            "kind: Deployment\n"
            "spec:\n"
            "  template:\n"
            "    spec:\n"
            "      containers:\n"
            "      - image: loom-worker:public-beta-1bbc323\n"
            "      - image: loom-service:public-beta-1bbc323\n"
            "---\n"
            "apiVersion: apps/v1\n"
            "kind: Deployment\n"
            "spec:\n"
            "  template:\n"
            "    spec:\n"
            "      containers:\n"
            "      - image: loom-llm-gateway:public-beta-1bbc323\n"
        )
        assert set(parse_images_from_manifest_text(yaml)) == {
            "loom-worker:public-beta-1bbc323",
            "loom-service:public-beta-1bbc323",
            "loom-llm-gateway:public-beta-1bbc323",
        }

    def test_deduplicates_repeated_images(self) -> None:
        yaml = (
            "spec:\n"
            "  template:\n"
            "    spec:\n"
            "      containers:\n"
            "      - image: loom-worker:public-beta-1bbc323\n"
            "      - image: loom-worker:public-beta-1bbc323\n"
        )
        assert parse_images_from_manifest_text(yaml) == [
            "loom-worker:public-beta-1bbc323"
        ]

    def test_skips_docker_hub_qualified_images(self) -> None:
        """Images already on docker.io / a registry can be pulled."""
        yaml = """
        spec:
          template:
            spec:
              containers:
              - image: docker.io/library/postgres:16
              - image: gcr.io/foo/bar:baz
              - image: registry.k8s.io/something:v1
              - image: loom-worker:public-beta-1bbc323
        """
        # Only the local (no-registry-prefix) image is a candidate for kind load.
        assert parse_images_from_manifest_text(yaml) == [
            "loom-worker:public-beta-1bbc323"
        ]

    def test_returns_empty_for_empty_yaml(self) -> None:
        assert parse_images_from_manifest_text("") == []

    def test_returns_empty_when_no_containers(self) -> None:
        yaml = "kind: ConfigMap\ndata:\n  key: value"
        assert parse_images_from_manifest_text(yaml) == []

    def test_extracts_from_init_containers(self) -> None:
        yaml = """
        spec:
          template:
            spec:
              initContainers:
              - image: loom-init:public-beta-1bbc323
              containers:
              - image: loom-worker:public-beta-1bbc323
        """
        assert set(parse_images_from_manifest_text(yaml)) == {
            "loom-init:public-beta-1bbc323",
            "loom-worker:public-beta-1bbc323",
        }


class TestResolveImages:
    """Merges explicit --image args + manifest-file parsing into one list."""

    def test_returns_explicit_images_only(self) -> None:
        assert resolve_images(
            explicit=["loom-worker:public-beta-a"],
            manifest_paths=[],
        ) == ["loom-worker:public-beta-a"]

    def test_reads_and_deduplicates_manifests(
        self, tmp_path: Path
    ) -> None:
        m1 = tmp_path / "a.yaml"
        m1.write_text(
            "spec:\n  template:\n    spec:\n      containers:\n"
            "      - image: loom-worker:public-beta-a\n"
        )
        m2 = tmp_path / "b.yaml"
        m2.write_text(
            "spec:\n  template:\n    spec:\n      containers:\n"
            "      - image: loom-service:public-beta-a\n"
            "      - image: loom-worker:public-beta-a\n"  # dupe
        )
        got = resolve_images(explicit=[], manifest_paths=[m1, m2])
        assert set(got) == {
            "loom-worker:public-beta-a",
            "loom-service:public-beta-a",
        }
        assert len(got) == 2  # no dupes

    def test_explicit_and_manifests_merged(self, tmp_path: Path) -> None:
        m = tmp_path / "a.yaml"
        m.write_text(
            "spec:\n  template:\n    spec:\n      containers:\n"
            "      - image: loom-worker:public-beta-a\n"
        )
        got = resolve_images(
            explicit=["loom-service:public-beta-a"],
            manifest_paths=[m],
        )
        assert set(got) == {
            "loom-worker:public-beta-a",
            "loom-service:public-beta-a",
        }

    def test_missing_manifest_raises(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.yaml"
        with pytest.raises(FileNotFoundError):
            resolve_images(explicit=[], manifest_paths=[missing])


class TestRunKindLoad:
    """Runs `kind load docker-image --name CLUSTER TAG`."""

    def test_calls_kind_binary_with_expected_args(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called: dict[str, Any] = {}

        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            called["argv"] = argv
            called["kwargs"] = kwargs

            class FakeProc:
                returncode = 0
                stdout = "Image: loom-worker:public-beta-a present in nodes\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        result = run_kind_load(
            cluster_name="loom-public-beta",
            image="loom-worker:public-beta-a",
            kind_bin="kind",
        )
        assert called["argv"] == [
            "kind", "load", "docker-image",
            "--name", "loom-public-beta",
            "loom-worker:public-beta-a",
        ]
        assert result.returncode == 0
        assert result.image == "loom-worker:public-beta-a"

    def test_nonzero_returncode_carried_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 1
                stdout = ""
                stderr = "Error: no image with name loom-worker:public-beta-a\n"

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        result = run_kind_load(
            cluster_name="loom-public-beta",
            image="loom-worker:public-beta-a",
            kind_bin="kind",
        )
        assert result.returncode == 1
        assert "no image with name" in result.stderr


class TestCheckKindImageLoaded:
    """Verifies image existence in kind node's containerd via crictl."""

    def test_returns_present_when_crictl_lists_image(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 0
                stdout = (
                    "IMAGE                                     TAG                  IMAGE ID\n"
                    "docker.io/library/loom-worker             public-beta-a        abc\n"
                )
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        assert check_kind_image_loaded(
            cluster_name="loom-public-beta",
            image="loom-worker:public-beta-a",
            docker_bin="docker",
        ) == ImageStatus.PRESENT

    def test_returns_missing_when_image_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 0
                stdout = "IMAGE\ndocker.io/library/postgres  16\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        assert check_kind_image_loaded(
            cluster_name="loom-public-beta",
            image="loom-worker:public-beta-a",
            docker_bin="docker",
        ) == ImageStatus.MISSING

    def test_returns_unknown_when_crictl_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 1
                stdout = ""
                stderr = "docker exec: no such container\n"

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        assert check_kind_image_loaded(
            cluster_name="loom-public-beta",
            image="loom-worker:public-beta-a",
            docker_bin="docker",
        ) == ImageStatus.UNKNOWN


class TestLoadImagesIntoKind:
    """Top-level orchestrator."""

    def test_check_only_reports_missing_without_loading(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # crictl output shows only postgres — our target image is missing.
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 0
                stdout = "IMAGE\ndocker.io/library/postgres 16\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        result = load_images_into_kind(
            cluster_name="loom-public-beta",
            images=["loom-worker:public-beta-a"],
            check_only=True,
        )
        assert isinstance(result, LoadResult)
        assert result.missing == ["loom-worker:public-beta-a"]
        assert result.loaded == []
        assert result.failed == []

    def test_load_calls_kind_load_for_each_image(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[list[str]] = []

        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            calls.append(argv)

            class FakeProc:
                returncode = 0
                stdout = "loaded\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        result = load_images_into_kind(
            cluster_name="loom-public-beta",
            images=[
                "loom-worker:public-beta-a",
                "loom-service:public-beta-a",
            ],
            check_only=False,
        )
        assert result.loaded == [
            "loom-worker:public-beta-a",
            "loom-service:public-beta-a",
        ]
        assert result.failed == []
        # Kind load was called for each image.
        assert len(calls) == 2
        for c in calls:
            assert c[:4] == ["kind", "load", "docker-image", "--name"]


class TestCLIDispatch:
    """End-to-end CLI: `loom cluster load-images ...`."""

    def test_check_only_returns_nonzero_when_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 0
                stdout = "IMAGE\ndocker.io/library/postgres 16\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        rc = main([
            "cluster", "load-images",
            "--cluster-name", "loom-public-beta",
            "--image", "loom-worker:public-beta-a",
            "--check-only",
        ])
        assert rc == 1
        err = capsys.readouterr().err
        # Actionable diagnostic: names the missing image AND suggests fix.
        assert "loom-worker:public-beta-a" in err
        assert "load-images" in err  # suggested rerun command

    def test_load_returns_zero_when_all_loaded(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 0
                stdout = "loaded\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        rc = main([
            "cluster", "load-images",
            "--cluster-name", "loom-public-beta",
            "--image", "loom-worker:public-beta-a",
            "--image", "loom-service:public-beta-a",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "loom-worker:public-beta-a" in out
        assert "loom-service:public-beta-a" in out

    def test_load_returns_nonzero_when_kind_load_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            class FakeProc:
                returncode = 1
                stdout = ""
                stderr = "Error: no image with name loom-worker:public-beta-a\n"

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        rc = main([
            "cluster", "load-images",
            "--cluster-name", "loom-public-beta",
            "--image", "loom-worker:public-beta-a",
        ])
        assert rc == 1
        err = capsys.readouterr().err
        assert "loom-worker:public-beta-a" in err

    def test_from_manifest_expands_images(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        m = tmp_path / "manifest.yaml"
        m.write_text(
            "spec:\n  template:\n    spec:\n      containers:\n"
            "      - image: loom-worker:public-beta-a\n"
            "      - image: loom-service:public-beta-a\n"
        )
        called: list[list[str]] = []

        def fake_run(argv: list[str], **kwargs: Any) -> Any:
            called.append(argv)

            class FakeProc:
                returncode = 0
                stdout = "loaded\n"
                stderr = ""

            return FakeProc()

        monkeypatch.setattr(
            "loom_cli.cluster_load_images.subprocess.run", fake_run
        )
        rc = main([
            "cluster", "load-images",
            "--cluster-name", "loom-public-beta",
            "--from-manifest", str(m),
        ])
        assert rc == 0
        loaded_images = {c[-1] for c in called if c[0] == "kind"}
        assert loaded_images == {
            "loom-worker:public-beta-a",
            "loom-service:public-beta-a",
        }

    def test_requires_at_least_one_image_source(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        rc = main([
            "cluster", "load-images",
            "--cluster-name", "loom-public-beta",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "--image" in err or "--from-manifest" in err
