from __future__ import annotations

import base64
import hashlib
import inspect
import json
import re
import tomllib
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import ci_runner_pool as pool

CANDIDATE_SHA = "a" * 40


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.containers: set[str] = set()

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: int | None = None,
    ) -> pool.CommandResult:
        del timeout
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return pool.CommandResult(0, CANDIDATE_SHA + "\n")
        if command[:2] == ("docker", "inspect"):
            name = command[-1]
            if name in self.containers:
                return pool.CommandResult(0, "true\n")
            return pool.CommandResult(1, "", "No such container")
        if command[:3] == ("docker", "rm", "-f"):
            self.containers.discard(command[-1])
            return pool.CommandResult(0)
        if "genisoimage" in command:
            output = command[command.index("-o") + 1]
            state_mount = next(
                item
                for item in command
                if item.startswith("type=bind,src=") and ",dst=/state" in item
            )
            state_root = Path(
                state_mount.removeprefix("type=bind,src=").split(",dst=", 1)[0],
            )
            (state_root / Path(output).name).write_bytes(b"test-iso")
            return pool.CommandResult(0)
        if command[:2] == ("docker", "run") and "-d" in command:
            name = command[command.index("--name") + 1]
            self.containers.add(name)
            return pool.CommandResult(0, "container-id\n")
        return pool.CommandResult(0)


class FakeAPI:
    def __init__(self) -> None:
        self.runners: dict[int, dict[str, Any]] = {}
        self.next_id = 100
        self.deleted: list[int] = []
        self.generated: list[tuple[str, tuple[str, ...]]] = []
        self.route_variables_present: set[str] = set()

    def list_runners(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self.runners.values()]

    def routing_variable_present(self, name: str) -> bool:
        return name in self.route_variables_present

    def generate_jit_config(
        self,
        *,
        name: str,
        labels: Sequence[str],
    ) -> tuple[int, str]:
        runner_id = self.next_id
        self.next_id += 1
        self.generated.append((name, tuple(labels)))
        self.runners[runner_id] = {
            "id": runner_id,
            "name": name,
            "status": "offline",
            "busy": False,
            "labels": [{"name": label} for label in labels],
        }
        return runner_id, f"opaque-jit-{runner_id}"

    def delete_runner(self, runner_id: int) -> None:
        self.deleted.append(runner_id)
        self.runners.pop(runner_id, None)


def _profile(tmp_path: Path, *, slots: int = 2) -> pool.PoolProfile:
    work_classes = (
        pool.WorkClassProfile(
            name="normal",
            label="loom-ci-normal",
            slots=slots,
            routing_variable="LOOM_CI_NORMAL_RUNS_ON",
            hosted_overflow_after_seconds=300,
        ),
        pool.WorkClassProfile(
            name="image",
            label="loom-ci-image",
            slots=0,
            routing_variable="LOOM_CI_IMAGE_RUNS_ON",
            hosted_overflow_after_seconds=900,
        ),
        pool.WorkClassProfile(
            name="smoke",
            label="loom-ci-smoke",
            slots=0,
            routing_variable="LOOM_CI_SMOKE_RUNS_ON",
            hosted_overflow_after_seconds=300,
        ),
    )
    profile = pool.PoolProfile(
        schema_version=2,
        repository="qianyi-sun/loom",
        expected_hostname="trt-eai-oldlab-5",
        state_root=tmp_path / "state",
        cache_root=tmp_path / "cache",
        qemu_image="loom-ci-runner-qemu:ubuntu-24.04-v1",
        runner_name_prefix="oldlab5-kvm",
        slots=slots,
        vcpus_per_slot=8,
        memory_mib_per_slot=6144,
        disk_gib_per_slot=64,
        reconcile_seconds=30,
        host_cpu_budget=22,
        host_memory_budget_mib=81_920,
        labels=pool.EXPECTED_LABELS,
        work_classes=work_classes,
        cloud_image_url=(
            "https://cloud-images.ubuntu.com/releases/noble/release/"
            "ubuntu-24.04-server-cloudimg-amd64.img"
        ),
        cloud_image_sha256="b" * 64,
        actions_runner_url=(
            "https://github.com/actions/runner/releases/download/v2.336.0/"
            "actions-runner-linux-x64-2.336.0.tar.gz"
        ),
        actions_runner_sha256="c" * 64,
    )
    profile.validate()
    return profile


def _seal_golden(profile: pool.PoolProfile) -> None:
    profile.cache_root.mkdir(parents=True)
    profile.golden_image.write_bytes(b"sealed-golden")
    digest = hashlib.sha256(b"sealed-golden").hexdigest()
    profile.golden_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "candidate_sha": CANDIDATE_SHA,
                "cloud_image_sha256": profile.cloud_image_sha256,
                "actions_runner_sha256": profile.actions_runner_sha256,
                "uv_version": pool.UV_VERSION,
                "uv_archive_sha256": pool.UV_ARCHIVE_SHA256,
                "base_images": list(pool.GUEST_BASE_IMAGES),
                "golden_sha256": digest,
            },
        ),
        encoding="utf-8",
    )


def _allow_execute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pool, "_verify_host", lambda _profile: None)
    monkeypatch.setattr(
        pool,
        "_verify_qemu_image",
        lambda _profile, _sha, _runner: None,
    )


def test_checked_in_oldlab5_profile_is_bounded_and_pinned() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    profile = pool.load_profile(repo_root / "deploy/ci-runners/oldlab5.toml")

    assert profile.schema_version == 2
    assert profile.slots == 11
    assert profile.vcpus_per_slot == 8
    assert profile.total_guest_vcpus == 88
    assert profile.host_cpu_budget == 22
    assert profile.total_memory_mib == 67_584
    assert profile.host_memory_budget_mib == 81_920
    assert profile.reconcile_seconds == 10
    assert profile.labels == pool.EXPECTED_LABELS
    assert {
        work_class.name: work_class.slots for work_class in profile.work_classes
    } == {"normal": 5, "image": 4, "smoke": 2}
    assert profile.labels_for_slot(0)[-1] == "loom-ci-normal"
    assert profile.labels_for_slot(4)[-1] == "loom-ci-normal"
    assert profile.labels_for_slot(5)[-1] == "loom-ci-image"
    assert profile.labels_for_slot(10)[-1] == "loom-ci-smoke"
    assert profile.cloud_image_sha256 == (
        "d1940f7d69d343355e183dff1e08a59852d32e7309baa7a4bad8365b11b005ac"
    )
    assert profile.actions_runner_sha256 == (
        "04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d"
    )


def test_runner_timer_arms_when_enabled_after_boot() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    profile = pool.load_profile(repo_root / "deploy/ci-runners/oldlab5.toml")
    timer = (
        repo_root / "deploy/ci-runners/loom-ci-runner-pool.timer"
    ).read_text(encoding="utf-8")

    assert f"OnActiveSec={profile.reconcile_seconds}s" in timer
    assert "OnBootSec=" not in timer
    assert f"OnUnitActiveSec={profile.reconcile_seconds}s" in timer
    assert "AccuracySec=1s" in timer


def test_profile_rejects_resource_budget_or_label_expansion(tmp_path: Path) -> None:
    profile = _profile(tmp_path, slots=11)
    with pytest.raises(pool.PoolConfigError, match="host_cpu_budget"):
        replace(profile, host_cpu_budget=23).validate()

    with pytest.raises(pool.PoolConfigError, match="aggregate memory"):
        replace(profile, memory_mib_per_slot=8192).validate()

    with pytest.raises(pool.PoolConfigError, match="labels must be exactly"):
        replace(
            profile,
            labels=(*pool.EXPECTED_LABELS, "production"),
        ).validate()

    with pytest.raises(pool.PoolConfigError, match="work class slots"):
        replace(
            profile,
            work_classes=(
                replace(profile.work_classes[0], slots=profile.slots - 1),
                *profile.work_classes[1:],
            ),
        ).validate()

    with pytest.raises(pool.PoolConfigError, match="contract is invalid"):
        replace(
            profile,
            work_classes=(
                replace(profile.work_classes[0], label="production"),
                *profile.work_classes[1:],
            ),
        ).validate()


def test_token_source_requires_private_regular_file(tmp_path: Path) -> None:
    token = tmp_path / "github-token"
    token.write_text("secret-token\n", encoding="utf-8")
    token.chmod(0o640)

    with pytest.raises(pool.PoolOperationError, match="group/other"):
        pool.read_token(token)

    token.chmod(0o600)
    assert pool.read_token(token) == "secret-token"


def test_dockerhub_credentials_require_private_exact_json(tmp_path: Path) -> None:
    credentials = tmp_path / "dockerhub-credentials.json"
    credentials.write_text(
        json.dumps({"username": "loom-ci", "token": "secret-token"}),
        encoding="utf-8",
    )
    credentials.chmod(0o640)

    with pytest.raises(pool.PoolOperationError, match="group/other"):
        pool.read_dockerhub_credentials(credentials)

    credentials.chmod(0o600)
    assert pool.read_dockerhub_credentials(credentials) == pool.DockerHubCredentials(
        username="loom-ci",
        token="secret-token",
    )

    credentials.write_text(
        json.dumps(
            {
                "username": "loom-ci",
                "token": "secret-token",
                "unexpected": True,
            },
        ),
        encoding="utf-8",
    )
    with pytest.raises(pool.PoolOperationError, match="fields"):
        pool.read_dockerhub_credentials(credentials)


def test_dry_run_never_calls_github_or_docker(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    runner = FakeRunner()

    plan = pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=False,
        api=None,
        runner=runner,
    )

    assert plan["mutation_authorized"] is False
    assert plan["create_slots"] == [0, 1]
    assert runner.calls == []
    assert not profile.state_root.exists()


def test_agent_sandbox_benchmark_is_dry_run_by_default(tmp_path: Path) -> None:
    profile = _profile(tmp_path, slots=1)
    runner = FakeRunner()

    plan = pool.benchmark_agent_sandbox(
        profile,
        candidate_sha=CANDIDATE_SHA,
        vcpus=8,
        execute=False,
        runner=runner,
    )

    assert plan["mutation_authorized"] is False
    assert plan["vcpus"] == 8
    assert plan["memory_mib"] == 6144
    assert runner.calls == []


def test_agent_sandbox_benchmark_matches_ci_build_shape() -> None:
    script = pool._agent_sandbox_benchmark_script(CANDIDATE_SHA, 8)

    assert f"checkout --detach {CANDIDATE_SHA}" in script
    assert "tonistiigi/binfmt --install all" in script
    assert "--platform linux/amd64,linux/arm64" in script
    assert "--file /opt/loom/deploy/Dockerfile.agent-sandbox" in script
    assert "LOOM_CI_BENCHMARK_RESULT vcpus=8" in script


def test_qemu_command_has_kvm_boundary_and_no_host_docker_socket(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    slot_root = profile.state_root / "slot-00"
    command = pool._qemu_container_command(
        profile,
        slot=0,
        slot_root=slot_root,
        container_name="loom-ci-runner-00-test",
        detach=True,
    )
    rendered = " ".join(command)

    assert "--device /dev/kvm" in rendered
    assert "--dns 1.1.1.1 --dns 8.8.8.8" in rendered
    assert "192.168." not in rendered
    assert "--cgroup-parent loom-ci-runner-pool.slice" in rendered
    assert "--cpu-shares 1024" in rendered
    assert "--cpus" not in command
    assert "--cap-drop ALL --cap-add NET_ADMIN --cap-add SETPCAP" in rendered
    assert "--security-opt no-new-privileges" in rendered
    assert "--read-only" in command
    assert "/var/run/docker.sock" not in rendered
    assert "/run/docker.sock" not in rendered
    assert str(slot_root) in rendered
    assert str(profile.cache_root) in rendered
    assert "hostfwd" not in rendered
    assert ",rw" not in rendered
    assert ",ro" not in rendered
    assert "dst=/cache,readonly" in rendered


def test_outer_qemu_image_blocks_private_networks_before_dropping_caps() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    entrypoint = (
        repo_root / "deploy/ci-runners/qemu-entrypoint.sh"
    ).read_text(encoding="utf-8")
    dockerfile = (
        repo_root / "deploy/ci-runners/qemu.Dockerfile"
    ).read_text(encoding="utf-8")

    for cidr in (
        "10.0.0.0/8",
        "100.64.0.0/10",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
    ):
        assert cidr in entrypoint
    assert "ip6 daddr != ::1 reject" in entrypoint
    assert entrypoint.index("nft -f") < entrypoint.index("setpriv")
    assert "--bounding-set=-all" in entrypoint
    assert "FROM ubuntu@sha256:" in dockerfile
    assert "LOOM_CANDIDATE_SHA" in dockerfile


def test_slot_cloud_init_keeps_jit_material_out_of_user_data() -> None:
    config = pool._cloud_config(
        "/usr/local/sbin/loom-ci-run-once",
        pool._slot_run_script(),
    )

    assert "opaque-jit" not in config
    assert "LOOMCIJIT" in base64.b64decode(
        json.loads(config.removeprefix("#cloud-config\n"))["write_files"][0][
            "content"
        ],
    ).decode()
    assert "umask 0022" in pool._slot_run_script()
    assert "truncate -s 0 /var/log/loom-actions-runner.log" in pool._slot_run_script()
    assert "poweroff" in pool._slot_run_script()


def test_golden_guest_installs_github_hosted_compatibility_tools(
    tmp_path: Path,
) -> None:
    script = pool._base_install_script(_profile(tmp_path))

    for package in (
        "build-essential",
        "docker-buildx",
        "docker-compose-v2",
        "python-is-python3",
    ):
        assert package in script
    assert "sudo -u runner docker compose version" in script
    assert "sudo -u runner python --version" in script
    assert "test -x /usr/bin/cc" in script


def test_golden_guest_serves_checksum_pinned_uv_assets_on_loopback(
    tmp_path: Path,
) -> None:
    script = pool._base_install_script(_profile(tmp_path))
    toolchain = tomllib.loads(
        (
            Path(__file__).resolve().parents[2] / "config/uv-toolchain.toml"
        ).read_text(encoding="utf-8"),
    )

    assert pool.UV_VERSION == toolchain["version"]
    assert pool.UV_ARCHIVE_NAME == toolchain["archives"]["linux-x86_64"]["asset"]
    assert (
        pool.UV_ARCHIVE_SHA256
        == toolchain["archives"]["linux-x86_64"]["sha256"]
    )
    assert f"{pool.UV_ARCHIVE_SHA256}  /mnt/loom-ci-assets/" in script
    assert pool._uv_manifest() in script
    assert "--bind 127.0.0.1" in script
    assert "ProtectSystem=strict" in script
    assert "systemctl enable --now loom-ci-uv-assets.service" in script
    assert f"curl --fail --silent {pool.UV_MANIFEST_URL}" in script
    assert "for _attempt in $(seq 1 30)" in script
    assert "journalctl --no-pager -u loom-ci-uv-assets.service -n 50" in script


def test_golden_guest_seeds_images_without_retaining_registry_credentials(
    tmp_path: Path,
) -> None:
    script = pool._base_install_script(_profile(tmp_path))

    assert "docker login" in script
    assert "--password-stdin" in script
    assert "skopeo copy" in script
    assert "--all" in script
    assert "--src-authfile /root/.docker/config.json" in script
    for image in pool.GUEST_BASE_IMAGES:
        assert image in script
    assert "docker logout" in script
    assert "rm -rf /root/.docker /run/loom-ci-registry" in script
    assert "test ! -e /root/.docker/config.json" in script
    assert "readonly:" in script
    assert "registry-mirrors" in script
    assert "/etc/buildkit/loom-ci.toml" in script
    assert "skopeo inspect" in script
    assert "--raw" in script
    assert '.platform.architecture == "amd64"' in script
    assert 'docker pull "${source_ref}"' in script
    assert 'docker tag "${source_ref}" "${image}"' in script
    assert '"docker-daemon:${image}"' not in script
    assert 'docker pull --platform linux/amd64 "${image}"' not in script
    assert script.count("systemctl restart docker-registry\nwait_for_registry") == 2
    assert "journalctl --no-pager -u docker-registry -n 50" in script
    assert "proxy:" not in script
    assert "secret-token" not in script


def test_golden_mirror_covers_ci_dockerhub_inputs() -> None:
    from testcontainers.core.config import testcontainers_config
    from testcontainers.minio import MinioContainer

    def dockerhub_ref(raw_ref: str) -> str | None:
        ref = raw_ref.split("@", 1)[0]
        if not ref or ref.startswith(("$", "loom-")):
            return None
        if "/" in ref:
            registry = ref.split("/", 1)[0]
            if registry == "localhost" or "." in registry or ":" in registry:
                return None
        if ":" not in ref.rsplit("/", 1)[-1]:
            ref += ":latest"
        return ref

    expected = {
        "busybox:latest",
        "edoburu/pgbouncer:latest",
        "kindest/node:v1.31.0",
        "moby/buildkit:buildx-stable-1",
        "postgres:16-alpine",
        "tonistiigi/binfmt:latest",
    }
    minio_default = inspect.signature(MinioContainer.__init__).parameters["image"].default
    assert isinstance(minio_default, str)
    expected.update((minio_default, testcontainers_config.ryuk_image))

    image_call = re.compile(
        r"(?:DockerContainer|DockerDriver|PostgresContainer)\(\s*"
        r"(?:image\s*=\s*)?[\"']([^\"']+)",
    )
    for test_root in (Path("tests/integration"), Path("tests/contract")):
        for path in test_root.rglob("*.py"):
            for raw_ref in image_call.findall(path.read_text(encoding="utf-8")):
                if (ref := dockerhub_ref(raw_ref)) is not None:
                    expected.add(ref)

    manifest = tomllib.loads(
        Path("config/component-ownership.toml").read_text(encoding="utf-8"),
    )
    for component in manifest["components"]:
        if component["kind"] != "release-image":
            continue
        dockerfile = Path(component["dockerfile"]).read_text(encoding="utf-8")
        for line in dockerfile.splitlines():
            if not line.lstrip().startswith("FROM "):
                continue
            tokens = line.split()
            raw_ref = tokens[2] if tokens[1].startswith("--platform=") else tokens[1]
            if (ref := dockerhub_ref(raw_ref)) is not None:
                expected.add(ref)

    for path in (
        *Path("deploy").glob("*compose*.yml"),
        *Path("deploy/k8s").glob("*.yaml"),
    ):
        for raw_ref in re.findall(
            r"^\s*image:\s*[\"']?([^\"'\s]+)",
            path.read_text(encoding="utf-8"),
            re.MULTILINE,
        ):
            if (ref := dockerhub_ref(raw_ref)) is not None:
                expected.add(ref)

    assert expected <= set(pool.GUEST_BASE_IMAGES)


def test_runner_process_sets_github_hosted_umask() -> None:
    script = pool._slot_run_script()

    assert "sudo -u runner /bin/bash -c" in script
    assert "umask 0022" in script
    assert "exec /opt/actions-runner/run.sh" in script


def test_iso_commands_preserve_long_filenames(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()

    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )

    iso_commands = [command for command in runner.calls if "genisoimage" in command]
    assert iso_commands
    assert all("-R" in command and "-graft-points" in command for command in iso_commands)


def test_reconcile_creates_one_ephemeral_vm_per_slot_without_jit_in_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()

    result = pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )

    assert result["created_slots"] == [0, 1]
    assert len(api.generated) == 2
    assert len(runner.containers) == 2
    assert all(
        labels == (*pool.EXPECTED_LABELS, "loom-ci-normal")
        for _, labels in api.generated
    )
    assert all("-normal-" in name for name, _ in api.generated)
    for slot in (0, 1):
        state_root = profile.state_root / f"slot-{slot:02d}"
        state = json.loads((state_root / "state.json").read_text())
        assert "jit" not in json.dumps(state).lower()
        assert not (state_root / "jitconfig").exists()
        assert state["candidate_sha"] == CANDIDATE_SHA


def test_reconcile_replaces_completed_slot_and_preserves_live_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    states = pool._existing_states(profile)
    completed = states[0]
    runner.containers.remove(completed.container_name)

    result = pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )

    assert result["cleaned_slots"] == [0]
    assert result["created_slots"] == [0]
    assert completed.runner_id in api.deleted
    assert len(runner.containers) == 2


def test_reconcile_removes_jit_iso_after_runner_is_online(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    state = next(iter(pool._existing_states(profile).values()))
    jit_iso = profile.state_root / "slot-00/jit.iso"
    assert jit_iso.is_file()
    assert jit_iso.stat().st_mode & 0o777 == 0o600

    api.runners[state.runner_id]["status"] = "online"
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )

    assert not jit_iso.exists()


def test_reconcile_replaces_online_runner_with_wrong_work_class_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    state = next(iter(pool._existing_states(profile).values()))
    api.runners[state.runner_id]["status"] = "online"
    api.runners[state.runner_id]["labels"] = [
        {"name": label} for label in pool.EXPECTED_LABELS
    ]

    result = pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )

    assert result["cleaned_slots"] == [0]
    assert result["created_slots"] == [0]
    assert state.runner_id in api.deleted
    replacement = next(iter(pool._existing_states(profile).values()))
    assert replacement.runner_id != state.runner_id


def test_drain_refuses_while_routing_variable_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    api.route_variables_present.add(pool.ROUTING_VARIABLE)

    with pytest.raises(pool.PoolOperationError, match=pool.ROUTING_VARIABLE):
        pool.drain(profile, execute=True, api=api, runner=runner)

    assert runner.containers
    assert not api.deleted


def test_drain_refuses_while_any_class_route_variable_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    api.route_variables_present.add("LOOM_CI_IMAGE_RUNS_ON")

    with pytest.raises(pool.PoolOperationError, match="LOOM_CI_IMAGE_RUNS_ON"):
        pool.drain(profile, execute=True, api=api, runner=runner)

    assert runner.containers
    assert not api.deleted


def test_drain_refuses_while_dynamic_route_mode_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    api.route_variables_present.add(pool.ROUTING_MODE_VARIABLE)

    with pytest.raises(pool.PoolOperationError, match=pool.ROUTING_MODE_VARIABLE):
        pool.drain(profile, execute=True, api=api, runner=runner)

    assert runner.containers
    assert not api.deleted


def test_drain_preserves_busy_runner_and_removes_idle_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    states = pool._existing_states(profile)
    api.runners[states[0].runner_id]["busy"] = True

    result = pool.drain(profile, execute=True, api=api, runner=runner)

    assert result["status"] == "draining"
    assert result["busy_slots"] == [0]
    assert result["removed_slots"] == [1]
    assert states[0].runner_id not in api.deleted
    assert states[1].runner_id in api.deleted
    assert (profile.state_root / "slot-00").exists()
    assert not (profile.state_root / "slot-01").exists()


def test_status_is_secret_safe_and_reports_online_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path, slots=1)
    _seal_golden(profile)
    _allow_execute(monkeypatch)
    runner = FakeRunner()
    api = FakeAPI()
    pool.reconcile(
        profile,
        candidate_sha=CANDIDATE_SHA,
        execute=True,
        api=api,
        runner=runner,
    )
    state = next(iter(pool._existing_states(profile).values()))
    api.runners[state.runner_id]["status"] = "online"

    report = pool.status(profile, api=api, runner=runner)

    assert report["healthy"] is True
    assert report["ready_slots"] == 1
    assert report["work_classes"]["normal"]["ready_slots"] == 1
    assert report["work_classes"]["image"]["ready_slots"] == 0
    assert report["slots"][0]["work_class"] == "normal"
    assert report["slots"][0]["labels_match"] is True
    assert report["routing_variables_present"] == {
        name: False for name in pool.ROUTING_VARIABLES
    }
    assert "opaque-jit" not in json.dumps(report)


def test_systemd_unit_uses_credential_and_candidate_sources() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    service = (
        repo_root / "deploy/ci-runners/loom-ci-runner-pool.service"
    ).read_text(encoding="utf-8")

    assert "LoadCredential=github-token:" in service
    assert "${CREDENTIALS_DIRECTORY}/github-token" in service
    assert "${LOOM_CI_RUNNER_CANDIDATE_SHA}" in service
    assert "Environment=GITHUB_TOKEN" not in service
    assert "ProtectSystem=strict" in service

    slice_unit = (
        repo_root / "deploy/ci-runners/loom-ci-runner-pool.slice"
    ).read_text(encoding="utf-8")
    assert "CPUQuota=2200%" in slice_unit
    assert "MemoryMax=80G" in slice_unit
