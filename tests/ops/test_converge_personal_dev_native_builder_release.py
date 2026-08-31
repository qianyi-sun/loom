from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

import pytest
from scripts.ops.converge_personal_dev_native_builder_release import (
    NativeBuilderImageRecord,
    NativeBuilderReleaseConfig,
    NativeBuilderReleaseImage,
    PersonalDevNativeBuilderReleaseConverger,
    PersonalDevNativeBuilderReleaseError,
    main,
)

_SOURCE = "https://github.com/qianyi-sun/loom"
_MANAGED = "io.loom.personal-dev.native-builder.release-managed"
_AGENT_REPOSITORY = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
)
_BUILDER_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-builder"
_PRIMARY = "unix:///var/run/docker.sock"
_DEDICATED = "unix:///run/loom-personal-dev-builder/docker.sock"


def _reference(repository: str, value: str) -> str:
    return f"{repository}@sha256:{value * 64}"


def _release_image(
    repository: str,
    value: str,
    *,
    revision_value: str | None = None,
) -> NativeBuilderReleaseImage:
    return NativeBuilderReleaseImage(
        reference=_reference(repository, value),
        revision=(revision_value or value) * 40,
    )


def _config(*, previous: bool = True) -> NativeBuilderReleaseConfig:
    return NativeBuilderReleaseConfig(
        current_agent=_release_image(
            _AGENT_REPOSITORY,
            "a",
            revision_value="1",
        ),
        current_builder=_release_image(
            _BUILDER_REPOSITORY,
            "b",
            revision_value="1",
        ),
        previous_agent=(
            _release_image(
                _AGENT_REPOSITORY,
                "c",
                revision_value="2",
            )
            if previous
            else None
        ),
        previous_builder=(
            _release_image(
                _BUILDER_REPOSITORY,
                "d",
                revision_value="2",
            )
            if previous
            else None
        ),
    )


def _record(
    release: NativeBuilderReleaseImage,
    *,
    image_id_value: str | None = None,
    architecture: str = "arm64",
    source: str = _SOURCE,
    managed: str = "true",
) -> NativeBuilderImageRecord:
    digest_value = release.reference.rsplit(":", 1)[1]
    return NativeBuilderImageRecord(
        image_id="sha256:" + (image_id_value or digest_value),
        repo_digests=(release.reference,),
        os="linux",
        architecture=architecture,
        labels={
            "org.opencontainers.image.source": source,
            "org.opencontainers.image.revision": release.revision,
            _MANAGED: managed,
        },
    )


@dataclass(slots=True)
class FakeDockerApi:
    endpoint: str
    records: list[NativeBuilderImageRecord] = field(default_factory=list)
    pull_records: dict[str, NativeBuilderImageRecord] = field(default_factory=dict)
    container_results: dict[str, list[tuple[str, ...]]] = field(
        default_factory=dict
    )
    operations: list[tuple[str, ...]] = field(default_factory=list)

    def images(self) -> tuple[NativeBuilderImageRecord, ...]:
        self.operations.append(("images", self.endpoint))
        return tuple(self.records)

    def pull(self, reference: str) -> None:
        self.operations.append(("pull", self.endpoint, reference))
        try:
            record = self.pull_records[reference]
        except KeyError as exc:
            raise AssertionError(f"unexpected pull: {reference}") from exc
        if all(reference not in item.repo_digests for item in self.records):
            self.records.append(record)

    def containers_using(self, image_id: str) -> tuple[str, ...]:
        self.operations.append(("containers", self.endpoint, image_id))
        results = self.container_results.get(image_id)
        if not results:
            return ()
        if len(results) == 1:
            return results[0]
        return results.pop(0)

    def remove(self, reference: str) -> None:
        self.operations.append(("remove", self.endpoint, reference))
        for index, record in enumerate(self.records):
            if reference not in record.repo_digests:
                continue
            remaining = tuple(
                value for value in record.repo_digests if value != reference
            )
            if remaining:
                self.records[index] = replace(record, repo_digests=remaining)
            else:
                self.records.pop(index)
            return
        raise AssertionError(f"unexpected removal: {reference}")


def _converger(
    config: NativeBuilderReleaseConfig,
    primary: FakeDockerApi,
    dedicated: FakeDockerApi,
) -> PersonalDevNativeBuilderReleaseConverger:
    return PersonalDevNativeBuilderReleaseConverger(
        config=config,
        primary=primary,
        dedicated=dedicated,
    )


def test_plan_is_canonical_read_only_and_uses_separate_daemons() -> None:
    config = _config()
    primary = FakeDockerApi(
        _PRIMARY,
        records=[_record(config.current_agent), _record(config.previous_agent)],
    )
    dedicated = FakeDockerApi(
        _DEDICATED,
        records=[
            _record(config.current_builder),
            _record(config.previous_builder),
        ],
    )

    plan = _converger(config, primary, dedicated).plan()

    assert plan == {
        "dedicated": {
            "endpoint": _DEDICATED,
            "pull": [],
            "remove": [],
            "retain": [
                config.current_builder.reference,
                config.previous_builder.reference,
            ],
        },
        "operation": "plan",
        "primary": {
            "endpoint": _PRIMARY,
            "pull": [],
            "remove": [],
            "retain": [
                config.current_agent.reference,
                config.previous_agent.reference,
            ],
        },
        "schema": "loom.personal-dev-native-builder-release-convergence.v1",
    }
    assert all(operation[0] == "images" for operation in primary.operations)
    assert all(operation[0] == "images" for operation in dedicated.operations)
    assert json.dumps(plan, sort_keys=True, separators=(",", ":"))


def test_apply_pulls_missing_current_and_optional_previous_then_verifies() -> None:
    config = _config()
    primary = FakeDockerApi(
        _PRIMARY,
        pull_records={
            config.current_agent.reference: _record(config.current_agent),
            config.previous_agent.reference: _record(config.previous_agent),
        },
    )
    dedicated = FakeDockerApi(
        _DEDICATED,
        pull_records={
            config.current_builder.reference: _record(config.current_builder),
            config.previous_builder.reference: _record(config.previous_builder),
        },
    )

    receipt = _converger(config, primary, dedicated).apply()

    assert receipt["operation"] == "apply"
    assert receipt["state"] == "converged"
    assert [item[2] for item in primary.operations if item[0] == "pull"] == [
        config.current_agent.reference,
        config.previous_agent.reference,
    ]
    assert [item[2] for item in dedicated.operations if item[0] == "pull"] == [
        config.current_builder.reference,
        config.previous_builder.reference,
    ]
    assert not [item for item in primary.operations if item[0] == "remove"]
    assert not [item for item in dedicated.operations if item[0] == "remove"]


def test_apply_removes_only_older_exact_managed_repository_digest() -> None:
    config = _config(previous=False)
    old_agent = _release_image(_AGENT_REPOSITORY, "e")
    old_builder = _release_image(_BUILDER_REPOSITORY, "f")
    unrelated = NativeBuilderImageRecord(
        image_id="sha256:" + "9" * 64,
        repo_digests=("example.invalid/unrelated@sha256:" + "9" * 64,),
        os="linux",
        architecture="arm64",
        labels={},
    )
    primary = FakeDockerApi(
        _PRIMARY,
        records=[_record(config.current_agent), _record(old_agent), unrelated],
    )
    dedicated = FakeDockerApi(
        _DEDICATED,
        records=[_record(config.current_builder), _record(old_builder)],
    )

    _converger(config, primary, dedicated).apply()

    for api, old in ((primary, old_agent), (dedicated, old_builder)):
        old_id = "sha256:" + old.reference.rsplit(":", 1)[1]
        assert [item for item in api.operations if item[0] == "containers"] == [
            ("containers", api.endpoint, old_id),
            ("containers", api.endpoint, old_id),
        ]
        assert [item for item in api.operations if item[0] == "remove"] == [
            ("remove", api.endpoint, old.reference)
        ]
    assert unrelated in primary.records
    assert not any("prune" in value for call in primary.operations for value in call)
    assert not any("prune" in value for call in dedicated.operations for value in call)
    assert not any(
        forbidden in value
        for call in (*primary.operations, *dedicated.operations)
        for value in call
        for forbidden in ("slurm", "task", "capacity")
    )


def test_apply_refuses_image_that_becomes_used_before_deletion() -> None:
    config = _config(previous=False)
    old = _release_image(_AGENT_REPOSITORY, "e")
    old_record = _record(old)
    primary = FakeDockerApi(
        _PRIMARY,
        records=[_record(config.current_agent), old_record],
        container_results={old_record.image_id: [(), ("new-container",)]},
    )
    dedicated = FakeDockerApi(
        _DEDICATED,
        records=[_record(config.current_builder)],
    )

    with pytest.raises(PersonalDevNativeBuilderReleaseError, match="image_in_use"):
        _converger(config, primary, dedicated).apply()

    assert not [item for item in primary.operations if item[0] == "remove"]


def test_apply_validates_both_daemons_before_any_optional_deletion() -> None:
    config = _config(previous=False)
    old = _release_image(_AGENT_REPOSITORY, "e")
    primary = FakeDockerApi(
        _PRIMARY,
        records=[_record(config.current_agent), _record(old)],
    )
    dedicated = FakeDockerApi(_DEDICATED)
    converger = _converger(config, primary, dedicated)

    with pytest.raises(AssertionError, match="unexpected pull"):
        converger.apply()

    assert old.reference in {
        reference for record in primary.records for reference in record.repo_digests
    }
    assert not [item for item in primary.operations if item[0] == "remove"]


@pytest.mark.parametrize(
    "record",
    [
        _record(_release_image(_AGENT_REPOSITORY, "a"), architecture="amd64"),
        _record(
            _release_image(_AGENT_REPOSITORY, "a"),
            source="https://example.invalid/foreign",
        ),
        _record(_release_image(_AGENT_REPOSITORY, "a"), managed="false"),
        replace(
            _record(_release_image(_AGENT_REPOSITORY, "a")),
            labels={
                "org.opencontainers.image.source": _SOURCE,
                "org.opencontainers.image.revision": "f" * 40,
                _MANAGED: "true",
            },
        ),
    ],
)
def test_plan_rejects_wrong_platform_source_managed_or_revision(
    record: NativeBuilderImageRecord,
) -> None:
    config = _config(previous=False)
    primary = FakeDockerApi(_PRIMARY, records=[record])
    dedicated = FakeDockerApi(
        _DEDICATED,
        records=[_record(config.current_builder)],
    )

    with pytest.raises(PersonalDevNativeBuilderReleaseError):
        _converger(config, primary, dedicated).plan()


@pytest.mark.parametrize(
    "change",
    (
        "mutable",
        "agent-repository",
        "builder-repository",
        "unpaired-previous",
        "revision-mismatch",
    ),
)
def test_config_rejects_mutable_wrong_repository_or_unpaired_previous(
    change: str,
) -> None:
    config = _config()
    if change == "mutable":
        config = replace(
            config,
            current_agent=replace(config.current_agent, reference="repo:latest"),
        )
    elif change == "agent-repository":
        config = replace(
            config,
            current_agent=_release_image("example.invalid/wrong", "a"),
        )
    elif change == "builder-repository":
        config = replace(
            config,
            current_builder=_release_image("example.invalid/wrong", "b"),
        )
    elif change == "unpaired-previous":
        config = replace(config, previous_builder=None)
    else:
        config = replace(
            config,
            current_builder=replace(
                config.current_builder,
                revision="f" * 40,
            ),
        )

    with pytest.raises(PersonalDevNativeBuilderReleaseError):
        PersonalDevNativeBuilderReleaseConverger(
            config=config,
            primary=FakeDockerApi(_PRIMARY),
            dedicated=FakeDockerApi(_DEDICATED),
        )


def test_verify_requires_current_and_retains_no_unapproved_older_image() -> None:
    config = _config(previous=False)
    primary = FakeDockerApi(_PRIMARY)
    dedicated = FakeDockerApi(
        _DEDICATED,
        records=[_record(config.current_builder)],
    )
    converger = _converger(config, primary, dedicated)

    with pytest.raises(PersonalDevNativeBuilderReleaseError, match="missing"):
        converger.verify()

    primary.records.append(_record(config.current_agent))
    primary.records.append(_record(_release_image(_AGENT_REPOSITORY, "e")))
    with pytest.raises(PersonalDevNativeBuilderReleaseError, match="retention"):
        converger.verify()


def test_cli_emits_canonical_receipt_and_requires_complete_release_arguments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeConverger:
        def verify(self) -> dict[str, object]:
            return {"z": 1, "a": "safe"}

    config = _config(previous=False)
    argv = [
        "verify",
        "--current-agent",
        config.current_agent.reference,
        "--current-builder",
        config.current_builder.reference,
        "--current-revision",
        config.current_agent.revision,
    ]
    assert main(argv, converger_factory=lambda release: FakeConverger()) == 0
    assert capsys.readouterr().out == '{"a":"safe","z":1}\n'

    assert main(
        [
            *argv,
            "--previous-agent",
            _reference(_AGENT_REPOSITORY, "c"),
        ],
        converger_factory=lambda release: FakeConverger(),
    ) == 2
    assert capsys.readouterr().err == "error:arguments_invalid\n"
