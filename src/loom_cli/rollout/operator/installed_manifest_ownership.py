"""Installed exact-candidate composition for manifest ownership maintenance."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.manifest_ownership_adoption import (
    managed_fields_cleanup_argv,
    ownership_adoption_argv,
    ownership_manifest_identities,
)
from loom_cli.rollout.manifest_ownership_journal import ManifestOwnershipJournal
from loom_cli.rollout.manifest_ownership_operator import ManifestOwnershipOperator
from loom_cli.rollout.operator.manifest_apply_contract import server_side_apply_argv
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore

from .config import OperatorConfig, candidate_sha_from_runner_repo, environment_authority
from .installed_preflight_commands import InstalledPreflightCommands
from .manifest_ownership_epoch import ManifestOwnershipEpochClaimer
from .model import CandidateBinding
from .policy import sanitized_child_environment
from .protected_apply_executor import SubprocessProtectedApplyCommandRunner

Resource = Mapping[str, object]
_MAX_DOCUMENTS = 512
_MAX_YAML_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class InstalledManifestOwnershipService:
    config: OperatorConfig
    service_uid: int
    read_mutation_epoch: Callable[[], int]

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
        ):
            raise ValueError("installed ownership maintenance authority is invalid")

    def inventory(self, candidate: CandidateBinding) -> dict[str, object]:
        return self._operator(candidate).inventory().to_document()

    def apply(
        self,
        candidate: CandidateBinding,
        *,
        request_id: str,
        approved_inventory_sha256: str,
    ) -> dict[str, object]:
        return self._operator(candidate).apply(
            request_id=request_id,
            approved_inventory_sha256=approved_inventory_sha256,
        )

    def _operator(self, candidate: CandidateBinding) -> ManifestOwnershipOperator:
        # Authority: an explicit (version, policy) gate, not the source-mode label
        # (#1085 phase 3). Integrity always comes from *exactness* below — the
        # operator only ever runs against the exact installer-pinned candidate.
        if not self.config.ownership_maintenance_permitted():
            raise ValueError("ownership maintenance is not permitted for this runner")
        if candidate.source_mode != self.config.source_mode or candidate.resolved_tree is None:
            raise ValueError("ownership maintenance candidate does not match the runner source")
        if self.config.source_mode == "sealed-cumulative":
            # Sealed identity: the candidate must equal the independently reviewed
            # frozen source pinned in the config (unchanged from the prior gate).
            if (
                candidate.resolved_sha != self.config.source_commit_sha
                or candidate.resolved_tree != self.config.source_tree_sha
            ):
                raise ValueError("ownership maintenance candidate is not the exact sealed source")
        else:
            # Non-sealed: the pinned version is the installer-published candidate
            # encoded in runner_repo (<candidate-runtime-root>/<sha>/repo), and the
            # candidate binding derives its sha from that same path — so exactness
            # here means the candidate is precisely the runner's installed source.
            expected_sha = candidate_sha_from_runner_repo(
                self.config.runner_repo,
                authority=environment_authority(self.config.short_name),
            )
            if candidate.resolved_sha != expected_sha:
                raise ValueError("ownership maintenance candidate is not the exact pinned source")
        epoch = self.read_mutation_epoch()
        if epoch < 0:
            raise ValueError("ownership maintenance mutation epoch is invalid")
        commands = InstalledPreflightCommands(
            self.config,
            sanitized_child_environment(self.config, service_uid=self.service_uid),
        )
        artifacts = PreflightArtifactStore(
            self.config.state_root,
            service_uid=self.service_uid,
        ).load_exact(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            mutation_epoch=epoch,
            image_tag=candidate.image_tag,
            namespace=self.config.namespace,
            image_run=commands.image,
            container_registry_push=str(
                load_cluster_config(self.config.cluster_config_path).container_registry_push
                if self.config.cluster_config_path.is_file()
                else ""
            ),
        )
        runner = SubprocessProtectedApplyCommandRunner(max_output_bytes=_MAX_YAML_BYTES)
        environment = runner.environment

        def read_exact_epoch() -> int:
            observed = self.read_mutation_epoch()
            if observed != epoch:
                raise ValueError("ownership maintenance mutation epoch drifted")
            return observed

        def load_live() -> tuple[Resource, ...]:
            resources: list[Resource] = []
            for identity in ownership_manifest_identities(
                artifacts.manifests.rendered_yaml,
                namespace=self.config.namespace,
            ):
                _api_version, kind, namespace, name = identity.split("|", 3)
                if namespace not in {"", self.config.namespace}:
                    raise ValueError("ownership maintenance target namespace drifted")
                argv = ["kubectl", "--kubeconfig", str(self.config.kubeconfig_path)]
                if namespace:
                    argv.extend(("--namespace", namespace))
                argv.extend(
                    (
                        "get",
                        f"{kind.lower()}/{name}",
                        "--ignore-not-found=true",
                        "--show-managed-fields=true",
                        "--output=json",
                        "--request-timeout=60s",
                    )
                )
                payload = runner.capture_stdout(
                    argv,
                    env=environment,
                    timeout_seconds=60,
                )
                if payload.strip():
                    resources.append(_decode_resource(payload))
            return tuple(resources)

        def action(payload: str, *, force: bool, dry_run: bool) -> tuple[Resource, ...]:
            documents = _parse_documents(payload)
            results: list[Resource] = []
            for document in documents:
                rendered = cast(str, yaml.safe_dump(document, sort_keys=True))
                argv = (
                    ownership_adoption_argv(
                        kubeconfig=self.config.kubeconfig_path,
                        dry_run=dry_run,
                        output_json=True,
                    )
                    if force
                    else server_side_apply_argv(
                        self.config.namespace,
                        kubeconfig=self.config.kubeconfig_path,
                        dry_run=dry_run,
                        output_json=True,
                    )
                )
                results.append(
                    _decode_resource(
                        runner.capture_stdout_with_input(
                            argv,
                            env=environment,
                            input_payload=rendered.encode(),
                            timeout_seconds=120,
                        )
                    )
                )
            return tuple(results)

        def managed_fields_action(
            identity: str,
            patch_json: str,
            dry_run: bool,
        ) -> Resource:
            argv = (
                *managed_fields_cleanup_argv(
                    identity=identity,
                    kubeconfig=self.config.kubeconfig_path,
                    dry_run=dry_run,
                ),
                patch_json,
            )
            return _decode_resource(
                runner.capture_stdout(
                    argv,
                    env=environment,
                    timeout_seconds=60,
                )
            )

        return ManifestOwnershipOperator(
            artifact=artifacts.manifests,
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree,
            read_mutation_epoch=read_exact_epoch,
            load_live=load_live,
            force_dry_run=lambda payload: action(payload, force=True, dry_run=True),
            force_apply=lambda payload: action(payload, force=True, dry_run=False),
            managed_fields_action=managed_fields_action,
            no_force_dry_run=lambda payload: action(payload, force=False, dry_run=True),
            no_force_apply=lambda payload: action(payload, force=False, dry_run=False),
            claim_epoch=ManifestOwnershipEpochClaimer(
                runner=runner,
                environment=environment,
            ),
            journal=ManifestOwnershipJournal(
                self.config.state_root,
                service_uid=self.service_uid,
            ),
            now=lambda: datetime.now(UTC),
        )


def _parse_documents(payload: str) -> tuple[dict[str, object], ...]:
    if not payload or len(payload.encode()) > _MAX_YAML_BYTES:
        raise ValueError("ownership maintenance manifest payload is invalid")
    try:
        documents = tuple(item for item in yaml.safe_load_all(payload) if item is not None)
    except yaml.YAMLError as exc:
        raise ValueError("ownership maintenance manifest YAML is invalid") from exc
    if not 1 <= len(documents) <= _MAX_DOCUMENTS or any(
        not isinstance(item, dict) for item in documents
    ):
        raise ValueError("ownership maintenance manifest set is invalid")
    return cast(tuple[dict[str, object], ...], documents)


def _decode_resource(payload: bytes) -> Resource:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("ownership maintenance Kubernetes output is invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeError("ownership maintenance Kubernetes resource is invalid")
    return value


__all__ = ["InstalledManifestOwnershipService"]
