"""Journaled exact production-default convergence for protected staging apply."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    ProviderPricingDefault,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_MAX_ARTIFACT_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_TOKEN_BYTES = 64 * 1024
_QUERY_TIMEOUT_SECONDS = 30.0
_REQUEST_TIMEOUT_SECONDS = 60.0
_ADMIN_ACTOR = "rollout-production-defaults"
_PROVIDER_PATH_RE = re.compile(
    r"^/api/v1/provider-connections/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_READ_SQL = """
SELECT jsonb_build_object(
  'rate_cards', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'id', id,
      'source_url', "table"->>'source_url',
      'group', "table"->>'group'
    ) ORDER BY captured_at DESC, id)
    FROM rate_cards
    WHERE "table"->>'provider' = 'yibuapi'
  ), '[]'::jsonb)
)::text;
""".strip()
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    json.dumps(
        {
            "admin_actor": _ADMIN_ACTOR,
            "read_sql": _READ_SQL,
            "provider_list_path": "/api/v1/provider-connections",
            "sync_path": "/api/v1/rate-cards/sync/yibuapi",
            "update_path": "/api/v1/provider-connections/{id}",
            "version": "v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


class ProtectedProductionDefaultsCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


class ProductionDefaultsTransport(Protocol):
    def __call__(
        self,
        *,
        base_url: str,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, object] | None,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> tuple[int, bytes]: ...


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class HttpxProductionDefaultsTransport:
    """Fixed HTTPS transport that never exposes the service token in argv."""

    def __call__(
        self,
        *,
        base_url: str,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, object] | None,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> tuple[int, bytes]:
        if (
            base_url != "https://yylx.world/dev"
            or not (
                (method == "GET" and path == "/api/v1/provider-connections")
                or (method == "POST" and path == "/api/v1/rate-cards/sync/yibuapi")
                or (method == "PATCH" and _PROVIDER_PATH_RE.fullmatch(path) is not None)
            )
            or not path.startswith("/")
            or "?" in path
            or set(headers) != {"X-Loom-Admin-Actor"}
            or headers["X-Loom-Admin-Actor"] != _ADMIN_ACTOR
            or not token
            or not _request_payload_valid(method, payload)
            or not 0 < timeout_seconds <= _REQUEST_TIMEOUT_SECONDS
        ):
            raise ValueError("production defaults transport authority is invalid")
        try:
            with httpx.Client(
                base_url=base_url,
                timeout=timeout_seconds,
                follow_redirects=False,
                trust_env=False,
                headers={"Authorization": f"Bearer {token}"},
            ) as client:
                response = client.request(
                    method,
                    path,
                    json=None if payload is None else dict(payload),
                    headers=dict(headers),
                )
        except httpx.HTTPError as exc:
            raise RuntimeError("production defaults request failed safely") from exc
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise RuntimeError("production defaults response exceeded its bound")
        return response.status_code, response.content


@dataclass(frozen=True, slots=True)
class KubernetesProtectedProductionDefaultsComponent:
    """Converge only the exact preflight-published defaults artifact."""

    runner: ProtectedProductionDefaultsCommandRunner
    environment: Mapping[str, str]
    service_uid: int
    epoch_guard: EpochGuard
    request: ProductionDefaultsTransport

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or "KUBECONFIG" not in self.environment
            or not callable(self.epoch_guard)
            or not callable(self.request)
        ):
            raise ValueError("protected production defaults authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="production-defaults",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "artifact_sha256": plan.production_defaults_sha256,
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "route": plan.route,
                    "service_token_metadata": plan.secret_metadata_fingerprints.get("service"),
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(plan, ComponentState.DRIFTED, epoch.evidence_digest, {})
        artifact = self._read_artifact(plan)
        if artifact.yibuapi_sync is None and not artifact.providers:
            return self._observation(
                plan,
                ComponentState.EXACT,
                epoch.evidence_digest,
                {"providers": {}, "rate_card_exact": True},
            )
        token = self._read_token(plan)
        relevant, state = _classify_inventory(artifact, self._read_inventory(plan, token))
        return self._observation(plan, state, epoch.evidence_digest, relevant)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("production defaults epoch ownership changed before apply")
        artifact = self._read_artifact(plan)
        if artifact.yibuapi_sync is None and not artifact.providers:
            raise RuntimeError("production defaults state changed before apply")
        token = self._read_token(plan)
        relevant, state = _classify_inventory(artifact, self._read_inventory(plan, token))
        if state is not ComponentState.READY:
            raise RuntimeError("production defaults state changed before apply")

        if artifact.yibuapi_sync is not None and not bool(relevant["rate_card_exact"]):
            self._expect_json(
                plan,
                token,
                method="POST",
                path="/api/v1/rate-cards/sync/yibuapi",
                payload=dict(artifact.yibuapi_sync),
            )
        providers = relevant["providers"]
        assert isinstance(providers, dict)
        for desired in artifact.providers:
            observed = providers[desired.name]
            assert isinstance(observed, dict)
            if _provider_exact(desired, observed):
                continue
            provider_id = observed["id"]
            assert isinstance(provider_id, str)
            payload: dict[str, object] = {"pricing_source": desired.pricing_source}
            if desired.rate_card_provider is not None:
                payload["rate_card_provider"] = desired.rate_card_provider
            response = self._expect_json(
                plan,
                token,
                method="PATCH",
                path=f"/api/v1/provider-connections/{provider_id}",
                payload=payload,
            )
            if not _provider_exact(desired, response):
                raise RuntimeError("production defaults provider did not converge exactly")

    def _read_artifact(self, plan: FinalGatePlan) -> ProductionDefaultsArtifact:
        trusted = read_trusted_file(
            Path(plan.production_defaults_path),
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_ARTIFACT_BYTES,
            require_nonempty=True,
        )
        artifact = ProductionDefaultsArtifact.from_bytes(trusted.payload)
        if (
            artifact.artifact_digest != plan.production_defaults_sha256
            or artifact.candidate_sha != plan.candidate_sha
            or artifact.candidate_tree != plan.candidate_tree
            or artifact.environment != plan.environment
        ):
            raise ValueError("production defaults artifact content drifted")
        return artifact

    def _read_token(self, plan: FinalGatePlan) -> str:
        path = Path(plan.service_token_source.removeprefix("file:"))
        trusted = read_trusted_file(
            path,
            service_uid=self.service_uid,
            private=True,
            allow_qianyi_owner=True,
            max_bytes=_MAX_TOKEN_BYTES,
            require_nonempty=True,
        )
        if (
            plan.secret_metadata_fingerprints.get("service")
            != f"sha256:{trusted.metadata_fingerprint}"
        ):
            raise ValueError("production defaults service token metadata drifted")
        try:
            token = trusted.payload.strip().decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("production defaults service token is invalid") from exc
        if not token or any(character.isspace() for character in token):
            raise ValueError("production defaults service token is invalid")
        return token

    def _read_inventory(self, plan: FinalGatePlan, token: str) -> Mapping[str, object]:
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                "loom-staging",
                "exec",
                "statefulset/loom-postgres",
                "--",
                "sh",
                "-ceu",
                'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                _READ_SQL,
            ),
            env=self.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        try:
            value = json.loads(payload.decode("utf-8").strip())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("production defaults inventory returned invalid JSON") from exc
        if not isinstance(value, dict) or set(value) != {"rate_cards"}:
            raise ValueError("production defaults inventory shape is invalid")
        provider_response = self._expect_json(
            plan,
            token,
            method="GET",
            path="/api/v1/provider-connections",
            payload=None,
        )
        provider_items = provider_response.get("items")
        if not isinstance(provider_items, list):
            raise ValueError("production defaults provider inventory is invalid")
        return {
            "providers": [
                {
                    "id": item.get("id") if isinstance(item, dict) else None,
                    "name": item.get("name") if isinstance(item, dict) else None,
                    "pricing_source": (
                        item.get("pricing_source") if isinstance(item, dict) else None
                    ),
                    "rate_card_provider": (
                        item.get("rate_card_provider") if isinstance(item, dict) else None
                    ),
                }
                for item in provider_items
            ],
            "rate_cards": value["rate_cards"],
        }

    def _expect_json(
        self,
        plan: FinalGatePlan,
        token: str,
        *,
        method: str,
        path: str,
        payload: Mapping[str, object] | None,
    ) -> Mapping[str, object]:
        status, body = self.request(
            base_url=plan.route,
            method=method,
            path=path,
            token=token,
            payload=payload,
            headers={"X-Loom-Admin-Actor": _ADMIN_ACTOR},
            timeout_seconds=_REQUEST_TIMEOUT_SECONDS,
        )
        if status not in {200, 201} or len(body) > _MAX_RESPONSE_BYTES:
            raise RuntimeError("production defaults request was rejected")
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("production defaults response was invalid") from exc
        if not isinstance(value, dict):
            raise RuntimeError("production defaults response was invalid")
        return value

    @staticmethod
    def _observation(
        plan: FinalGatePlan,
        state: ComponentState,
        epoch_evidence_digest: str,
        relevant: Mapping[str, object],
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "artifact_sha256": plan.production_defaults_sha256,
                    "epoch_evidence_digest": epoch_evidence_digest,
                    "relevant_inventory": relevant,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _classify_inventory(
    artifact: ProductionDefaultsArtifact,
    inventory: Mapping[str, object],
) -> tuple[dict[str, object], ComponentState]:
    providers = inventory.get("providers")
    rate_cards = inventory.get("rate_cards")
    if not isinstance(providers, list) or not isinstance(rate_cards, list):
        raise ValueError("production defaults inventory lists are invalid")
    indexed: dict[str, list[dict[str, object]]] = {}
    for row in providers:
        parsed = _provider_row(row)
        indexed.setdefault(str(parsed["name"]), []).append(parsed)
    selected: dict[str, dict[str, object]] = {}
    for desired in artifact.providers:
        matches = indexed.get(desired.name, [])
        if len(matches) != 1:
            return ({"providers": {}, "rate_card_exact": False}, ComponentState.DRIFTED)
        selected[desired.name] = matches[0]

    rate_card_exact = artifact.yibuapi_sync is None
    if artifact.yibuapi_sync is not None:
        parsed_cards = tuple(_rate_card_row(row) for row in rate_cards)
        rate_card_exact = bool(
            parsed_cards
            and parsed_cards[0]["group"] == artifact.yibuapi_sync["group"]
            and parsed_cards[0]["source_url"] == artifact.yibuapi_sync["source_url"]
        )
    providers_exact = all(
        _provider_exact(desired, selected[desired.name]) for desired in artifact.providers
    )
    relevant: dict[str, object] = {
        "providers": {name: selected[name] for name in sorted(selected)},
        "rate_card_exact": rate_card_exact,
    }
    state = ComponentState.EXACT if providers_exact and rate_card_exact else ComponentState.READY
    return relevant, state


def _provider_row(value: object) -> dict[str, object]:
    if (
        not isinstance(value, dict)
        or set(value) != {"id", "name", "pricing_source", "rate_card_provider"}
        or not all(isinstance(value[key], str) and value[key] for key in ("id", "name"))
        or _PROVIDER_PATH_RE.fullmatch(f"/api/v1/provider-connections/{value['id']}") is None
        or value["pricing_source"] not in {"rate-card", "tokens-only", "operator-supplied"}
        or (
            value["rate_card_provider"] is not None
            and (
                not isinstance(value["rate_card_provider"], str) or not value["rate_card_provider"]
            )
        )
    ):
        raise ValueError("production defaults provider inventory is invalid")
    return dict(value)


def _rate_card_row(value: object) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != {"id", "source_url", "group"}
        or not all(isinstance(value[key], str) and value[key] for key in value)
    ):
        raise ValueError("production defaults rate-card inventory is invalid")
    return {key: str(value[key]) for key in value}


def _provider_exact(
    desired: ProviderPricingDefault,
    observed: Mapping[str, object],
) -> bool:
    return bool(
        observed.get("name") == desired.name
        and observed.get("pricing_source") == desired.pricing_source
        and (
            desired.rate_card_provider is None
            or observed.get("rate_card_provider") == desired.rate_card_provider
        )
    )


def _request_payload_valid(
    method: str,
    payload: Mapping[str, object] | None,
) -> bool:
    if method == "GET":
        return payload is None
    if payload is None:
        return False
    if method == "POST":
        return bool(
            set(payload) == {"group", "source_url"}
            and all(isinstance(value, str) and value for value in payload.values())
        )
    if method == "PATCH":
        if set(payload) not in (
            {"pricing_source"},
            {"pricing_source", "rate_card_provider"},
        ):
            return False
        return bool(
            payload.get("pricing_source") in {"rate-card", "tokens-only"}
            and (
                "rate_card_provider" not in payload
                or (
                    isinstance(payload["rate_card_provider"], str)
                    and bool(payload["rate_card_provider"])
                )
            )
        )
    return False


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "HttpxProductionDefaultsTransport",
    "KubernetesProtectedProductionDefaultsComponent",
    "ProductionDefaultsTransport",
    "ProtectedProductionDefaultsCommandRunner",
]
