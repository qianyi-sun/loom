from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_production_defaults_component import (
    HttpxProductionDefaultsTransport,
    KubernetesProtectedProductionDefaultsComponent,
)
from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    ProviderPricingDefault,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
)


class Runner:
    def __init__(self, inventory: dict[str, object]) -> None:
        self.inventory = inventory
        self.calls = 0

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 30.0
        assert "FROM rate_cards" in " ".join(argv)
        assert "FROM provider_connections" not in " ".join(argv)
        self.calls += 1
        return json.dumps({"rate_cards": self.inventory["rate_cards"]}).encode()


class Transport:
    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def __call__(
        self,
        *,
        base_url,
        method,
        path,
        token,
        payload,
        headers,
        timeout_seconds,
    ):
        assert base_url == "https://yylx.world/dev"
        assert token == "service-secret"
        assert headers == {"X-Loom-Admin-Actor": "rollout-production-defaults"}
        assert timeout_seconds == 60.0
        body = {} if payload is None else dict(payload)
        self.calls.append((method, path, body))
        if method == "GET":
            return 200, json.dumps({"items": self.runner.inventory["providers"]}).encode()
        if path == "/api/v1/rate-cards/sync/yibuapi":
            self.runner.inventory["rate_cards"] = [
                {
                    "id": "yibuapi-v1",
                    "group": body["group"],
                    "source_url": body["source_url"],
                }
            ]
            return 201, b'{"id":"yibuapi-v1"}'
        provider_id = path.rsplit("/", 1)[-1]
        providers = self.runner.inventory["providers"]
        assert isinstance(providers, list)
        provider = next(item for item in providers if item["id"] == provider_id)
        provider.update(body)
        return 200, json.dumps(provider).encode()


def _artifact() -> ProductionDefaultsArtifact:
    providers = (
        ProviderPricingDefault(
            name="hosted-openai",
            pricing_source="rate-card",
            rate_card_provider="yibuapi",
            required=True,
        ),
    )
    payload = {
        "schema_version": 1,
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "environment": "staging",
        "yibuapi_sync": {
            "group": "default",
            "source_url": "https://rates.example.invalid/catalog.json",
        },
        "providers": [provider.to_dict() for provider in providers],
    }
    digest = (
        __import__("hashlib")
        .sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        .hexdigest()
    )
    return ProductionDefaultsArtifact(
        schema_version=1,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        environment="staging",
        yibuapi_sync=payload["yibuapi_sync"],
        providers=providers,
        artifact_digest=digest,
    )


def _plan(tmp_path: Path):
    plan = _published_plan(tmp_path)
    artifact = _artifact()
    artifact_path = Path(plan.production_defaults_path)
    artifact_path.write_bytes(artifact.to_bytes())
    artifact_path.chmod(0o600)
    token_path = tmp_path / "service-token"
    token_path.write_text("service-secret\n")
    token_path.chmod(0o600)
    token = read_trusted_file(
        token_path,
        service_uid=os.geteuid(),
        private=True,
        require_nonempty=True,
    )
    return replace(
        plan,
        production_defaults_sha256=artifact.artifact_digest,
        service_token_source=f"file:{token_path}",
        secret_metadata_fingerprints={
            **plan.secret_metadata_fingerprints,
            "service": f"sha256:{token.metadata_fingerprint}",
        },
    )


def _epoch(state: ComponentState = ComponentState.EXACT):
    def classify(plan):
        return ComponentObservation(
            state=state,
            evidence_digest="e" * 64,
            observed_epoch=plan.starting_mutation_epoch + 1,
        )

    return classify


def test_defaults_component_converges_artifact_and_recovers_exactly(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    runner = Runner(
        {
            "providers": [
                {
                    "id": "11111111-1111-4111-8111-111111111111",
                    "name": "hosted-openai",
                    "pricing_source": "tokens-only",
                    "rate_card_provider": None,
                }
            ],
            "rate_cards": [],
        }
    )
    transport = Transport(runner)
    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=transport,
    )

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)
    exact = authority.classify(plan)

    assert exact.state is ComponentState.EXACT
    assert [call[:2] for call in transport.calls if call[0] != "GET"] == [
        ("POST", "/api/v1/rate-cards/sync/yibuapi"),
        ("PATCH", "/api/v1/provider-connections/11111111-1111-4111-8111-111111111111"),
    ]
    before_mutations = tuple(call for call in transport.calls if call[0] != "GET")
    assert authority.classify(plan).evidence_digest == exact.evidence_digest
    assert tuple(call for call in transport.calls if call[0] != "GET") == before_mutations


def test_defaults_component_fails_closed_on_missing_or_duplicate_provider(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    for providers in (
        [],
        [
            {
                "id": "11111111-1111-4111-8111-111111111111",
                "name": "hosted-openai",
                "pricing_source": "tokens-only",
                "rate_card_provider": None,
            },
            {
                "id": "22222222-2222-4222-8222-222222222222",
                "name": "hosted-openai",
                "pricing_source": "tokens-only",
                "rate_card_provider": None,
            },
        ],
    ):
        runner = Runner({"providers": providers, "rate_cards": []})
        authority = KubernetesProtectedProductionDefaultsComponent(
            runner=runner,
            environment={"KUBECONFIG": "/exact"},
            service_uid=os.geteuid(),
            epoch_guard=_epoch(),
            request=Transport(runner),
        )
        assert authority.classify(plan).state is ComponentState.DRIFTED
        with pytest.raises(RuntimeError, match="state changed before apply"):
            authority.apply(plan)


def test_defaults_component_rejects_epoch_artifact_and_token_drift(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    runner = Runner({"providers": [], "rate_cards": []})
    drifted_epoch = KubernetesProtectedProductionDefaultsComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(ComponentState.READY),
        request=Transport(runner),
    )
    assert drifted_epoch.classify(plan).state is ComponentState.DRIFTED
    assert runner.calls == 0

    Path(plan.production_defaults_path).write_text("{}\n")
    with pytest.raises(ValueError, match="artifact"):
        drifted_epoch._read_artifact(plan)

    plan = _plan(tmp_path / "token-drift")
    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=Runner({"providers": [], "rate_cards": []}),
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=lambda **_kwargs: (200, b"{}"),
    )
    Path(plan.service_token_source.removeprefix("file:")).write_text("changed\n")
    with pytest.raises(ValueError, match="metadata drifted"):
        authority._read_token(plan)


def test_defaults_component_retries_only_retryable_http_rejections(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    responses = iter(
        (
            (502, b'{"detail":"upstream unavailable"}'),
            (503, b'{"detail":"gateway starting"}'),
            (201, b'{"id":"yibuapi-v1"}'),
        )
    )
    requests: list[str] = []
    delays: list[float] = []

    def request(**kwargs):
        requests.append(kwargs["method"])
        return next(responses)

    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=Runner({"providers": [], "rate_cards": []}),
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=request,
        sleep=delays.append,
    )

    assert authority._expect_json(
        plan,
        "service-secret",
        method="POST",
        path="/api/v1/rate-cards/sync/yibuapi",
        payload={"group": "default", "source_url": "https://rates.example.invalid"},
    ) == {"id": "yibuapi-v1"}
    assert requests == ["POST", "POST", "POST"]
    assert delays == [1.0, 2.0]


def test_defaults_component_rejects_permanent_http_status_without_retry(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    requests = 0
    delays: list[float] = []

    def request(**_kwargs):
        nonlocal requests
        requests += 1
        return 403, b'{"detail":"forbidden"}'

    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=Runner({"providers": [], "rate_cards": []}),
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=request,
        sleep=delays.append,
    )

    with pytest.raises(RuntimeError, match="HTTP 403"):
        authority._expect_json(
            plan,
            "service-secret",
            method="POST",
            path="/api/v1/rate-cards/sync/yibuapi",
            payload={"group": "default", "source_url": "https://rates.example.invalid"},
        )
    assert requests == 1
    assert delays == []


def test_defaults_component_bounds_retryable_http_rejections(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    requests = 0
    delays: list[float] = []

    def request(**_kwargs):
        nonlocal requests
        requests += 1
        return 502, b'{"detail":"upstream unavailable"}'

    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=Runner({"providers": [], "rate_cards": []}),
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=request,
        sleep=delays.append,
    )

    with pytest.raises(RuntimeError, match="HTTP 502"):
        authority._expect_json(
            plan,
            "service-secret",
            method="POST",
            path="/api/v1/rate-cards/sync/yibuapi",
            payload={"group": "default", "source_url": "https://rates.example.invalid"},
        )
    assert requests == 4
    assert delays == [1.0, 2.0, 4.0]


def test_empty_defaults_are_exact_without_token_or_live_inventory(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    runner = Runner({"providers": [], "rate_cards": []})
    authority = KubernetesProtectedProductionDefaultsComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(),
        request=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected request")),
    )

    assert authority.classify(plan).state is ComponentState.EXACT
    assert runner.calls == 0


def test_http_transport_rejects_nonfixed_route_without_network() -> None:
    with pytest.raises(ValueError, match="authority is invalid"):
        HttpxProductionDefaultsTransport()(
            base_url="https://example.invalid",
            method="POST",
            path="/api/v1/rate-cards/sync/yibuapi",
            token="secret",
            payload={},
            headers={"X-Loom-Admin-Actor": "rollout-production-defaults"},
            timeout_seconds=60,
        )


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("GET", "/api/v1/provider-connections", {}),
        (
            "PATCH",
            "/api/v1/provider-connections/11111111-1111-4111-8111-111111111111",
            {"api_key": "forbidden"},
        ),
        ("POST", "/api/v1/rate-cards/sync/yibuapi", {"group": "default"}),
    ],
)
def test_http_transport_rejects_payload_authority_without_network(
    method: str,
    path: str,
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="authority is invalid"):
        HttpxProductionDefaultsTransport()(
            base_url="https://yylx.world/dev",
            method=method,
            path=path,
            token="secret",
            payload=payload,
            headers={"X-Loom-Admin-Actor": "rollout-production-defaults"},
            timeout_seconds=60,
        )
