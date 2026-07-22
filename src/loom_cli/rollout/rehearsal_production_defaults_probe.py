"""Secret-safe production-default convergence inside the isolated rehearsal."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from loom_cli.rollout.production_defaults_readiness import (
    PRODUCTION_DEFAULTS_ADMIN_ACTOR,
    YIBUAPI_RATE_CARD_INVENTORY_SQL,
    ProductionDefaultsArtifact,
    ProductionDefaultsConvergencePlan,
    plan_production_defaults_convergence,
    production_defaults_inventory,
)
from loom_cli.rollout.rehearsal_smoke_probe import load_rehearsal_admin_token

_SERVICE_BASE = "http://loom-service:8090"
_ADMIN_SECRET_PATH = Path("/var/run/loom/rehearsal-admin/secrets.toml")
_ADMIN_ACTOR = PRODUCTION_DEFAULTS_ADMIN_ACTOR
_MAX_ARTIFACT_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
_READ_SQL = YIBUAPI_RATE_CARD_INVENTORY_SQL

Request = Callable[[str, str, str, Mapping[str, object] | None], Mapping[str, object]]
RateCardReader = Callable[[], list[object]]


class RehearsalProductionDefaultsError(RuntimeError):
    """A bounded, non-secret Tier-3 convergence failure."""


def _http_request(
    method: str,
    path: str,
    token: str,
    payload: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if method not in {"GET", "POST", "PATCH"} or not path.startswith("/api/v1/"):
        raise RehearsalProductionDefaultsError("service request authority is invalid")
    body = None if payload is None else json.dumps(dict(payload), sort_keys=True).encode()
    request = urllib.request.Request(_SERVICE_BASE + path, data=body, method=method)
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("X-Loom-Admin-Actor", _ADMIN_ACTOR)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = response.status
            response_body = response.read(_MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_body = exc.read(_MAX_RESPONSE_BYTES + 1)
    except (OSError, urllib.error.URLError) as exc:
        raise RehearsalProductionDefaultsError("service request failed") from exc
    if status not in {200, 201} or len(response_body) > _MAX_RESPONSE_BYTES:
        raise RehearsalProductionDefaultsError("service request was rejected")
    try:
        decoded = json.loads(response_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RehearsalProductionDefaultsError("service response was invalid") from exc
    if not isinstance(decoded, dict):
        raise RehearsalProductionDefaultsError("service response was invalid")
    return decoded


def _read_rate_cards() -> list[object]:
    database_url = os.environ.get("LOOM_SVC_DB_URL", "")
    if database_url.startswith("postgresql+psycopg://"):
        database_url = "postgresql://" + database_url.removeprefix("postgresql+psycopg://")
    if not database_url.startswith("postgresql://"):
        raise RehearsalProductionDefaultsError("rehearsal database authority is invalid")
    try:
        import psycopg

        with psycopg.connect(database_url, connect_timeout=15) as connection:
            with connection.cursor() as cursor:
                cursor.execute(_READ_SQL)
                row = cursor.fetchone()
    except Exception as exc:  # psycopg exposes driver-specific subclasses
        raise RehearsalProductionDefaultsError("rehearsal database inventory failed") from exc
    if row is None or len(row) != 1:
        raise RehearsalProductionDefaultsError("rehearsal database inventory was invalid")
    value = row[0]
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RehearsalProductionDefaultsError(
                "rehearsal database inventory was invalid"
            ) from exc
    if not isinstance(value, dict) or set(value) != {"rate_cards"}:
        raise RehearsalProductionDefaultsError("rehearsal database inventory was invalid")
    rate_cards = value["rate_cards"]
    if not isinstance(rate_cards, list):
        raise RehearsalProductionDefaultsError("rehearsal database inventory was invalid")
    return rate_cards


def _inventory(request: Request, read_rate_cards: RateCardReader) -> dict[str, object]:
    response = request("GET", "/api/v1/provider-connections", "", None)
    try:
        return production_defaults_inventory(response, read_rate_cards())
    except ValueError as exc:
        raise RehearsalProductionDefaultsError("provider inventory was invalid") from exc


def run_probe(
    *,
    artifact_bytes: bytes,
    plan_sha256: str,
    expected_artifact_sha256: str,
    expected_candidate_sha: str,
    expected_candidate_tree: str,
    request: Request | None = None,
    read_rate_cards: RateCardReader = _read_rate_cards,
    admin_secret_path: Path = _ADMIN_SECRET_PATH,
    expected_owner_uid: int = 0,
    allowed_group_gid: int | None = None,
) -> dict[str, object]:
    """Converge the cloned state using the same classifier as final apply."""
    if (
        not 1 <= len(artifact_bytes) <= _MAX_ARTIFACT_BYTES
        or len(plan_sha256) != 64
        or len(expected_artifact_sha256) != 64
        or len(expected_candidate_sha) not in {40, 64}
        or len(expected_candidate_tree) != 40
        or any(
            character not in "0123456789abcdef"
            for value in (
                plan_sha256,
                expected_artifact_sha256,
                expected_candidate_sha,
                expected_candidate_tree,
            )
            for character in value
        )
    ):
        raise RehearsalProductionDefaultsError("production defaults binding is invalid")
    try:
        artifact = ProductionDefaultsArtifact.from_bytes(artifact_bytes)
    except ValueError as exc:
        raise RehearsalProductionDefaultsError("production defaults artifact is invalid") from exc
    if (
        artifact.artifact_digest != expected_artifact_sha256
        or artifact.candidate_sha != expected_candidate_sha
        or artifact.candidate_tree != expected_candidate_tree
        or artifact.environment != "staging"
    ):
        raise RehearsalProductionDefaultsError("production defaults identity drifted")
    if artifact.yibuapi_sync is None and not artifact.providers:
        empty = plan_production_defaults_convergence(
            artifact,
            {"providers": [], "rate_cards": []},
        )
        return _result(
            artifact=artifact,
            plan_sha256=plan_sha256,
            before=empty,
            after=empty,
        )

    token = load_rehearsal_admin_token(
        admin_secret_path,
        expected_owner_uid=expected_owner_uid,
        allowed_group_gid=allowed_group_gid,
    )
    transport = request or (
        lambda method, path, _token, payload: _http_request(method, path, token, payload)
    )

    def bound_request(
        method: str,
        path: str,
        _ignored_token: str,
        payload: Mapping[str, object] | None,
    ) -> Mapping[str, object]:
        return transport(method, path, token, payload)

    before = plan_production_defaults_convergence(
        artifact,
        _inventory(bound_request, read_rate_cards),
    )
    if before.state == "drifted":
        raise RehearsalProductionDefaultsError("production defaults inventory drifted")
    for mutation in before.mutations:
        bound_request(mutation.method, mutation.path, "", mutation.payload)
    after = plan_production_defaults_convergence(
        artifact,
        _inventory(bound_request, read_rate_cards),
    )
    if after.state != "exact":
        raise RehearsalProductionDefaultsError("production defaults did not converge exactly")
    return _result(
        artifact=artifact,
        plan_sha256=plan_sha256,
        before=before,
        after=after,
    )


def _result(
    *,
    artifact: ProductionDefaultsArtifact,
    plan_sha256: str,
    before: ProductionDefaultsConvergencePlan,
    after: ProductionDefaultsConvergencePlan,
) -> dict[str, object]:
    return {
        "artifact_sha256": artifact.artifact_digest,
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "evidence_sha256": hashlib.sha256(
            json.dumps(
                {
                    "after": dict(after.relevant_inventory),
                    "before": dict(before.relevant_inventory),
                    "mutation_operations": [item.operation for item in before.mutations],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
        "mutation_count": len(before.mutations),
        "plan_sha256": plan_sha256,
        "schema_version": 1,
        "status": "ready",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loom-rehearsal-production-defaults-probe")
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--artifact-sha256", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    artifact = sys.stdin.buffer.read(_MAX_ARTIFACT_BYTES + 1)
    try:
        result = run_probe(
            artifact_bytes=artifact,
            plan_sha256=args.plan_sha256,
            expected_artifact_sha256=args.artifact_sha256,
            expected_candidate_sha=args.candidate_sha,
            expected_candidate_tree=args.candidate_tree,
        )
    except (RehearsalProductionDefaultsError, ValueError):
        print(
            json.dumps(
                {"failure_code": "rehearsal-production-defaults-failed", "status": "blocked"},
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI boundary
    raise SystemExit(main())


__all__ = ["RehearsalProductionDefaultsError", "run_probe"]
