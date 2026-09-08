"""Strict membership-only management transport with explicit retry outcomes."""

from __future__ import annotations

import json
from typing import Any

import httpx

from loom.personal_dev_capacity import PersonalDevCapacityManagerConnection
from loom.personal_dev_membership_checkpoint import (
    PersonalDevMembershipEnvelopeV1,
    validate_membership_outcome,
    validate_membership_response,
)
from loom_capacity_agent.client import (
    build_reporter_tls_context,
    canonical_manager_origin,
    read_owner_only_bearer_token,
)
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, canonical_bytes, canonical_digest
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipResponseV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationCommittedV1,
    PersonalMembershipOperationOutcomeQueryV1,
    PersonalMembershipOperationOutcomeV1,
    parse_membership_operation_outcome,
)
from loom_capacity_manager.membership_subject_status import (
    PersonalMembershipReleaseObservationV1,
    PersonalMembershipSubjectQueryV1,
    PersonalMembershipSubjectStatusV1,
    parse_membership_release_observation,
    parse_membership_subject_status,
)

MAX_MEMBERSHIP_RESPONSE_BYTES = MAX_CONTRACT_BYTES


class PersonalDevMembershipError(RuntimeError):
    """The request's outcome is not confirmed; retain its durable bytes/key."""


class PersonalDevMembershipRevisionConflictError(PersonalDevMembershipError):
    """Exact authenticated revision rejection, eligible for same-authority refresh."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate membership response field")
        if key == "schema_version" and type(value) is not int:
            raise ValueError("membership response versions must be exact integers")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError("non-JSON membership response constant")


class CapacityManagerPersonalDevMembershipClient:
    """No implicit retry, shadow fallback, lease change, or checkpoint refresh."""

    def __init__(
        self,
        *,
        manager_origin: str,
        bearer_token: str,
        http_client: httpx.AsyncClient,
        owns_http_client: bool = False,
    ) -> None:
        if (
            not bearer_token
            or len(bearer_token.encode("utf-8")) > 16 * 1024
            or not bearer_token.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in bearer_token)
        ):
            raise ValueError("membership management bearer credential is invalid")
        if not isinstance(http_client, httpx.AsyncClient):
            raise TypeError("membership HTTP client must be asynchronous")
        self._origin = canonical_manager_origin(manager_origin)
        self._token = bearer_token
        self._http = http_client
        self._owns_http = owns_http_client

    @classmethod
    def from_files(
        cls,
        connection: PersonalDevCapacityManagerConnection,
    ) -> CapacityManagerPersonalDevMembershipClient:
        token = read_owner_only_bearer_token(connection.bearer_token_file)
        tls = build_reporter_tls_context(connection.tls_files)
        return cls(
            manager_origin=connection.manager_origin,
            bearer_token=token,
            http_client=httpx.AsyncClient(
                verify=tls,
                timeout=httpx.Timeout(connection.timeout_seconds),
                follow_redirects=False,
                trust_env=False,
            ),
            owns_http_client=True,
        )

    async def _exchange(
        self,
        method: str,
        path: str,
        *,
        content: bytes | None = None,
        idempotency_key: str | None = None,
        allow_revision_conflict: bool = False,
    ) -> bytes:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        if content is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        try:
            async with self._http.stream(
                method,
                f"{self._origin}{path}",
                headers=headers,
                content=content,
                follow_redirects=False,
            ) as response:
                status = response.status_code
                if status != 200 and not (status == 409 and allow_revision_conflict):
                    raise PersonalDevMembershipError(f"membership request returned {status}")
                if response.headers.get("content-type", "").split(";", 1)[
                    0
                ].strip() != "application/json" or response.headers.get(
                    "content-encoding", ""
                ).strip().lower() not in {"", "identity"}:
                    raise PersonalDevMembershipError("membership response is not uncompressed JSON")
                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                    if len(body) + len(chunk) > MAX_MEMBERSHIP_RESPONSE_BYTES:
                        raise PersonalDevMembershipError(
                            "membership response exceeds its size bound"
                        )
                    body.extend(chunk)
        except httpx.HTTPError as exc:
            raise PersonalDevMembershipError("membership request outcome is unconfirmed") from exc
        try:
            payload = json.loads(
                body, object_pairs_hook=_unique_object, parse_constant=_invalid_constant
            )
            if not isinstance(payload, dict):
                raise ValueError("membership response must be an object")
            if status == 200 and (
                type(payload.get("schema_version")) is not int or payload["schema_version"] != 1
            ):
                raise ValueError("membership response version is invalid")
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise PersonalDevMembershipError("membership response is invalid") from exc
        if status == 409:
            if payload == {"detail": {"code": "membership_revision_conflict"}}:
                raise PersonalDevMembershipRevisionConflictError("membership revision changed")
            raise PersonalDevMembershipError("membership authority or request was rejected")
        return bytes(body)

    async def membership_checkpoint(self) -> PersonalMembershipCheckpointV1:
        payload = await self._exchange("GET", "/v1/personal-memberships/checkpoint")
        try:
            return PersonalMembershipCheckpointV1.model_validate_json(payload)
        except ValueError as exc:
            raise PersonalDevMembershipError("membership checkpoint is invalid") from exc

    async def mutate_membership(
        self,
        envelope: PersonalDevMembershipEnvelopeV1,
    ) -> PersonalApplicationMembershipResponseV1:
        # Revalidation catches accidentally corrupted stored envelopes before IO;
        # no retry may reconstruct this from a newer runtime observation.
        try:
            saved = PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(envelope))
            if saved.result is not None or saved.historical_outcome is not None:
                raise ValueError("membership request already has a durable result")
        except ValueError as exc:
            raise PersonalDevMembershipError("saved membership request is invalid") from exc
        payload = await self._exchange(
            "PUT",
            f"/v1/personal-memberships/{saved.request.projection.subject_id}",
            content=canonical_bytes(saved.request),
            idempotency_key=str(saved.idempotency_key),
            allow_revision_conflict=True,
        )
        try:
            response = PersonalApplicationMembershipResponseV1.model_validate_json(payload)
            validate_membership_response(saved, response)
            return response
        except ValueError as exc:
            raise PersonalDevMembershipError(
                "membership receipt differs from saved request"
            ) from exc

    async def membership_operation_outcome(
        self,
        envelope: PersonalDevMembershipEnvelopeV1,
    ) -> PersonalMembershipOperationOutcomeV1:
        """Resolve old evidence with this client's current unbound read credential.

        A historical commit is not current readiness. Unresolved outcomes retain
        the original pending request; only authenticated retirement can establish
        terminal absence. This method does not change local lifecycle state.
        """

        try:
            saved = PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(envelope))
            query = PersonalMembershipOperationOutcomeQueryV1(
                original_actor=saved.management_principal_id,
                idempotency_key=saved.idempotency_key,
                request=saved.request,
            )
        except ValueError as exc:
            raise PersonalDevMembershipError("saved membership request is invalid") from exc
        payload = await self._exchange(
            "POST",
            "/v1/personal-memberships/operation-outcomes/query",
            content=canonical_bytes(query),
        )
        try:
            outcome = parse_membership_operation_outcome(payload)
            validate_membership_outcome(saved, outcome)
            return outcome
        except ValueError as exc:
            raise PersonalDevMembershipError(
                "membership outcome differs from saved request"
            ) from exc

    async def _query_subject(
        self,
        envelope: PersonalDevMembershipEnvelopeV1,
        *,
        path: str,
    ) -> tuple[PersonalDevMembershipEnvelopeV1, PersonalMembershipSubjectQueryV1, bytes]:
        try:
            saved = PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(envelope))
            receipt = saved.result
            if receipt is None and isinstance(
                saved.historical_outcome, PersonalMembershipOperationCommittedV1
            ):
                receipt = saved.historical_outcome.receipt
            if receipt is None:
                raise ValueError("subject query requires a committed membership receipt")
            query = PersonalMembershipSubjectQueryV1(membership_receipt=receipt)
        except ValueError as exc:
            raise PersonalDevMembershipError("saved membership receipt is invalid") from exc
        payload = await self._exchange("POST", path, content=canonical_bytes(query))
        return saved, query, payload

    async def membership_subject_status(
        self,
        envelope: PersonalDevMembershipEnvelopeV1,
    ) -> PersonalMembershipSubjectStatusV1:
        saved, query, payload = await self._query_subject(
            envelope,
            path="/v1/personal-memberships/subjects/status/query",
        )
        try:
            response = parse_membership_subject_status(payload)
            if response.query_sha256 != canonical_digest(query):
                raise ValueError("subject status identifies another query")
            validate_membership_response(saved, response.membership_receipt)
            return response
        except ValueError as exc:
            raise PersonalDevMembershipError("historical subject status is invalid") from exc

    async def membership_subject_release(
        self,
        envelope: PersonalDevMembershipEnvelopeV1,
    ) -> PersonalMembershipReleaseObservationV1:
        saved, query, payload = await self._query_subject(
            envelope,
            path="/v1/personal-memberships/subjects/release/query",
        )
        try:
            response = parse_membership_release_observation(payload)
            if response.query_sha256 != canonical_digest(query):
                raise ValueError("subject release identifies another query")
            validate_membership_response(saved, response.membership_receipt)
            return response
        except ValueError as exc:
            raise PersonalDevMembershipError("historical subject release is invalid") from exc

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()
