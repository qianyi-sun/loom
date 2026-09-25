"""Material references only; no key generation, access grant or revocation."""
from __future__ import annotations

from loom.nebius_application_contract import ApplicationRegistrationV1


def application_credential_names(registration: ApplicationRegistrationV1) -> dict[str, str]:
    """An old template must never implicitly mount a newer access credential."""
    row = ApplicationRegistrationV1.model_validate(registration.model_dump())
    if row.access_generation > 2**63 - 1:
        raise ValueError("application credential generation exceeds database identity")
    return {purpose: f"loom-application-{purpose}-{row.incarnation.hex}-g{row.access_generation}"
            for purpose in ("db", "storage", "auth")}
