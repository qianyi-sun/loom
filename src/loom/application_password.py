"""Bounded original-credential checks, not credential-writer admission authority.

The protected caller must recover the original credential through its durable
operation and admit any concurrent same-password writer. Password, LOGIN, role,
membership and schema changes by other actors remain externally serialized.
"""

import base64
import binascii
import hashlib
import hmac
import re
import secrets

from loom.application_database_connection import ApplicationDatabaseConnection, application_sql


class ApplicationPasswordError(RuntimeError):
    """A sealed runtime no longer carries the admitted original credential."""


def _valid_password(password: object) -> bool:
    return (
        isinstance(password, str)
        and 1 <= len(password) <= 1024
        and all(0x21 <= ord(character) <= 0x7E for character in password)
    )


def application_scram_verifier(password: str) -> str:
    """Generate PostgreSQL SCRAM locally; never send plaintext in SQL."""
    if not _valid_password(password):
        raise ValueError("application password is invalid")
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"), salt, 4096)
    stored = hashlib.sha256(hmac.digest(salted, b"Client Key", "sha256")).digest()
    server = hmac.digest(salted, b"Server Key", "sha256")
    encoded_salt, encoded_stored, encoded_server = (
        base64.b64encode(value).decode("ascii") for value in (salt, stored, server)
    )
    return f"SCRAM-SHA-256$4096:{encoded_salt}${encoded_stored}:{encoded_server}"


def matches_application_scram(password: str, verifier: object) -> bool:
    """Bound work and compare both SCRAM keys; refuse legacy/malformed verifiers."""
    if not _valid_password(password) or not isinstance(verifier, str) or len(verifier) > 1024:
        return False
    match = re.fullmatch(r"SCRAM-SHA-256\$([0-9]{1,6}):([^$]+)\$([^:]+):([^:]+)", verifier)
    if match is None or not 4096 <= int(match[1]) <= 65536:
        return False
    try:
        salt, stored, server = (
            base64.b64decode(match[index], validate=True) for index in (2, 3, 4)
        )
    except (ValueError, binascii.Error):
        return False
    if not 16 <= len(salt) <= 64 or len(stored) != 32 or len(server) != 32:
        return False
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"), salt, int(match[1]))
    client_key = hmac.digest(salted, b"Client Key", "sha256")
    return hmac.compare_digest(hashlib.sha256(client_key).digest(), stored) and hmac.compare_digest(
        hmac.digest(salted, b"Server Key", "sha256"), server
    )


def require_sealed_runtime_password(
    connection: ApplicationDatabaseConnection, *, role: str, password: str | None
) -> None:
    """Keep strict NULL by default; optional original password never permits LOGIN.

    This observes only the runtime credential, not role attributes/memberships,
    ownership, connection startup or writer authority. Those checks remain the
    caller's responsibility and cannot be replaced by passing a password here.
    """
    if password is not None and not _valid_password(password):
        raise ApplicationPasswordError("application preserved runtime password is invalid")
    state = connection.execute(
        application_sql(
            "SELECT rolcanlogin,rolpassword FROM pg_catalog.pg_authid WHERE rolname={}", role
        )
    ).fetchone()
    if (
        state is None
        or state[0] is not False
        or (
            state[1] is not None
            and (password is None or not matches_application_scram(password, state[1]))
        )
    ):
        raise ApplicationPasswordError("application sealed runtime credential state changed")
