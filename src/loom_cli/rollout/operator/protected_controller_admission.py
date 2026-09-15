"""Bounded staging-only SQL admission material for an already prepared controller."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from uuid import NAMESPACE_URL, uuid5

from cryptography import x509
from sqlalchemy.engine import URL, make_url

from loom_capacity_executor.admission_client import (
    ExecutableAdmissionClientError,
    _database_url_from_bytes,
)
from loom_capacity_executor.runtime import (
    ActivationRuntimeDocumentV2,
    AdmissionBindingDirectoryV2,
    AdmissionBindingEntryV2,
)
from loom_capacity_manager.contracts import SubjectConfigurationV1
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)

from .protected_executor_admission_journal import ExecutorAdmissionRecord

ADMISSION_CA_PATH = PurePosixPath(
    "/opt/loom-capacity-executor-releases/.active-trust/postgres-ca.pem"
)
_SUBJECT = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")
_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")


@dataclass(frozen=True, slots=True)
class ControllerAdmissionBundle:
    entry: AdmissionBindingEntryV2
    database_url: bytes = field(repr=False)
    ca_certificate: bytes = field(repr=False)
    issuance_digest: str

    def __post_init__(self) -> None:
        try:
            entry = AdmissionBindingEntryV2.model_validate_json(self.entry.model_dump_json())
            if (
                entry.subject_id != _SUBJECT
                or entry.subject_incarnation != _INCARNATION
                or entry.environment_name != "staging"
                or not isinstance(self.database_url, bytes)
                or not isinstance(self.ca_certificate, bytes)
                or not 64 <= len(self.ca_certificate) <= 65536
                or hashlib.sha256(self.database_url).hexdigest() != entry.database_url_sha256
                or not isinstance(self.issuance_digest, str)
                or len(self.issuance_digest) != 64
                or any(c not in "0123456789abcdef" for c in self.issuance_digest)
                or self.issuance_digest == "0" * 64
            ):
                raise ValueError
            url = make_url(_database_url_from_bytes(self.database_url))
            if (
                url.username != "loom_cap_staging_executor"
                or url.database != "loom"
                or url.host != "loom-postgres-rw.loom-staging.svc.cluster.local"
                or url.port != 31432
                or dict(url.query) != {"sslmode": "verify-full", "hostaddr": "192.168.50.103"}
                or not isinstance(url.password, str)
                or not 32 <= len(url.password) <= 1024
            ):
                raise ValueError
            certificates = x509.load_pem_x509_certificates(self.ca_certificate)
            if len(certificates) != 1:
                raise ValueError
            ca = certificates[0]
            now = datetime.now(UTC)
            if (
                not ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
                or not ca.not_valid_before_utc <= now < ca.not_valid_after_utc
            ):
                raise ValueError
        except (
            ValueError,
            TypeError,
            AttributeError,
            x509.ExtensionNotFound,
            ExecutableAdmissionClientError,
        ):
            raise ValueError("controller admission material is invalid") from None

    @property
    def directory_sha256(self) -> str:
        return canonical_executable_digest(AdmissionBindingDirectoryV2(entries=(self.entry,)))

    def files(self, document: ActivationRuntimeDocumentV2) -> Mapping[str, bytes]:
        state = PurePosixPath(document.state_directory)
        url_path = state / "admission-credentials" / "staging.url"
        if (
            self.entry.database_url_file != str(url_path)
            or document.admission_directory != str(state / "admission")
            or self.directory_sha256 != document.admission_directory_sha256
            or self.entry.configuration_generation != document.execution.configuration_epoch
        ):
            raise ValueError("controller admission document binding changed")
        return {
            str(url_path): self.database_url,
            str(
                state
                / "admission"
                / f"{self.entry.subject_id.hex}-{self.entry.subject_incarnation.hex}.json"
            ): canonical_executable_bytes(self.entry),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "entry": self.entry.model_dump(mode="json"),
            "database_url": base64.b64encode(self.database_url).decode("ascii"),
            "ca_certificate": base64.b64encode(self.ca_certificate).decode("ascii"),
            "issuance_digest": self.issuance_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> ControllerAdmissionBundle:
        try:
            if (
                not isinstance(value, dict)
                or set(value) != {"entry", "database_url", "ca_certificate", "issuance_digest"}
                or any(
                    not isinstance(value[key], str)
                    for key in ("database_url", "ca_certificate", "issuance_digest")
                )
                or len(value["database_url"]) > 24000
                or len(value["ca_certificate"]) > 90000
            ):
                raise ValueError
            return cls(
                AdmissionBindingEntryV2.model_validate_json(json.dumps(value["entry"])),
                base64.b64decode(value["database_url"], validate=True),
                base64.b64decode(value["ca_certificate"], validate=True),
                value["issuance_digest"],
            )
        except (ValueError, TypeError):
            raise ValueError("controller admission record is invalid") from None


def build_controller_admission_bundle(
    record: ExecutorAdmissionRecord, *, subject: SubjectConfigurationV1,
    state_directory: str, protected_admission_sha256: str, ca_certificate: bytes,
) -> ControllerAdmissionBundle:
    """Derive a fixed endpoint from retained issuance, never a new credential."""
    record = ExecutorAdmissionRecord.from_dict(record.to_dict())
    subject = SubjectConfigurationV1.model_validate_json(subject.model_dump_json())
    if subject.tier_id != "staging":
        raise ValueError("controller admission subject is not staging")
    url = URL.create("postgresql+psycopg", username="loom_cap_staging_executor",
        password=record.password, host="loom-postgres-rw.loom-staging.svc.cluster.local",
        port=31432, database="loom", query={"sslmode": "verify-full", "hostaddr": "192.168.50.103"}
    ).render_as_string(hide_password=False).encode("ascii")
    entry = AdmissionBindingEntryV2(subject_id=subject.subject_id, subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation, deployment_generation=subject.deployment_generation,
        candidate_generation=subject.candidate_generation, protected_admission_sha256=protected_admission_sha256,
        database_url_file=str(PurePosixPath(state_directory) / "admission-credentials" / "staging.url"),
        database_url_sha256=hashlib.sha256(url).hexdigest(), environment_name="staging")
    return ControllerAdmissionBundle(entry, url, ca_certificate, record.digest)
