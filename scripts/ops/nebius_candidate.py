#!/usr/bin/env python3
"""Build and authenticate one independent Nebius integration candidate."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from scripts.component_ownership import load_manifest, release_image_matrix
from scripts.install_trivy import install_trivy
from scripts.ops.collect_nebius_runtime_evidence_via_gateway import _severity
from scripts.ops.nebius_registry_auth import refresh_registry_auth
from scripts.validate_trivy_release_report import validate_trivy_release_report
from scripts.write_trivy_release_policy import write_release_policy

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    ImageAdmissionStatementV1,
    SignedImageAdmissionV1,
    verify_execution_image_admission,
)
from loom.pipeline.keys import canonical_document
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

ROOT = Path(__file__).resolve().parents[2]
SOURCE_REF = "refs/heads/codex/nebius-main"
REPOSITORY = "qianyi-sun/loom"
WORKFLOW = ".github/workflows/nebius-candidate.yml"
COMPONENTS = {
    "service": "loom-service",
    "control_plane": "loom-control-plane",
    "web": "loom-web",
    "gateway": "loom-llm-gateway",
    "execution_runtime": "loom-execution-runtime",
    "execution_actuator": "loom-execution-actuator",
}
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
REGISTRY = re.compile(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+\Z")
_diagnostic_dir: Path | None = None


def encoded(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("expected a bounded regular JSON file")
    result = json.loads(path.read_bytes(), object_pairs_hook=_unique)
    if not isinstance(result, dict):
        raise ValueError("expected a JSON object")
    return result


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(encoded(value))


def _private_key(path: Path) -> Ed25519PrivateKey:
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("established signing key must be an owner-only regular file")
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("signing key must be Ed25519")
    return key


def _trusted_signer(path: Path, key_id: str, keyring_json: str) -> Ed25519PrivateKey:
    ImageAdmissionKeyring.from_json(keyring_json)
    key = _private_key(path)
    matching = [row for row in json.loads(keyring_json)["keys"] if row["signing_key_id"] == key_id]
    public = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    if len(matching) != 1 or base64.b64decode(matching[0]["public_key_base64"]) != public:
        raise ValueError("publication signing key does not match independent runtime trust")
    return key


def validate_identity(document: dict[str, Any]) -> None:
    if (
        document.get("schema_version") != "loom.nebius-candidate.v1"
        or document.get("repository") != REPOSITORY
        or document.get("source_ref") != SOURCE_REF
        or document.get("workflow_path") != WORKFLOW
        or SHA.fullmatch(str(document.get("candidate_sha"))) is None
        or SHA.fullmatch(str(document.get("source_tree"))) is None
        or type(document.get("run_id")) is not int
        or document["run_id"] <= 0
        or REGISTRY.fullmatch(str(document.get("registry_prefix"))) is None
    ):
        raise ValueError("candidate source identity is invalid")
    images = document.get("images")
    if not isinstance(images, dict) or set(images) != set(COMPONENTS):
        raise ValueError("candidate must contain exactly the six platform images")
    for component, name in COMPONENTS.items():
        row = images[component]
        prefix = f"{document['registry_prefix']}/{name}@"
        if (
            not isinstance(row, dict)
            or row.get("source_sha") != document["candidate_sha"]
            or row.get("platform") != "linux/amd64"
            or not str(row.get("image_ref", "")).startswith(prefix)
            or DIGEST.fullmatch(str(row.get("image_ref", ""))[len(prefix) :]) is None
        ):
            raise ValueError(f"candidate image identity is invalid: {component}")
        for field in ("sbom_sha256", "vulnerability_report_sha256"):
            if DIGEST.fullmatch(str(row.get(field))) is None:
                raise ValueError(f"candidate image evidence is invalid: {component}")
        if row.get("highest_vulnerability_severity") not in {
            "none",
            "negligible",
            "low",
            "medium",
            "high",
            "unknown",
        }:
            raise ValueError(f"candidate image scan is not admissible: {component}")
    if DIGEST.fullmatch(str(document.get("runtime_binary_sha256"))) is None:
        raise ValueError("runtime binary digest is invalid")
    if DIGEST.fullmatch(str(document.get("policy_sha256"))) is None:
        raise ValueError("scan policy digest is invalid")


def validate_candidate(
    document: dict[str, Any],
    profile: dict[str, Any],
    keyring_json: str,
) -> dict[str, Any]:
    """Verify with environment-owned trust, never trust a candidate-supplied key."""
    validate_identity(document)
    keyring = ImageAdmissionKeyring.from_json(keyring_json)
    signature = document.get("signature")
    if not isinstance(signature, dict) or set(signature) != {"signing_key_id", "signature_base64"}:
        raise ValueError("candidate signature is missing")
    public_keys = json.loads(keyring_json)["keys"]
    matching = [row for row in public_keys if row["signing_key_id"] == signature["signing_key_id"]]
    if len(matching) != 1:
        raise ValueError("candidate signer is not trusted")
    public_key = Ed25519PublicKey.from_public_bytes(
        base64.b64decode(matching[0]["public_key_base64"])
    )
    unsigned = {key: value for key, value in document.items() if key != "signature"}
    public_key.verify(
        base64.b64decode(signature["signature_base64"], validate=True), canonical_document(unsigned)
    )
    if document.get("profile_sha256") != sha256(encoded(profile)):
        raise ValueError("runtime profile digest does not match candidate")
    parsed = ServiceExecutionRuntimeProfileV1.model_validate(profile)
    if (
        parsed.candidate_sha != document["candidate_sha"]
        or parsed.task_image_ref != document["images"]["service"]["image_ref"]
        or parsed.runtime_image_ref != document["images"]["execution_runtime"]["image_ref"]
        or parsed.runtime_binary_sha256 != document["runtime_binary_sha256"]
        or parsed.execution_class_id != "linux-amd64-cpu-pod-v1"
    ):
        raise ValueError("runtime profile does not match candidate images")
    verify_execution_image_admission(
        parsed.image_admission,
        required_image_refs=(parsed.task_image_ref, parsed.runtime_image_ref),
        keyring=keyring,
    )
    provenance = {
        key: value for key, value in document.items() if key not in {"signature", "profile_sha256"}
    }
    by_ref = {row["image_ref"]: row for row in document["images"].values()}
    for admission in parsed.image_admission.admissions:
        statement = admission.statement
        row = by_ref[statement.image_ref]
        if (
            statement.sbom_sha256 != row["sbom_sha256"]
            or statement.vulnerability_report_sha256 != row["vulnerability_report_sha256"]
            or statement.highest_vulnerability_severity != row["highest_vulnerability_severity"]
            or statement.policy_sha256 != document["policy_sha256"]
            or statement.provenance_sha256 != sha256(encoded(provenance))
        ):
            raise ValueError("runtime admission does not match candidate evidence")
    return document


def create_candidate(
    document: dict[str, Any],
    *,
    signing_key: Path,
    signing_key_id: str,
    keyring_json: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_identity(document)
    if "signature" in document or "profile_sha256" in document:
        raise ValueError("candidate inputs are already signed")
    key = _trusted_signer(signing_key, signing_key_id, keyring_json)
    now = datetime.now(UTC)
    admissions = []
    for component in ("service", "execution_runtime"):
        row = document["images"][component]
        statement = ImageAdmissionStatementV1(
            schema_version="loom.image-admission-statement.v1",
            image_ref=row["image_ref"],
            platform="linux/x86_64",
            sbom_sha256=row["sbom_sha256"],
            vulnerability_report_sha256=row["vulnerability_report_sha256"],
            provenance_sha256=sha256(encoded(document)),
            policy_sha256=document["policy_sha256"],
            highest_vulnerability_severity=row["highest_vulnerability_severity"],
            issued_at=now,
            expires_at=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
        )
        admissions.append(
            SignedImageAdmissionV1(
                statement=statement,
                signing_key_id=signing_key_id,
                signature_base64=base64.b64encode(
                    key.sign(canonical_document(statement.model_dump(mode="json")))
                ).decode(),
            )
        )
    profile = ServiceExecutionRuntimeProfileV1(
        candidate_sha=document["candidate_sha"],
        execution_class_id="linux-amd64-cpu-pod-v1",
        task_image_ref=document["images"]["service"]["image_ref"],
        runtime_image_ref=document["images"]["execution_runtime"]["image_ref"],
        runtime_binary_sha256=document["runtime_binary_sha256"],
        image_admission=ExecutionImageAdmissionBundleV1(
            schema_version="loom.execution-image-admission.v1",
            admissions=tuple(admissions),
        ),
    ).model_dump(mode="json")
    result = {**document, "profile_sha256": sha256(encoded(profile))}
    result["signature"] = {
        "signing_key_id": signing_key_id,
        "signature_base64": base64.b64encode(key.sign(canonical_document(result))).decode(),
    }
    validate_candidate(result, profile, keyring_json)
    return result, profile


def _run(*command: str) -> str:
    operation = Path(command[0]).name + (" " + command[1] if len(command) > 1 else "")
    print(f"Nebius publication: {operation}", flush=True)
    result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True, text=True)
    if result.returncode:
        text = (result.stderr or result.stdout)[-16_384:]
        for name, value in os.environ.items():
            if len(value) >= 8 and re.search(r"TOKEN|PASSWORD|SECRET|PRIVATE_KEY|API_KEY", name):
                text = text.replace(value, "[redacted]")
        text = re.sub(r"https?://[^\s]+", "[url]", text)
        text = re.sub(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", "[ip]", text)
        text = "\n".join(
            "[redacted credential-related diagnostic]"
            if re.search(
                r"password|authorization|credential|private.key|api.key|bearer|access.token",
                line,
                re.IGNORECASE,
            )
            else line
            for line in text.splitlines()
        )
        if _diagnostic_dir is not None:
            write_json(
                _diagnostic_dir / "failed-command.json",
                {
                    "operation": operation,
                    "returncode": result.returncode,
                    "diagnostic": text,
                },
            )
        raise ValueError(
            f"{operation} failed with exit code {result.returncode}; inspect failed-command.json"
        )
    return result.stdout.strip()


def inspect_oci_archive(
    archive: Path, *, candidate: str, runtime: bool = False
) -> tuple[str, str | None]:
    """Bind the exact scanned OCI bytes; never execute an image to inspect it."""
    with tarfile.open(archive) as bundle:

        def blob(descriptor: dict[str, Any]) -> bytes:
            digest = descriptor["digest"]
            if DIGEST.fullmatch(digest) is None:
                raise ValueError("invalid OCI blob digest")
            member = bundle.getmember("blobs/sha256/" + digest.split(":")[1])
            if not member.isfile():
                raise ValueError("OCI blob is not a regular file")
            stream = bundle.extractfile(member)
            assert stream is not None
            payload = stream.read()
            if sha256(payload) != digest or len(payload) != descriptor["size"]:
                raise ValueError("OCI blob checksum mismatch")
            return payload

        index_stream = bundle.extractfile("index.json")
        if index_stream is None:
            raise ValueError("missing OCI index")
        index = json.load(index_stream)
        if len(index["manifests"]) != 1:
            raise ValueError("candidate archive must contain exactly one platform image")
        descriptor = index["manifests"][0]
        manifest = json.loads(blob(descriptor))
        config = json.loads(blob(manifest["config"]))
        if (
            config.get("architecture") != "amd64"
            or config.get("os") != "linux"
            or config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
            != candidate
        ):
            raise ValueError("built OCI image source/platform mismatch")
        binary_digest = None
        if runtime:
            import io

            # The scratch runtime image has one regular binary. Refuse symlink
            # replacement or whiteouts rather than infer a misleading digest.
            for layer in manifest["layers"]:
                with tarfile.open(fileobj=io.BytesIO(blob(layer)), mode="r:*") as files:
                    for member in files:
                        name = member.name.removeprefix("./").lstrip("/")
                        if name in {".wh.loom-execution-runtime", ".wh..wh..opq"}:
                            binary_digest = None
                        elif name == "loom-execution-runtime":
                            if not member.isfile() or not member.mode & 0o111:
                                raise ValueError("runtime binary must be a regular executable")
                            stream = files.extractfile(member)
                            assert stream is not None
                            binary_digest = sha256(stream.read())
            if binary_digest is None:
                raise ValueError("runtime binary is missing from candidate OCI image")
        return str(descriptor["digest"]), binary_digest


def build(args: argparse.Namespace) -> None:
    """Only the protected integration workflow may build/publish this candidate."""
    global _diagnostic_dir
    candidate = os.environ.get("GITHUB_SHA", "")
    if (
        os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
        or os.environ.get("GITHUB_REF") != SOURCE_REF
        or os.environ.get("GITHUB_EVENT_NAME") not in {"workflow_dispatch", "push"}
        or os.environ.get("GITHUB_WORKFLOW_REF") != f"{REPOSITORY}/{WORKFLOW}@{SOURCE_REF}"
        or SHA.fullmatch(candidate) is None
        or _run("git", "rev-parse", "HEAD") != candidate
        or _run("git", "status", "--porcelain", "--untracked-files=normal")
        or REGISTRY.fullmatch(args.registry_prefix) is None
    ):
        raise ValueError("build must run from the fixed protected Nebius workflow checkout")
    if _run("uname", "-m") != "x86_64":
        raise ValueError("Nebius release requires a native AMD64 runner")
    _trusted_signer(args.signing_key, args.signing_key_id, args.trusted_keyring.read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    _diagnostic_dir = args.output
    rows = release_image_matrix(load_manifest(ROOT / "config/component-ownership.toml"))
    ownership = {row["image_name"]: row for row in rows}
    document: dict[str, Any] = {
        "schema_version": "loom.nebius-candidate.v1",
        "repository": REPOSITORY,
        "source_ref": SOURCE_REF,
        "candidate_sha": candidate,
        "source_tree": _run("git", "rev-parse", "HEAD^{tree}"),
        "workflow_path": WORKFLOW,
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "registry_prefix": args.registry_prefix,
        "images": {},
    }
    with tempfile.TemporaryDirectory(prefix="loom-nebius-build-") as temporary:
        work = Path(temporary)
        scanner = install_trivy(work, architecture="amd64")
        policy, exceptions = work / "trivy.yaml", work / "ignore.yaml"
        write_release_policy(policy, exceptions)
        document["policy_sha256"] = sha256(policy.read_bytes() + exceptions.read_bytes())
        for component, name in COMPONENTS.items():
            owner = ownership[name]
            tag = f"{args.registry_prefix}/{name}:candidate-{candidate}"
            archive = Path(f"/tmp/{owner['image']}-amd64.release.docker.tar")
            if archive.exists():
                raise ValueError("release archive already exists; use a clean ephemeral runner")
            report = args.output / f"{component}.vulnerability.json"
            policy_report = args.output / f"{component}.policy.json"
            sbom = args.output / f"{component}.sbom.cdx.json"
            print(f"Building and scanning {component}", flush=True)
            _run(
                os.environ.get(
                    "BUILDKIT_CLIENT", os.environ.get("BUILDKIT_BIN", "/opt/buildkit/buildctl")
                ),
                "build",
                "--frontend",
                "dockerfile.v0",
                "--local",
                f"context={ROOT / owner['context']}",
                "--local",
                f"dockerfile={(ROOT / owner['dockerfile']).parent}",
                "--opt",
                f"filename={Path(owner['dockerfile']).name}",
                "--opt",
                "platform=linux/amd64",
                "--opt",
                f"label:org.opencontainers.image.revision={candidate}",
                "--opt",
                f"build-arg:LOOM_BUILD_SHA={candidate}",
                "--output",
                f"type=oci,oci-mediatypes=true,name={tag},dest={archive}",
            )
            scanned_digest, runtime_digest = inspect_oci_archive(
                archive, candidate=candidate, runtime=component == "execution_runtime"
            )
            _run(
                str(scanner),
                "--config",
                str(policy),
                "image",
                "--input",
                str(archive),
                "--format",
                "json",
                "--output",
                str(policy_report),
                "--ignorefile",
                str(exceptions),
                "--show-suppressed",
            )
            validate_trivy_release_report(owner["image"], "amd64", policy_report, exceptions)
            _run(
                str(scanner),
                "image",
                "--input",
                str(archive),
                "--scanners",
                "vuln",
                "--format",
                "json",
                "--output",
                str(report),
                "--ignorefile",
                str(exceptions),
            )
            _run(
                str(scanner),
                "image",
                "--input",
                str(archive),
                "--format",
                "cyclonedx",
                "--output",
                str(sbom),
            )
            if component == "execution_runtime":
                document["runtime_binary_sha256"] = runtime_digest
            refresh_registry_auth(
                Path(os.environ["NEBIUS_REGISTRY_CREDENTIALS_FILE"]),
                args.registry_prefix,
                Path(os.environ["REGISTRY_AUTH_FILE"]),
            )
            _run(
                "skopeo", "copy", "--preserve-digests", f"oci-archive:{archive}", f"docker://{tag}"
            )
            published_digest = _run(
                "skopeo", "inspect", "--format", "{{.Digest}}", f"docker://{tag}"
            )
            if published_digest != scanned_digest:
                raise ValueError("published manifest differs from the scanned OCI bytes")
            document["images"][component] = {
                "image_ref": f"{args.registry_prefix}/{name}@{scanned_digest}",
                "source_sha": candidate,
                "platform": "linux/amd64",
                "sbom_sha256": sha256(sbom.read_bytes()),
                "vulnerability_report_sha256": sha256(report.read_bytes()),
                "highest_vulnerability_severity": _severity(report.read_bytes()),
            }
            archive.unlink()
        manifest, profile = create_candidate(
            document,
            signing_key=args.signing_key,
            signing_key_id=args.signing_key_id,
            keyring_json=args.trusted_keyring.read_text(),
        )
        write_json(args.output / "candidate.json", manifest)
        write_json(args.output / "runtime-profile.json", profile)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--candidate", type=Path, required=True)
    verify.add_argument("--runtime-profile", type=Path, required=True)
    create = commands.add_parser("create")
    create.add_argument("--build-record", type=Path, required=True)
    builder = commands.add_parser("build")
    builder.add_argument("--registry-prefix", required=True)
    for command in (create, builder):
        command.add_argument("--signing-key", type=Path, required=True)
        command.add_argument("--signing-key-id", required=True)
        command.add_argument("--output", type=Path, required=True)
    for command in (verify, create, builder):
        command.add_argument("--trusted-keyring", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "verify":
            validate_candidate(
                read_json(args.candidate),
                read_json(args.runtime_profile),
                args.trusted_keyring.read_text(),
            )
        elif args.command == "create":
            manifest, profile = create_candidate(
                read_json(args.build_record),
                signing_key=args.signing_key,
                signing_key_id=args.signing_key_id,
                keyring_json=args.trusted_keyring.read_text(),
            )
            args.output.mkdir(parents=True, exist_ok=False)
            write_json(args.output / "candidate.json", manifest)
            write_json(args.output / "runtime-profile.json", profile)
        else:
            build(args)
    except Exception as exc:
        # Avoid serializing subprocess output or credential-bearing inputs.
        print(f"Nebius candidate {args.command} failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(f"Nebius candidate {args.command} succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
