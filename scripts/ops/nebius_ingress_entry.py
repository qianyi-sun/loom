"""Private fixed ingress entrypoint; never accepts command or manifest input."""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import loom_bundle_checksum  # noqa: F401 -- first-party wheel import qualification
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_certificate_gateway import _read
from scripts.ops.nebius_certificates import load_installation
from scripts.ops.nebius_ingress_bootstrap import validate_config
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_image import DIGEST
from scripts.ops.nebius_ingress_operation import LiveIngressAPI, install_ingress, rollback_ingress


def main(config_path: str, action: str) -> int:
    try:
        if action not in {"install", "rollback", "qualify", "image-intent"}:
            raise ValueError()
        config = json.loads(_read(Path(config_path), 16_384))
        validate_config(config)
        binding = TLSBinding(**config["binding"])
        endpoint = urlsplit(config["api_server"])
        if (endpoint.scheme != "https" or not endpoint.hostname or endpoint.username or endpoint.password
                or endpoint.path not in {"", "/"} or endpoint.query or endpoint.fragment
                or not re.fullmatch(r"mk8s-[a-z0-9]+", config["cluster_id"])
                or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", config["ingress_class"])
                or not re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-shared-ingress@" + re.escape(DIGEST), config["image"])):
            raise ValueError()
        if action == "qualify":
            print(json.dumps({"status": "tooling_qualified"}))
            return 0
        if action == "image-intent":
            # Persist before replying. A missing reply consumes the one-copy
            # grant; later Actions runners can only inspect the fixed digest.
            state = Path(config["state_dir"])
            identity = {"schema": "loom.nebius-ingress-image-intent.v1", "image": config["image"],
                        "installation_id": binding.installation_id}
            with private_state._locked_state(state):
                journal = state / "image-publication.json"
                if journal.exists() or journal.is_symlink():
                    if json.loads(private_state._private_read(journal)) != identity:
                        raise ValueError()
                    status = "image_readback_only"
                else:
                    private_state._atomic_json(journal, identity)
                    status = "image_copy_once"
            print(json.dumps({"status": status, "image": config["image"], "installation_id": binding.installation_id,
                              "candidate": config["candidate"], "namespace": binding.namespace}, sort_keys=True))
            return 0
        certificate = None
        if action == "install":
            certificate = load_installation(Path(config["certificate_config"]))
            if (certificate["installation_id"] != binding.certificate_installation_id
                    or certificate["child_domain"] != binding.child_domain
                    or certificate["management_host"] != binding.management_host):
                raise ValueError()
        api = LiveIngressAPI(Path(config["kubeconfig"]), binding=binding, executable=Path(config["kubectl"]),
                             candidate=config["candidate"], cluster_id=config["cluster_id"], api_server=config["api_server"],
                             ingress_class=config["ingress_class"], image=config["image"])
        state = Path(config["state_dir"])
        if action == "rollback":
            result = rollback_ingress(api=api, state_dir=state)
        else:
            assert certificate is not None
            result = install_ingress(api=api, certificate_config=certificate, state_dir=state)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "reason": "ingress operation incomplete; retain private recovery state"}))
        return 1
