"""Protected child identity, separate from the management installation authority."""

from __future__ import annotations

import json
import os
import stat

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.config import LoomServiceSettings
from loom_service.public_links import configured_public_base_url


def load_child_registration(settings: LoomServiceSettings) -> EnvironmentRegistrationV1 | None:
    path = settings.managed_environment_config_file
    if path is None:
        return None
    try:
        with path.open("rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError
            payload = stream.read(262145)
        if len(payload) > 262144:
            raise ValueError
        config = json.loads(payload)
        row = EnvironmentRegistrationV1.model_validate(config["registration"])
        if (config.get("schema_version") != "loom.nebius-managed-environment.v1"
                or settings.service_mode != "application" or row.scope != "personal" or row.owner_user_id is None
                or row.desired_state != "active" or row.binding_mode != "generated"
                or config.get("namespace") != row.application_namespace or config.get("public_host") != row.public_host
                or str(configured_public_base_url(settings.public_base_url)).rstrip("/") != "https://" + row.public_host):
            raise ValueError
        return row
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError("invalid_managed_child_configuration") from None
