"""Pure shared HTTPS foundation rendering; no certificate delivery or cutover.

The standard Traefik Ingress provider is trusted cluster infrastructure. Its
read-only Secret discovery is cluster-wide, not restricted by ingress class.
"""

from __future__ import annotations

import re
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_platform_render import (
    _deployment,
    _network_policy,
    _obj,
    _service,
    canonical,
    digest,
)

CONTROLLER = "loom-shared-ingress"


class SharedIngressInstallation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.nebius-shared-ingress.v1"] = "loom.nebius-shared-ingress.v1"
    installation_id: UUID
    foundation: FoundationBinding
    image: str
    tls_secret_name: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")

    @model_validator(mode="after")
    def _bound(self) -> SharedIngressInstallation:
        config = self.foundation.platform_config
        if self.installation_id.int == 0:
            raise ValueError("ingress installation ID cannot be nil")
        if (self.foundation.ingress_namespace != config["namespace"]
                or self.foundation.ingress_controller_label != CONTROLLER):
            raise ValueError("ingress must match the existing public Service namespace and selector")
        if not re.fullmatch(
            r"cr\." + re.escape(config["region"]) + r"\.nebius\.cloud/[a-zA-Z0-9_./-]+@sha256:[0-9a-f]{64}",
            self.image,
        ):
            raise ValueError("ingress image must be mirrored and digest-pinned in the foundation region")
        if self.tls_secret_name in {config["tls_secret_name"], config["db_tls_secret_name"]}:
            raise ValueError("shared ingress requires a separate certificate Secret")
        if (config["public_host"] == self.foundation.public_dns_zone
                or config["public_host"].endswith("." + self.foundation.public_dns_zone)):
            raise ValueError("standalone passthrough hostname must be outside the child zone")
        return self


def render_shared_ingress(installation: SharedIngressInstallation) -> list[dict[str, Any]]:
    """Render prerequisites only; caller must qualify ownership, TLS and cutover.

    Requires Traefik 3.7.13's strictPrefixMatching, crossProviderNamespaces and
    disableResponseBuffer contracts, verified by the disposable-cluster test.
    Steady requests: 100m CPU/128Mi memory/2Gi scratch; rollout reserves two Pods.
    """
    foundation = installation.foundation
    config = foundation.platform_config
    ns = foundation.ingress_namespace
    revision = digest(installation.model_dump(mode="json"))
    static = {
        "global": {"checkNewVersion": False, "sendAnonymousUsage": False},
        "entryPoints": {
            "websecure": {
                "address": ":8443", "asDefault": True,
                "http": {"tls": {}, "middlewares": ["in-flight@file", "bounded-request@file"]},
                "transport": {"respondingTimeouts": {
                    "readTimeout": "3600s", "writeTimeout": "0s", "idleTimeout": "180s",
                }},
            },
            "health": {"address": ":9000"},
        },
        "ping": {"entryPoint": "health"},
        "providers": {
            "kubernetesIngress": {
                "ingressClass": foundation.ingress_class_name,
                "allowExternalNameServices": False, "strictPrefixMatching": True,
                "crossProviderNamespaces": [],
            },
            "file": {"filename": "/etc/loom-ingress/routes.json", "watch": True},
        },
        "serversTransport": {"forwardingTimeouts": {"responseHeaderTimeout": "3600s"}},
        # No dashboard, access log containing user requests, ACME or cloud key.
        "log": {"level": "ERROR", "format": "json"},
    }
    dynamic = {
        "tcp": {
            "routers": {"standalone": {
                "rule": "HostSNI(`" + config["public_host"] + "`)",
                "entryPoints": ["websecure"], "service": "standalone", "tls": {"passthrough": True},
            }},
            "services": {"standalone": {"loadBalancer": {"servers": [{
                "address": "loom-web-origin." + ns + ".svc.cluster.local:443",
            }]}}},
        },
        "http": {"middlewares": {
            # Bound request-buffer disk across concurrent uploads; stream responses.
            "in-flight": {"inFlightReq": {"amount": 16}},
            "bounded-request": {"buffering": {"maxRequestBodyBytes": 104857600,
                "memRequestBodyBytes": 1048576, "disableResponseBuffer": True}},
        }},
        "tls": {
            "stores": {"default": {"defaultCertificate": {
                "certFile": "/var/run/loom-ingress-tls/tls.crt",
                "keyFile": "/var/run/loom-ingress-tls/tls.key",
            }}},
            "options": {"default": {"minVersion": "VersionTLS12", "sniStrict": True}},
        },
    }
    cm = _obj("ConfigMap", CONTROLLER, ns)
    cm["data"] = {"traefik.json": canonical(static).decode(), "routes.json": canonical(dynamic).decode()}
    role_name = CONTROLLER + "-" + ns
    role = _obj("ClusterRole", role_name, None, api="rbac.authorization.k8s.io/v1")
    role["rules"] = [
        {"apiGroups": [""], "resources": ["services", "secrets", "nodes"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "ingressclasses"], "verbs": ["get", "list", "watch"]},
    ]
    binding = _obj("ClusterRoleBinding", role_name, None, api="rbac.authorization.k8s.io/v1")
    binding["roleRef"] = {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_name}
    binding["subjects"] = [{"kind": "ServiceAccount", "name": CONTROLLER, "namespace": ns}]
    ingress_class = _obj("IngressClass", foundation.ingress_class_name, None,
                         {"controller": "traefik.io/ingress-controller"}, api="networking.k8s.io/v1")
    deployment = _deployment(CONTROLLER, ns, installation.image, 9000, "/ping", [], revision,
                             cpu="100m", memory="128Mi")
    deployment["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/name"] = CONTROLLER
    pod = deployment["spec"]["template"]["spec"]
    pod.update(serviceAccountName=CONTROLLER, automountServiceAccountToken=True)
    pod["volumes"] = [
        {"name": "config", "configMap": {"name": CONTROLLER}},
        {"name": "tls", "secret": {"secretName": installation.tls_secret_name, "defaultMode": 0o440}},
        {"name": "scratch", "emptyDir": {"sizeLimit": "2Gi"}},
    ]
    container = pod["containers"][0]
    container.update(command=["traefik"], args=["--configFile=/etc/loom-ingress/traefik.json"],
                     ports=[{"name": "https", "containerPort": 8443}, {"name": "health", "containerPort": 9000}])
    container["resources"]["requests"]["ephemeral-storage"] = "2Gi"
    container["resources"]["limits"]["ephemeral-storage"] = "2Gi"
    container["securityContext"]["readOnlyRootFilesystem"] = True
    container["volumeMounts"] = [
        {"name": "config", "mountPath": "/etc/loom-ingress", "readOnly": True},
        {"name": "tls", "mountPath": "/var/run/loom-ingress-tls", "readOnly": True},
        {"name": "scratch", "mountPath": "/tmp"},
    ]
    origin = _service("loom-web-origin", ns, 443, 8443)
    origin["spec"]["selector"] = {"app": "loom-web"}
    network = _network_policy(CONTROLLER, ns, {"matchLabels": {"app": CONTROLLER}},
                              [{"ports": [{"protocol": "TCP", "port": 8443}]}])
    docs = [_obj("ServiceAccount", CONTROLLER, ns), role, binding, ingress_class,
            cm, origin, network, deployment]
    for doc in docs:
        doc["metadata"].setdefault("labels", {})["loom.nebius/ingress-installation-id"] = str(installation.installation_id)
    return docs
