"""Public/internal boundary auditor tests (#77)."""

from __future__ import annotations

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_boundary import (
    BoundaryViolation,
    audit_boundary,
    format_violations,
)

# ──────────────────────────────────────────────────────────────────────
# audit_boundary — happy path
# ──────────────────────────────────────────────────────────────────────


_CLEAN_MANIFESTS = """\
apiVersion: v1
kind: Service
metadata: { name: loom-control-plane }
spec:
  type: ClusterIP
  ports: [{ port: 8080 }]
---
apiVersion: v1
kind: Service
metadata: { name: loom-service }
spec:
  ports: [{ port: 8090 }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loom-ingress }
spec:
  rules:
    - host: loom.example.com
      http:
        paths:
          - path: /api/v1
            pathType: Prefix
            backend:
              service:
                name: loom-service
                port: { number: 8090 }
          - path: /
            pathType: Prefix
            backend:
              service:
                name: loom-web
                port: { number: 80 }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: loom-control-plane }
spec:
  template:
    spec:
      containers:
        - name: cp
          ports: [{ containerPort: 8080 }]
"""


def test_audit_clean_manifests_yields_no_violations() -> None:
    """Default manifest shape — ClusterIP services, Ingress only
    points at loom-service + loom-web, no hostPorts."""
    assert audit_boundary(_CLEAN_MANIFESTS, require_network_policies=False) == []


# ──────────────────────────────────────────────────────────────────────
# Service type checks
# ──────────────────────────────────────────────────────────────────────


def test_audit_flags_load_balancer_service() -> None:
    yaml_text = """\
apiVersion: v1
kind: Service
metadata: { name: loom-control-plane }
spec:
  type: LoadBalancer
  ports: [{ port: 8080 }]
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "service-type"
    assert v.object_name == "loom-control-plane"
    assert "LoadBalancer" in v.detail


def test_audit_flags_node_port_service() -> None:
    yaml_text = """\
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
spec:
  type: NodePort
  ports: [{ port: 5432, nodePort: 32432 }]
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "service-type"
    assert "NodePort" in violations[0].detail


def test_audit_accepts_cluster_ip_service() -> None:
    yaml_text = """\
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
spec:
  type: ClusterIP
  ports: [{ port: 5432 }]
"""
    assert audit_boundary(yaml_text, require_network_policies=False) == []


def test_audit_accepts_implicit_cluster_ip_service() -> None:
    """When `type` is omitted, Kubernetes defaults to ClusterIP."""
    yaml_text = """\
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
spec:
  ports: [{ port: 5432 }]
"""
    assert audit_boundary(yaml_text, require_network_policies=False) == []


# ──────────────────────────────────────────────────────────────────────
# Ingress backend checks
# ──────────────────────────────────────────────────────────────────────


def test_audit_flags_ingress_backend_off_allowlist() -> None:
    """`loom-control-plane` is not on the public allowlist — flag it
    if an Ingress backends to it."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: bad-ingress }
spec:
  rules:
    - host: leaky.example.com
      http:
        paths:
          - path: /
            backend:
              service:
                name: loom-control-plane
                port: { number: 8080 }
  tls:
    - hosts: [leaky.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "ingress-backend"
    assert "loom-control-plane" in v.detail


def test_audit_flags_gateway_when_not_opted_in() -> None:
    """`loom-llm-gateway` is always internal-only in public beta."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loom-ingress }
spec:
  rules:
    - host: gateway.example.com
      http:
        paths:
          - path: /
            backend:
              service:
                name: loom-llm-gateway
                port: { number: 9100 }
  tls:
    - hosts: [gateway.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert "loom-llm-gateway" in violations[0].detail


def test_audit_rejects_gateway_even_with_deprecated_opt_in() -> None:
    """The public-beta boundary no longer allows a public gateway host."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loom-ingress }
spec:
  rules:
    - host: gateway.example.com
      http:
        paths:
          - path: /
            backend:
              service:
                name: loom-llm-gateway
                port: { number: 9100 }
  tls:
    - hosts: [gateway.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(
        yaml_text, gateway_public_host="gateway.example.com",
        require_network_policies=False,
    )
    assert len(violations) == 1
    assert violations[0].kind == "ingress-backend"


def test_audit_flags_default_backend_off_allowlist() -> None:
    """An Ingress with defaultBackend (catch-all for unmatched
    requests) pointing at an internal service is the same boundary
    violation as listing it under `rules`. The `rules` field can be
    absent entirely on a defaultBackend-only Ingress."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: catchall }
spec:
  defaultBackend:
    service:
      name: loom-control-plane
      port: { number: 8080 }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "ingress-default-backend"
    assert "defaultBackend" in v.detail
    assert "loom-control-plane" in v.detail


def test_audit_flags_default_backend_alongside_rules() -> None:
    """An Ingress can have both rules (specific paths) AND a
    defaultBackend (catch-all). Flag the defaultBackend even when
    rules are clean."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: mixed }
spec:
  defaultBackend:
    service:
      name: loom-llm-gateway
      port: { number: 9100 }
  rules:
    - host: loom.example.com
      http:
        paths:
          - { path: /, backend: { service: { name: loom-web, port: { number: 80 } } } }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "ingress-default-backend"
    assert "defaultBackend" in violations[0].detail
    assert "loom-llm-gateway" in violations[0].detail


def test_audit_default_backend_allowlisted_is_still_rejected() -> None:
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: catchall }
spec:
  defaultBackend:
    service: { name: loom-web, port: { number: 80 } }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "ingress-default-backend"


def test_audit_rejects_gateway_default_backend_even_with_deprecated_opt_in() -> None:
    """gateway_public_host no longer opts the gateway into Ingress."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: gw }
spec:
  defaultBackend:
    service: { name: loom-llm-gateway, port: { number: 9100 } }
  tls:
    - hosts: [gateway.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(
        yaml_text, gateway_public_host="gateway.example.com",
        require_network_policies=False,
    )
    assert len(violations) == 1
    assert violations[0].kind == "ingress-default-backend"


def test_audit_accepts_allowlisted_ingress_backends() -> None:
    """loom-service + loom-web are both on the default allowlist."""
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loom-ingress }
spec:
  rules:
    - host: loom.example.com
      http:
        paths:
          - { path: /api/v1, backend: { service: { name: loom-service, port: { number: 8090 } } } }
          - { path: /, backend: { service: { name: loom-web, port: { number: 80 } } } }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    assert audit_boundary(yaml_text, require_network_policies=False) == []


def test_audit_accepts_hostless_tls_ingress_for_ip_entrypoint() -> None:
    """Hostless Ingress is the Kubernetes-valid shape for HTTPS on a
    raw IP address. Boundary safety still comes from TLS, explicit
    paths, and allowlisted backends.
    """
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loom-ingress }
spec:
  rules:
    - http:
        paths:
          - { path: /api/v1, backend: { service: { name: loom-service, port: { number: 8090 } } } }
          - { path: /, backend: { service: { name: loom-web, port: { number: 80 } } } }
  tls:
    - secretName: loom-ip-tls
"""
    assert audit_boundary(yaml_text, require_network_policies=False) == []


def test_audit_flags_api_backend_on_non_api_path() -> None:
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: bad-api-path }
spec:
  rules:
    - host: loom.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: loom-service
                port: { number: 8090 }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "ingress-path"
    assert "/api/v1" in violations[0].detail


def test_audit_flags_spa_backend_on_api_path() -> None:
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: bad-spa-path }
spec:
  rules:
    - host: loom.example.com
      http:
        paths:
          - path: /api/v1
            pathType: Prefix
            backend:
              service:
                name: loom-web
                port: { number: 80 }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "ingress-path"
    assert "loom-web" in violations[0].detail


def test_audit_flags_ingress_without_tls() -> None:
    yaml_text = """\
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: no-tls }
spec:
  rules:
    - host: loom.example.com
      http:
        paths:
          - path: /api/v1
            pathType: Prefix
            backend:
              service:
                name: loom-service
                port: { number: 8090 }
          - path: /
            pathType: Prefix
            backend:
              service:
                name: loom-web
                port: { number: 80 }
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "ingress-tls"


# ──────────────────────────────────────────────────────────────────────
# hostPort checks
# ──────────────────────────────────────────────────────────────────────


def test_audit_flags_host_port_on_deployment() -> None:
    yaml_text = """\
apiVersion: apps/v1
kind: Deployment
metadata: { name: sneaky-deploy }
spec:
  template:
    spec:
      containers:
        - name: sneaky
          ports:
            - containerPort: 8080
              hostPort: 30080
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "host-port"
    assert v.object_kind == "Deployment"
    assert "30080" in v.detail


def test_audit_flags_host_port_on_init_container() -> None:
    """initContainers run in the pod's network namespace just like
    regular containers — a hostPort declared there binds the node
    interface and must be caught."""
    yaml_text = """\
apiVersion: apps/v1
kind: Deployment
metadata: { name: sneaky-init }
spec:
  template:
    spec:
      initContainers:
        - name: init
          ports:
            - containerPort: 5000
              hostPort: 5000
      containers:
        - name: main
          ports: [{ containerPort: 8080 }]
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "host-port"
    assert "init" in v.detail
    assert "initContainers" in v.detail


def test_audit_flags_host_port_on_daemonset() -> None:
    """DaemonSets in particular need this guard — hostPort + DaemonSet
    is the pattern operators reach for to bypass Ingress."""
    yaml_text = """\
apiVersion: apps/v1
kind: DaemonSet
metadata: { name: gateway-router }
spec:
  template:
    spec:
      containers:
        - name: router
          ports:
            - containerPort: 30443
              hostPort: 30443
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    assert len(violations) == 1
    assert violations[0].kind == "host-port"
    assert violations[0].object_kind == "DaemonSet"


def test_audit_accepts_container_port_without_host_port() -> None:
    yaml_text = """\
apiVersion: apps/v1
kind: Deployment
metadata: { name: loom-service }
spec:
  template:
    spec:
      containers:
        - name: svc
          ports:
            - containerPort: 8090
"""
    assert audit_boundary(yaml_text, require_network_policies=False) == []


# ──────────────────────────────────────────────────────────────────────
# Multi-violation + format_violations
# ──────────────────────────────────────────────────────────────────────


def test_audit_reports_multiple_violations() -> None:
    """When several violations coexist, all are reported (audits are
    not short-circuited)."""
    yaml_text = """\
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
spec:
  type: LoadBalancer
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: bad-ingress }
spec:
  rules:
    - http:
        paths:
          - backend:
              service:
                name: postgres
                port: { number: 5432 }
  tls:
    - hosts: [loom.example.com]
      secretName: loom-tls
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: leaky }
spec:
  template:
    spec:
      containers:
        - name: c
          ports: [{ containerPort: 80, hostPort: 80 }]
"""
    violations = audit_boundary(yaml_text, require_network_policies=False)
    kinds = {v.kind for v in violations}
    assert kinds == {"service-type", "ingress-backend", "host-port"}


def test_format_violations_clean() -> None:
    out = format_violations([])
    assert "no violations" in out


def test_format_violations_lists_each_one() -> None:
    out = format_violations([
        BoundaryViolation(
            kind="service-type", object_kind="Service",
            object_name="loom-postgres",
            detail="Service type 'LoadBalancer' exposes pods",
        ),
        BoundaryViolation(
            kind="host-port", object_kind="DaemonSet",
            object_name="router", detail="hostPort=80",
        ),
    ])
    assert "2 violation" in out
    assert "loom-postgres" in out
    assert "router" in out


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def test_cli_audit_default_config_clean(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The default rendered manifests must be boundary-clean. If
    they aren't, the docs + smoke check are broken."""
    rc = main(["cluster", "audit"])
    assert rc == 0
    assert "no violations" in capsys.readouterr().out


def test_cli_audit_returns_1_when_config_invalid(
    capsys: pytest.CaptureFixture[str], tmp_path: object,
) -> None:
    from pathlib import Path
    p = Path(str(tmp_path)) / "does-not-exist.toml"  # type: ignore[arg-type]
    rc = main(["cluster", "audit", "--config", str(p)])
    assert rc == 2
    assert "error" in capsys.readouterr().err.lower()


# ──────────────────────────────────────────────────────────────────────
# NetworkPolicy coverage check (#78 slice C)
# ──────────────────────────────────────────────────────────────────────


def test_audit_flags_components_without_network_policy() -> None:
    """A manifest set with no NetworkPolicies flags every required
    component. Catches the namespace-default-allow-all hole."""
    # Minimal namespace with just the seven required components but
    # zero NetworkPolicies.
    yaml_text = "\n---\n".join(
        f"apiVersion: apps/v1\nkind: Deployment\n"
        f"metadata: {{ name: {app} }}\n"
        f"spec:\n  template:\n    spec:\n      "
        f"containers: [{{ name: c, ports: [{{ containerPort: 8080 }}] }}]\n"
        for app in (
            "loom-control-plane", "loom-llm-gateway", "loom-service",
            "loom-web", "loom-worker", "loom-postgres", "loom-minio", "loom-gateway-router",
        )
    )
    violations = audit_boundary(yaml_text)
    missing = {
        v.object_name for v in violations
        if v.kind == "missing-network-policy"
    }
    assert missing == {
        "loom-control-plane", "loom-llm-gateway", "loom-service",
        "loom-web", "loom-worker", "loom-postgres", "loom-minio", "loom-gateway-router",
    }


def test_audit_passes_when_all_components_have_network_policy() -> None:
    """When every required component has a matching NetworkPolicy,
    the coverage check fires no violations."""
    yaml_text = "\n---\n".join(
        f"apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\n"
        f"metadata: {{ name: {app} }}\n"
        f"spec:\n  podSelector:\n    matchLabels: {{ app: {app} }}\n"
        f"  policyTypes: [Ingress, Egress]\n"
        f"  ingress: []\n  egress: []\n"
        for app in (
            "loom-control-plane", "loom-llm-gateway", "loom-service",
            "loom-web", "loom-worker", "loom-postgres", "loom-minio", "loom-gateway-router",
        )
    )
    violations = audit_boundary(yaml_text)
    assert violations == []


def test_audit_partial_coverage_flags_only_missing_components() -> None:
    """Coverage check is per-component — only the uncovered ones get
    flagged."""
    yaml_text = (
        "apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\n"
        "metadata: { name: loom-postgres }\n"
        "spec:\n  podSelector:\n    matchLabels: { app: loom-postgres }\n"
        "  policyTypes: [Ingress]\n  ingress: []\n"
    )
    violations = audit_boundary(yaml_text)
    missing = {
        v.object_name for v in violations
        if v.kind == "missing-network-policy"
    }
    assert "loom-postgres" not in missing
    assert "loom-control-plane" in missing
    assert "loom-worker" in missing


def test_audit_require_network_policies_opt_out(
) -> None:
    """Backwards-compat for tests that pre-date slice C: when
    `require_network_policies=False`, the coverage check is skipped
    even on manifests with no NetworkPolicies at all."""
    yaml_text = (
        "apiVersion: apps/v1\nkind: Deployment\n"
        "metadata: { name: loom-control-plane }\n"
        "spec:\n  template:\n    spec:\n      "
        "containers: [{ name: c, ports: [{ containerPort: 8080 }] }]\n"
    )
    violations = audit_boundary(
        yaml_text, require_network_policies=False,
    )
    assert violations == []


def test_audit_network_policy_with_non_app_selector_ignored() -> None:
    """A NetworkPolicy whose podSelector uses non-`app` labels (or
    matchExpressions) doesn't contribute to the coverage set. This
    matches reality: future operator-added policies on third-party
    selectors shouldn't accidentally satisfy the loom-component
    coverage requirement."""
    yaml_text = (
        "apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\n"
        "metadata: { name: other }\n"
        "spec:\n  podSelector:\n    matchLabels: { team: research }\n"
    )
    violations = audit_boundary(yaml_text)
    # All required components still flagged — the team-research
    # selector doesn't cover any of them.
    missing = {
        v.object_name for v in violations
        if v.kind == "missing-network-policy"
    }
    assert missing == {
        "loom-control-plane", "loom-llm-gateway", "loom-service",
        "loom-web", "loom-worker", "loom-postgres", "loom-minio", "loom-gateway-router",
    }
