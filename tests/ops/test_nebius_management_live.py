"""Live adapter composes fixed stages and actual evidence, not readiness flags."""
from __future__ import annotations

import copy
import ssl
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class Checks:
    def __init__(self):
        self.calls = []

    def preflight(self, request, rendered):
        self.calls.append("preflight")

    def public_route(self, request):
        self.calls.append("route")


def make_live(request, checks):
    from scripts.ops.nebius_management_live import HTTPSManagementInstallationAPI

    return HTTPSManagementInstallationAPI(request=request, api_server="https://cluster.example.com",
        ssl_context=ssl.create_default_context(), runtime_ca_pem=None,
        token="operator-token", checks=checks)


def test_live_adapter_only_selects_fixed_phase_resource_transports(installation):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    api = make_live(request, Checks())
    expected = {"config": {"ConfigMap", "NetworkPolicy", "ServiceAccount"}, "authority": {
        "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "ClusterRole", "ClusterRoleBinding"},
        "supplied": {"Secret"}, "database": {"Service", "StatefulSet"}, "migration": {"Job"},
        "backup": {"Job"}, "schedule": {"CronJob"}, "service": {"Service", "Deployment"}, "public": {"Ingress"}}
    for phase, kinds in expected.items():
        with api.resources(binding, phase) as transport:
            assert {doc["kind"] for doc in transport.documents.values()} == kinds
    with pytest.raises(ManagementInstallError):
        api.resources(binding, "arbitrary.yaml")
    wrong = copy.copy(binding)
    object.__setattr__(wrong, "installation_id", str(uuid4()))
    with pytest.raises(ManagementInstallError):
        api.resources(wrong, "supplied")


def test_live_preflight_checks_actual_cluster_before_other_prerequisites(installation, monkeypatch):
    from scripts.ops.nebius_management_bootstrap import HTTPSBootstrapAPI
    from scripts.ops.nebius_management_install import ManagementInstallError, render_installation

    request, _ = installation
    checks = Checks()
    api = make_live(request, checks)
    real = HTTPSBootstrapAPI.__init__

    def setup(self, **kwargs):
        real(self, **kwargs)
        self.client.close()
        self.client = httpx.Client(base_url=self.api_server, transport=httpx.MockTransport(lambda req:
            httpx.Response(200, json={"kind": "Namespace", "metadata": {"name": "kube-system", "uid": str(uuid4())}})))

    monkeypatch.setattr(HTTPSBootstrapAPI, "__init__", setup)
    with pytest.raises(ManagementInstallError):
        api.preflight(request, render_installation(request))
    assert checks.calls == []


def test_missing_authority_journal_cannot_issue_token_or_restage_policy(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    api = make_live(request, Checks())
    with pytest.raises(ManagementInstallError):
        api.qualify_authority(binding, tmp_path / "missing")
    assert not (tmp_path / "missing").exists()
