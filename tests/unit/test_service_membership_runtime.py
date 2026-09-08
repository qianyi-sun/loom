"""Active runtime builds independent credentials and closes partial construction."""

from datetime import timedelta
from importlib import import_module
from types import SimpleNamespace

import pytest

from loom.personal_dev_capacity import PersonalDevCapacityManagerConnection
from loom_capacity_agent.client import DemandReporterTLSFiles
from tests.unit.test_personal_dev_membership_admission import _wire, admission_values
from tests.unit.test_personal_dev_reconciler import _NOW
from tests.unit.test_service_dev_instance_runtime import _settings


def _configured(tmp_path):
    values = {
        "personal_dev_runtime_mode": "membership-v1",
        "personal_dev_acceptance_binding_json": "{}",
        "personal_dev_acceptance_plan_sha256": "",
        "personal_dev_membership_binding_json": _wire(admission_values()).decode(),
        "personal_dev_membership_plan_sha256": "a" * 64,
        "personal_dev_membership_observer_principal_id": "current-observer",
    }
    for role in ("lifecycle", "observer", "agent"):
        prefix = "personal_dev_capacity" + ("" if role == "agent" else "_" + role)
        for kind in ("ca", "certificate", "private_key", "bearer_token"):
            if role == "agent" and kind == "bearer_token":
                continue
            path = tmp_path / f"{role}-{kind}"
            path.write_text(f"{role}-{kind}")
            path.chmod(0o600)
            values[f"{prefix}_{kind}_file"] = path
    return _settings(tmp_path, **values)


@pytest.mark.parametrize("change", ("invalid", "mixed", "same_principal"))
async def test_active_runtime_rejects_invalid_binding_before_credentials(tmp_path, monkeypatch, change):
    module = import_module("loom_service.personal_dev_membership")
    settings = _configured(tmp_path)
    if change == "invalid":
        settings.personal_dev_membership_binding_json = "{}"
    elif change == "mixed":
        settings.personal_dev_operational_plan_sha256 = "b" * 64
    else:
        settings.personal_dev_membership_observer_principal_id = admission_values()[
            "preparation"
        ]["personal_membership"]["management_principal_id"]

    def unexpected(*args, **kwargs):
        pytest.fail("invalid configuration opened installation credentials")

    monkeypatch.setattr(module, "build_personal_dev_capacity_installation", unexpected)
    with pytest.raises(RuntimeError, match="membership"):
        await module.build_personal_dev_membership_runtime(settings)


@pytest.mark.parametrize("fail_observer", (False, True))
async def test_active_runtime_pins_installer_and_owns_partial_clients(tmp_path, monkeypatch, fail_observer):
    module = import_module("loom_service.personal_dev_membership")
    settings = _configured(tmp_path)
    events, clients = [], []
    installer, kubectl = object(), object()
    connection = PersonalDevCapacityManagerConnection(
        manager_origin=settings.personal_dev_capacity_manager_origin,
        bearer_token_file=settings.personal_dev_capacity_lifecycle_bearer_token_file,
        tls_files=DemandReporterTLSFiles(
            ca_file=settings.personal_dev_capacity_lifecycle_ca_file,
            certificate_file=settings.personal_dev_capacity_lifecycle_certificate_file,
            private_key_file=settings.personal_dev_capacity_lifecycle_private_key_file,
        ),
    )

    def installation(_settings, *, membership_execution):
        events.append(membership_execution.model_dump(mode="json"))
        return installer, kubectl, connection

    class Client:
        def __init__(self, label):
            self.label = label

        async def aclose(self):
            events.append("closed:" + self.label)

    def membership_client(connection):
        label = "observer" if connection.bearer_token_file == settings.personal_dev_capacity_observer_bearer_token_file else "delegate"
        if label == "observer" and fail_observer:
            raise ValueError("invalid observer credential")
        result = Client(label)
        clients.append(result)
        return result

    projector = Client("projector")
    monkeypatch.setattr(module, "build_personal_dev_capacity_installation", installation)
    monkeypatch.setattr(module.CapacityManagerPersonalDevMembershipClient, "from_files", membership_client)
    monkeypatch.setattr(module.CapacityManagerPersonalDevProjector, "from_files", lambda _: projector)
    monkeypatch.setattr(module, "PersonalDevCapacityStatusReader", lambda **kw: SimpleNamespace(**kw))
    if fail_observer:
        with pytest.raises(RuntimeError, match="credentials"):
            await module.build_personal_dev_membership_runtime(settings)
        assert events[1:] == ["closed:delegate"]
    else:
        runtime = await module.build_personal_dev_membership_runtime(settings)
        assert runtime.membership.installer is installer
        assert runtime.membership.client is clients[0]
        assert runtime.membership.observer is clients[1]
        assert runtime.membership.admission is not None
        assert runtime.owned_membership_clients == tuple(clients)
        assert runtime.acceptance_interlock is runtime.operational_interlock is None
        assert runtime.status_reader.projector is projector
        assert events == [admission_values()["execution"]]


async def test_observer_latency_cannot_extend_active_acceptance_window(monkeypatch):
    module = import_module("loom_service.personal_dev_membership")
    binding = module.parse_membership_acceptance_binding(
        _wire(admission_values()), expected_plan_sha256="a" * 64,
    )
    ticks = iter((0.0, 3601.0))
    monkeypatch.setattr(module, "monotonic", lambda: next(ticks), raising=False)
    times = []

    class Observer:
        async def current_manager_binding(self):
            return SimpleNamespace(
                authority_incarnation=binding.execution.authority_incarnation,
                observer_principal_id="current-observer",
            )

    class Interlock:
        async def assert_admission_ready(self, *, now):
            times.append(now)
            if now >= binding.expires_at:
                raise module.PersonalDevMembershipAdmissionError("expired")

    interlock = Interlock()
    interlock.binding = binding
    admission = module.PersonalDevMembershipServiceAdmission(
        interlock=interlock, observer=Observer(), observer_principal_id="current-observer",
    )
    with pytest.raises(module.PersonalDevMembershipAdmissionError, match="expired"):
        await admission.assert_admission_ready(now=_NOW)
    assert times == [_NOW + timedelta(seconds=3601)]


@pytest.mark.parametrize("state", ("expired", "unavailable", "wrong-principal", "wrong-authority", "ready"))
async def test_service_admission_checks_observer_before_delegated_authority(state):
    module = import_module("loom_service.personal_dev_membership")
    binding = module.parse_membership_acceptance_binding(
        _wire(admission_values()), expected_plan_sha256="a" * 64,
    )
    events = []

    class Observer:
        async def current_manager_binding(self):
            events.append("observer")
            if state == "unavailable":
                raise module.PersonalDevCapacityProjectionError("unavailable")
            return SimpleNamespace(
                authority_incarnation=None if state == "wrong-authority" else binding.execution.authority_incarnation,
                observer_principal_id="wrong" if state == "wrong-principal" else "current-observer",
            )

    class Interlock:
        async def assert_admission_ready(self, *, now):
            events.append("delegate")

    interlock = Interlock()
    interlock.binding = binding
    admission = module.PersonalDevMembershipServiceAdmission(
        interlock=interlock, observer=Observer(), observer_principal_id="current-observer",
    )
    if state == "ready":
        await admission.assert_admission_ready(now=_NOW)
        assert events == ["observer", "delegate"]
    else:
        with pytest.raises(module.PersonalDevMembershipAdmissionError):
            await admission.assert_admission_ready(now=binding.expires_at if state == "expired" else _NOW)
        assert events == ([] if state == "expired" else ["observer"])
