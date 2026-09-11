"""Actual isolated Kubernetes admission, not live staging or CNPG retirement."""

import copy
import json
import shutil
import subprocess
import time

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s


@pytest.mark.timeout(240)
@pytest.mark.parametrize("existing_pooler", [False, True])
def test_cnpg_input_fence_enforces_on_disposable_kubernetes(existing_pooler, tmp_path, monkeypatch):
    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    original_client_configuration = client.Configuration.get_default_copy()
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        api = client.ApiClient()
        extensions = client.ApiextensionsV1Api(api)
        admission = client.AdmissionregistrationV1Api(api)
        custom = client.CustomObjectsApi(api)
        for namespace in ("loom-staging", "foreign"):
            core.create_namespace({"metadata": {"name": namespace}})
        # Exact relevant CNPG v1.25.1 structural types, with descriptions omitted:
        # config/crd/bases/postgresql.cnpg.io_clusters.yaml status fields. A fully
        # opaque status hides CEL type-checking defects that the real CRD exposes.
        string_list = {"type": "array", "items": {"type": "string"}}
        writer_status_fields = {
            "poolerIntegrations": {"type": "object", "properties": {
                "pgBouncerIntegration": {"type": "object", "properties": {"secrets": string_list}}}},
            "managedRolesStatus": {"type": "object", "properties": {
                "byStatus": {"type": "object", "additionalProperties": string_list},
                "cannotReconcile": {"type": "object", "additionalProperties": string_list},
                "passwordStatus": {"type": "object", "additionalProperties": {
                    "type": "object", "properties": {"resourceVersion": {"type": "string"},
                                                       "transactionID": {"type": "integer", "format": "int64"}}}}}},
        }
        for plural, singular, kind in (("clusters", "cluster", "Cluster"),
                                        ("databases", "database", "Database"),
                                        ("poolers", "pooler", "Pooler"),
                                        ("publications", "publication", "Publication"),
                                        ("subscriptions", "subscription", "Subscription")):
            subresources = {"status": {}}
            if plural in {"clusters", "poolers"}:
                subresources["scale"] = {"specReplicasPath": ".spec.instances", "statusReplicasPath": ".status.instances"}
            extensions.create_custom_resource_definition({
                "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
                "metadata": {"name": plural + ".postgresql.cnpg.io"},
                "spec": {"group": "postgresql.cnpg.io", "scope": "Namespaced",
                         "names": {"plural": plural, "singular": singular, "kind": kind},
                         "versions": [{"name": "v1", "served": True, "storage": True,
                                       "subresources": subresources,
                                       "schema": {"openAPIV3Schema": {"type": "object",
                                                  "properties": {"spec": {"type": "object", "x-kubernetes-preserve-unknown-fields": True},
                                                                 "status": {"type": "object", "x-kubernetes-preserve-unknown-fields": True,
                                                                            "properties": writer_status_fields if plural == "clusters" else {}}}}}}]},
            })
        deadline = time.monotonic() + 30
        while not all(any(c.type == "Established" and c.status == "True" for c in
                         extensions.read_custom_resource_definition(p + ".postgresql.cnpg.io").status.conditions or [])
                      for p in ("clusters", "databases", "poolers", "publications", "subscriptions")):
            assert time.monotonic() < deadline, "isolated CRDs not established"
            time.sleep(0.1)

        def create(plural, name, namespace="loom-staging", **spec):
            kind = {"clusters": "Cluster", "databases": "Database", "poolers": "Pooler",
                    "publications": "Publication", "subscriptions": "Subscription"}[plural]
            return custom.create_namespaced_custom_object("postgresql.cnpg.io", "v1", namespace,
                plural, {"apiVersion": "postgresql.cnpg.io/v1", "kind": kind,
                         "metadata": {"name": name}, "spec": spec})

        create("clusters", "loom-postgres", instances=3)
        create("clusters", "foreign-db", instances=3)
        create("clusters", "loom-postgres", namespace="foreign", instances=3)
        create("databases", "foreign", cluster={"name": "foreign-db"})
        for plural in ("databases", "poolers", "publications", "subscriptions"):
            if plural == "poolers" and not existing_pooler:
                continue
            create(plural, "existing-target", cluster={"name": "loom-postgres"}, instances=1)
        for name in ("loom-secrets", "loom-postgres-cnpg-credentials", "foreign"):
            core.create_namespaced_secret("loom-staging", {"metadata": {"name": name}, "data": {"x": "eA=="}})
        core.create_namespaced_config_map("loom-staging", {"metadata": {"name": "cnpg-default-monitoring"}, "data": {"queries": "original"}})
        # Exercise the actual fixed-command installed-runner implementation while
        # replacing ONLY test transport's kubeconfig with this owned API. Never
        # allow a subprocess to fall back to the host's protected kubeconfig.
        from loom_cli.rollout.operator.protected_apply_executor import (
            SubprocessProtectedApplyCommandRunner,
        )
        from loom_cli.rollout.operator.protected_cnpg_fence_acquisition import (
            acquire_cnpg_input_fence,
        )
        from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
        from tests.loom_cli.rollout.operator.test_application_credential_recovery import _sources
        from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal

        runner = SubprocessProtectedApplyCommandRunner()
        kubectl = shutil.which("kubectl")
        assert kubectl is not None, "kubectl required for admission probe conformance"
        config_result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert config_result.exit_code == 0
        kubeconfig = yaml.safe_load(config_result.output.decode())
        kubeconfig["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{container.get_exposed_port(6443)}"
        config_path = tmp_path / "disposable-kubeconfig"
        config_path.write_text(yaml.safe_dump(kubeconfig))
        config_path.chmod(0o600)
        subprocess_run = subprocess.run
        probe_diagnostics = []
        lose_create_reply = [True]
        intent_digest = "a" * 64

        def isolated_run(argv, **kwargs):
            assert argv[0] == "kubectl"
            if "--dry-run=server" not in argv:
                assert argv[1] in {"get", "create", "patch"}
            assert kwargs["env"] == runner.environment
            kwargs["env"] = {**kwargs["env"], "KUBECONFIG": str(config_path)}
            result = subprocess_run((kubectl, *argv[1:]), **kwargs)
            probe_diagnostics.append((argv[3:5], result.returncode, result.stderr.decode()))
            if argv[1] == "create" and "--dry-run=server" not in argv and result.returncode == 0 and lose_create_reply[0]:
                lose_create_reply[0] = False
                raise subprocess.TimeoutExpired("disposable create reply lost", 30)
            return result

        def enforced():
            with monkeypatch.context() as transport:
                transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
                try:
                    return runner.probe_cnpg_input_fence(
                        intent_digest=intent_digest,
                        target_pooler_names=("existing-target",) if existing_pooler else (),
                    )
                except RuntimeError:
                    pytest.fail(f"Disposable API probe diagnostics: {probe_diagnostics!r}")

        assert enforced() is False
        before = custom.get_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres")
        assert "loom.dev/cnpg-fence-probe" not in before["metadata"].get("annotations", {})
        plan, _ = _sources(tmp_path)
        journal = _journal(tmp_path)
        acquisitions = []
        documents = ()

        def acquire(_):
            nonlocal documents, intent_digest
            request = journal.prepare_application_cnpg_fence(
                plan,
                target_pooler_names=("existing-target",) if existing_pooler else (),
            )
            documents, intent_digest = request.documents(), request.intent_digest
            try:
                acquisitions.append(acquire_cnpg_input_fence(plan, journal=journal, runner=runner))
            except ValueError as exc:
                details = [(p.metadata.name, api.sanitize_for_serialization(p.metadata.managed_fields),
                            api.sanitize_for_serialization(p.status))
                           for p in admission.list_validating_admission_policy().items]
                pytest.fail(f"{exc}; disposable policy diagnostics: {json.dumps(details)}")
            raise RuntimeError("stop after acquired fence")

        with monkeypatch.context() as transport:
            transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
            with pytest.raises(subprocess.TimeoutExpired, match="reply lost"):
                journal.execute(plan, [_component(acquire)])
            first = admission.list_validating_admission_policy().items
            assert len(first) == 1
            original_uid = first[0].metadata.uid
            for _ in range(2):
                deadline = time.monotonic() + 30
                while True:
                    try:
                        journal.execute(plan, [_component(acquire)])
                    except RuntimeError as exc:
                        if str(exc) == "stop after acquired fence":
                            break
                        if str(exc) not in {"CNPG input fence is not enforcing all protected inputs",
                                            "CNPG fence type checking is not ready"}:
                            raise
                    assert time.monotonic() < deadline, "fence acquisition never enforced"
                    time.sleep(0.1)
        assert len(acquisitions) == 2 and acquisitions[0] == acquisitions[1]
        assert acquisitions[0][0].uid == original_uid

        def deny(call):
            with pytest.raises(ApiException) as failure:
                call()
            assert failure.value.status == 403, failure.value
            assert "loom-cnpg-fence" in failure.value.body, failure.value.body

        def scale(plural, name, count):
            return api.call_api(
                "/apis/postgresql.cnpg.io/v1/namespaces/loom-staging/" + plural + "/" + name + "/scale",
                "PATCH", body={"spec": {"replicas": count}},
                header_params={"Content-Type": "application/merge-patch+json"},
                auth_settings=["BearerToken"], response_type="object", _return_http_data_only=True,
            )

        # Establish enforcement through actual dry-run rejection, not a delay or
        # policy existence readback. No mutation may be used as the readiness probe.
        deadline = time.monotonic() + 30
        while True:
            try:
                core.patch_namespaced_secret("loom-secrets", "loom-staging", {"data": {"x": "eQ=="}}, dry_run="All")
            except ApiException as exc:
                assert exc.status == 403, exc
                break
            assert time.monotonic() < deadline, "fence never enforced"
            time.sleep(0.1)
        deadline = time.monotonic() + 30
        while not enforced():
            assert time.monotonic() < deadline, "installed-runner probes never observed every fence"
            time.sleep(0.1)
        for name in ("loom-secrets", "loom-postgres-cnpg-credentials"):
            deny(lambda name=name: core.create_namespaced_secret("loom-staging", {"metadata": {"name": name}, "data": {"x": "eQ=="}}))
            deny(lambda name=name: core.patch_namespaced_secret(name, "loom-staging", {"data": {"x": "eQ=="}}))
            deny(lambda name=name: core.delete_namespaced_secret(name, "loom-staging"))
        deny(lambda: core.patch_namespaced_config_map("cnpg-default-monitoring", "loom-staging", {"data": {"queries": "changed"}}))
        deny(lambda: core.delete_namespaced_config_map("cnpg-default-monitoring", "loom-staging"))
        core.patch_namespaced_secret("foreign", "loom-staging", {"data": {"x": "eQ=="}})
        deny(lambda: scale("clusters", "loom-postgres", 1))
        if existing_pooler:
            deny(lambda: scale("poolers", "existing-target", 2))
        scale("clusters", "foreign-db", 2)
        for plural in ("databases", "poolers", "publications", "subscriptions"):
            deny(lambda plural=plural: create(plural, "target", cluster={"name": "loom-postgres"}))
            create(plural, "other", cluster={"name": "foreign-db"}, instances=1)
            deny(lambda plural=plural: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", plural, "other", {"spec": {"cluster": {"name": "loom-postgres"}}}))
            if plural == "poolers" and not existing_pooler:
                continue
            deny(lambda plural=plural: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", plural, "existing-target", {"spec": {"cluster": {"name": "foreign-db"}}}))
            deny(lambda plural=plural: custom.delete_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", plural, "existing-target"))
            deny(lambda plural=plural: custom.patch_namespaced_custom_object_status("postgresql.cnpg.io", "v1", "loom-staging", plural, "existing-target", {"status": {"observedGeneration": 1}}))
        scale("poolers", "other", 2)
        deny(lambda: create("clusters", "loom-postgres", instances=1))
        deny(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"spec": {"managed": {"roles": [{"name": "loom", "login": True}]}}}))
        deny(lambda: custom.delete_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres"))
        deny(lambda: custom.patch_namespaced_custom_object_status("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"status": {"poolerIntegrations": {"pgBouncerIntegration": {"secrets": ["foreign"]}}}}))
        deny(lambda: custom.patch_namespaced_custom_object_status("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"status": {"managedRolesStatus": {"byStatus": {"reconciled": ["loom"]}}}}))
        for empty in ({}, None):
            custom.patch_namespaced_custom_object_status("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres",
                                                        {"status": {"poolerIntegrations": empty, "managedRolesStatus": empty}})
        custom.patch_namespaced_custom_object_status("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"status": {"readyInstances": 3}})
        for name, namespace in (("foreign-db", "loom-staging"), ("loom-postgres", "foreign")):
            custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", namespace, "clusters", name, {"spec": {"instances": 2}})
        restart = {"metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": "2026-09-09T17:00:00Z"}}}
        # A different principal has full RBAC here, so its denial proves CEL
        # authority enforcement, not a missing Kubernetes permission.
        client.RbacAuthorizationV1Api(api).create_cluster_role_binding({
            "metadata": {"name": "other-admin"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
            "subjects": [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "other-admin"}],
        })
        other_api = client.ApiClient()
        other_api.default_headers["Impersonate-User"] = "other-admin"
        other_custom = client.CustomObjectsApi(other_api)
        other_custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "foreign-db", {"spec": {"instances": 1}})
        deny(lambda: other_custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", restart))
        wrong = copy.deepcopy(restart)
        wrong["metadata"]["annotations"]["kubectl.kubernetes.io/restartedAt"] = "2026-09-09T17:00:01Z"
        deny(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", wrong))
        # Even the previously named administrator must not restart PostgreSQL
        # beneath the surviving database-backed handoff guard.
        deny(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", restart))
        deny(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"metadata": {"annotations": {"unrelated": "changed"}}}))
        deny(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"metadata": {"finalizers": ["foreign/finalizer"]}}))
        for document in documents:
            if document["kind"] == "ValidatingAdmissionPolicy":
                observed = admission.read_validating_admission_policy(document["metadata"]["name"])
                if observed.status is not None and observed.status.type_checking is not None:
                    warnings = observed.status.type_checking.expression_warnings
                    assert not warnings, json.dumps([item.to_dict() for item in warnings])

        # Mechanism proof only, not a live safe-outcome/release authority. Retain
        # every object name while changing only each policy's match condition.
        from loom_cli.rollout.operator.protected_cnpg_fence_retirement import (
            prepare_cnpg_fence_retirement_patch,
        )

        retired_policies = []
        pending_documents = []

        def patch_policy(name, payload):
            # Use the protected runner's actual kubectl transport. Unlike the
            # Python client's json.dumps, kubectl normalizes JSON scalar escapes
            # to Go's encoding; the apiserver patch library compares those bytes
            # for CEL strings (notably &&) in the exact full-spec test.
            with monkeypatch.context() as transport:
                transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
                return runner.capture_stdout_with_input(
                    ("kubectl", "patch", "validatingadmissionpolicies", name,
                     "--type=json", "--patch-file=/dev/stdin", "--field-manager=loom-cnpg-fence", "-o=json"),
                    env=runner.environment, input_payload=payload, timeout_seconds=30,
                )

        def patch_conflict():
            _, code, error = probe_diagnostics[-1]
            return code == 1 and error.startswith((
                "The request is invalid:", "Error from server (Conflict):",
            ))

        def retire(_):
            request = journal.read_application_cnpg_fence(plan)
            assert request is not None
            for receipt in acquisitions[0]:
                pending = journal.read_application_cnpg_fence_create(plan, ordinal=receipt.ordinal)
                document = pending.document(request)
                pending_documents.append(document)
                if document["kind"] != "ValidatingAdmissionPolicy":
                    continue
                name = document["metadata"]["name"]
                deadline = time.monotonic() + 30
                while True:
                    observed = api.sanitize_for_serialization(admission.read_validating_admission_policy(name))
                    patch = prepare_cnpg_fence_retirement_patch(
                        request=request, pending=pending, receipt=receipt, observed=json.dumps(observed).encode())
                    assert patch is not None
                    try:
                        patch_policy(name, patch)
                        break
                    except RuntimeError:
                        if not patch_conflict():
                            raise
                        latest = api.sanitize_for_serialization(admission.read_validating_admission_policy(name))
                        if latest["metadata"]["resourceVersion"] == observed["metadata"]["resourceVersion"]:
                            pytest.fail(f"Retirement rejected without resourceVersion conflict: {probe_diagnostics[-1]!r}")
                    assert time.monotonic() < deadline, "retirement resourceVersion never stabilized"
                # A lost patch reply recovers by exact retained readback; repeating
                # the stale request cannot pass its original resourceVersion test.
                observed = api.sanitize_for_serialization(admission.read_validating_admission_policy(name))
                assert observed["metadata"]["uid"] == receipt.uid
                assert prepare_cnpg_fence_retirement_patch(
                    request=request, pending=pending, receipt=receipt, observed=json.dumps(observed).encode()) is None
                with pytest.raises(RuntimeError):
                    patch_policy(name, patch)
                assert patch_conflict(), probe_diagnostics[-1]
                retired_policies.append(observed)
            raise RuntimeError("stop after retirement mechanism")

        with pytest.raises(RuntimeError, match="retirement mechanism"):
            journal.execute(plan, [_component(retire)])
        assert len(retired_policies) == 5
        for document in pending_documents:
            with pytest.raises(ApiException) as late:
                if document["kind"] == "ValidatingAdmissionPolicy":
                    admission.create_validating_admission_policy(document)
                else:
                    admission.create_validating_admission_policy_binding(document)
            assert late.value.status == 409
        assert len(admission.list_validating_admission_policy().items) == 5
        assert len(admission.list_validating_admission_policy_binding().items) == 5
        for receipt, document in zip(acquisitions[0], pending_documents, strict=True):
            name = document["metadata"]["name"]
            read = (admission.read_validating_admission_policy if document["kind"] == "ValidatingAdmissionPolicy"
                    else admission.read_validating_admission_policy_binding)
            assert read(name).metadata.uid == receipt.uid
        # Every previously denied scope must actually become usable, not merely
        # exhibit a patched policy object while admission caches retain old state.
        def eventually_allowed(call):
            deadline = time.monotonic() + 30
            while True:
                try:
                    return call()
                except ApiException as exc:
                    if exc.status != 403 or "loom-cnpg-fence" not in exc.body:
                        raise
                assert time.monotonic() < deadline, "retirement never propagated"
                time.sleep(0.1)

        for name in ("loom-secrets", "loom-postgres-cnpg-credentials"):
            eventually_allowed(lambda name=name: core.patch_namespaced_secret(name, "loom-staging", {"metadata": {"annotations": {"retired": "yes"}}}))
        eventually_allowed(lambda: core.patch_namespaced_config_map("cnpg-default-monitoring", "loom-staging", {"data": {"queries": "retired"}}))
        eventually_allowed(lambda: scale("clusters", "loom-postgres", 2))
        if existing_pooler:
            eventually_allowed(lambda: scale("poolers", "existing-target", 2))
        for plural in ("databases", "poolers", "publications", "subscriptions"):
            eventually_allowed(lambda plural=plural: create(plural, "post-retirement", cluster={"name": "loom-postgres"}))
        eventually_allowed(lambda: custom.patch_namespaced_custom_object("postgresql.cnpg.io", "v1", "loom-staging", "clusters", "loom-postgres", {"metadata": {"annotations": {"retired": "yes"}}}))
        # The active acquisition helper has no reactivation/update path.
        with monkeypatch.context() as transport:
            transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
            def reacquire(_):
                acquire_cnpg_input_fence(plan, journal=journal, runner=runner)
            with pytest.raises(ValueError, match="CNPG"):
                journal.execute(plan, [_component(reacquire)])
    finally:
        try:
            container.stop()
        finally:
            client.Configuration.set_default(original_client_configuration)
