"""V3 runtime assembly pins purpose-specific inputs without enabling execution."""

import json
from hashlib import sha256
from importlib import import_module

import pytest

from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    PurposeLaunchPolicyV3,
    canonical_pool_launch_policy_digest,
    full_launch_profile_digest,
)
from loom_capacity_executor.runtime import (
    RuntimeAssemblyError,
    build_executable_runtime,
    canonical_approved_profiles_digest,
    load_activation_runtime_artifact,
)
from loom_capacity_executor.typed_admission import TypedAdmissionDirectoryV3, TypedAdmissionEntryV3
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.unit.test_capacity_agent_client import _owner_file
from tests.unit.test_capacity_build_pinned_transport import pinned_inputs
from tests.unit.test_capacity_executor_config import executor_files
from tests.unit.test_capacity_executor_runtime import _slurm_authority_for_config
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def runtime_inputs(tmp_path,pool,purpose):
    module = import_module("loom_capacity_executor.typed_runtime")
    files = executor_files(tmp_path,pool_id=pool)
    config = PoolExecutorConfig.from_files(files.config)
    context = typed_context(pool=pool,purpose=purpose)
    active = config.execution.model_copy(update={"execution_state":"active",
        "executable_new_capacity_ceiling":1,"executable_new_capacity_rate_per_minute":1})
    profile = context.profiles[0].model_copy(update={
        "pool_generation":config.pool_generation,"profile_id":config.profile_id,
        "profile_generation":config.profile_generation,"profile_digest":config.profile_digest,
        "slurm_cluster":config.slurm_cluster,"controller_host":config.controller_host,
        "partition":config.partition,"association":config.association,"submitter":config.submitter,
        "qos":config.qos,"trusted_launcher_release_sha256":active.trusted_fleet_release_sha256})
    policy = PoolLaunchPolicyV3(pool_id=pool,pool_generation=config.pool_generation,
        entries=(PurposeLaunchPolicyV3(purpose=purpose,profile_sha256=full_launch_profile_digest(profile)),))
    root = canonical_pool_launch_policy_digest(policy)
    profile = profile.model_copy(update={"controller_authority_sha256":root})
    values = json.loads(files.config.read_text())
    values.update(controller_authority_sha256=root,approved_profiles_sha256=canonical_approved_profiles_digest((profile,)))
    _owner_file(files.config,json.dumps(values).encode())
    config = PoolExecutorConfig.from_files(files.config)
    database = _owner_file(tmp_path/"database-url",b"postgresql+psycopg://executor:fixture@database.test/app?sslmode=verify-full")
    entry = TypedAdmissionEntryV3(subject_id=context.binding.subject_id,subject_incarnation=context.binding.subject_incarnation,
        configuration_epoch=active.configuration_epoch,deployment_generation=context.binding.deployment_generation,
        candidate_generation=context.binding.candidate_generation,candidate_sha256="a"*64,
        account_id=context.binding.account_id,purpose=purpose,protected_admission_sha256="b"*64,
        database={"path":str(database),"sha256":sha256(database.read_bytes()).hexdigest()} if purpose=="application-worker" else None,
        build={"origin":"https://management.test",**pinned_inputs(tmp_path)} if purpose=="personal-build-worker" else None)
    routes = TypedAdmissionDirectoryV3(executor={"pool_id":pool,"pool_generation":config.pool_generation,
        "executor_id":config.executor_id,"executor_incarnation":config.executor_incarnation},entries=(entry,))
    wire = canonical_executable_bytes(routes)
    path = _owner_file(tmp_path/"routes.json",wire)
    handoff = tmp_path/"handoff"
    handoff.mkdir(mode=0o700)
    artifact = module.ActivationRuntimeArtifactV3(execution=active,pool_id=pool,pool_generation=config.pool_generation,
        executor_id=config.executor_id,executor_incarnation=config.executor_incarnation,
        controller_authority_sha256=root,approved_profiles_sha256=config.approved_profiles_sha256,
        local_authority_sha256=config.local_authority_sha256,signing_key_id=config.signing_key_id,
        signing_key_sha256=config.signing_key_sha256,immutable_manifest_sha256=config.manifest.sha256(),
        admission={"path":str(path),"sha256":sha256(wire).hexdigest()},policy=policy,
        handoff_directory=str(handoff),journal_file=str(config.journal_file),state_directory=str(config.state_directory),
        slurm_authority=_slurm_authority_for_config(tmp_path/"slurm-bin",profile,config),profiles=(profile,))
    return module,config,artifact


@pytest.mark.parametrize("pool", ["oldlab","gb10"])
@pytest.mark.parametrize("purpose", ["application-worker","personal-build-worker"])
async def test_typed_runtime_assembles_exact_routes_but_remains_interlocked(tmp_path,pool,purpose):
    module,config,artifact = runtime_inputs(tmp_path,pool,purpose)
    runtime = module.build_typed_executable_runtime(config,artifact,manager_client=object(),
        current_context=artifact.execution,slurm_backend_factory=lambda authority: object())
    try:
        assert runtime.typed_policy == artifact.policy
        assert runtime.profiles == artifact.profiles
        assert runtime.admission.__class__.__name__ == "TypedAdmissionRouter"
        with pytest.raises(RuntimeAssemblyError,match="consumers"):
            await runtime.tick()
        assert runtime.journal.head.sequence == 0
    finally:
        runtime.journal.close()
    with pytest.raises(RuntimeAssemblyError):
        build_executable_runtime(config,artifact,manager_client=object(),current_context=artifact.execution)


@pytest.mark.parametrize("boundary", ["policy","profiles","local-manifest","route-root","route-epoch","context"])
def test_typed_runtime_rejects_drift_before_opening_resources(tmp_path,boundary):
    from pathlib import Path

    module,config,artifact = runtime_inputs(tmp_path,"gb10","personal-build-worker")
    current = artifact.execution
    if boundary == "policy":
        artifact = artifact.model_copy(update={"policy":artifact.policy.model_copy(update={"pool_generation":99})})
    elif boundary == "profiles":
        artifact = artifact.model_copy(update={"profiles":(artifact.profiles[0].model_copy(update={"image_digest":"ghcr.io/qianyi-sun/foreign@sha256:"+"f"*64}),)})
    elif boundary == "local-manifest":
        artifact = artifact.model_copy(update={"immutable_manifest_sha256":"f"*64})
    elif boundary == "context":
        current = current.model_copy(update={"configuration_epoch":99})
    else:
        path = Path(artifact.admission.path)
        if boundary == "route-root":
            path.write_bytes(path.read_bytes()+b" ")
        else:
            routes = TypedAdmissionDirectoryV3.model_validate_json(path.read_bytes())
            changed = routes.model_copy(update={"entries":(routes.entries[0].model_copy(update={"configuration_epoch":99}),)})
            wire = canonical_executable_bytes(changed)
            path.write_bytes(wire)
            artifact = artifact.model_copy(update={"admission":artifact.admission.model_copy(update={"sha256":sha256(wire).hexdigest()})})

    def unexpected(authority):
        pytest.fail("invalid typed inputs must fail before scheduler construction")

    assert not config.journal_file.exists()
    with pytest.raises((ValueError,RuntimeAssemblyError)):
        module.build_typed_executable_runtime(config,artifact,manager_client=object(),current_context=current,
            slurm_backend_factory=unexpected)
    assert not config.journal_file.exists()


@pytest.mark.parametrize("boundary", ["exact","digest","noncanonical","schema","mode"])
def test_typed_artifact_loader_requires_canonical_pinned_owner_file(tmp_path,boundary):
    module,_config,artifact = runtime_inputs(tmp_path,"oldlab","application-worker")
    wire = canonical_executable_bytes(artifact)
    if boundary == "noncanonical":
        wire += b" "
    elif boundary == "schema":
        wire = wire.replace(b'"schema_version":3',b'"schema_version":true',1)
    path = _owner_file(tmp_path/"activation.json",wire)
    if boundary == "mode":
        path.chmod(0o644)
    digest = "f"*64 if boundary == "digest" else sha256(wire).hexdigest()
    if boundary == "exact":
        assert module.load_typed_activation_runtime_artifact(path,expected_sha256=digest) == artifact
        with pytest.raises(RuntimeAssemblyError):
            load_activation_runtime_artifact(path)
    else:
        with pytest.raises((ValueError,RuntimeError,OSError)):
            module.load_typed_activation_runtime_artifact(path,expected_sha256=digest)
