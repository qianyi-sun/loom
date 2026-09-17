"""Compose actual external CNPG input observations under coordinated exclusion.

This callback provides fresh operator/process/storage evidence, not a claim that
administrator credentials or host operations have been paused. The installed
caller must retain that separately established, issue-scoped authority throughout
classification, mutation and recovery. Inventory digests cannot manufacture it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .protected_cnpg_operator_admission import CNPGOperatorRunner, observe_cnpg_operator
from .protected_cnpg_runtime_admission import CNPGPrimaryRuntime
from .protected_cnpg_volume_admission import CNPGVolumeRunner, observe_cnpg_volume


class CNPGExternalRunner(CNPGOperatorRunner, CNPGVolumeRunner, Protocol):
    pass


def observe_cnpg_external_inputs(
    plan: FinalGatePlan, runtime: CNPGPrimaryRuntime, *, runner: CNPGExternalRunner,
) -> str:
    if (plan.environment != 'staging' or plan.namespace != 'loom-staging'
            or plan.checkpoint_schema_version != 3 or type(runtime) is not CNPGPrimaryRuntime):
        raise ValueError('CNPG external observation scope changed')
    operator = observe_cnpg_operator(runner)
    volume = observe_cnpg_volume(runner, runtime=runtime)
    if observe_cnpg_operator(runner) != operator:
        raise ValueError('CNPG external operator changed during storage observation')
    value = {'schema_version': 1, 'request_id': plan.request_id, 'attempt_number': plan.attempt_number,
             'plan_digest': plan.plan_digest, 'cluster_uid': runtime.cluster_uid,
             'operator': operator.digest, 'volume': volume.digest}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
