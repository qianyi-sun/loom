"""External observation must preserve operator identity across storage admission."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from tests.loom_cli.rollout.operator.test_cnpg_runtime_admission import _runtime
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


@pytest.mark.parametrize('change', [None, 'operator', 'storage', 'plan'])
def test_external_admission_combines_actual_inputs_without_certifying_the_window(tmp_path, monkeypatch, change):
    from loom_cli.rollout.operator import protected_cnpg_external_admission as module

    plan, runtime = _plan(tmp_path), _runtime()
    events = []
    def operator(runner):
        events.append('operator')
        return SimpleNamespace(digest=('f' if change == 'operator' and len(events) > 1 else 'a') * 64)
    def volume(runner, *, runtime):
        events.append('volume')
        if change == 'storage':
            raise ValueError('CNPG volume drift')
        return SimpleNamespace(digest='b' * 64)
    monkeypatch.setattr(module, 'observe_cnpg_operator', operator)
    monkeypatch.setattr(module, 'observe_cnpg_volume', volume)
    if change == 'plan':
        plan = replace(plan, checkpoint_schema_version=2)
    if change:
        with pytest.raises(ValueError, match='CNPG'):
            module.observe_cnpg_external_inputs(plan, runtime, runner=object())
    else:
        first = module.observe_cnpg_external_inputs(plan, runtime, runner=object())
        assert len(first) == 64 and events == ['operator', 'volume', 'operator']
        # The deliberately replaced instance manager is not an external-input
        # change: operator, Pod, storage and containing node remain unchanged.
        replaced = replace(runtime, manager=replace(runtime.manager, executable_inode=runtime.manager.executable_inode + 1))
        assert module.observe_cnpg_external_inputs(plan, replaced, runner=object()) == first
