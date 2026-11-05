"""Withdraw-only owner seams, with injected providers and real bounded proof storage."""
import asyncio
import copy
import hashlib
import json
import os
from types import SimpleNamespace

import pytest

from harflow_mcp import resources, state_ownership, store, task_supervision, task_supervisor
from harflow_mcp import task_withdrawal as withdrawal
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.workflow_receipt_documents import encode


@pytest.fixture
def world(tmp_path):
    layout = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(layout):
        log_dir = layout.tasks_directory
        log_dir.mkdir(exist_ok=True)
        info = log_dir.stat()
        owner = {'pid': 81111, 'start_time': 'injected-owner', 'markers': []}
        launch = {'format': task_supervision.FORMAT, 'product': 'harflow', 'product_version': task_supervision.PRODUCT_VERSION, 'v': 1,
            'task_id': 'original', 'generation': 'a' * 32, 'root': str(log_dir.parent),
            'directory_identity': [info.st_dev, info.st_ino], 'directory_binding': {'path': str(log_dir.resolve()), 'inode': info.st_ino},
            'state': 'claimed', 'request_sha256': 'b' * 64, 'configuration_sha256': 'c' * 64, 'backend': 'claude', 'owner': owner,
            'repo_path': str(tmp_path), 'freedom': 'write_in_repo'}
        task_supervision._write(log_dir / 'original.launch.json', launch)
        owners = log_dir.parent / 'workflow-owners'
        owners.mkdir(exist_ok=True)
        task_supervision._write(owners / 'original.json', encode({'workflow_run_id': 'root-workflow', 'root_workflow_run_id': 'root-workflow', 'workflow_execution_id': 'original-execution'}, 'original'))
        registry = SimpleNamespace(log_dir=log_dir, _log_dir=log_dir, state_layout=layout, _owner=owner, _launch_generations={'original': launch['generation']})
        yield registry, launch


def request(registry, launch, **changes):
    return withdrawal.request_withdrawal(registry.log_dir, 'original', launch['generation'], launch['request_sha256'], launch['configuration_sha256'],
        changes.get('replan_id', 'replan'), root_workflow_run_id='root-workflow', source_execution_id='original-execution')


def stopped(registry):
    with pytest.raises(withdrawal.LaunchWithdrawn) as caught:
        withdrawal.check_prestart(registry, 'original')
    return caught.value


def test_owner_ack_is_bound_to_cleanup_and_permanent_identity_retirement(world):
    registry, launch = world
    intent = request(registry, launch)
    assert request(registry, launch) == intent
    proof = stopped(registry)
    ack = withdrawal.settle_withdrawn(registry, 'original', proof)
    assert ack['outcome'] == 'withdrawn'
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) == ack
    assert task_supervision.read_launch(registry.log_dir, 'original')['not_started_reason'] == 'withdrawn'
    assert withdrawal.settle_withdrawn(registry, 'original', proof) == ack
    with pytest.raises(withdrawal.WithdrawalError, match='retired'):
        withdrawal.refuse_retired_task_id(registry.log_dir, 'original')
    withdrawal.refuse_retired_task_id(registry.log_dir, 'distinct-replacement')
    assert not (registry.log_dir / 'original.launch-cancel.json').exists()


def test_positive_ack_cannot_be_forged_from_intent_and_absent_record(world):
    registry, launch = world
    intent = request(registry, launch)
    fake = {key: intent[key] for key in withdrawal.BINDING} | {'format': withdrawal.ACK_FORMAT, 'outcome': 'withdrawn', 'proof': withdrawal._proof(intent, 'owner_prestart')}
    withdrawal._write(withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw-ack'), fake)
    with pytest.raises(withdrawal.WithdrawalError, match='terminal launch proof'):
        withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])


def test_failed_followup_cleanup_never_publishes_positive_ack(world, monkeypatch):
    from harflow_mcp import followups
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], followups={'task_id': 'source', 'ids': ['message']})
    request(registry, launch)
    monkeypatch.setattr(followups, 'finish', lambda *args: False)
    with pytest.raises(withdrawal.WithdrawalError, match='release'):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == 'claimed'


def test_unresolved_resource_borrowers_prevent_positive_ack(world):
    registry, launch = world
    ledger = resources.ResourceLedger(registry.log_dir.parent)
    held = ledger.request(['slot'], {'kind': 'task', 'task_id': 'original'}, reservation_id='old-reservation')
    borrower = ledger.request(['slot'], {'kind': 'task', 'task_id': 'borrower'}, scope=resources.Scope(held.reservation_id), reservation_id='borrowed-reservation')
    assert borrower.held
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], resources={'names': ['slot'], 'reservation_id': held.reservation_id, 'state': 'granted'})
    request(registry, launch)
    proof = stopped(registry)
    with pytest.raises(withdrawal.WithdrawalError, match='pending'):
        withdrawal.settle_withdrawn(registry, 'original', proof)
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    assert ledger.get(held.reservation_id).release_requested
    ledger.release(borrower.reservation_id)
    assert withdrawal.settle_withdrawn(registry, 'original', proof)['outcome'] == 'withdrawn'


def test_ack_write_loss_recovers_only_from_exact_positive_terminal_marker(world, monkeypatch):
    registry, launch = world
    request(registry, launch)
    publish = withdrawal._publish_ack
    def lost(*args):
        raise OSError('Injected acknowledgement write loss')
    monkeypatch.setattr(withdrawal, '_publish_ack', lost)
    with pytest.raises(OSError):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    terminal = task_supervision.read_launch(registry.log_dir, 'original')
    assert terminal['state'] == 'not_started' and terminal['withdrawal_proof']['resources_released'] is True
    monkeypatch.setattr(withdrawal, '_publish_ack', publish)
    ack = withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation'])
    assert ack['outcome'] == 'withdrawn'
    assert withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation']) == ack


@pytest.mark.parametrize('state', ['claimed', 'granted', 'waiting_for_resources'])
def test_record_absence_and_unknown_owner_never_prove_withdrawal(world, monkeypatch, state):
    from harflow_mcp import identity
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state=state)
    request(registry, launch)
    monkeypatch.setattr(identity, 'identity_check', lambda value: 'undecidable')
    assert withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation']) is None
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == state


def test_late_request_after_last_check_is_unknown_and_never_signals(world):
    registry, launch = world
    signals = []
    registry.cancel_cascade = lambda *args, **kwargs: signals.append(args)
    withdrawal.check_prestart(registry, 'original')
    # The injected verification/spawn seam has passed its final prestart check.
    request(registry, launch)
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='spawned')
    assert withdrawal.acknowledge_issued(registry, 'original')['outcome'] == 'issued_or_unknown'
    assert not signals
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])['proof'] is None
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == 'spawned'


async def test_owner_entrypoint_honors_withdrawal_before_prepared_provider(world, monkeypatch):
    registry, launch = world
    raw = json.dumps({'task_id': 'original', 'generation': launch['generation'], 'root': str(registry.log_dir.parent), 'backend': 'unused', 'invocation': {}, 'kwargs': {}, 'grants': []}).encode()
    launch['request_sha256'] = hashlib.sha256(raw).hexdigest()
    configuration = json.loads(raw)
    configuration.pop('generation')
    configuration['kwargs'] = {key: value for key, value in configuration['kwargs'].items() if key != 'dispatch_origin'}
    launch['configuration_sha256'] = hashlib.sha256(json.dumps(configuration, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='reserved', owner=None, request_sha256=launch['request_sha256'], configuration_sha256=launch['configuration_sha256'])
    registry._launch_generations = {}
    request(registry, launch)
    calls = []
    async def execute(*args, **kwargs):
        calls.append('provider')
        pytest.fail('A withdrawn launch must not reach the prepared provider')
    registry._execute_prepared = execute
    monkeypatch.setattr(task_supervisor, 'TaskRegistry', lambda **kwargs: registry)
    (registry.log_dir / 'original.launch.lock').touch()
    read_fd, write_fd = os.pipe()
    os.write(write_fd, raw)
    os.close(write_fd)
    await task_supervisor.run(read_fd, 'original', launch['generation'])
    assert not calls
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])['outcome'] == 'withdrawn'


def test_ordinary_cancel_file_keeps_preexisting_priority(world):
    from harflow_mcp import task_resources
    registry, launch = world
    request(registry, launch)
    task_resources.request_cancel(registry.log_dir, 'original')
    withdrawal.check_prestart(registry, 'original')
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    assert task_resources.cancel_requested(registry.log_dir, 'original')


@pytest.mark.parametrize("ordinary_cancel", [False, True])
async def test_owner_late_withdrawal_after_verification_seam_never_enters_cancel_path(world, monkeypatch, ordinary_cancel):
    from harflow_mcp.backends.base import Invocation
    registry, launch = world
    prepared = task_supervision.invocation_request(Invocation(argv=('injected-provider',)))
    private = {'task_id': 'original', 'generation': launch['generation'], 'root': str(registry.log_dir.parent),
        'backend': 'claude', 'invocation': prepared, 'kwargs': {'repo_path': launch['repo_path']}, 'grants': []}
    raw = json.dumps(private).encode()
    launch['request_sha256'] = hashlib.sha256(raw).hexdigest()
    configuration = json.loads(raw)
    configuration.pop('generation')
    configuration['kwargs'] = {key: value for key, value in configuration['kwargs'].items() if key != 'dispatch_origin'}
    launch['configuration_sha256'] = hashlib.sha256(json.dumps(configuration, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='reserved', owner=None, request_sha256=launch['request_sha256'], configuration_sha256=launch['configuration_sha256'])
    registry._launch_generations = {}
    effects, signals = [], []
    async def execute(*args, **kwargs):
        withdrawal.check_prestart(registry, 'original')
        effects.append('final-check')
        # An explicit intent crosses the same asynchronous verification/spawn gap.
        await asyncio.sleep(0)
        request(registry, launch)
        effects.append('provider-spawn')
        if ordinary_cancel:
            from harflow_mcp import task_resources
            task_resources.request_cancel(registry.log_dir, 'original')
        done = asyncio.Event()
        done.set()
        return SimpleNamespace(task_id='original', done=done, monitor=None, exit_code=0,
            finished_at='2026-11-05T00:00:00Z', status='completed', latest_record_landed=True)
    async def cancel(*args, **kwargs):
        signals.append(args)
    async def capture(*args):
        return None
    registry._execute_prepared = execute
    registry.cancel_cascade = cancel
    monkeypatch.setattr(task_supervisor, 'TaskRegistry', lambda **kwargs: registry)
    monkeypatch.setattr(task_supervisor, '_capture_changes', capture)
    (registry.log_dir / 'original.launch.lock').touch()
    read_fd, write_fd = os.pipe()
    os.write(write_fd, raw)
    os.close(write_fd)
    await task_supervisor.run(read_fd, 'original', launch['generation'])
    assert effects == ['final-check', 'provider-spawn']
    assert signals == ([('original',)] if ordinary_cancel else [])
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])['outcome'] == 'issued_or_unknown'
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == 'settled'


async def test_launch_reservation_cannot_reuse_withdrawn_identity_even_for_same_configuration(world, monkeypatch):
    from harflow_mcp import backends, worktrees
    registry, launch = world
    class Backend:
        name = 'fixture'
        def assert_safe(self, *args):
            return None
    backend = Backend()
    kwargs = {'backend': backend, 'freedom': 'write_in_repo', 'repo_path': launch['repo_path'], 'task_id': 'original'}
    config = {'task_id': 'original', 'root': str(registry.state_layout.root), 'backend': backend.name,
        'invocation': {}, 'grants': [], 'kwargs': {key: value for key, value in kwargs.items() if key != 'backend'}}
    launch['configuration_sha256'] = hashlib.sha256(json.dumps(config, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], configuration_sha256=launch['configuration_sha256'])
    request(registry, launch)
    withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    monkeypatch.setattr(backends, 'get', lambda name: backend)
    monkeypatch.setattr(task_supervision, 'invocation_request', lambda invocation: {})
    def containing(*args):
        return None
    monkeypatch.setattr(worktrees, 'containing_worktree', containing)
    launches = []
    def fake_exec(*args, **kwargs):
        launches.append(args)
        raise OSError('Injected detached-owner exec refusal; no subprocess was started')
    monkeypatch.setattr(task_supervision.subprocess, 'Popen', fake_exec)
    with pytest.raises(withdrawal.WithdrawalError, match='retired'):
        await task_supervision.launch(registry, SimpleNamespace(control_pipe=False), kwargs)
    assert not launches
    assert task_supervision.read_launch(registry.log_dir, 'original')['generation'] == launch['generation']
    with pytest.raises(OSError, match='Injected'):
        await task_supervision.launch(registry, SimpleNamespace(control_pipe=False), {**kwargs, 'task_id': 'distinct-replacement'})
    assert len(launches) == 1
    assert task_supervision.read_launch(registry.log_dir, 'distinct-replacement')['state'] == 'not_started'


@pytest.mark.parametrize('damage', ['duplicate', 'extra', 'oversize', 'boolean-version'])
def test_malformed_exact_generation_intent_fails_closed(world, damage):
    registry, launch = world
    intent = request(registry, launch)
    path = withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw')
    if damage == 'duplicate':
        path.write_text('{"format":"x","format":"y"}')
    elif damage == 'oversize':
        path.write_text(' ' * (withdrawal.MAX_DOCUMENT_BYTES + 1))
    else:
        intent = {**intent, **({'unexpected': True} if damage == 'extra' else {'v': True})}
        path.write_text(json.dumps(intent))
    with pytest.raises((withdrawal.WithdrawalError, ValueError)):
        withdrawal.check_prestart(registry, 'original')
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == 'claimed'
    assert not withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw-ack').exists()


def test_actual_private_configuration_mismatch_never_withdraws_the_owner(world):
    registry, launch = world
    request(registry, launch)
    registry._launch_withdrawal_fingerprints = {'original': {'request_sha256': launch['request_sha256'], 'configuration_sha256': 'e' * 64}}
    withdrawal.check_prestart(registry, 'original')
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    assert task_supervision.read_launch(registry.log_dir, 'original')['state'] == 'claimed'


def in_process_registry(world, monkeypatch):
    from harflow_mcp import backends, command_grants, git_environment, identity, task_outcomes
    from harflow_mcp.backends.base import ExtraPathsUnsupported, Invocation, Enforcement
    from harflow_mcp.tasks import TaskRegistry
    original, launch = world
    class Backend(ExtraPathsUnsupported):
        name = 'withdrawal-in-process-fixture'
        capabilities = backends.get('claude').capabilities._replace(chooses_session_id=False, supports_live_input=False, system_guidance=None)
        def build_start_argv(self, prompt, **kwargs):
            return Invocation(argv=('injected-provider',))
        def assert_safe(self, invocation, freedom, network=None):
            return None
        def enforcement(self, freedom, network=None):
            return Enforcement(freedom=freedom, mechanism='fixture', os_enforced=False, writes_confined=False)
    backend = Backend()
    monkeypatch.setattr(identity, 'own_identity', lambda: original._owner)
    registry = TaskRegistry(log_dir=original.log_dir)
    async def caller():
        return None
    monkeypatch.setattr(registry, '_mutation_caller', caller)
    monkeypatch.setattr(command_grants, 'check_branch_preparation', lambda *args: None)
    monkeypatch.setattr(git_environment, 'auth_status', lambda: 'unknown')
    monkeypatch.setattr(task_outcomes, 'baseline', lambda *args: task_outcomes.empty_baseline())
    return registry, backend


async def test_default_in_process_start_cannot_bypass_permanent_retirement(world, monkeypatch):
    registry, launch = world
    request(registry, launch)
    withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    actual, backend = in_process_registry(world, monkeypatch)
    assert actual.execution_mode == 'in_process'
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(withdrawal.WithdrawalError, match='retired'):
        await actual.start('Exact assignment', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert not effects
    with pytest.raises(OSError, match='Injected provider boundary'):
        await actual.start('Exact assignment', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='new-planned-identity')
    assert len(effects) == 1


async def test_actual_original_owner_before_withdrawal_passes_all_mode_guard(world, monkeypatch):
    original, launch = world
    actual, backend = in_process_registry(world, monkeypatch)
    actual._launch_generations = {'original': launch['generation']}
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected original-owner provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(OSError, match='Injected original-owner'):
        await actual.start('Original assignment', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert len(effects) == 1


async def test_in_process_start_cannot_race_another_pending_launch_owner(world, monkeypatch):
    actual, backend = in_process_registry(world, monkeypatch)
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(withdrawal.WithdrawalError, match='reserved'):
        await actual.start('An unrelated start', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert not effects


async def test_retirement_during_async_verification_refuses_before_in_process_spawn(world, monkeypatch):
    original, launch = world
    actual, backend = in_process_registry(world, monkeypatch)
    actual._launch_generations = {'original': launch['generation']}
    request(original, launch)
    def verify():
        withdrawal.settle_withdrawn(original, 'original', stopped(original))
    monkeypatch.setattr(backend, 'verify_launch_identity', verify)
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(withdrawal.WithdrawalError, match='retired'):
        await actual.start('Original assignment', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert not effects


async def test_in_process_guard_honors_terminal_proof_before_retirement_sidecar_publication(world, monkeypatch):
    original, launch = world
    request(original, launch)
    retire = withdrawal._retire
    def lost(*args):
        raise OSError('Injected retirement publication loss')
    monkeypatch.setattr(withdrawal, '_retire', lost)
    with pytest.raises(OSError, match='publication loss'):
        withdrawal.settle_withdrawn(original, 'original', stopped(original))
    assert not (original.log_dir / 'original.withdraw-retirement.json').exists()
    assert task_supervision.read_launch(original.log_dir, 'original')['withdrawal_proof']
    monkeypatch.setattr(withdrawal, '_retire', retire)
    actual, backend = in_process_registry(world, monkeypatch)
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(withdrawal.WithdrawalError, match='cannot be reused'):
        await actual.start('An old identity must stay retired', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert not effects
    assert withdrawal.reconcile_withdrawal(original.log_dir, 'original', launch['generation'])['outcome'] == 'withdrawn'


async def test_generation_string_alone_cannot_claim_an_unknown_active_launch_owner(world, monkeypatch):
    original, launch = world
    task_supervision.mark(original.log_dir, 'original', launch['generation'], state='reserved', owner=None)
    actual, backend = in_process_registry(world, monkeypatch)
    actual._launch_generations = {'original': launch['generation']}
    actual._owner = None
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(withdrawal.WithdrawalError, match='reserved'):
        await actual.start('A generation string does not grant ownership', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert not effects


async def test_nonwithdrawn_not_started_identity_keeps_ordinary_in_process_reuse(world, monkeypatch):
    original, launch = world
    task_supervision.mark(original.log_dir, 'original', launch['generation'], state='not_started', not_started_reason='owner_error')
    actual, backend = in_process_registry(world, monkeypatch)
    effects = []
    async def spawn(*args, **kwargs):
        effects.append(args)
        raise OSError('Injected ordinary-reuse provider boundary')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    with pytest.raises(OSError, match='Injected ordinary-reuse'):
        await actual.start('Ordinary explicit reuse remains supported', actual.log_dir.parent, backend=backend, freedom='read_only', task_id='original')
    assert len(effects) == 1


def test_persistent_device_drift_preserves_intent_ack_and_exact_request_replay(world, monkeypatch):
    registry, launch = world
    binding = withdrawal._binding
    def older_device(*args):
        result = binding(*args)
        result['directory_identity'][0] += 1000
        return result
    with monkeypatch.context() as context:
        context.setattr(withdrawal, '_binding', older_device)
        original = request(registry, launch)
    assert original['directory_identity'][0] != registry.log_dir.stat().st_dev
    ack = withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    assert ack['outcome'] == 'withdrawn'
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) == ack
    assert request(registry, launch) == original
    assert withdrawal.read_withdrawal(registry.log_dir, 'original', launch['generation']) == original
    with pytest.raises(withdrawal.WithdrawalError, match='binding'):
        request(registry, launch, replan_id='different-replan')


@pytest.mark.parametrize('damage', ['path', 'inode', 'internal-inode'])
def test_durable_binding_still_refuses_wrong_path_or_inode(world, damage):
    registry, launch = world
    original = request(registry, launch)
    withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    broken = copy.deepcopy(original)
    if damage == 'path':
        broken['directory_binding']['path'] += '-another-directory'
    elif damage == 'inode':
        broken['directory_identity'][1] += 1
        broken['directory_binding']['inode'] += 1
    else:
        broken['directory_identity'][1] += 1
    withdrawal._write(withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw'), broken)
    with pytest.raises((withdrawal.WithdrawalError, task_supervision.LaunchDocumentError)):
        withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])
    with pytest.raises((withdrawal.WithdrawalError, task_supervision.LaunchDocumentError)):
        request(registry, launch)


@pytest.mark.parametrize('fault', ['resource-before', 'resource-after', 'followup'])
def test_granted_owner_persists_nospawn_claim_before_cleanup_and_recovers_after_leaf_repair(world, monkeypatch, fault):
    from harflow_mcp import followups, identity
    registry, launch = world
    ledger = resources.ResourceLedger(registry.log_dir.parent)
    held = ledger.request(['slot'], {'kind': 'task', 'task_id': 'original'}, reservation_id='accepted-grant')
    changes = {'state': 'granted', 'resources': {'names': ['slot'], 'reservation_id': held.reservation_id, 'state': 'granted'}}
    if fault == 'followup':
        changes['followups'] = {'task_id': 'message-source', 'ids': ['one-message']}
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], **changes)
    request(registry, launch)
    release = resources.ResourceLedger.release_owned_task
    followup_calls = []
    def failed_release(self, reservation_id, task_id):
        if fault == 'resource-after':
            release(self, reservation_id, task_id)
        raise OSError('Injected grant release failure after accepted-no-spawn')
    def failed_followup(*args):
        followup_calls.append(args)
        return False
    if fault.startswith('resource'):
        monkeypatch.setattr(resources.ResourceLedger, 'release_owned_task', failed_release)
    else:
        monkeypatch.setattr(followups, 'finish', failed_followup)
    with pytest.raises((OSError, withdrawal.WithdrawalError)):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    pending = task_supervision.read_launch(registry.log_dir, 'original')
    assert pending['state'] == 'granted'
    claim = pending['withdrawal_claim']
    assert claim['stage'] == 'accepted_no_spawn' and claim['origin'] == 'owner_prestart'
    assert claim['releases']['resources'] == ('released' if fault == 'followup' else 'pending')
    assert 'withdrawal_proof' not in pending
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    with pytest.raises(withdrawal.WithdrawalError, match='claim'):
        withdrawal.check_execution_identity(registry, 'original')
    # Recovery consumes the actual durable owner promise, not a new owner-death
    # inference about this GRANTED receipt and not an absent TaskRecord alone.
    monkeypatch.setattr(identity, 'identity_check', lambda *args: pytest.fail('A qualified no-spawn claim requires no process/death probe'))
    monkeypatch.setattr(resources.ResourceLedger, 'release_owned_task', release)
    monkeypatch.setattr(followups, 'finish', lambda *args: followup_calls.append(args) or True)
    ack = withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation'])
    assert ack['outcome'] == 'withdrawn' and ack['proof']['origin'] == 'owner_prestart'
    complete = task_supervision.read_launch(registry.log_dir, 'original')
    assert complete['state'] == 'not_started' and complete['not_started_reason'] == 'withdrawn'
    assert complete['withdrawal_claim']['releases'] == {'resources': 'released', 'followups': 'released'}
    assert ledger.get(held.reservation_id) is None
    assert store.read(registry.log_dir, 'original') is None
    assert not (registry.log_dir / 'original.launch-cancel.json').exists()
    assert withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation']) == ack
    if fault == 'followup':
        assert len(followup_calls) == 2


def test_dead_granted_without_owner_nospawn_claim_remains_unknown(world, monkeypatch):
    from harflow_mcp import identity
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='granted')
    request(registry, launch)
    monkeypatch.setattr(identity, 'identity_check', lambda *args: 'dead')
    assert withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation']) is None
    current = task_supervision.read_launch(registry.log_dir, 'original')
    assert current['state'] == 'granted' and 'withdrawal_claim' not in current
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None


@pytest.mark.parametrize('damage', ['digest', 'owner', 'resource-binding', 'release-shape'])
def test_corrupt_owner_claim_never_becomes_a_positive_ack_or_execution_permit(world, monkeypatch, damage):
    from harflow_mcp import followups
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='granted', followups={'task_id': 'source', 'ids': ['message']})
    request(registry, launch)
    monkeypatch.setattr(followups, 'finish', lambda *args: False)
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    current = task_supervision.read_launch(registry.log_dir, 'original')
    claim = current['withdrawal_claim']
    if damage == 'digest':
        claim['intent_sha256'] = '0' * 64
    elif damage == 'owner':
        claim['owner']['pid'] += 1
    elif damage == 'resource-binding':
        claim['cleanup_sha256'] = '0' * 64
    else:
        claim['releases']['resources'] = True
    task_supervision._write(registry.log_dir / 'original.launch.json', current)
    with pytest.raises(task_supervision.LaunchDocumentError, match='claim'):
        withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation'])
    with pytest.raises(task_supervision.LaunchDocumentError, match='claim'):
        withdrawal.check_execution_identity(registry, 'original')
    assert not withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw-ack').exists()


def test_qualified_terminal_proof_without_new_claim_still_recovers_lost_ack(world):
    registry, launch = world
    request(registry, launch)
    original = withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    terminal = task_supervision.read_launch(registry.log_dir, 'original')
    terminal.pop('withdrawal_claim')
    task_supervision._write(registry.log_dir / 'original.launch.json', terminal)
    withdrawal._path(registry.log_dir, 'original', launch['generation'], 'withdraw-ack').unlink()
    assert withdrawal.reconcile_withdrawal(registry.log_dir, 'original', launch['generation']) == original


def test_ordinary_cancel_repairs_dead_waiting_accepted_claim_without_corrupting_it(world, monkeypatch):
    from harflow_mcp import followups, identity, task_resources
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='waiting_for_resources',
        followups={'task_id': 'source', 'ids': ['message']}, resources={'names': ['slot'], 'state': 'waiting'})
    request(registry, launch)
    monkeypatch.setattr(followups, 'finish', lambda *args: False)
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    accepted = task_supervision.read_launch(registry.log_dir, 'original')['withdrawal_claim']
    monkeypatch.setattr(identity, 'identity_check', lambda *args: 'dead')
    refused = task_resources.cancel_launch(registry.log_dir, 'original', authorized=True)
    assert refused['spawned'] is False
    assert refused['cascade']['pending_settled'] == []
    assert refused['cascade']['withdrawal_unsettled'] == ['original']
    assert refused['cascade']['cascade_incomplete'] is True
    assert task_resources.cancel_requested(registry.log_dir, 'original')
    still_pending = task_supervision.read_launch(registry.log_dir, 'original')
    assert still_pending['state'] == 'waiting_for_resources' and still_pending['withdrawal_claim'] == accepted
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation']) is None
    monkeypatch.setattr(followups, 'finish', lambda *args: True)
    completed = task_resources.cancel_launch(registry.log_dir, 'original', authorized=True)
    assert completed['spawned'] is False and completed['cascade']['pending_settled'] == ['original']
    assert not completed['cascade'].get('cascade_incomplete', False)
    assert not task_resources.cancel_requested(registry.log_dir, 'original')
    final = task_supervision.read_launch(registry.log_dir, 'original')
    assert final['state'] == 'not_started' and final['not_started_reason'] == 'withdrawn'
    assert final['withdrawal_claim']['intent_sha256'] == accepted['intent_sha256']
    assert withdrawal.read_ack(registry.log_dir, 'original', launch['generation'])['outcome'] == 'withdrawn'
    assert store.read(registry.log_dir, 'original') is None


def test_ordinary_dead_waiting_cancel_without_withdrawal_claim_remains_unchanged(world, monkeypatch):
    from harflow_mcp import identity, task_resources
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='waiting_for_resources', resources={'names': ['slot'], 'state': 'waiting'})
    monkeypatch.setattr(identity, 'identity_check', lambda *args: 'dead')
    result = task_resources.cancel_launch(registry.log_dir, 'original', authorized=True)
    assert result['spawned'] is False and result['cascade']['pending_settled'] == ['original']
    assert not result['cascade'].get('cascade_incomplete', False)
    assert not task_resources.cancel_requested(registry.log_dir, 'original')
    final = task_supervision.read_launch(registry.log_dir, 'original')
    assert final['state'] == 'not_started' and final['not_started_reason'] == 'cancelled'
    assert 'withdrawal_claim' not in final and 'withdrawal_proof' not in final


def test_ordinary_cancel_rereads_claim_accepted_after_its_waiting_snapshot(world, monkeypatch):
    from harflow_mcp import followups, task_resources
    registry, launch = world
    task_supervision.mark(registry.log_dir, 'original', launch['generation'], state='waiting_for_resources',
        followups={'task_id': 'source', 'ids': ['message']}, resources={'names': ['slot'], 'state': 'waiting'})
    snapshot = task_supervision.read_launch(registry.log_dir, 'original')
    assert 'withdrawal_claim' not in snapshot
    request(registry, launch)
    monkeypatch.setattr(followups, 'finish', lambda *args: False)
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.settle_withdrawn(registry, 'original', stopped(registry))
    task_resources.request_cancel(registry.log_dir, 'original')
    assert task_resources.settle_cancelled(registry.log_dir, snapshot) is False
    current = task_supervision.read_launch(registry.log_dir, 'original')
    assert current['state'] == 'waiting_for_resources' and current['withdrawal_claim']['stage'] == 'accepted_no_spawn'
    assert task_resources.cancel_requested(registry.log_dir, 'original')
    monkeypatch.setattr(followups, 'finish', lambda *args: True)
    assert task_resources.settle_cancelled(registry.log_dir, snapshot) is True
    current = task_supervision.read_launch(registry.log_dir, 'original')
    assert current['not_started_reason'] == 'withdrawn'
    assert not task_resources.cancel_requested(registry.log_dir, 'original')
