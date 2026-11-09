"""Generation-bound prestart withdrawal. This protocol never signals a process.

Only an owner at a qualified prestart seam (or a confirmed dead WAITING owner)
can publish ``withdrawn``. Missing records and late requests alone prove nothing.
Private intents, acknowledgements and retirement proofs are retained, not expired.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from . import store
from .state_access import task_operation
from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line

INTENT_FORMAT = 'harflow.task-withdrawal'
ACK_FORMAT = 'harflow.task-withdrawal-ack'
RETIREMENT_FORMAT = 'harflow.task-withdrawal-retirement'
MAX_DOCUMENT_BYTES = 16 * 1024
BINDING = {'product', 'product_version', 'v', 'root', 'directory_identity', 'directory_binding', 'task_id', 'generation',
           'request_sha256', 'configuration_sha256', 'replan_id', 'root_workflow_run_id', 'source_execution_id'}
CLAIM_KEYS = {'v', 'intent_sha256', 'generation', 'request_sha256', 'configuration_sha256', 'replan_id',
              'root_workflow_run_id', 'source_execution_id', 'origin', 'owner', 'stage', 'cleanup_sha256', 'releases'}
PROOF_KEYS = {'v', 'intent_sha256', 'generation', 'request_sha256', 'configuration_sha256', 'replan_id',
              'root_workflow_run_id', 'source_execution_id', 'origin', 'resources_released', 'followups_released'}


class WithdrawalError(ValueError):
    code = 'withdrawal_unavailable'


class LaunchWithdrawn(Exception):
    """Positive no-spawn control flow, issued only by this owner's prestart check."""
    harflow_not_started = True
    def __init__(self, intent, registry=None, acknowledgement=None):
        super().__init__('withdrawn before the provider started')
        self.intent = intent
        self._registry = registry
        self.acknowledgement = acknowledgement
        self.proof = acknowledgement['proof'] if acknowledgement is not None else None


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _generation(value):
    if type(value) is not str or re.fullmatch('[a-f0-9]{32}', value) is None:
        raise WithdrawalError('Invalid withdrawal generation')
    return value


def _path(log_dir, task_id, generation, suffix):
    store.validate_task_id(task_id)
    _generation(generation)
    return log_dir / f'{task_id}.{generation}.{suffix}.json'


def _binding(log_dir, launch, replan_id, root_workflow_run_id, source_execution_id):
    for identifier in (replan_id, root_workflow_run_id, source_execution_id):
        store.validate_task_id(identifier)
    info = log_dir.stat()
    return {'product': PRODUCT, 'product_version': PRODUCT_VERSION, 'v': 1, 'root': str(log_dir.parent),
            'directory_identity': [info.st_dev, info.st_ino], 'directory_binding': {'path': str(log_dir.resolve()), 'inode': info.st_ino},
            **{key: launch[key] for key in ('task_id', 'generation', 'request_sha256', 'configuration_sha256')},
            'replan_id': replan_id, 'root_workflow_run_id': root_workflow_run_id, 'source_execution_id': source_execution_id}


def _validate_binding(value, log_dir, task_id, generation):
    if (value.get('product') != PRODUCT or type(value.get('v')) is not int or value['v'] != 1
            or not compatible_line(value.get('product_version'), PRODUCT_VERSION)):
        raise WithdrawalError('Incompatible withdrawal document')
    if value.get('task_id') != task_id or value.get('generation') != _generation(generation):
        raise WithdrawalError('Withdrawal task/generation binding differs')
    for key in ('request_sha256', 'configuration_sha256'):
        if type(value.get(key)) is not str or re.fullmatch('[a-f0-9]{64}', value[key]) is None:
            raise WithdrawalError('Invalid withdrawal fingerprint')
    for key in ('replan_id', 'root_workflow_run_id', 'source_execution_id'):
        store.validate_task_id(value.get(key))
    identity = value.get('directory_identity')
    binding = value.get('directory_binding')
    if (type(identity) is not list or len(identity) != 2 or any(type(item) is not int for item in identity)
            or type(binding) is not dict or set(binding) != {'path', 'inode'} or type(binding.get('path')) is not str or type(binding.get('inode')) is not int):
        raise WithdrawalError('Malformed withdrawal directory binding')
    info = log_dir.stat()
    # Device numbers are runtime disclosure, not durable authority after a remount.
    if (value.get('root') != str(log_dir.parent) or identity[1] != binding['inode']
            or binding != {'path': str(log_dir.resolve()), 'inode': info.st_ino}):
        raise WithdrawalError('Withdrawal selected root/task directory identity differs')


def _read(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_DOCUMENT_BYTES:
            raise WithdrawalError('Withdrawal document is not a bounded regular file')
        from .observation_budget import current
        budget = current.get()
        with os.fdopen(os.dup(fd), 'rb', buffering=0 if budget is not None else -1) as stream:
            if budget is not None:
                budget.charge(MAX_DOCUMENT_BYTES + 1)
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
        after = path.lstat()
        identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
        if identity(before) != identity(os.fstat(fd)) or identity(before) != identity(after) or len(raw) > MAX_DOCUMENT_BYTES:
            raise WithdrawalError('Withdrawal document changed or exceeded its read bound')
    finally:
        os.close(fd)
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise WithdrawalError('Duplicate withdrawal field')
            result[key] = item
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda value: (_ for _ in ()).throw(WithdrawalError('Nonfinite withdrawal value')))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise WithdrawalError('Malformed withdrawal document') from exc
    if type(value) is not dict:
        raise WithdrawalError('Withdrawal document must be an object')
    return value


def _write(path, value):
    if len(_canonical(value)) > MAX_DOCUMENT_BYTES:
        raise WithdrawalError('Withdrawal document exceeds write bound')
    from .task_supervision import _write as atomic_write
    atomic_write(path, value)


@contextmanager
def _lock(log_dir, task_id, generation):
    path = _path(log_dir, task_id, generation, 'withdraw').with_suffix('.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise WithdrawalError('Withdrawal lock is not a regular file')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


@task_operation
def read_withdrawal(log_dir: Path, task_id: str, generation: str):
    value = _read(_path(log_dir, task_id, generation, 'withdraw'))
    if value is None:
        return None
    if set(value) != BINDING | {'format'} or value.get('format') != INTENT_FORMAT:
        raise WithdrawalError('Invalid withdrawal intent fields')
    _validate_binding(value, log_dir, task_id, generation)
    return value


def _matches(launch, intent):
    return launch is not None and all(launch.get(key) == intent[key] for key in ('task_id', 'generation', 'request_sha256', 'configuration_sha256'))


def _owner_binding(log_dir, intent):
    from .workflow_receipt_io import read
    receipt, _ = read(log_dir.parent / 'workflow-owners' / (intent['task_id'] + '.json'))
    if (receipt.get('root_workflow_run_id', receipt.get('workflow_run_id')) != intent['root_workflow_run_id']
            or receipt.get('workflow_execution_id') != intent['source_execution_id']):
        raise WithdrawalError('Withdrawal workflow root/execution owner differs')


@task_operation
def request_withdrawal(log_dir: Path, task_id: str, generation: str, request_sha256: str, configuration_sha256: str,
                       replan_id: str, *, root_workflow_run_id: str, source_execution_id: str):
    """Publish one immutable internal intent. Caller separately authorizes the replan."""
    from .task_supervision import read_launch
    with _lock(log_dir, task_id, generation):
        launch = read_launch(log_dir, task_id)
        if launch is None or (launch['generation'], launch['request_sha256'], launch.get('configuration_sha256')) != (generation, request_sha256, configuration_sha256):
            raise WithdrawalError('Withdrawal source generation/fingerprints changed')
        previous = read_withdrawal(log_dir, task_id, generation)
        if previous is not None:
            expected = {'replan_id': replan_id, 'root_workflow_run_id': root_workflow_run_id, 'source_execution_id': source_execution_id}
            if not _matches(launch, previous) or any(previous[key] != item for key, item in expected.items()):
                raise WithdrawalError('Withdrawal intent already has a different replan binding')
            _owner_binding(log_dir, previous)
            return previous  # Preserve the original device disclosure and intent digest.
        value = {'format': INTENT_FORMAT, **_binding(log_dir, launch, replan_id, root_workflow_run_id, source_execution_id)}
        _validate_binding(value, log_dir, task_id, generation)
        _owner_binding(log_dir, value)
        _write(_path(log_dir, task_id, generation, 'withdraw'), value)
        return value


def _cleanup_digest(launch):
    held = launch.get('resources') or {}
    resource = {'reservation_id': held['reservation_id'], 'names': held.get('names')} if held.get('reservation_id') else None
    return _digest({'resources': resource, 'followups': launch.get('followups')})


def _claim(intent, launch, origin, releases=None):
    held = launch.get('resources') or {}
    return {'v': 1, 'intent_sha256': _digest(intent),
            **{key: intent[key] for key in ('generation', 'request_sha256', 'configuration_sha256', 'replan_id', 'root_workflow_run_id', 'source_execution_id')},
            'origin': origin, 'owner': launch['owner'], 'stage': 'accepted_no_spawn', 'cleanup_sha256': _cleanup_digest(launch),
            'releases': releases or {'resources': 'pending' if held.get('reservation_id') else 'released',
                                    'followups': 'pending' if launch.get('followups') else 'released'}}


def validate_nospawn_claim(log_dir, task_id, launch, claim):
    """An irrevocable owner promise is distinct from a completed cleanup proof."""
    if (type(launch) is not dict or type(claim) is not dict or set(claim) != CLAIM_KEYS or type(claim.get('v')) is not int or claim['v'] != 1
            or type(claim.get('origin')) is not str or claim['origin'] not in {'owner_prestart', 'dead_waiting_owner'}
            or claim.get('stage') != 'accepted_no_spawn' or type(claim.get('owner')) is not dict
            or claim['owner'] != launch.get('owner') or type(claim.get('releases')) is not dict
            or set(claim['releases']) != {'resources', 'followups'}
            or any(type(value) is not str or value not in {'pending', 'released'} for value in claim['releases'].values())
            or launch.get('state') not in {'reserved', 'claimed', 'waiting_for_resources', 'granted', 'not_started'}
            or launch.get('state') == 'not_started' and launch.get('not_started_reason') != 'withdrawn'):
        raise WithdrawalError('Invalid accepted-no-spawn withdrawal claim')
    intent = read_withdrawal(log_dir, task_id, launch['generation'])
    if intent is None or not _matches(launch, intent) or claim != _claim(intent, launch, claim['origin'], claim['releases']):
        raise WithdrawalError('Withdrawal no-spawn claim differs from its exact source and cleanup bindings')
    return intent


def _ensure_claim(log_dir, intent, owner, origin):
    from .task_supervision import read_launch, read_task_record, mark
    task_id, generation = intent['task_id'], intent['generation']
    with _lock(log_dir, task_id, generation):
        current = read_launch(log_dir, task_id)
        if not _matches(current, intent) or current.get('owner') != owner:
            raise WithdrawalError('Withdrawal owner/source changed before its no-spawn claim')
        if read_task_record(log_dir, task_id, include_prompt=False) is not None:
            raise WithdrawalError('A provider record prevents a no-spawn withdrawal claim')
        existing = current.get('withdrawal_claim')
        if existing is not None:
            validate_nospawn_claim(log_dir, task_id, current, existing)
            return current
        permitted = {'reserved', 'claimed', 'waiting_for_resources', 'granted'} if origin == 'owner_prestart' else {'waiting_for_resources'}
        if current['state'] not in permitted or type(owner) is not dict:
            raise WithdrawalError('Source is outside the qualified no-spawn claim seam')
        _owner_binding(log_dir, intent)
        mark(log_dir, task_id, generation, withdrawal_claim=_claim(intent, current, origin))
        return read_launch(log_dir, task_id)


def _release_resource(log_dir, task_id, launch, grant=None):
    from .task_resources import ledger_for
    saved = launch.get('resources') or {}
    if grant is not None and (grant.get('reservation_id') != saved.get('reservation_id') or grant.get('names') != saved.get('names')):
        raise WithdrawalError('Withdrawal resource grant differs from the launch reservation')
    held = grant or saved
    if held.get('reservation_id'):
        disposition = ledger_for(log_dir).release_owned_task(held['reservation_id'], task_id)
        if disposition not in {'released', 'absent'}:
            raise WithdrawalError('Withdrawal resource release is still pending or uncertain')


def _release_followup(log_dir, task_id, launch, request=None):
    if request is not None and request.get('followups') != launch.get('followups'):
        raise WithdrawalError('Withdrawal follow-up handoff differs from the launch reservation')
    handed = launch.get('followups')
    if handed:
        from .followups import finish
        if finish(log_dir, handed['task_id'], task_id, handed['ids'], 'released') is not True:
            raise WithdrawalError('Follow-up release was not durably confirmed')


def _released(log_dir, intent, leaf):
    from .task_supervision import read_launch, mark
    task_id, generation = intent['task_id'], intent['generation']
    with _lock(log_dir, task_id, generation):
        current = read_launch(log_dir, task_id)
        claim = current.get('withdrawal_claim')
        validate_nospawn_claim(log_dir, task_id, current, claim)
        if not _matches(current, intent):
            raise WithdrawalError('Withdrawal cleanup source changed')
        releases = dict(claim['releases'], **{leaf: 'released'})
        mark(log_dir, task_id, generation, withdrawal_claim=dict(claim, releases=releases))
    return read_launch(log_dir, task_id)


def _complete_claim(log_dir, intent, request=None, grant=None):
    from .task_supervision import read_launch, read_task_record, mark
    task_id, generation = intent['task_id'], intent['generation']
    current = read_launch(log_dir, task_id)
    validate_nospawn_claim(log_dir, task_id, current, current.get('withdrawal_claim'))
    _owner_binding(log_dir, intent)
    if read_task_record(log_dir, task_id, include_prompt=False) is not None:
        raise WithdrawalError('A provider record contradicts the accepted no-spawn claim')
    # Resource release is re-observed even when journaled: a repair may have read
    # the old grant before the claim or final terminal receipt was published.
    _release_resource(log_dir, task_id, current, grant)
    current = _released(log_dir, intent, 'resources')
    if current['withdrawal_claim']['releases']['followups'] != 'released':
        _release_followup(log_dir, task_id, current, request)
        current = _released(log_dir, intent, 'followups')
    if read_task_record(log_dir, task_id, include_prompt=False) is not None:
        raise WithdrawalError('A provider record appeared before withdrawal finalization')
    proof = _proof(intent, current['withdrawal_claim']['origin'])
    changes = {'state': 'not_started', 'not_started_reason': 'withdrawn', 'withdrawal_proof': proof}
    if current.get('resources'):
        changes['resources'] = current['resources'] | {'state': 'not_started'}
    mark(log_dir, task_id, generation, **changes)
    _retire(log_dir, intent, proof)
    _release_resource(log_dir, task_id, read_launch(log_dir, task_id), grant)
    return _publish_ack(log_dir, intent, 'withdrawn', proof)


def _proof(intent, origin):
    return {'v': 1, 'intent_sha256': _digest(intent), **{key: intent[key] for key in ('generation', 'request_sha256', 'configuration_sha256', 'replan_id', 'root_workflow_run_id', 'source_execution_id')},
            'origin': origin, 'resources_released': True, 'followups_released': True}


def validate_retirement_proof(log_dir, task_id, launch, proof):
    """Strict qualification of the optional positive terminal launch marker."""
    if (type(proof) is not dict or set(proof) != PROOF_KEYS or type(proof.get('v')) is not int or proof['v'] != 1
            or type(proof.get('origin')) is not str or proof['origin'] not in {'owner_prestart', 'dead_waiting_owner'}
            or proof.get('resources_released') is not True or proof.get('followups_released') is not True
            or launch.get('state') != 'not_started' or launch.get('not_started_reason') != 'withdrawn'):
        raise WithdrawalError('Invalid positive withdrawal retirement proof')
    claim = launch.get('withdrawal_claim')
    if claim is not None:
        validate_nospawn_claim(log_dir, task_id, launch, claim)
        if claim['origin'] != proof['origin'] or any(value != 'released' for value in claim['releases'].values()):
            raise WithdrawalError('Final withdrawal proof precedes its accepted cleanup journal')
    intent = read_withdrawal(log_dir, task_id, launch['generation'])
    if intent is None or not _matches(launch, intent) or proof != _proof(intent, proof['origin']):
        raise WithdrawalError('Withdrawal retirement proof lacks its exact intent/fingerprint')
    return intent


@task_operation
def read_ack(log_dir: Path, task_id: str, generation: str):
    value = _read(_path(log_dir, task_id, generation, 'withdraw-ack'))
    if value is None:
        return None
    if set(value) != BINDING | {'format', 'outcome', 'proof'} or value.get('format') != ACK_FORMAT:
        raise WithdrawalError('Invalid withdrawal acknowledgement fields')
    _validate_binding(value, log_dir, task_id, generation)
    intent = read_withdrawal(log_dir, task_id, generation)
    if intent is None or any(value[key] != intent[key] for key in BINDING):
        raise WithdrawalError('Withdrawal acknowledgement differs from intent')
    if value['outcome'] == 'withdrawn':
        if type(value.get('proof')) is not dict or set(value['proof']) != PROOF_KEYS or type(value['proof'].get('v')) is not int or type(value['proof'].get('origin')) is not str:
            raise WithdrawalError('Invalid withdrawn acknowledgement proof')
        expected = _proof(intent, value['proof'].get('origin'))
        if expected['origin'] not in {'owner_prestart', 'dead_waiting_owner'} or value['proof'] != expected:
            raise WithdrawalError('Invalid withdrawn acknowledgement proof')
        from .task_supervision import read_launch, read_task_record
        launch = read_launch(log_dir, task_id)
        if not _matches(launch, intent) or launch.get('withdrawal_proof') != value['proof']:
            raise WithdrawalError('Acknowledgement has no exact positive terminal launch proof')
        validate_retirement_proof(log_dir, task_id, launch, launch['withdrawal_proof'])
        _owner_binding(log_dir, intent)
        held = launch.get('resources') or {}
        if held.get('reservation_id'):
            from .task_resources import ledger_for
            if ledger_for(log_dir).get(held['reservation_id']) is not None:
                raise WithdrawalError('Withdrawn acknowledgement resources have not positively released')
        if read_task_record(log_dir, task_id, include_prompt=False) is not None:
            raise WithdrawalError('Withdrawn acknowledgement contradicts a task record')
        retired = _read(log_dir / (task_id + '.withdraw-retirement.json'))
        expected_retirement = {key: intent[key] for key in BINDING} | {'format': RETIREMENT_FORMAT, 'proof': value['proof']}
        if retired is not None:
            _validate_retirement(log_dir, task_id, retired)
        if retired != expected_retirement:
            raise WithdrawalError('Withdrawn acknowledgement lacks its permanent identity retirement proof')
    elif value['outcome'] != 'issued_or_unknown' or value['proof'] is not None:
        raise WithdrawalError('Unknown withdrawal acknowledgement outcome')
    return value


def _publish_ack(log_dir, intent, outcome, proof=None):
    value = {key: intent[key] for key in BINDING} | {'format': ACK_FORMAT, 'outcome': outcome, 'proof': proof}
    previous = read_ack(log_dir, intent['task_id'], intent['generation'])
    if previous is not None:
        if previous != value:
            raise WithdrawalError('Withdrawal acknowledgement is already terminal with a different outcome')
        return previous
    _write(_path(log_dir, intent['task_id'], intent['generation'], 'withdraw-ack'), value)
    return read_ack(log_dir, intent['task_id'], intent['generation'])


def check_prestart(registry, task_id):
    """Called only at actual owner prestart seams, never inferred from a record absence."""
    from .task_supervision import read_launch
    from .task_resources import cancel_requested
    if cancel_requested(registry.log_dir, task_id):
        return  # Existing ordinary cancellation keeps its original semantics.
    generation = getattr(registry, '_launch_generations', {}).get(task_id)
    if generation is None:
        return
    intent = read_withdrawal(registry.log_dir, task_id, generation)
    if intent is None:
        return
    launch = read_launch(registry.log_dir, task_id)
    if not _matches(launch, intent):
        return  # A valid but stale fingerprint cannot withdraw this launch.
    actual = getattr(registry, '_launch_withdrawal_fingerprints', {}).get(task_id)
    if actual is not None and any(actual.get(key) != intent[key] for key in ('request_sha256', 'configuration_sha256')):
        return  # The current private authorization, not metadata, decides the owner binding.
    _owner_binding(registry.log_dir, intent)
    if type(registry._owner) is not dict or launch.get('owner') != registry._owner:
        raise WithdrawalError('Withdrawal owner generation cannot be qualified')
    if launch['state'] in {'spawned', 'settled'}:
        acknowledge_issued(registry, task_id)
        raise WithdrawalError('Withdrawal arrived after provider issuance')
    if launch['state'] == 'not_started' and launch.get('not_started_reason') != 'withdrawn':
        raise WithdrawalError('Launch already ended for a different reason')
    raise LaunchWithdrawn(intent, registry)


def _release(log_dir, task_id, launch, request=None, grant=None):
    _release_resource(log_dir, task_id, launch, grant)
    _release_followup(log_dir, task_id, launch, request)


def _retire(log_dir, intent, proof):
    path = log_dir / (intent['task_id'] + '.withdraw-retirement.json')
    value = {key: intent[key] for key in BINDING} | {'format': RETIREMENT_FORMAT, 'proof': proof}
    prior = _read(path)
    if prior is not None and prior != value:
        raise WithdrawalError('Task identity already has a contradictory retirement proof')
    if prior is None:
        _write(path, value)


def settle_withdrawn(registry, task_id, exception, request=None, grant=None):
    """Owner-only positive ack after strict resources/followups and launch settlement."""
    from .task_supervision import read_launch, read_task_record, mark
    if not isinstance(exception, LaunchWithdrawn) or exception._registry is not registry:
        raise WithdrawalError('Withdrawal no-spawn proof is not from this owner')
    intent = exception.intent
    generation = getattr(registry, '_launch_generations', {}).get(task_id)
    launch = read_launch(registry.log_dir, task_id)
    if generation != intent['generation'] or not _matches(launch, intent) or type(registry._owner) is not dict or launch.get('owner') != registry._owner:
        raise WithdrawalError('Withdrawal launch or owner changed before settlement')
    actual = getattr(registry, '_launch_withdrawal_fingerprints', {}).get(task_id)
    if actual is not None and any(actual.get(key) != intent[key] for key in ('request_sha256', 'configuration_sha256')):
        raise WithdrawalError('Withdrawal differs from the actual owner authorization')
    if launch['state'] not in {'reserved', 'claimed', 'waiting_for_resources', 'granted'} and not (launch['state'] == 'not_started' and launch.get('withdrawal_proof')):
        raise WithdrawalError('Launch advanced beyond the qualified prestart withdrawal seam')
    if read_task_record(registry.log_dir, task_id, include_prompt=False) is not None:
        raise WithdrawalError('A task record appeared; withdrawal cannot prove no spawn')
    _owner_binding(registry.log_dir, intent)
    saved = launch.get('resources') or {}
    if grant is not None and (grant.get('reservation_id') != saved.get('reservation_id') or grant.get('names') != saved.get('names')):
        raise WithdrawalError('Withdrawal resource grant differs from the accepted source')
    if request is not None and request.get('followups') != launch.get('followups'):
        raise WithdrawalError('Withdrawal follow-up handoff differs from the accepted source')
    if launch.get('withdrawal_proof'):
        return reconcile_withdrawal(registry.log_dir, task_id, generation)
    _ensure_claim(registry.log_dir, intent, registry._owner, 'owner_prestart')
    return _complete_claim(registry.log_dir, intent, request, grant)


def acknowledge_issued(registry, task_id):
    """Late withdrawal is observation only: never signals, cancellation or replacement."""
    from .task_supervision import read_launch
    generation = getattr(registry, '_launch_generations', {}).get(task_id)
    if generation is None:
        return None
    intent = read_withdrawal(registry.log_dir, task_id, generation)
    launch = read_launch(registry.log_dir, task_id)
    if intent is None or not _matches(launch, intent):
        return None
    return _publish_ack(registry.log_dir, intent, 'issued_or_unknown')


@task_operation
def reconcile_withdrawal(log_dir: Path, task_id: str, generation: str):
    """Recover an ack only from a positive terminal marker or dead WAITING proof."""
    from .task_supervision import read_launch, read_task_record, mark
    from .task_resources import owner_confirmed_gone
    ack = read_ack(log_dir, task_id, generation)
    if ack is not None:
        return ack
    intent = read_withdrawal(log_dir, task_id, generation)
    launch = read_launch(log_dir, task_id)
    if intent is None or not _matches(launch, intent):
        return None
    _owner_binding(log_dir, intent)
    if launch.get('withdrawal_claim'):
        return _complete_claim(log_dir, intent)
    if launch.get('withdrawal_proof'):
        validate_retirement_proof(log_dir, task_id, launch, launch['withdrawal_proof'])
        if read_task_record(log_dir, task_id, include_prompt=False) is not None:
            raise WithdrawalError('Withdrawn launch unexpectedly has a task record')
        _release(log_dir, task_id, launch)
        _retire(log_dir, intent, launch['withdrawal_proof'])
        return _publish_ack(log_dir, intent, 'withdrawn', launch['withdrawal_proof'])
    fresh = owner_confirmed_gone(log_dir, launch, ('waiting_for_resources',))
    if fresh is None or not _matches(fresh, intent):
        return None
    _ensure_claim(log_dir, intent, fresh['owner'], 'dead_waiting_owner')
    return _complete_claim(log_dir, intent)


def _validate_retirement(log_dir, task_id, value):
    if set(value) != BINDING | {'format', 'proof'} or value.get('format') != RETIREMENT_FORMAT:
        raise WithdrawalError('Malformed task withdrawal retirement proof')
    _validate_binding(value, log_dir, task_id, value.get('generation'))
    intent = read_withdrawal(log_dir, task_id, value['generation'])
    proof = value.get('proof')
    if (type(proof) is not dict or set(proof) != PROOF_KEYS or type(proof.get('v')) is not int
            or type(proof.get('origin')) is not str or proof['origin'] not in {'owner_prestart', 'dead_waiting_owner'} or intent is None
            or any(value[key] != intent[key] for key in BINDING) or proof != _proof(intent, proof['origin'])):
        raise WithdrawalError('Task withdrawal retirement binding cannot be qualified')
    return intent


@task_operation
def refuse_retired_task_id(log_dir: Path, task_id: str, launch=None):
    """Retirement is identity-wide, not a permit to reuse an older task's generation."""
    store.validate_task_id(task_id)
    value = _read(log_dir / (task_id + '.withdraw-retirement.json'))
    if value is not None:
        _validate_retirement(log_dir, task_id, value)
        raise WithdrawalError('Task ID was permanently retired by prestart withdrawal; use the linked replacement identity')
    if launch and launch.get('withdrawal_claim'):
        validate_nospawn_claim(log_dir, task_id, launch, launch['withdrawal_claim'])
        raise WithdrawalError('Task ID has an accepted no-spawn withdrawal claim and cannot be reused')
    if launch and launch.get('withdrawal_proof'):
        validate_retirement_proof(log_dir, task_id, launch, launch['withdrawal_proof'])
        raise WithdrawalError('Task ID has a positive withdrawn launch proof and cannot be reused')


def check_execution_identity(registry, task_id):
    """All execution modes honor permanent retirement and an active launch's owner."""
    from .task_supervision import read_launch
    launch = read_launch(registry.log_dir, task_id)
    refuse_retired_task_id(registry.log_dir, task_id, launch)
    if launch is not None and launch['state'] in {'reserved', 'claimed', 'waiting_for_resources', 'granted', 'spawned'}:
        generation = getattr(registry, '_launch_generations', {}).get(task_id)
        if generation != launch['generation'] or type(registry._owner) is not dict or launch.get('owner') is None or launch.get('owner') != registry._owner:
            raise WithdrawalError('Task ID is reserved by another launch owner; use its exact managed execution')
