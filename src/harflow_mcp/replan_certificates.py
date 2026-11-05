"""Permanent bounded dual-index lineage proofs, distinct from replay detail.

A certificate is not a redispatch permit. Both immutable indexes and the exact
permanent owner withdrawal acknowledgement must qualify before use.
"""
from __future__ import annotations
import copy
import json
import os
import re
from pathlib import Path

from .state_access import storage_operation
from .workflow_run_documents import identifier
from .workflow_steering import SteeringError, _digest

FORMAT = 'harflow.replan-lineage'
MAX_BYTES = 128 * 1024
FIELDS = {'format', 'v', 'product', 'product_version', 'root_workflow_run_id', 'selected_root', 'directory_binding', 'replan_id', 'predecessor_execution_id', 'predecessor_task_id', 'queued_turn_id', 'queued_task_id', 'generation', 'request_sha256', 'configuration_sha256', 'assignment_digest', 'resolved_configuration_digest', 'node_id', 'visit_id', 'definition_digest', 'goal_revision', 'attempt_in_visit', 'replacement_execution_id', 'replacement_task_id', 'ledger_epoch', 'candidate', 'assignment', 'withdrawal_ack_digest', 'source_dispatcher_identity', 'source_job_id', 'quiescence', 'source_configuration'}


def _paths(storage, replan_id, predecessor):
    for value in (replan_id, predecessor): identifier(value)
    directory = storage.root / 'replan-lineage'
    return directory / 'replans' / (replan_id + '.json'), directory / 'predecessors' / (predecessor + '.json')


def qualify(storage, value):
    from .replan_schema import validate_quiescence
    from .workflow_run_documents import _candidate, _identity
    from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line
    if type(value) is not dict or set(value) != FIELDS or value['format'] != FORMAT or type(value['v']) is not int or value['v'] != 1 or value['product'] != PRODUCT or not compatible_line(value['product_version'], PRODUCT_VERSION):
        raise SteeringError('replan_source_unavailable', 'Malformed compact replan certificate')
    for key in ('root_workflow_run_id','replan_id','predecessor_execution_id','predecessor_task_id','queued_turn_id','queued_task_id','node_id','visit_id','replacement_execution_id','replacement_task_id','source_job_id'):
        identifier(value[key])
    for key in ('goal_revision','attempt_in_visit','ledger_epoch'):
        if type(value[key]) is not int or value[key] < (1 if key == 'attempt_in_visit' else 0):
            raise SteeringError('replan_source_unavailable', 'Malformed compact replan counters')
    for key in ('assignment_digest','resolved_configuration_digest','withdrawal_ack_digest'):
        if type(value[key]) is not str or re.fullmatch('sha256:[a-f0-9]{64}', value[key]) is None:
            raise SteeringError('replan_source_unavailable', 'Malformed compact replan digest')
    for key, length in (('generation',32),('request_sha256',64),('configuration_sha256',64),('definition_digest',64)):
        if type(value[key]) is not str or re.fullmatch('[a-f0-9]{' + str(length) + '}', value[key]) is None:
            raise SteeringError('replan_source_unavailable', 'Malformed compact replan fingerprint')
    if type(value['source_configuration']) is not dict:
        raise SteeringError('replan_source_unavailable', 'Malformed compact original configuration')
    from .backends import FREEDOMS
    if value['source_configuration'].get('freedom') not in FREEDOMS:
        raise SteeringError('replan_source_unavailable', 'Unknown compact configured freedom')
    _candidate(value['candidate'], 'certificate.candidate')
    _identity(value['source_dispatcher_identity'], 'certificate.dispatcher')
    validate_quiescence(value['quiescence'], value['replan_id'], value['predecessor_execution_id'], value['predecessor_task_id'], value['source_dispatcher_identity'], value['source_job_id'])
    info = (storage.root / 'tasks').stat()
    if value['selected_root'] != str(storage.root) or value['directory_binding'] != {'path': str((storage.root / 'tasks').resolve()), 'inode': info.st_ino}:
        raise SteeringError('replan_source_unavailable', 'Compact replan selected-directory identity changed')
    # The top assignment_digest is the richer engine-issued worker witness;
    # assignment.digest independently binds the immutable assignment snapshot.
    if type(value['assignment']) is not dict or set(value['assignment']) != {'digest'} or value['assignment']['digest'] != value['source_configuration'].get('assignment_digest') or _digest(value['source_configuration']) != value['resolved_configuration_digest'] or value['source_configuration'].get('candidate') != value['candidate']:
        raise SteeringError('replan_source_unavailable', 'Malformed compact immutable assignment')
    cfg = value['source_configuration']
    expected_cfg = {'definition_digest','node_id','node_configuration_digest','goal_revision','goal_digest','candidate','assignment_digest','freedom','network','repo_path','worktree_digest','attempt_grant'}
    if set(cfg) != expected_cfg or cfg['definition_digest'] != value['definition_digest'] or cfg['node_id'] != value['node_id'] or type(cfg['goal_revision']) is not int or cfg['goal_revision'] != value['goal_revision'] or type(cfg['attempt_grant']) is not int or cfg['attempt_grant'] < 0 or type(cfg['repo_path']) is not str or not Path(cfg['repo_path']).is_absolute() or cfg['network'] is not None and type(cfg['network']) is not bool or type(cfg['freedom']) is not str:
        raise SteeringError('replan_source_unavailable', 'Compact configured provenance contradicts its lineage')
    for key in ('node_configuration_digest','goal_digest','assignment_digest','worktree_digest'):
        if type(cfg[key]) is not str or re.fullmatch('sha256:[a-f0-9]{64}', cfg[key]) is None:
            raise SteeringError('replan_source_unavailable', 'Malformed compact configured provenance digest')
    if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode()) > MAX_BYTES:
        raise SteeringError('replan_source_unavailable', 'Compact replan certificate exceeds bound')
    return value


def _read(path, budget=None):
    import stat
    from .steering_output import strict_loads
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BYTES:
            raise SteeringError('replan_source_unavailable', 'Compact index is not a bounded regular document')
        raw = os.read(fd, MAX_BYTES + 1)
        same = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        if len(raw) > MAX_BYTES or same(before) != same(os.fstat(fd)) or same(before) != same(path.lstat()):
            raise SteeringError('replan_source_unavailable', 'Compact index changed during qualification')
    finally:
        os.close(fd)
    if budget is not None:
        budget['remaining'] -= len(raw)
        if budget['remaining'] < 0:
            raise SteeringError('replan_source_unavailable', 'Direct lineage proof qualification exceeds bounded read budget')
    return strict_loads(raw.decode('utf-8'))


@storage_operation
def read(storage, *, replan_id=None, predecessor_execution_id=None, budget=None):
    directory = storage.root / 'replan-lineage'
    if (replan_id is None) == (predecessor_execution_id is None):
        raise ValueError('Select exactly one compact lineage index')
    key = replan_id or predecessor_execution_id
    identifier(key)
    path = directory / ('replans' if replan_id else 'predecessors') / (key + '.json')
    try:
        value = qualify(storage, _read(path, budget))
        left, right = _paths(storage, value['replan_id'], value['predecessor_execution_id'])
        if _read(left, budget) != value or _read(right, budget) != value:
            raise SteeringError('replan_source_unavailable', 'Compact replan indexes disagree')
        if replan_id and value['replan_id'] != replan_id or predecessor_execution_id and value['predecessor_execution_id'] != predecessor_execution_id:
            raise SteeringError('replan_source_unavailable', 'Compact replan index identity differs')
        from .workflow_receipt_io import read as read_owner
        owner, _ = read_owner(storage.owners / (value['predecessor_task_id'] + '.json'))
        if owner.get('workflow_run_id') != value['root_workflow_run_id'] or owner.get('workflow_execution_id') != value['predecessor_execution_id'] or owner.get('replan_lineage') != {'v': 1, 'replan_id': value['replan_id'], 'certificate_digest': _digest(value)}:
            raise SteeringError('replan_source_unavailable', 'Compact certificate contradicts independent durable source ownership binding')
        from .task_withdrawal import read_ack
        from .task_supervision import read_launch, read_task_record
        ack = read_ack(storage.root / 'tasks', value['predecessor_task_id'], value['generation'])
        launch = read_launch(storage.root / 'tasks', value['predecessor_task_id'])
        if (ack is None or _digest(ack) != value['withdrawal_ack_digest'] or ack.get('replan_id') != value['replan_id'] or ack.get('source_execution_id') != value['predecessor_execution_id']
                or ack.get('root_workflow_run_id') != value['root_workflow_run_id'] or ack.get('request_sha256') != value['request_sha256'] or ack.get('configuration_sha256') != value['configuration_sha256']
                or not launch or launch.get('state') != 'not_started' or not launch.get('withdrawal_proof') or read_task_record(storage.root / 'tasks', value['predecessor_task_id'], include_prompt=False) is not None):
            raise SteeringError('replan_source_unavailable', 'Compact replan permanent withdrawal proof is unavailable')
        return copy.deepcopy(value)
    except FileNotFoundError as exc:
        raise SteeringError('replan_source_unavailable', 'Required compact replan index is missing') from exc


@storage_operation
def publish(storage, run, record):
    from .workflow_replan import validate_replacement
    old = next(a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id'])
    new = next(a for a in run['activations'] if a['id'] == record['planned_activation_id'])
    validate_replacement(storage, run, new, live=False)
    w = record['withdrawal_witness']
    info = (storage.root / 'tasks').stat()
    from .wire_contracts import PRODUCT, PRODUCT_VERSION
    value = {'format': FORMAT, 'v': 1, 'product': PRODUCT, 'product_version': PRODUCT_VERSION, 'root_workflow_run_id': run['workflow_run_id'], 'selected_root': str(storage.root), 'directory_binding': {'path': str((storage.root / 'tasks').resolve()), 'inode': info.st_ino},
        'replan_id': record['replan_id'], 'predecessor_execution_id': old['id'], 'predecessor_task_id': w['task_id'], 'queued_turn_id': w['queued_turn_id'], 'queued_task_id': w['queued_task_id'], 'generation': w['launch_generation'], 'request_sha256': w['request_sha256'], 'configuration_sha256': w['configuration_sha256'],
        'assignment_digest': w['assignment_digest'], 'resolved_configuration_digest': w['resolved_configuration_digest'], 'node_id': old['node_id'], 'visit_id': w['visit_id'], 'definition_digest': record['target']['definition_digest'], 'goal_revision': record['target']['goal_revision'],
        'attempt_in_visit': record['attempt_in_visit'], 'replacement_execution_id': new['id'], 'replacement_task_id': record['planned_task_id'], 'ledger_epoch': run['steering']['requests'][record['request_key']]['ledger_epoch'],
        'candidate': {k: copy.deepcopy(v) for k, v in record['candidate'].items() if k != 'fallbacks'}, 'assignment': {'digest': record['source_configuration']['assignment_digest']}, 'source_configuration': copy.deepcopy(record['source_configuration']),
        'withdrawal_ack_digest': _digest(record['withdrawal_ack']), 'source_dispatcher_identity': copy.deepcopy(record['source_dispatcher_identity']), 'source_job_id': record['source_job_id'], 'quiescence': copy.deepcopy(record['quiescence'])}
    qualify(storage, value)
    from .workflows import _write
    for path in _paths(storage, record['replan_id'], old['id']):
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            prior = _read(path)
        except FileNotFoundError:
            prior = None
        if prior is not None and prior != value:
            raise SteeringError('replan_source_unavailable', 'Permanent compact lineage index contradicts the committed edge')
        if prior is None:
            _write(path, value, layout=storage.state_layout)
    from .workflow_receipt_io import read as read_owner
    from .workflow_receipt_documents import encode
    owner_path = storage.owners / (w['task_id'] + '.json')
    owner, _ = read_owner(owner_path)
    expected = {'v': 1, 'replan_id': record['replan_id'], 'certificate_digest': _digest(value)}
    if owner.get('replan_lineage') not in (None, expected):
        raise SteeringError('replan_source_unavailable', 'Source ownership already binds another compact certificate')
    owner['replan_lineage'] = expected
    _write(owner_path, encode(owner, w['task_id']), layout=storage.state_layout)
    return read(storage, replan_id=record['replan_id'])
