#!/usr/bin/env python3
"""messaging.v1 ctl specimens for existing engine behaviour.

Every document is produced by the real `ctl.main` dispatch, the production messaging, follow-up
ledger, workflow ledger and projection code, and the production envelope serializer. Only the
seams named in each case's provenance are controlled: clocks, UUIDs, process identities, caller
detection, seeded qualified records, and (where stated) a replaced `continue_task` or
`workflows._launch`. No agent process is spawned and no provider or CLI runs. Fixtures are
synthetic qualification evidence, not runtime certification.

`--check` regenerates the bundle twice in separate state roots, requires identical bytes, and
compares them with the checked-in files.
"""
import argparse
import asyncio
import contextlib
import datetime
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import tempfile
import types
import uuid
from unittest.mock import patch

from harflow_mcp import ctl, followup_documents, followups, identity, lineage, state_ownership, store, task_supervision
from harflow_mcp import server, task_continuation_stop, tasks, workflow_messages, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope
from harflow_mcp.workflow_receipt_documents import encode as encode_receipt

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
STAMP = '2026-10-15T00:00:00+00:00'
EPOCH = datetime.datetime(2026, 10, 15, tzinfo=datetime.timezone.utc)
CAPABILITY = 'messaging.v1'
TASK = 'a' * 32
SUCCESSOR = 'b' * 32
RUN = 'c' * 32
NODE_TASK = 'd' * 32
LATER_TASK = 'e' * 32
ORCHESTRATOR_TASK = 'f' * 32
OWN = dict(pid=4242, start_time='fixture-own', markers=[])
LIVE_PIDS = {6000, 6001}
SEAM_BASE = ('Controlled seams: isolated qualified state root (redacted to /workspace); fixed clocks (task duration, ledger, workflow and continuation-stop observation stamps), deterministic UUIDs, '
             'pinned own process identity and process liveness probes, human caller detection; seeded qualified records; follow-up ledger state rewritten through the production follow-up writer and workflow owner receipts written through task_supervision, where stated. ')
SEAMS = {}


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


class FixedDatetime(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        return EPOCH.astimezone(tz) if tz else EPOCH.replace(tzinfo=None)


class Clock:
    """Strictly increasing fake `time` module for workflow ledgers."""

    def __init__(self):
        self.value = 1_000_000.0

    def time(self):
        self.value += 1.0
        return self.value

    def __getattr__(self, name):
        import time
        return getattr(time, name)


class Case:
    """One isolated state root with the controlled seams installed."""

    def __init__(self, base, name):
        self.base, self.name = base, name
        self.layout = resolve_layout(root=base / name)
        self.stack = contextlib.ExitStack()
        self.docs = []

    def __enter__(self):
        stack, layout = self.stack.__enter__(), self.layout
        stack.enter_context(patch.dict(os.environ, dict(HOME=str(self.base), HARFLOW_STATE_ROOT=str(layout.root),
                                                        PATH=os.environ.get('PATH', '')), clear=True))
        stack.enter_context(state_ownership.state_session(layout))
        self.logs = layout.tasks_directory
        self.logs.mkdir(parents=True)
        self.repo = layout.root / 'repo'
        self.repo.mkdir()
        stack.enter_context(patch.object(ctl, 'default_log_dir', return_value=self.logs))
        stack.enter_context(patch.object(lineage, 'detect_caller', return_value=None))
        stack.enter_context(patch.object(lineage, 'detect_caller_detail', return_value=lineage.Detection(None)))
        stack.enter_context(patch.object(identity, 'own_identity', return_value=OWN))
        stack.enter_context(patch.object(identity, 'identity_check', side_effect=lambda value: 'alive' if value and value.get('pid') in LIVE_PIDS else 'dead'))
        stack.enter_context(patch.object(identity, 'check_detail', side_effect=lambda value: ('alive', 'fixture') if value and value.get('pid') in LIVE_PIDS else ('dead', 'fixture')))
        stack.enter_context(patch.object(server, '_registry', None))
        counter, names = itertools.count(1), self.name
        stack.enter_context(patch.object(uuid, 'uuid4', side_effect=lambda: uuid.UUID(bytes=hashlib.sha256(f'{names}-{next(counter)}'.encode()).digest()[:16], version=4)))
        ticks = itertools.count()
        stack.enter_context(patch.object(followups, '_now', side_effect=lambda: (EPOCH + datetime.timedelta(seconds=next(ticks))).isoformat()))
        stack.enter_context(patch.object(store, 'datetime', FixedDatetime))
        stack.enter_context(patch.object(task_continuation_stop, '_now', return_value=STAMP))
        clock = Clock()
        stack.enter_context(patch.object(workflows, 'time', clock))
        stack.enter_context(patch.object(workflow_messages, 'time', clock))
        stack.enter_context(patch.object(workflows.backends, 'is_installed', return_value=True))
        followups._PENDING.clear()
        followups._INFLIGHT.clear()
        followups._SUCCESSOR_CACHE.clear()
        return self

    def __exit__(self, *exc):
        followups._PENDING.clear()
        followups._INFLIGHT.clear()
        followups._SUCCESSOR_CACHE.clear()
        return self.stack.__exit__(*exc)

    def redact(self, doc):
        text = json.dumps(doc)
        for path in sorted({str(self.base), str(self.base.resolve())}, key=len, reverse=True):
            text = text.replace(path, '/workspace')
        assert str(self.base.name) not in text, 'an original state root path remains'
        return json.loads(text)

    def run(self, argv, expect=0):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = ctl.main([*argv, '--json'])
        assert code == expect, (argv[0], code, out.getvalue(), err.getvalue())
        doc = json.loads(out.getvalue())
        validate_envelope(doc, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
        return self.redact(doc)

    def emit(self, name, doc, facts, *, exit_code=0):
        self.docs.append((name, doc, facts, exit_code))

    # --- standalone task records --------------------------------------------------------------

    def record(self, task_id=TASK, **changes):
        value = dict(status='completed', exit_code=0, output_drained=True, finished_at=STAMP, session_id='session-' + task_id[:4],
                     markers=['session-' + task_id[:4]], prompt='Original assignment', max_turns=3, owner=OWN)
        value.update(changes)
        record = store.TaskRecord(task_id, 'claude', value.pop('session_id'), str(self.repo), STAMP, **value)
        assert store.write_landed(self.logs, record)
        return record

    def running(self, task_id=TASK):
        return self.record(task_id, status='running', exit_code=None, output_drained=False, finished_at=None, pid=6000,
                           start_time='fixture-task', owner=dict(pid=6001, start_time='fixture-owner', markers=[]))

    def send(self, text, *extra, task_id=TASK, expect=0):
        return self.run(['send', task_id, text, *extra], expect)

    def ledger(self, change, task_id=TASK):
        """Rewrite the follow-up ledger through the production reader and writer."""
        with followups._locked(self.logs, task_id):
            doc = followups._load(self.logs, task_id)
            change(doc)
            followups._store(self.logs, task_id, doc)

    def status(self, task_id=TASK):
        return self.run(['status', task_id])


def item_of(doc, message_id):
    return next(item for item in doc['items'] if item['id'] == message_id)


# --- standalone scenarios ---------------------------------------------------------------------

def standalone_queued(case):
    case.running()
    doc = case.send('Plain queue acknowledgement for a running task')
    assert doc['result']['status'] == 'queued' and 'target_settled' not in doc['result']
    case.emit('queued-ack', doc, 'running claude task record with a controlled live pid; a real send queues it on the follow-up ledger')


def standalone_target_settled(case):
    case.record()
    doc = case.send('Queued after the task finished')
    result = doc['result']
    assert result['status'] == 'queued' and result['target_settled'] is True and result['target_status'] == 'completed'
    case.emit('queued-target-settled', doc, 'completed task record; a real send reports target_settled, target_status and a note')


def standalone_cancel(case):
    case.record()
    first = case.send('Message to withdraw')['result']['message_id']
    doc = case.run(['cancel-message', TASK, first])
    assert doc['result'] == {'task_id': TASK, 'message_id': first, 'status': 'cancelled'}
    case.emit('cancel', doc, 'completed task record; a real send then a real cancel-message withdraws it while still queued')


def standalone_cancel_refused(case):
    case.record()
    message = case.send('Message the continuation already consumed')['result']['message_id']
    def consume(doc):
        index = next(i for i, item in enumerate(doc['items']) if item['id'] == message)
        doc['items'][index] = followups._final(doc['items'][index] | {'successor_task_id': SUCCESSOR}, followups.CONSUMED, '2026-10-15T00:01:00+00:00')
        doc['sealed_by'] = SUCCESSOR
    case.ledger(consume)
    case.record(SUCCESSOR, continuation_of_task_id=TASK, session_id='session-' + TASK[:4])
    doc = case.run(['cancel-message', TASK, message], expect=1)
    assert doc['error']['code'] == 'not_cancellable' and 'result' not in doc
    case.emit('cancel-refused-not-queued', doc, 'completed task record; the real send is rewritten to consumed (with its successor id) through the production ledger writer, a completed successor record is seeded, then a real cancel-message is refused', exit_code=1)


def standalone_status_queued_cancelled(case):
    case.record()
    keep = case.send('Message that stays queued')['result']['message_id']
    drop = case.send('Message that is withdrawn')['result']['message_id']
    case.run(['cancel-message', TASK, drop])
    doc = case.status()
    messages = {m['id']: m for m in doc['task']['messages']}
    assert messages[keep]['status'] == 'queued' and 'text' in messages[keep]
    assert messages[drop]['status'] == 'cancelled' and 'text' not in messages[drop]
    case.emit('status-queued-cancelled', doc, 'completed task record; two real sends and a real cancel-message, then a real status read. Only the messages and queued_followups entries are the subject')


def standalone_status_reserved(case):
    case.record()
    message = case.send('Handed to a successor that is still starting')['result']['message_id']
    def reserve(doc):
        item_of(doc, message).update(status='reserved', successor_task_id=SUCCESSOR, owner=dict(pid=6001, start_time='fixture-owner', markers=[]), at='2026-10-15T00:01:00+00:00')
        doc['sealed_by'] = SUCCESSOR
    case.ledger(reserve)
    doc = case.status()
    entry = next(m for m in doc['task']['messages'] if m['id'] == message)
    assert entry['status'] == 'delivering' and entry['successor_task_id'] == SUCCESSOR and 'reason' not in entry
    case.emit('status-reserved', doc, 'completed task record; the real send is rewritten to reserved for a successor with no record, owned by a controlled live process, so the production read-time rules leave it delivering. Only the messages and queued_followups entries are the subject')


def standalone_status_delivery_unknown(case):
    case.record()
    message = case.send('Reserved by an owner that stopped')['result']['message_id']
    def reserve(doc):
        item_of(doc, message).update(status='reserved', successor_task_id=SUCCESSOR, owner=dict(pid=7000, start_time='fixture-gone', markers=[]), at='2026-10-15T00:01:00+00:00')
        doc['sealed_by'] = SUCCESSOR
    case.ledger(reserve)
    doc = case.status()
    entry = next(m for m in doc['task']['messages'] if m['id'] == message)
    assert entry['status'] == 'delivering' and entry['reason'] and entry['text'] == 'Reserved by an owner that stopped'
    case.emit('status-delivery-unknown', doc, 'completed task record; the real send is rewritten to reserved by a controlled dead owner with no successor record, and the production read-time rules resolve it to delivery_unknown (delivering with a reason, text kept). Only the messages and queued_followups entries are the subject')


def standalone_status_delivered(case):
    case.record()
    message = case.send('Consumed by the continuation')['result']['message_id']
    def consume(doc):
        index = next(i for i, item in enumerate(doc['items']) if item['id'] == message)
        doc['items'][index] = followups._final(doc['items'][index] | {'successor_task_id': SUCCESSOR}, followups.CONSUMED, '2026-10-15T00:01:00+00:00')
        doc['sealed_by'] = SUCCESSOR
    case.ledger(consume)
    case.record(SUCCESSOR, continuation_of_task_id=TASK, session_id='session-' + TASK[:4])
    doc = case.status()
    entry = next(m for m in doc['task']['messages'] if m['id'] == message)
    assert entry['status'] == 'delivered' and 'text' not in entry and entry['successor_task_id'] == SUCCESSOR
    case.emit('status-delivered', doc, 'completed task record; the real send is rewritten to consumed through the production ledger writer and a completed successor record is seeded. Delivered records keep metadata only (no text). Only the messages and queued_followups entries are the subject')


def standalone_status_superseded(case):
    case.record()
    message = case.send('Queued before another continuation started')['result']['message_id']
    case.record(SUCCESSOR, continuation_of_task_id=TASK, session_id='session-' + TASK[:4])
    doc = case.status()
    entry = next(m for m in doc['task']['messages'] if m['id'] == message)
    assert entry['status'] == 'undeliverable' and SUCCESSOR in entry['reason'] and entry['text']
    case.emit('status-superseded', doc, 'completed task record; a real send is admitted, then a qualified successor record is added; the production read-time rules mark the message undeliverable (superseded, text kept). Only the messages and queued_followups entries are the subject')


def standalone_send_error_superseded(case):
    case.record()
    case.record(SUCCESSOR, continuation_of_task_id=TASK, session_id='session-' + TASK[:4])
    doc = case.send('Too late', expect=1)
    assert doc['error']['code'] == 'superseded'
    case.emit('send-error-superseded', doc, 'completed task with a qualified successor record; a real send is refused', exit_code=1)


def standalone_send_error_too_large(case):
    case.record()
    exact = case.send('x' * 32768)
    assert exact['result']['status'] == 'queued'
    doc = case.send('x' * 32769, expect=1)
    assert doc['error']['code'] == 'message_too_large'
    case.emit('send-error-too-large', doc, 'completed task record; a 32,769 byte message is refused (an exactly 32 KiB message is accepted in the same scenario)', exit_code=1)


def standalone_send_error_too_many(case):
    case.record()
    for number in range(16):
        case.send(f'Outstanding message {number + 1}')
    doc = case.send('The seventeenth outstanding message', expect=1)
    assert doc['error']['code'] == 'queue_full'
    case.emit('send-error-too-many', doc, 'completed task record with sixteen real outstanding messages; the seventeenth send is refused', exit_code=1)


def with_continue(case, replacement):
    return patch.object(tasks.TaskRegistry, 'continue_task', replacement)


def standalone_now_started(case):
    case.record()
    calls = []
    async def continued(registry, task_id, **kwargs):
        calls.append((task_id, kwargs))
        return types.SimpleNamespace(task_id=SUCCESSOR)
    with with_continue(case, continued):
        doc = case.send('Continue now', '--delivery', 'now')
    assert calls == [(TASK, {'consume_followups': True})]
    result = doc['result']
    assert result['status'] == 'delivering' and result['successor_task_id'] == SUCCESSOR and result['delivery'] == 'now'
    case.emit('now-continuation-started', doc, 'completed task record; TaskRegistry.continue_task is replaced at class level by a stub that returns a successor id (called with consume_followups=True). This is a synthetic continuation return: nothing is reserved, launched or spawned, so it is not proof of a real reservation and is never paired with a status read')


def standalone_now_refused(case):
    case.record(prompt='truncated preview', prompt_truncated=True, prompt_source=f'{TASK}.prompt.{"0" * 64}.txt')
    doc = case.send('Continue now', '--delivery', 'now')
    result = doc['result']
    assert result['status'] == 'queued' and result['refusal']['code'] == 'continuation_refused', result
    assert followups.view(case.logs, TASK)['items'][0]['status'] == 'queued'
    case.emit('now-continuation-refused', doc, 'completed task record whose full prompt source is absent; the real continue_task refuses under its continuation lock before any reservation, and the message stays queued')


def standalone_now_uncertain(case):
    case.record()
    async def fault(registry, task_id, **kwargs):
        try:
            await asyncio.to_thread(followups._reserve, case.logs, task_id, SUCCESSOR, OWN)
        finally:
            followups._INFLIGHT.discard(followups._key(case.logs, SUCCESSOR))
        raise RuntimeError('controlled launch fault after reservation')
    with with_continue(case, fault):
        doc = case.send('Continue now', '--delivery', 'now')
    result = doc['result']
    assert result['status'] == 'delivering' and result['refusal']['code'] == 'continuation_uncertain', result
    case.emit('now-continuation-uncertain', doc, 'completed task record; TaskRegistry.continue_task is replaced at class level by a fault that reserves the queued message through the real follow-up reservation and then raises, so the item is no longer queued. Synthetic fault, not a real launch failure')


# --- workflow scenarios -----------------------------------------------------------------------

DEFINITION = {'name': 'fixture', 'orchestrator': {'backend': 'codex'}, 'nodes': [
    {'id': 'start', 'type': 'start', 'prompt': 'fixture'},
    {'id': 'work', 'type': 'agent', 'instructions': 'fixture', 'agent': {'backend': 'codex'}},
    {'id': 'end', 'type': 'end'}],
    'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'}, {'id': 'finish', 'source': 'work', 'target': 'end'}]}


class Runs:
    """Runs created through the production WorkflowStore; `_launch` is replaced so no supervisor or agent starts."""

    def __init__(self, case):
        self.case = case
        case.stack.enter_context(patch.object(workflows, '_launch', side_effect=lambda *args: None))
        self.storage = workflows.WorkflowStore(case.layout.root)

    def create(self, status='running', kind='workflow', **changes):
        run = self.storage.create_run(workflows.validate_definition(DEFINITION), 'fixture', self.case.repo, run_id=RUN, kind=kind)
        self.storage.update_run(RUN, lambda r: r.update(status=status, **changes), 'fixture_state')
        return run

    def attempt(self, task_id, status, *, node_id='work', role='node', activation=None):
        def add(run):
            run['activations'].append({'id': activation or 'activation-' + task_id[:4], 'node_id': node_id, 'role': role, 'status': status, 'created_at': 0.0,
                                       'tasks': [{'task_id': task_id, 'candidate': {'backend': 'codex'}, 'status': status}]})
        self.storage.update_run(RUN, add, 'fixture_attempt')
        self.case.record(task_id, status='completed')
        owners = self.case.layout.root / 'workflow-owners'
        owners.mkdir(exist_ok=True)
        task_supervision._write(owners / f'{task_id}.json', encode_receipt(dict(
            workflow_run_id=RUN, root_workflow_run_id=RUN, workflow_execution_id=activation or 'activation-' + task_id[:4]), task_id))

    def claim(self, task_id, status, *, node_id='work', role='node', activation=None):
        """The production dispatch claim: messages become delivering in the same mutation that reserves the attempt."""
        taken = []
        identifier = activation or 'activation-' + task_id[:4]
        def take(run):
            entry = {'id': identifier, 'node_id': node_id, 'role': role, 'status': status, 'created_at': 0.0,
                     'tasks': [{'task_id': task_id, 'candidate': {'backend': 'codex'}, 'status': 'reserved'}]}
            run['activations'].append(entry)
            taken.extend(workflow_messages.claim(run, entry, task_id))
        self.storage.update_run(RUN, take, 'fixture_dispatch_reserved')
        if store.read(self.case.logs, task_id, include_prompt=False) is None:
            self.case.record(task_id, status='completed')
        owners = self.case.layout.root / 'workflow-owners'
        owners.mkdir(exist_ok=True)
        task_supervision._write(owners / f'{task_id}.json', encode_receipt(dict(
            workflow_run_id=RUN, root_workflow_run_id=RUN, workflow_execution_id=identifier), task_id))
        return taken

    def settle(self, task_id, status):
        def mark(run):
            for activation in run['activations']:
                for task in activation['tasks']:
                    if task['task_id'] == task_id:
                        task['status'] = activation['status'] = status
        self.storage.update_run(RUN, mark, 'fixture_attempt_settled')

    def send(self, text, *extra, expect=0):
        return self.case.run(['workflow-send', RUN, text, *extra], expect)


def workflow_send(case):
    runs = Runs(case)
    runs.create()
    doc = runs.send('Context for the next node attempt')
    result = doc['result']
    assert result['status'] == 'queued' and result['delivery'] == 'queue' and 'target_settled' not in result
    case.emit('workflow-send', doc, 'running workflow run created through the production WorkflowStore; a real workflow-send queues a workflow-level message (workflows._launch replaced so no supervisor starts)')


def workflow_cancel(case):
    runs = Runs(case)
    runs.create()
    message = runs.send('Message to withdraw')['result']['message_id']
    doc = case.run(['workflow-cancel-message', RUN, message])
    assert doc['result'] == {'workflow_run_id': RUN, 'message_id': message, 'status': 'cancelled'}
    case.emit('workflow-cancel', doc, 'running workflow run; a real workflow-send then a real workflow-cancel-message (workflows._launch replaced)')


def workflow_settled(status, name, expect_settled, kind='workflow'):
    def build(case):
        runs = Runs(case)
        runs.create(status=status, kind=kind)
        doc = runs.send('Queued for a finished run')
        result = doc['result']
        assert ('target_settled' in result) is expect_settled, result
        if expect_settled:
            assert result['status'] == 'undeliverable' and result['target_status'] == status and result['reason']
        else:
            assert result['status'] == 'queued'
        case.emit(name, doc, f'{status} {kind} run whose status is seeded through the production WorkflowStore; a real workflow-send reports ' + ('target_settled, target_status, reason and note' if expect_settled else 'no target_settled fields because the run can still consume a message') + ' (workflows._launch replaced)')
    build.__name__ = 'workflow_' + name.replace('-', '_')
    return build


def workflow_now_terminal(case):
    runs = Runs(case)
    runs.create(status='cancelled')
    doc = runs.send('Deliver now to a cancelled run', '--delivery', 'now')
    result = doc['result']
    assert result['status'] == 'undeliverable' and result['refusal']['code'] == 'workflow_terminal'
    case.emit('workflow-now-terminal', doc, 'cancelled workflow run seeded through the production WorkflowStore; a real workflow-send --delivery now is refused as workflow_terminal and the message is undeliverable (workflows._launch replaced)')


def now_checkpoint_required(case):
    runs = Runs(case)
    runs.create(status='paused', input_question='Which branch should the next node use?')
    doc = runs.send('Deliver now while a question is open', '--delivery', 'now')
    result = doc['result']
    assert result['status'] == 'queued' and result['refusal']['code'] == 'workflow_checkpoint_required', result
    case.emit('now-checkpoint-required', doc, 'paused workflow run with an open input question seeded through the production WorkflowStore; a real workflow-send --delivery now is refused as workflow_checkpoint_required and the message stays queued (workflows._launch replaced)')


def workflow_now_started(case):
    runs = Runs(case)
    runs.create(status='paused')
    doc = runs.send('Deliver now to a paused run', '--delivery', 'now')
    result = doc['result']
    assert result.get('delivery_started') is True and 'refusal' not in result, result
    case.emit('workflow-now-delivery-started', doc, 'paused workflow run seeded through the production WorkflowStore with no live attempt or open question; a real workflow-send --delivery now resumes it through the existing control (workflows._launch replaced, so no supervisor or agent starts)')


def workflow_status_undeliverable(case):
    runs = Runs(case)
    runs.create(status='running')
    runs.attempt(NODE_TASK, 'completed')
    queued = runs.send('Node message the run can no longer deliver', '--task-id', NODE_TASK)['result']
    assert queued['status'] == 'queued'
    runs.storage.update_run(RUN, lambda r: r.update(status='cancelled'), 'fixture_cancelled')
    doc = case.run(['workflow-status', RUN])
    entry = next(m for m in doc['result']['messages'] if m['id'] == queued['message_id'])
    assert entry['status'] == 'undeliverable' and entry['text'] and entry['reason'] and entry['target']['task_id'] == NODE_TASK
    case.emit('workflow-status-undeliverable', doc, 'running workflow run with one completed node attempt; a real node-targeted workflow-send queues a message, the run is then cancelled through the store so the production reconciliation marks it undeliverable (text and reason kept), and a real workflow-status reads the complete run document. Only the messages entry is the subject (workflows._launch replaced)')


def workflow_status_truncated(case):
    runs = Runs(case)
    runs.create(status='cancelled')
    for number in range(9):
        runs.send(f'Message {number + 1} for a cancelled run')
    runs.send('Long message ' + 'x' * 300)
    doc = case.run(['workflow-status', RUN, '--monitor-view'])
    result = doc['result']
    assert result['messages_truncated'] is True and len(result['messages']) == 8
    assert result['messages'][-1]['text_truncated'] is True and len(result['messages'][-1]['text']) == 200
    case.emit('workflow-status-truncated', doc, 'cancelled workflow run with ten real workflow-send messages, all undeliverable; the compact workflow-status --monitor-view keeps the newest eight final messages, truncates text to 200 characters and reports messages_truncated (the only status read that carries that field) (workflows._launch replaced)')


def workflow_detail_messages(case):
    runs = Runs(case)
    runs.create(status='running')
    runs.attempt(NODE_TASK, 'completed')
    runs.send('Workflow-level message')
    runs.send('Node message', '--task-id', NODE_TASK)
    doc = case.run(['workflow-detail', RUN, '--view', 'messages'])
    result = doc['result']
    assert result['view'] == 'messages' and result['next_cursor'] is None and result['offset'] == 0
    assert len(json.loads(result['chunk'])) == 2
    case.emit('workflow-detail-messages', doc, 'running workflow run with a real workflow-level and a real node-targeted message; a real workflow-detail --view messages returns the complete single-page lossless chunk (workflows._launch replaced)')


def status_workflow_node(case):
    runs = Runs(case)
    runs.create(status='running')
    runs.attempt(NODE_TASK, 'completed')
    queued = runs.send('Message for the next attempt of this node', '--task-id', NODE_TASK)['result']['message_id']
    case.running(LATER_TASK)
    taken = runs.claim(LATER_TASK, 'running')
    assert [m['id'] for m in taken] == [queued]
    doc = case.status(LATER_TASK)
    assert doc['task']['status'] == 'running'
    entry = next(m for m in doc['task']['messages'] if m['id'] == queued)
    assert entry['status'] == 'delivering' and entry['delivery_task_id'] == LATER_TASK
    case.emit('status-workflow-node', doc, 'workflow run with a completed node attempt and a real node-targeted workflow-send; the production dispatch claim (workflow_messages.claim inside a WorkflowStore mutation, as the dispatcher does) reserves a later attempt of the same node, whose task record is a controlled live running record, so a real status read of that node task shows the message delivering with delivery_task_id. Only the messages entry is the subject (workflows._launch replaced)')


def status_workflow_orchestrator(case):
    runs = Runs(case)
    runs.create(status='running')
    runs.attempt(ORCHESTRATOR_TASK, 'completed', node_id='orchestrator', role='orchestrator', activation='activation-first')
    node_target = runs.send('Message addressed to the orchestrator task', '--task-id', ORCHESTRATOR_TASK)['result']['message_id']
    workflow_level = runs.send('Message addressed to the workflow')['result']['message_id']
    case.record(LATER_TASK, status='completed')
    taken = runs.claim(LATER_TASK, 'running', node_id='orchestrator', role='orchestrator', activation='activation-second')
    assert sorted(m['id'] for m in taken) == sorted([node_target, workflow_level])
    runs.settle(LATER_TASK, 'completed')
    doc = case.status(LATER_TASK)
    entries = {m['id']: m for m in doc['task']['messages']}
    assert entries[node_target]['status'] == entries[workflow_level]['status'] == 'delivered'
    assert all('text' not in entry for entry in entries.values())
    assert entries[node_target]['target']['kind'] == 'task' and entries[workflow_level]['target']['kind'] == 'workflow'
    case.emit('status-workflow-orchestrator', doc, 'workflow run with a completed orchestrator attempt and two real messages (one addressed to the orchestrator task, one to the workflow); the production dispatch claim reserves a later orchestrator attempt, which is then settled through the store so reconciliation marks both delivered (metadata only, with delivery_task_id), and a real status read of that orchestrator task shows them. Workflow-level messages are only ever claimed by orchestrator or builder activations (workflows._launch replaced)')


STANDALONE = (standalone_queued, standalone_target_settled, standalone_cancel, standalone_cancel_refused,
              standalone_status_queued_cancelled, standalone_status_reserved, standalone_status_delivery_unknown,
              standalone_status_delivered, standalone_status_superseded, standalone_send_error_superseded,
              standalone_send_error_too_large, standalone_send_error_too_many, standalone_now_started,
              standalone_now_refused, standalone_now_uncertain)
WORKFLOW = (workflow_send, workflow_cancel,
            workflow_settled('cancelled', 'workflow-queued-target-settled-cancelled', True),
            workflow_settled('completed', 'workflow-queued-target-settled-completed', True),
            workflow_settled('failed', 'workflow-queued-failed-run', False),
            workflow_settled('completed', 'workflow-queued-completed-builder', False, kind='builder'),
            workflow_now_terminal, now_checkpoint_required, workflow_now_started, workflow_status_undeliverable,
            workflow_status_truncated, workflow_detail_messages, status_workflow_node, status_workflow_orchestrator)


def documents(base):
    result = {}
    for function in (*STANDALONE, *WORKFLOW):
        name = function.__name__
        with Case(base, name) as case:
            function(case)
        for fixture, doc, facts, exit_code in case.docs:
            file = f'ctl-messaging-{fixture}.json'
            assert file not in result, file
            result[file] = (doc, facts, exit_code)
    return result


def generate():
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    with tempfile.TemporaryDirectory(prefix='harflow-messaging-fixtures-') as temporary:
        docs = documents(Path(temporary))
    output = {}
    for name, (doc, facts, exit_code) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION, format=CTL_FORMAT,
            required_capabilities=[CAPABILITY], valid=True, exit_code=exit_code,
            provenance=dict(producer='scripts/generate-messaging-fixtures.py through ctl.main dispatch and the production messaging, ledger and projection code',
                            controlled_facts=SEAM_BASE + facts[0].upper() + facts[1:] + '.', synthetic=True, runtime_qualification=False))
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    output = generate()
    if args.check:
        assert output == generate(), 'Two fresh generations differ'
        different = [str(path.relative_to(ROOT)) for path, raw in output.items() if not path.exists() or path.read_bytes() != raw]
        if different:
            raise SystemExit('Fixtures differ: ' + ', '.join(different))
    else:
        for path, raw in output.items():
            path.write_bytes(raw)
