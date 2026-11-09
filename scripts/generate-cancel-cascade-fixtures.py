#!/usr/bin/env python3
"""Ctl cancellation specimens: isolated qualified storage, controlled process facts.

Production cancellation traversal, collector, projection and ctl serialization stay
active. No provider executes and no real task process is signalled.
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from harflow_mcp import ctl, identity, lineage, state_ownership, store, task_resources, task_supervision
from harflow_mcp import task_cancel_observation as observation
from harflow_mcp import task_withdrawal as withdrawal
from harflow_mcp import tasks
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope
from harflow_mcp.workflow_receipt_documents import encode

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
STAMP = '2026-10-15T00:00:00+00:00'
TARGET = 'a' * 32
CHILD = 'b' * 32
OWNER = dict(pid=4242, start_time='fixture-owner', markers=[])


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def record(logs, task_id, **changes):
    value = dict(status='completed', exit_code=0, output_drained=True, finished_at=STAMP,
                 pid=6000, start_time='fixture-task', markers=['fixture'], owner=OWNER)
    value.update(changes)
    assert store.write_landed(logs, store.TaskRecord(task_id, 'claude', None, str(logs.parent / 'repo'), STAMP, **value))


def receipt(logs, task_id, **changes):
    info = logs.stat()
    value = dict(format=task_supervision.FORMAT, product='harflow', product_version=PRODUCT_VERSION,
                 v=1, task_id=task_id, generation='c' * 32, root=str(logs.parent),
                 directory_identity=[info.st_dev, info.st_ino],
                 directory_binding=dict(path=str(logs.resolve()), inode=info.st_ino),
                 state='waiting_for_resources', request_sha256='d' * 64, configuration_sha256='e' * 64,
                 backend='claude', owner=OWNER, resources=dict(names=['fixture']),
                 repo_path=str(logs.parent / 'repo'), freedom='read_only')
    value.update(changes)
    task_supervision._write(logs / f'{task_id}.launch.json', value)
    return value


def unsettled_child(logs):
    launch = receipt(logs, CHILD, spawned_by=TARGET, state='claimed')
    owners = logs.parent / 'workflow-owners'
    owners.mkdir(exist_ok=True)
    task_supervision._write(owners / f'{CHILD}.json', encode(dict(
        workflow_run_id='fixture-run', root_workflow_run_id='fixture-run', workflow_execution_id='fixture-execution'), CHILD))
    intent = withdrawal.request_withdrawal(logs, CHILD, launch['generation'], launch['request_sha256'],
        launch['configuration_sha256'], 'fixture-replan', root_workflow_run_id='fixture-run', source_execution_id='fixture-execution')
    withdrawal._ensure_claim(logs, intent, OWNER, 'owner_prestart')


def documents(base):
    result = {}
    for scenario in ('settled', 'partial', 'unknown', 'pending', 'incomplete', 'truncated',
                     'unrecorded', 'refused-documents', 'pending-child-withdrawal-unsettled'):
        layout = resolve_layout(root=base / scenario)
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, dict(HOME=str(base), HARFLOW_STATE_ROOT=str(layout.root),
                PATH=os.environ.get('PATH', '')), clear=True))
            stack.enter_context(state_ownership.state_session(layout))
            logs = layout.tasks_directory
            logs.mkdir(parents=True)
            stack.enter_context(patch.object(ctl, 'default_log_dir', return_value=logs))
            stack.enter_context(patch.object(lineage, 'detect_caller', return_value=None))
            stack.enter_context(patch.object(lineage, 'detect_caller_detail', return_value=lineage.Detection(None)))
            stack.enter_context(patch.object(identity, 'own_identity', return_value=OWNER))
            stack.enter_context(patch.object(identity, 'identity_check', side_effect=lambda value: 'alive' if value and value.get('pid') == 6001 else 'dead'))
            stack.enter_context(patch.object(identity, 'check_detail', return_value=('dead', 'start_time_differs')))
            original_fallback = observation.fallback
            def fixed_fallback():
                return original_fallback() | dict(observed_at=STAMP)
            stack.enter_context(patch.object(observation, 'fallback', side_effect=fixed_fallback))
            if scenario == 'pending':
                receipt(logs, TARGET)
            else:
                record(logs, TARGET)
            note = 'Seeded qualified terminal target with observed exit/drain and controlled dead captured task identity.'
            if scenario == 'partial':
                record(logs, CHILD, spawned_by=TARGET, pid=6001)
                note = 'Fresh live child identity overrides stale observed terminal fields; coverage remains complete.'
            elif scenario == 'unknown':
                stack.enter_context(patch.object(observation.Collector, 'project', side_effect=RuntimeError('controlled projection fault')))
                note = 'Controlled observer-construction failure after actual cancellation; exact minimal fallback.'
            elif scenario == 'pending':
                note = 'Qualified waiting launch, controlled dead owner; actual cancel_launch settles it before execution; generation/binding reread.'
            elif scenario == 'incomplete':
                stack.enter_context(patch.object(tasks, 'CASCADE_MAX_ROUNDS', 1))
                scan = tasks.TaskRegistry._cascade_targets
                count = 0
                def growing(registry, task_id, local_lineage):
                    nonlocal count
                    count += 1
                    if count > 1:
                        record(logs, f'child-{count:03}', spawned_by=TARGET)
                    return scan(registry, task_id, local_lineage)
                stack.enter_context(patch.object(tasks.TaskRegistry, '_cascade_targets', growing))
                note = 'Actual one-round traversal with a qualified child seeded at each scan; round bound leaves nonempty legacy unconverged.'
            elif scenario == 'truncated':
                for index in range(100):
                    record(logs, f'child-{index:03}', spawned_by=TARGET)
                note = '101 qualified targets; actual projection selects 100 before observer rereads/probes.'
            elif scenario == 'unrecorded':
                async def phase_loss(registry, timeout):
                    return ['fixture-phase-write']
                stack.enter_context(patch.object(tasks.TaskRegistry, 'settle_phase_writes', phase_loss))
                note = 'Injected unrecorded phase-write result after actual traversal; legacy warning/result retained, not settlement evidence.'
            elif scenario == 'refused-documents':
                store.record_path(logs, 'refused-record').write_text('{"task_id":"wrong"}')
                (logs / 'refused-launch.launch.json').write_text('{}')
                note = 'Malformed record and launch rejected by actual qualified scans; legacy refusal fields retained without descendant rows.'
            elif scenario == 'pending-child-withdrawal-unsettled':
                unsettled_child(logs)
                stack.enter_context(patch.object(withdrawal, 'reconcile_withdrawal', return_value=None))
                note = 'Qualified pending child and real accepted no-spawn claim; controlled unsettled cleanup. Later clean recorded pass cannot erase private withdrawal evidence.'
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = ctl.main(['cancel', TARGET, '--json'])
            assert code == 0, (scenario, out.getvalue(), err.getvalue())
            doc = json.loads(out.getvalue())
            validate_envelope(doc, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
            if scenario == 'incomplete':
                assert doc['result']['cascade']['unconverged']
            if scenario == 'pending-child-withdrawal-unsettled':
                rows = doc['result']['cascade']['observation']['items']
                assert rows[1]['task_id'] == CHILD and rows[1]['state'] == 'unknown'
            doc = json.loads(json.dumps(doc).replace(str(base), '/workspace'))
            result[f'ctl-task-cancel-{scenario}.json'] = (doc, note)
    return result


def generate():
    with tempfile.TemporaryDirectory(prefix='harflow-cancel-fixtures-') as temporary:
        docs = documents(Path(temporary))
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    output = {}
    for name, (doc, note) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
            format=CTL_FORMAT, required_capabilities=['task-cancel-cascade.v1'], valid=True, exit_code=0,
            provenance=note + ' Actual ctl.main/envelope serializer and production collector/projection, except explicitly injected fault. Controlled human authority, clocks and process probes; isolated state, path redaction; no provider or real signals. Synthetic qualification evidence, not runtime certification.')
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
