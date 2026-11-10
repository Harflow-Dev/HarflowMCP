#!/usr/bin/env python3
"""Generate pending-launch ctl specimens from qualified isolated storage and real ctl serialization."""
import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from harflow_mcp import ctl, lineage, state_ownership, store, task_pending_reads as pending, task_supervision, takeover
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CAPABILITIES, CTL_FORMAT, PRODUCT_VERSION

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
STAMP = '2026-11-10T00:00:00+00:00'
A, B = 'a' * 32, 'b' * 32


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def receipt(logs, task_id=A, state='reserved', **changes):
    info = logs.stat()
    value = dict(format=task_supervision.FORMAT, product='harflow', product_version=PRODUCT_VERSION,
                 v=1, task_id=task_id, generation='c' * 32, root=str(logs.parent),
                 directory_identity=[info.st_dev, info.st_ino],
                 directory_binding=dict(path=str(logs.resolve()), inode=info.st_ino),
                 state=state, request_sha256='d' * 64, backend='claude', owner=None,
                 repo_path='/tmp/harflow-fixture-repo', title='Fixture pending launch', created_at=STAMP)
    value.update(changes)
    task_supervision._write(logs / f'{task_id}.launch.json', value)


def documents(base):
    docs = {}
    scenarios = ['empty', 'states', 'next-page', 'refused', 'status-pending', 'status-recorded',
                 'status-not-found', 'status-unknown', 'status-refused', 'cursor-invalid', 'bound',
                 'existing-status-pending', 'existing-batch-pending']
    for scenario in scenarios:
        layout = resolve_layout(root=base / scenario)
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {'HARFLOW_STATE_ROOT': str(layout.root)}, clear=True))
            stack.enter_context(state_ownership.state_session(layout))
            logs = layout.tasks_directory
            logs.mkdir()
            stack.enter_context(patch.object(takeover, 'caller_refusal', return_value=None))
            stack.enter_context(patch.object(lineage, 'detect_caller_detail', return_value=lineage.Detection(None)))
            stack.enter_context(patch.object(lineage, 'detect_caller', return_value=None))
            stack.enter_context(patch.object(pending, 'observed_at', return_value=STAMP))
            # Only opaque cursor binding evidence is fixed; stored qualification uses real paths/inodes.
            stack.enter_context(patch.object(pending, '_root', return_value='f' * 64))
            stack.enter_context(patch.object(pending, '_identity', return_value=[1, 2, 1, 3]))
            args = ['task-pending-page', '--json']
            if scenario in ('states', 'next-page'):
                for i, state in enumerate(('reserved', 'claimed', 'waiting_for_resources', 'granted', 'not_started')):
                    receipt(logs, str(i + 1) * 32, state, **({'resources': {'names': ['fixture']}} if i > 1 else {}),
                            **({'not_started_reason': 'Fixture pre-spawn refusal'} if state == 'not_started' else {}))
                if scenario == 'next-page':
                    args += ['--limit', '2']
                    first = pending.pending_page(logs, limit=2)
                    args += ['--cursor', first['next_cursor']]
            elif scenario == 'refused':
                receipt(logs)
                (logs / f'{B}.launch.json').write_text('private invalid document')
                (logs / '.invalid.launch.json').write_text('{}')
            elif scenario.startswith('status-'):
                args = ['task-pending-status', '--task-id', A, '--json']
                if scenario != 'status-not-found': receipt(logs)
                if scenario == 'status-recorded':
                    store.write(logs, store.TaskRecord(A, 'claude', None, '/tmp/harflow-fixture-repo', STAMP,
                                                      status='completed', exit_code=0, output_drained=True))
                if scenario == 'status-unknown': receipt(logs, state='spawned')
                if scenario == 'status-refused': (logs / f'{A}.launch.json').write_text('{}')
            elif scenario == 'cursor-invalid': args += ['--cursor', 'invalid']
            elif scenario == 'bound':
                receipt(logs)
                stack.enter_context(patch.object(pending, 'MAX_ENTRIES', 0))
            elif scenario.startswith('existing-'):
                receipt(logs, state='waiting_for_resources', resources={'names': ['fixture']})
                if scenario == 'existing-status-pending': args = ['status', A, '--json']
                else:
                    receipt(logs, B, 'not_started', resources={'names': ['fixture']}, not_started_reason='Fixture pre-spawn refusal')
                    args = ['status-batch', '--task-ids', A + ',' + B, '--json']
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                exit_code = ctl.main(args)
            doc = json.loads(output.getvalue())
            docs['ctl-task-pending-' + scenario + '.json'] = (doc, exit_code)
    return docs


def generate():
    with tempfile.TemporaryDirectory(prefix='harflow-pending-fixtures-') as temporary:
        docs = documents(Path(temporary))
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    output = {}
    for name, (doc, exit_code) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
            format=CTL_FORMAT, required_capabilities=[] if 'existing-' in name else ['task-pending-launch.v1'],
            valid=True, exit_code=exit_code, provenance='Actual qualified launch/task readers, bounded pending projection and ctl.main serializer over isolated storage. Controlled direct caller, clock and opaque cursor identity; no live provider. Synthetic evidence, not runtime certification.')
    # Additive capability propagation preserves each older negative specimen's intentionally absent capability.
    for name, case in list(manifest['cases'].items()):
        path = BUNDLE / name
        if case.get('format') != 'harflow.version-info' or not path.exists(): continue
        doc = json.loads(path.read_bytes())
        if isinstance(doc.get('capabilities'), list) and name != 'invalid-s05-missing-task-pending-launch.json':
            doc['capabilities'] = ['task-pending-launch.v1'] + [cap for cap in doc['capabilities'] if cap != 'task-pending-launch.v1']
            output[path] = canonical(doc)
            manifest['files'][name] = hashlib.sha256(output[path]).hexdigest()
    name = 'invalid-s05-missing-task-pending-launch.json'
    doc = json.loads(output.get(BUNDLE / 'version-info.json', (BUNDLE / 'version-info.json').read_bytes()))
    doc['capabilities'].remove('task-pending-launch.v1')
    output[BUNDLE / name] = canonical(doc)
    manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
    manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
        format='harflow.version-info', required_capabilities=['task-pending-launch.v1'], valid=False)
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
        if different: raise SystemExit('Fixtures differ: ' + ', '.join(different))
    else:
        for path, raw in output.items(): path.write_bytes(raw)
