#!/usr/bin/env python3
"""Generate ctl-only continuation-stop specimens from isolated production state.

Production writers and ctl status/brief/batch dispatch serialize the observations.
The refusal comes from real locked admission, error_doc and the ctl parent path.
Controlled facts: direct-human ancestry, observation clock, stable task timestamps
and owner identity. Temporary paths are redacted; no provider is launched.
The handshake generator owns the missing-capability negative specimen.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from harflow_mcp import ctl, detached, identity, lineage, state_ownership, store, task_continuation_stop
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
CAPABILITY = 'task-continuation-stop.v1'
TASK = 'task-continuation-stop'
STAMP = '2026-10-15T00:00:00+00:00'


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def documents(base: Path):
    base = base.resolve()
    layout = resolve_layout(root=base / 'state')
    result = {}
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {'HOME': str(base), 'HARFLOW_STATE_ROOT': str(layout.root),
                                                   'PATH': os.environ.get('PATH', '')}, clear=True))
        stack.enter_context(state_ownership.state_session(layout))
        logs = layout.tasks_directory
        logs.mkdir(parents=True)
        repo = base / 'repo'
        repo.mkdir()
        stack.enter_context(patch.object(ctl, 'default_log_dir', return_value=logs))
        stack.enter_context(patch.object(lineage, '_default_process_table', return_value={os.getpid(): 1, 1: 0}))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 4242, 'start_time': 'fixture', 'markers': []}))
        stack.enter_context(patch.object(task_continuation_stop, '_now', return_value=STAMP))
        record = store.TaskRecord(TASK, 'claude', None, str(repo), STAMP, status='completed', exit_code=0,
                                  finished_at=STAMP, output_drained=True)
        store.write(logs, record)

        def call(name, argv, scenario, expected=0):
            out = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                code = ctl.main([*argv, '--json'])
            assert code == expected, (name, out.getvalue())
            doc = json.loads(out.getvalue())
            validate_envelope(doc, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
            result[name + '.json'] = (doc, code, scenario)

        call('ctl-task-status-continuation-not-stopped', ['status', TASK], 'Stable verified absence; no eligibility inference.')
        marker = task_continuation_stop.marker_path(logs, TASK)
        marker.write_text(json.dumps({'cancelled_task_id': TASK}))
        call('ctl-task-status-continuation-stopped', ['status', TASK], 'Stable chain marker with valid cancellation attribution.')
        call('ctl-task-status-brief-continuation-stopped', ['status', TASK, '--brief'], 'Brief preserves complete diagnostic identities and state.')
        call('ctl-task-status-batch-continuation-stop', ['status-batch', '--task-ids', TASK + ',missing-task'], 'Batch uses the same observer and retains per-id missing-task errors.')

        registry = TaskRegistry(log_dir=logs, open_monitor=False)
        try:
            asyncio.run(registry.continue_task(TASK))
        except ValueError as exc:
            assert type(exc) is ValueError and str(exc) == task_continuation_stop.REFUSAL_MESSAGE
            error = detached.error_doc(exc)
        else:
            raise AssertionError('Locked stopped-chain admission must refuse')
        with patch.object(detached, 'run_detached', return_value=detached.Outcome('error', error, 123)):
            call('ctl-continue-chain-stopped', ['continue', TASK], 'Real locked continuation admission; allowlisted exception data through detached and ctl parent serializers.', 1)
        marker.unlink()
        marker.symlink_to(logs / 'missing-marker')
        call('ctl-task-status-continuation-unknown', ['status', TASK], 'Dangling marker symlink is unsafe, not verified absence.')

    for name, (doc, code, scenario) in result.items():
        doc = json.loads(json.dumps(doc).replace(str(base), '/workspace'))
        result[name] = (doc, code, scenario)
    return result


def generate():
    with tempfile.TemporaryDirectory(prefix='harflow-continuation-stop-fixtures-') as temporary:
        docs = documents(Path(temporary))
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    output = {}
    for name, (doc, code, scenario) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        required = ['task-continue.v1', CAPABILITY] if name == 'ctl-continue-chain-stopped.json' else [CAPABILITY]
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION, format=CTL_FORMAT,
                                      required_capabilities=required, valid=True, exit_code=code,
                                      provenance=scenario + ' Controlled human ancestry, observation clock and owner identity; /workspace path redaction; isolated state, no provider execution.')
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    output = generate()
    if args.check:
        assert output == generate(), 'Two fresh runs differ'
        different = [str(path.relative_to(ROOT)) for path, data in output.items() if not path.exists() or path.read_bytes() != data]
        if different:
            raise SystemExit('Fixtures differ: ' + ', '.join(different))
    else:
        for path, data in output.items():
            path.write_bytes(data)


if __name__ == '__main__':
    main()
