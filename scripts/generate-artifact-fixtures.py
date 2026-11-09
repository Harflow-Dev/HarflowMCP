#!/usr/bin/env python3
"""Generate task-artifacts fixtures through real ctl dispatch and serialization.

Uses isolated state, production owner/manifest/page readers and stat observations.
Controlled facts: human process ancestry, clock, stat device/inode/mtime. Seeded
manifests represent registrar, legacy backend-plan and engine-output provenance.
Temporary paths are redacted to /workspace; cursor revision digests are recomputed
from the same redacted manifest bytes. No provider, registry or file content read.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch
import uuid

from harflow_mcp import artifacts, ctl, identity, lineage, state_ownership, store
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
CAPABILITY = 'task-artifacts.v1'
TASK = 'task-artifacts-owner'
STAMP = '2026-10-15T00:00:00+00:00'


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def documents(base: Path):
    base = base.resolve()
    layout = resolve_layout(root=base / 'state')
    result = {}
    revisions = {}
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {'HOME': str(base), 'HARFLOW_STATE_ROOT': str(layout.root)}))
        for key in ('HARFLOW_TASK_ID', 'HARFLOW_ROOT_TASK_ID', 'HARFLOW_DEPTH'):
            os.environ.pop(key, None)
        stack.enter_context(state_ownership.state_session(layout))
        logs = layout.tasks_directory
        logs.mkdir(parents=True)
        repo = base / 'repo'
        repo.mkdir()
        stack.enter_context(patch.object(ctl, 'default_log_dir', return_value=logs))
        stack.enter_context(patch.object(lineage, '_default_process_table', return_value={os.getpid(): 1, 1: 0}))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 4242, 'start_time': 'fixture', 'markers': []}))
        stack.enter_context(patch.object(artifacts, '_now', return_value=STAMP))
        stack.enter_context(patch.object(artifacts, '_observation', lambda info: dict(device=1, inode=2, size=info.st_size, mtime_ns=0)))
        store.write(logs, store.TaskRecord(TASK, 'codex', None, str(repo), STAMP, status='completed', exit_code=0))

        def call(label, *args, scenario, exit_code=0):
            out = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                code = ctl.main(['task-artifacts', TASK, *args, '--json'])
            assert code == exit_code, (label, out.getvalue())
            doc = json.loads(out.getvalue())
            validate_envelope(doc, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
            result[f'ctl-task-artifacts-{label}.json'] = (doc, code, scenario)
            return doc.get('result')

        call('empty', scenario='Qualified human, supported owner, absent manifest: authoritative empty registration set.')
        items = []
        for n, (method, state) in enumerate((('verified_process_lineage', 'available'),
                                           ('verified_process_lineage', 'changed'),
                                           ('engine_output', 'deleted'),
                                           ('observed_backend_plan', 'unavailable')), 1):
            if method == 'engine_output':
                from harflow_mcp.scratch import output_directory
                target = output_directory(logs, TASK) / f'report-{TASK}.md'
            else:
                target = repo / f'report-{n}.txt'
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('report')
            observed = artifacts._observation(target.stat())
            provenance = dict(method=method, task_id=TASK)
            if method == 'observed_backend_plan':
                provenance['tool_use_id'] = 'fixture-plan-write'
            items.append(dict(artifact_id=str(uuid.UUID(int=n)), task_id=TASK, path=str(target), kind='report',
                              note=f' Complete note {n}.\n Preserve whitespace. ', registered_at=STAMP,
                              provenance=provenance, registered_observation=observed))
            if state == 'changed':
                target.write_text('report changed')
            elif state == 'deleted':
                target.unlink()
        manifest = artifacts._path(logs, TASK)
        raw = canonical(dict(product='harflow', format='harflow.task.artifacts', v=1,
                             product_version=PRODUCT_VERSION, task_id=TASK, items=items))
        manifest.write_bytes(raw)
        revisions[hashlib.sha256(raw).hexdigest()] = hashlib.sha256(raw.replace(str(base).encode(), b'/workspace')).hexdigest()
        first = call('populated', '--limit', '2', scenario='First page: available and changed registrar references, deterministic registration order and complete verbatim notes.')
        call('next-page', '--limit', '2', '--cursor', first['next_cursor'], scenario='Cursor request: deleted engine output and unavailable legacy observed backend plan (saved directory identity absent).')
        call('page-error', '--cursor', 'not-a-cursor', scenario='Later cursor request fails with cursor_invalid; retain previous observations and offer a fresh first-page reload.', exit_code=1)
        call('error', '--limit', '0', scenario='Invalid limit: ordinary artifact_error without a reason.', exit_code=1)
        manifest.write_text('{')
        call('unavailable', scenario='Malformed manifest: unavailable with placeholder count zero, not an empty successful collection.')
        from harflow_mcp.workflows import WorkflowStore
        with patch.object(WorkflowStore, 'list_run_page', return_value={'bootstrap_pending': True}):
            call('preparing', scenario='Workflow ownership catalog preparation incomplete: ordinary artifact_error, retry the same request.', exit_code=1)

    def normalize(value):
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if isinstance(value, str):
            if value in revisions:
                return revisions[value]
            return value.replace(str(base), '/workspace')
        return value

    for name, (doc, code, scenario) in result.items():
        doc = normalize(doc)
        cursor = doc.get('result', {}).get('next_cursor')
        if cursor:
            decoded = json.loads(base64.urlsafe_b64decode(cursor))
            doc['result']['next_cursor'] = base64.urlsafe_b64encode(json.dumps(normalize(decoded)).encode()).decode()
        result[name] = (doc, code, scenario)
    return result


def generate():
    with tempfile.TemporaryDirectory(prefix='harflow-artifact-fixtures-') as temporary:
        docs = documents(Path(temporary))
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    output = {}
    required_capabilities = {name: [CAPABILITY] for name in docs}
    for name, (doc, code, scenario) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION, format=CTL_FORMAT,
                                     required_capabilities=required_capabilities[name], valid=True, exit_code=code,
                                     provenance=scenario + ' Controlled human ancestry, clock and stat identity; /workspace path and cursor-digest redaction; seeded manifests, no provider execution.')
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
