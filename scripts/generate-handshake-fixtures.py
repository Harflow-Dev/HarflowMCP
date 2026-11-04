#!/usr/bin/env python3
"""S05 boundary fixtures. No tool calls, registry startup, harness probes or forks.

Launch outcomes are injected into the real ctl serializer; they do not simulate
successful execution. MCP schemas come from list_tools without lifespan startup.
"""
import argparse
import asyncio
import base64
import contextlib
import hashlib
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from harflow_mcp import ctl, detached
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, VERSION_INFO_FORMAT, envelope

ROOT = Path(__file__).resolve().parents[1]
DEFERRED = sorted(('subscribe', 'unsubscribe', 'subscribe_workflow',
                   'unsubscribe_workflow', 'get_task_notifications',
                   'get_workflow_notifications', 'recent_limit_hits', 'workflow_limit_hits'))


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode()


def documents():
    result = {}
    # Synthetic bounded watcher specimens, not live execution evidence. Cursor
    # roots/epochs are fixed opaque examples; producer interleavings have tests.
    def watch_cursor(epoch, revision):
        return base64.urlsafe_b64encode(json.dumps(dict(v=1, scope='task-list-watch.v1',
            root='b' * 32, epoch=epoch * 32, revision=revision),
            sort_keys=True, separators=(',', ':')).encode()).decode()
    for state, epoch, revision, reason in (
        ('baseline', 'a', 0, None), ('changed', 'a', 2, None),
        ('timeout', 'a', 0, None), ('reset', 'c', 0, 'epoch_changed'),
        ('unavailable', 'a', 0, 'state_missing'),
    ):
        observation = dict(state=state, cursor=None if state == 'unavailable' else watch_cursor(epoch, revision))
        if reason:
            observation['reason'] = reason
        result[f'ctl-task-list-watch-{state}.json'] = (envelope(CTL_FORMAT, {'result': observation}), 0)
    for label, code, message in (
        ('cursor-invalid', 'task_list_watch_cursor_invalid', 'Cursor is not valid base64 JSON'),
        ('direct-caller-refused', 'agent_caller', 'Task-list watch requires a verified direct caller: managed'),
    ):
        result[f'ctl-task-list-watch-{label}.json'] = (envelope(CTL_FORMAT, {'error': dict(code=code, message=message)}), 1)
    # This is the shared run/resume parent serializer, not the detached launch.
    for operation in ('run', 'resume'):
        for label, kind, payload, expected in (
            ('accepted', 'task', {'task_id': 'task-fixture'}, 0),
            ('refused', 'error', {'code': 'invalid_params', 'message': 'fixture refusal'}, 1),
            ('unknown', 'unknown', {'message': 'fixture: owner did not report; a task may exist'}, 3),
        ):
            out = io.StringIO()
            with tempfile.TemporaryDirectory() as base, patch.object(ctl, 'default_log_dir', return_value=Path(base) / 'tasks'), patch.object(detached, 'run_detached', return_value=detached.Outcome(kind, payload, 123)), contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                code = ctl._cmd_detached(argparse.Namespace(json=True), None)
            assert code == expected
            result[f'ctl-launch-{operation}-{label}.json'] = (json.loads(out.getvalue()), code)
    # Parser-only unknown operation: no registry or selected state access.
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        try:
            code = ctl.main(['s05-unknown-operation', '--json'])
        except SystemExit as exc:
            code = exc.code
    assert code == 2
    result['ctl-launch-unknown-operation.json'] = (json.loads(out.getvalue()), code)
    return result


async def schemas():
    import inspect
    from harflow_mcp.server import mcp
    # Cleandoc descriptions so Python 3.11/3.13 docstring indentation cannot diverge.
    tools = []
    for tool in await mcp.list_tools():
        row = tool.model_dump(mode='json', by_alias=True, exclude_none=True)
        if isinstance(row.get('description'), str):
            row['description'] = inspect.cleandoc(row['description'])
        tools.append(row)
    names = {tool['name'] for tool in tools}
    assert set(DEFERRED) <= names
    return {'tools': tools, 'included': sorted(names - set(DEFERRED)), 'deferred': DEFERRED,
            'scope': 'Python source schemas; S03 proposed Rust v1 candidate partition, owner acceptance pending; not runtime qualification'}


def generate():
    bundle = ROOT / 'contracts/v1'
    manifest = json.loads((bundle / 'manifest.json').read_text())
    output = {}
    for name, (doc, code) in documents().items():
        output[bundle / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[bundle / name]).hexdigest()
        required = ['task-list-watch.v1'] if name.startswith('ctl-task-list-watch-') and code == 0 else []
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
                                     format=CTL_FORMAT, required_capabilities=required, valid=True, exit_code=code)
    # Synthetic additive/missing-capability handshake variants, based on the
    # canonical platform/root specimen; no host state or platform values leak in.
    version = json.loads((bundle / 'version-info.json').read_text())
    capabilities = ('task-event-page.v1', 'task-prompt.v1', 'task-successors.v1',
                    'workflow-live-activity.v1', 'statistics-cost-model.v1', 'task-changes.v1',
                    'task-list-watch.v1')
    variants = [('version-info-s05-additive.json',
                 {**version, 'optional_future_field': {'value': True},
                  'capabilities': version['capabilities'] + ['future-capability.v99']}, [], True)]
    for capability in capabilities:
        variants.append(('invalid-s05-missing-' + capability.removesuffix('.v1') + '.json',
                         {**version, 'capabilities': [c for c in version['capabilities'] if c != capability]},
                         [capability], False))
    for name, doc, required, valid in variants:
        output[bundle / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[bundle / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
                                     format=VERSION_INFO_FORMAT, required_capabilities=required, valid=valid)
    output[bundle / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[bundle / 'manifest.sha256'] = (hashlib.sha256(output[bundle / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    output[ROOT / 'contracts/mcp-v1/tools.json'] = canonical(asyncio.run(schemas()))
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = generate()
    if args.check:
        different = [str(path.relative_to(ROOT)) for path, content in outputs.items() if not path.exists() or path.read_bytes() != content]
        if different:
            raise SystemExit('Fixtures differ: ' + ', '.join(different))
    else:
        for path, content in outputs.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
