#!/usr/bin/env python3
"""Generate ST02 producer specimens from isolated service calls and a synthetic provider.

No backend CLI/provider is invoked. Admission/requests/status/history/final classification use
production application code with fixed clocks/identities and a fixture-only in-process task.
Owns ctl-steering-* specimens, steering version-info variants and their manifest entries.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import uuid

from harflow_mcp import identity, state_ownership, workflow_steering as steering, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CAPABILITIES, CTL_FORMAT, PRODUCT_VERSION, VERSION_INFO_FORMAT, envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
AT = 1793836800.0


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()


class FixtureRegistry:
    """Only this fake can produce a provider result in this generator."""
    def __init__(self, root, final="Let us discuss the configured branches first."):
        self.final = final
        self.log_dir = self._log_dir = root / 'tasks'

    async def start(self, prompt, repo, **kwargs):
        done = asyncio.Event()
        done.set()
        value = {'task_id': kwargs['task_id'], 'status': 'completed', 'exit_code': 0,
                 'output_drained': True, 'summary': self.final,
                 'session_id': 'fixture-session', 'usage': {'input_tokens': 10, 'output_tokens': 8},
                 'total_cost_usd': None, 'num_turns': 1}
        return SimpleNamespace(task_id=kwargs['task_id'], done=done, snapshot=lambda: value)


async def specimens(base):
    from itertools import count
    ids = count(100)
    output = {}
    descriptor = {'name': 'steering-example', 'orchestrator': {'backend': 'claude'}, 'nodes': [], 'connections': []}
    with ExitStack() as stack:
        stack.enter_context(patch.object(steering, '_transport', return_value=None))
        stack.enter_context(patch.object(workflows, '_launch'))
        stack.enter_context(patch('time.time', return_value=AT))
        stack.enter_context(patch('uuid.uuid4', side_effect=lambda: uuid.UUID(int=next(ids))))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 6789, 'start_time': 'fixture-generation', 'markers': []}))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=base)))
        storage = workflows.WorkflowStore(base)
        repo = base / 'repo'
        repo.mkdir()
        run = storage.create_run(descriptor, 'Discuss this workflow', repo, freedom='read_only', kind='builder', interaction_owner='monitor')
        rid = run['workflow_run_id']
        storage.update_run(rid, lambda r: r.update(status='completed', builder_draft=descriptor, draft_revision=0), 'fixture')
        def put(name, result):
            output['ctl-steering-' + name + '.json'] = (envelope(CTL_FORMAT, {'result': result}), 0)
        put('builder-ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        request = {'schema_version': 1, 'request_id': str(uuid.UUID(int=1)), 'action': 'open',
                   'expected_interaction_epoch': 0, 'expected_control_revision': 0,
                   'episode_id': None, 'expected_steering_revision': None,
                   'target': steering.target(storage.get_run(rid)),
                   'text': 'Can we discuss the division of work first?', 'allow_fresh_context': True}
        put('builder-admitted', await steering.steer_workflow(rid, request, root=base, monitor=True))
        put('builder-replayed', await steering.steer_workflow(rid, request, root=base, monitor=True))
        changed = {**request, 'text': 'Different payload under the same request identity'}
        try:
            await steering.steer_workflow(rid, changed, root=base, monitor=True)
        except steering.SteeringError as exc:
            output['ctl-steering-request-conflict.json'] = (envelope(CTL_FORMAT, {'error': {'code': exc.code, 'message': str(exc), 'data': exc.data}}), 1)
        supervisor = workflows.WorkflowSupervisor(FixtureRegistry(base), storage)
        await steering.run_builder_turn(supervisor, rid)
        put('builder-awaiting-caller', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('builder-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        current = steering.get_workflow_steering(rid, root=base, monitor=True)
        ep = current['episodes'][-1]
        reply = {**request, 'request_id': str(uuid.UUID(int=2)), 'action': 'reply',
                 'expected_control_revision': current['control_revision'],
                 'expected_interaction_epoch': current['interaction_epoch'],
                 'episode_id': ep['episode_id'], 'expected_steering_revision': ep['steering_revision'],
                 'target': current['targets'][0], 'text': 'Please propose the next draft.'}
        await steering.steer_workflow(rid, reply, root=base, monitor=True)
        await steering.run_builder_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, 'HARFLOW_COMMIT_V1\n{bad json\nEND_HARFLOW_COMMIT_V1'), storage), rid)
        put('builder-command-refused', steering.get_workflow_steering(rid, root=base, monitor=True))
        current = steering.get_workflow_steering(rid, root=base, monitor=True)
        ep = current['episodes'][-1]
        proposal_request = {**reply, 'request_id': str(uuid.UUID(int=4)),
            'expected_control_revision': current['control_revision'],
            'expected_steering_revision': ep['steering_revision'], 'target': current['targets'][0]}
        await steering.steer_workflow(rid, proposal_request, root=base, monitor=True)
        turn = storage.get_run(rid)['steering']['turns'][-1]
        proposal = {'name': 'steering-example', 'orchestrator': {'backend': 'claude'},
            'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'done', 'source': 'start', 'target': 'end'}]}
        wrapper = {key: turn[key] for key in ('episode_id', 'turn_id', 'commit_target_ref', 'steering_revision')}
        wrapper.update({key: turn['commit_target'][key] for key in ('interaction_epoch', 'checkpoint_revision', 'goal_revision')})
        wrapper.update(role='builder', command={'definition': proposal, 'expected_draft_revision': turn['commit_target']['draft_revision']})
        framed = 'HARFLOW_COMMIT_V1\n' + json.dumps(wrapper) + '\nEND_HARFLOW_COMMIT_V1'
        await steering.run_builder_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, framed), storage), rid)
        put('builder-committed', steering.get_workflow_steering(rid, root=base, monitor=True))
        active = storage.create_run(descriptor, 'Active builder', repo, freedom='read_only', kind='builder', interaction_owner='monitor')
        aid = active['workflow_run_id']
        storage.update_run(aid, lambda r: r.update(status='running', builder_draft=descriptor, draft_revision=0,
            activations=[{'id': 'old-activation', 'node_id': 'builder', 'role': 'builder', 'status': 'running',
                          'created_at': AT, 'tasks': [{'task_id': 'old-task', 'status': 'running'}]}]), 'fixture')
        queued = {**request, 'request_id': str(uuid.UUID(int=3)), 'target': steering.target(storage.get_run(aid))}
        put('builder-queued', await steering.steer_workflow(aid, queued, root=base, monitor=True))
        put('builder-summary', steering.summary(storage.get_run(rid)))
    return output


def generate():
    with tempfile.TemporaryDirectory() as temporary:
        documents = asyncio.run(specimens(Path(temporary) / 'state'))
    manifest = json.loads((BUNDLE / 'manifest.json').read_text())
    output = {}
    for name, (document, code) in documents.items():
        output[BUNDLE / name] = canonical(document)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = {'encoding': 'json', 'expected_product_version': PRODUCT_VERSION,
                                  'format': CTL_FORMAT, 'required_capabilities': ['workflow-steering-builder.v1'],
                                  'valid': True, 'exit_code': code}
    version = json.loads((BUNDLE / 'version-info.json').read_text())
    version.update(product_version=PRODUCT_VERSION, capabilities=list(CAPABILITIES))
    output[BUNDLE / 'version-info.json'] = canonical(version)
    manifest['files']['version-info.json'] = hashlib.sha256(output[BUNDLE / 'version-info.json']).hexdigest()
    for capability in ('workflow-steering-read.v1', 'workflow-steering-builder.v1'):
        name = 'invalid-steering-missing-' + capability.removesuffix('.v1') + '.json'
        doc = {**version, 'capabilities': [item for item in version['capabilities'] if item != capability]}
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = {'encoding': 'json', 'expected_product_version': PRODUCT_VERSION,
                                  'format': VERSION_INFO_FORMAT, 'required_capabilities': [capability], 'valid': False}
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = generate()
    if args.check:
        mismatches = [str(path.relative_to(ROOT)) for path, content in outputs.items() if not path.exists() or path.read_bytes() != content]
        if mismatches:
            raise SystemExit('Steering fixtures differ: ' + ', '.join(mismatches))
    else:
        for path, content in outputs.items():
            path.write_bytes(content)
