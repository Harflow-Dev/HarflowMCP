"""Explicit synthetic backend injection; never a stock backend qualification."""
import json
import os
from dataclasses import replace
from pathlib import Path
from harflow_mcp.backends.base import (ExtraPathsUnsupported, Capabilities, ReasoningEffort,
    NetworkControl, Enforcement, Invocation, UnsupportedCapability)
from harflow_mcp.backends.claude import ClaudeBackend

class FixtureBackend(ExtraPathsUnsupported):
    name = 'conformance_fake'
    capabilities = Capabilities(True, False, False, False, False, False,
        ReasoningEffort(False, (), '', False, False), NetworkControl((), ()), True,
        supports_caller_approvals=True)

    def __init__(self, python, fake):
        self.binary, self.fake = str(python), str(fake)

    def build_start_argv(self, prompt, **kwargs):
        if any(kwargs.get(k) is not None for k in ('max_turns', 'model', 'reasoning_effort', 'network')):
            raise UnsupportedCapability('fixture does not support optional execution grants')
        session = kwargs.get('session_id')
        if not isinstance(session, str) or not session:
            raise ValueError('fixture session identity required')
        return Invocation([self.binary, '-I', '-B', self.fake, '--session-id', session],
            stdin_mode='pipe', initial_input=self.encode_live_message(prompt))

    build_resume_argv = build_start_argv

    def assert_safe(self, invocation, freedom, network=None):
        if (network is not None or invocation.argv[:4] != [self.binary, '-I', '-B', self.fake]
                or len(invocation.argv) != 6 or invocation.argv[4] != '--session-id'
                or not invocation.argv[5] or invocation.env):
            raise UnsupportedCapability('noncanonical fixture invocation')

    def enforcement(self, freedom, network=None):
        if network is not None:
            raise UnsupportedCapability('fixture network authority is uncontrolled')
        return Enforcement(freedom=freedom, mechanism='synthetic_fixture_protocol',
            os_enforced=False, writes_confined=False,
            caveats=('Explicit fixture injection; no OS sandbox or real provider qualification.',))

    def ingest(self, event, acc):
        ClaudeBackend().ingest(event, acc)

    def normalize(self, event, acc):
        return ClaudeBackend().normalize(event, acc)

    def classify(self, acc, exit_code):
        return ClaudeBackend().classify(acc, exit_code)

    def encode_live_message(self, text):
        return (json.dumps({'type': 'input', 'text': text}) + '\n').encode()

    def interactive_resume_argv(self, session_id, repo_path):
        return None

    def validate_caller_approvals(self, enabled, freedom, max_turns, native=False):
        if max_turns is not None or native:
            raise UnsupportedCapability('fixture supports only generic uncapped caller approvals')

    def qualify_caller_approvals(self):
        return 's08-fixture-v1'

    def configure_caller_approvals(self, invocation, freedom, max_turns, version=None, write=False):
        if write:
            raise UnsupportedCapability('fixture has no write-tool approval transport')
        return replace(invocation, caller_approvals=True, caller_approval_version=version,
            stdin_mode='devnull', initial_input=None, control_pipe=True,
            control_context={'initial_frame': invocation.initial_input.decode('utf-8')})

    def outgoing_frames(self, phase, acc):
        if phase == 'initial' and acc is not None:
            return [acc.stream_state['control_context']['initial_frame'].encode('utf-8')]
        return []

    def control_pipe_finished(self, acc):
        return acc.result_count >= 1

    def approval_stream_version(self, acc):
        return 's08-fixture-v1'

    def approval_frame_kind(self, event):
        return 'request' if event.get('type') == 'ask' else None

    def approval_request_key(self, event):
        return 's:' + event['id'] if isinstance(event.get('id'), str) else None

    def decode_approval_request(self, event, qualification=None):
        return {'native_request_id': self.approval_request_key(event), 'tool_use_id': event['id'],
                'action': {'tool_name': 'cmd', 'input': {'command': event['command']}}}

    def approval_paths(self, decoded):
        return []

    def approval_write_tools(self):
        return frozenset()

    def encode_approval_response(self, request, decision):
        return (json.dumps({'answer': decision, 'id': request['native_request_id']}) + '\n').encode()
