"""Synthetic adapter protocol self-checks, not behavioral task conformance."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / 'src'))
    spec = importlib.util.spec_from_file_location('s08_independent_backend', ROOT / 'conformance/s08/backend.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.FixtureBackend('/fixture-python', '/fixture-agent.py')


def test_only_canonical_fixture_invocation_and_verbatim_stdin(backend):
    prompt = '  /fixed command\nretain spacing  '
    invocation = backend.build_start_argv(prompt, session_id='session-one')
    assert invocation.argv == ['/fixture-python','-I','-B','/fixture-agent.py','--session-id','session-one']
    assert invocation.stdin_mode == 'pipe'
    assert json.loads(invocation.initial_input) == {'type':'input','text':prompt}
    assert backend.assert_safe(invocation, 'read_only') is None
    assert backend.interactive_resume_argv('session-one', Path('/fixture')) is None
    enforcement = backend.enforcement('read_only')
    assert enforcement.os_enforced is False and enforcement.writes_confined is False


@pytest.mark.parametrize('option', ['max_turns','model','reasoning_effort','network'])
def test_unsupported_execution_options_cannot_silently_disappear(backend, option):
    with pytest.raises(ValueError, match='does not support'):
        backend.build_start_argv('fixed', session_id='owned', **{option:'requested'})


@pytest.mark.parametrize('argv', [[], ['/real-provider'], ['/fixture-python','-I','-B','/fixture-agent.py','--session-id','s','--extra'],
                                  ['/fixture-python','-I','-B','/fixture-agent.py','--session-id','']])
def test_noncanonical_argv_is_refused(backend, argv):
    invocation = SimpleNamespace(argv=argv, env={})
    with pytest.raises(ValueError, match='noncanonical'):
        backend.assert_safe(invocation, 'read_only')


def test_environment_or_network_authority_is_refused(backend):
    argv = ['/fixture-python','-I','-B','/fixture-agent.py','--session-id','owned']
    with pytest.raises(ValueError, match='noncanonical'):
        backend.assert_safe(SimpleNamespace(argv=argv, env={'EXTRA':'value'}), 'read_only')
    with pytest.raises(ValueError, match='uncontrolled'):
        backend.enforcement('read_only', network='allow')


def test_approval_request_identity_and_response_are_exact(backend):
    first = backend.decode_approval_request({'type':'ask','id':'A','command':'fixed action'})
    second = backend.decode_approval_request({'type':'ask','id':'B','command':'fixed action'})
    assert first['native_request_id'] == 's:A'
    assert second['native_request_id'] == 's:B'
    assert first['action'] == {'tool_name':'cmd','input':{'command':'fixed action'}}
    assert json.loads(backend.encode_approval_response(first, 'approve')) == {'answer':'approve','id':'s:A'}
    assert json.loads(backend.encode_approval_response(second, 'deny')) == {'answer':'deny','id':'s:B'}
    assert backend.approval_paths(first) == []
    assert backend.approval_write_tools() == frozenset()
