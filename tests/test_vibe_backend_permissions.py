"""Vibe backend integration and fail-closed feature discovery."""
from dataclasses import replace

import pytest

from harflow_mcp.backends.base import Invocation, UnsupportedCapability
from harflow_mcp.backends.vibe import VibeBackend, UnsafeInvocationError
from harflow_mcp.backends import vibe_acp, vibe_qualification


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setattr(vibe_acp, 'runtime_identity', lambda *_: {'launcher': '/fixture/vibe-acp', 'files': []})
    monkeypatch.setattr(vibe_qualification, 'qualify', lambda *_: '3.0.3')
    return VibeBackend()


def start(backend, tmp_path, **kw):
    return backend.build_start_argv('exact assignment', repo=tmp_path, freedom='write_in_repo',
        session_id=None, model=kw.get('model'), max_turns=2, reasoning_effort=None)


def test_model_grants_then_caller_conversion_preserves_all_requests(backend, tmp_path):
    invocation = start(backend, tmp_path, model='configured-alias')
    invocation = backend.configure_allowed_commands(invocation, ['make test'], 'write_in_repo', tmp_path, '3.0.3')
    invocation = backend.configure_caller_approvals(invocation, 'write_in_repo', 2, '3.0.3', True)
    backend.assert_safe(invocation, 'write_in_repo')
    assert invocation.caller_approvals and invocation.caller_write_approvals
    assert invocation.allowed_commands == ('make test',)
    assert invocation.control_context['model'] == 'configured-alias'
    assert invocation.control_context['prompt'] == 'exact assignment'
    assert invocation.control_context['max_turns'] == 2


def test_model_unknown_qualification_refuses_before_runtime_lookup(backend, tmp_path, monkeypatch):
    def refuse(*_):
        raise UnsupportedCapability('unqualified version')
    monkeypatch.setattr(vibe_qualification, 'qualify', refuse)
    with pytest.raises(UnsupportedCapability, match='unqualified version'):
        start(backend, tmp_path, model='configured-alias')
    # A release without ACP qualification can still use established ordinary dispatch.
    assert not start(backend, tmp_path).control_pipe


def test_resume_reconstructs_session_model_and_prompt(backend, tmp_path):
    invocation = backend.build_resume_argv('new assignment', repo=tmp_path, freedom='write_in_repo',
        session_id='saved-session', model='configured-alias', max_turns=2, reasoning_effort=None)
    backend.assert_safe(invocation, 'write_in_repo')
    assert invocation.control_context['session_id'] == 'saved-session'
    assert invocation.control_context['prompt'] == 'new assignment'
    assert invocation.control_context['model'] == 'configured-alias'


def test_grant_invocation_cannot_override_network_or_escape_to_plain_argv(backend, tmp_path):
    invocation = backend.configure_allowed_commands(start(backend, tmp_path), ['make test'], 'write_in_repo', tmp_path, '3.0.3')
    with pytest.raises(UnsupportedCapability):
        backend.assert_safe(invocation, 'write_in_repo', False)
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(invocation, argv=['vibe-acp', '--yolo']), 'write_in_repo')


def test_unknown_versions_have_no_permission_seed():
    specs = vibe_qualification.feature_specs()
    for feature in ('caller_approval_tools', 'caller_write_tools', 'allowed_commands', 'model_selection', 'acp_transport'):
        assert specs[feature].seeds.keys() == {'3.0.3'}
        assert specs[feature].platforms == ('darwin',)
        assert specs[feature].probe is None


def test_file_reads_use_the_shared_delivery_path_snapshot(backend):
    decoded = {"action": {"tool_name": "read_file", "input": {"filePath": "/fixture/read-target"}}}
    assert backend.approval_paths(decoded) == ["/fixture/read-target"]
    assert "read_file" not in backend.approval_write_tools()
