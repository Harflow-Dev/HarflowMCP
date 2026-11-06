"""Detached source admission needs private evidence + current accepted identity."""
from dataclasses import replace
from types import SimpleNamespace
from pathlib import Path
import uuid

import pytest

from harflow_mcp import backends, state_ownership, store
from harflow_mcp.backends.base import Invocation
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends import conversation_qualification as q
from harflow_mcp.backends.qualification import Identity
from harflow_mcp import conversation_profile_store as profiles
from harflow_mcp.state_layout import resolve_layout


class Backend(ClaudeBackend):
    name = 'profile-fixture'
    def assert_safe(self, invocation, freedom, network=None):
        assert invocation.argv == ['fixture']
        assert invocation.caller_approvals is False


@pytest.fixture
def profile(tmp_path, monkeypatch):
    layout = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(layout):
        backend = Backend()
        monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
        task_id = str(uuid.uuid4())
        owner = {'pid': 99, 'start_time': 'owner-start', 'markers': ['fixture-owner']}
        original = Invocation(['fixture'])
        context = dict(v=1, backend=backend.name, version='fixture-version', transport='acp',
                       cwd=str(tmp_path), freedom='read_only', requested_network=None,
                       caller_approvals=False, initial_prompt='private prompt', argv=['fixture'], env={})
        scope = q.fingerprint(q.canonical_profile(backend, original, context, repo_path=tmp_path))
        receipt = q.ConversationQualification('runtime-a', scope, q.fingerprint(context), 'a' * 64, 'fixture-version')
        descriptor = dict(contract='conversation_delivery_v1', conversation_id=str(uuid.uuid4()),
                          transport='acp', profile_fingerprint=q.fingerprint(context))
        task = SimpleNamespace(task_id=task_id, backend=backend.name, repo_path=tmp_path,
            freedom='read_only', network=None, session_id='session-a', proc=SimpleNamespace(pid=123),
            start_time='agent-start', markers=('fixture',), conversation=descriptor, conversation_context=context)
        registry = SimpleNamespace(state_layout=layout, _owner=owner)
        source = store.TaskRecord(task_id=task_id, backend=backend.name, repo_path=str(tmp_path),
            session_id='session-a', freedom='read_only', started_at='2026-11-06T00:00:00Z', pid=123, start_time='agent-start',
            markers=['fixture'], owner=owner, conversation=descriptor)
        attachment = SimpleNamespace(original_invocation=original, qualification_receipt=receipt)
        monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ({'runtime_fingerprint': 'runtime-a', 'profile_fingerprint': scope,
                                                    'evidence_sha256': 'a' * 64},))
        monkeypatch.setattr(q, 'identify_runtime', lambda backend: (Identity(backend.name, 'fixture-version', '/fixture', 'b' * 64, 'fixture'), 'runtime-a'))
        monkeypatch.setattr(profiles.identity, 'check_detail', lambda value: ('alive', None))
        monkeypatch.setattr(profiles.identity, 'identity_check', lambda value: 'alive')
        profiles.publish(registry, task, attachment)
        yield registry, source, task


def test_detached_profile_requires_current_accepted_runtime_and_exact_source(profile, monkeypatch):
    registry, source, task = profile
    assert profiles.qualify(registry, source)
    assert not profiles.qualify(registry, replace(source, pid=456))
    monkeypatch.setattr(q, 'identify_runtime', lambda backend: (Identity(backend.name, 'fixture-version', '/fixture', 'c' * 64, 'fixture'), 'runtime-b'))
    assert not profiles.qualify(registry, source)


def test_descriptor_without_private_profile_or_receipt_is_not_authority(profile, monkeypatch):
    registry, source, task = profile
    monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ())
    assert not profiles.qualify(registry, source)


def test_private_leaf_symlink_and_unknown_owner_refuse(profile, monkeypatch):
    registry, source, task = profile
    monkeypatch.setattr(profiles.identity, 'identity_check', lambda value: 'undecidable')
    assert not profiles.qualify(registry, source)
    path = registry.state_layout.root / 'conversation-profiles' / (source.task_id + '.json')
    retained = path.with_suffix('.retained')
    path.rename(retained)
    path.symlink_to(retained)
    assert not profiles.qualify(registry, source)


def test_sealed_commit_has_no_private_profile_or_large_prompt_budget_failure(profile):
    from harflow_mcp.conversation_task_transport import publish_source_profile
    registry, source, task = profile
    task.task_id = str(uuid.uuid4())
    task.conversation = None
    task.conversation_context = dict(task.conversation_context, initial_prompt='large commit frame ' * 20000)
    task._conversation_attachment = SimpleNamespace(original_invocation=Invocation(['fixture']),
        qualification_receipt=q.ConversationQualification('runtime-a', 'a' * 64, 'b' * 64, 'c' * 64, 'fixture-version'))
    path = registry.state_layout.root / 'conversation-profiles' / (task.task_id + '.json')
    publish_source_profile(registry, task)
    assert not path.exists()


def test_exact_profile_capacity_reserves_binding_and_rejects_duplicate_large_input(profile):
    registry, source, task = profile
    receipt = q.ConversationQualification('r','s','l','a'*64,'v')
    profiles.validate_capacity(registry, task, task.conversation_context, Invocation(['fixture']), receipt)
    text = 'x' * (200 * 1024)
    original = Invocation(['fixture'], initial_input=text.encode())
    context = dict(task.conversation_context, initial_prompt=text)
    from harflow_mcp.backends.conversation import ConversationRefused
    with pytest.raises(ConversationRefused, match='capacity'):
        profiles.validate_capacity(registry, task, context, original, receipt)


def test_bound_regular_owner_refuses_large_profile_before_driver_or_first_write(profile, monkeypatch):
    from harflow_mcp import conversation_task_transport as transport
    from harflow_mcp.backends.conversation import ConversationLaunch, ConversationRefused
    registry, source, task = profile
    text = 'x' * (200 * 1024)
    original = Invocation(['fixture'], initial_input=text.encode())
    context = dict(task.conversation_context, initial_prompt=text)
    port = SimpleNamespace(profile=context)
    receipt = q.ConversationQualification('r','s','l','a'*64,'v')
    task.proc.stdin = SimpleNamespace(write=lambda *_: pytest.fail('first write before refusal'))
    attachment = transport.ConversationAttachment(
        ConversationLaunch(replace(original, conversation_context=context), port), lambda task: object(),
        original_invocation=original, qualification_receipt=receipt)
    monkeypatch.setattr(transport, 'ConversationDriver', lambda *a, **kw: pytest.fail('driver before capacity refusal'))
    with pytest.raises(ConversationRefused, match='capacity'):
        transport.bind(registry, task, attachment)
    assert not hasattr(task, 'conversation_driver')


def test_bound_sealed_owner_bypasses_private_profile_capacity(profile, monkeypatch):
    from harflow_mcp import conversation_task_transport as transport
    from harflow_mcp.backends.conversation import ConversationLaunch
    registry, source, task = profile
    text = 'x' * (380 * 1024)
    original = Invocation(['fixture'], initial_input=text.encode())
    context = dict(task.conversation_context, initial_prompt=text)
    task.proc.stdin = object()
    task.prompt = text
    task.conversation = None
    port = SimpleNamespace(profile=context)
    receipt = q.ConversationQualification('r','s','l','a'*64,'v')
    sentinel = object()
    monkeypatch.setattr(transport, 'ConversationDriver', lambda *a, **kw: sentinel)
    attachment = transport.ConversationAttachment(
        ConversationLaunch(replace(original, conversation_context=context), port), lambda task: object(),
        original_invocation=original, qualification_receipt=receipt)
    assert transport.bind(registry, task, attachment) is sentinel
