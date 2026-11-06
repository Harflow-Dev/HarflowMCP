"""Qualification binds fresh binary identity and exact execution profile; no probes."""
from dataclasses import replace
from pathlib import Path
import json

import pytest

from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.conversation import ConversationRefused
from harflow_mcp.backends import conversation_qualification as q
from harflow_mcp.backends.conversation_native import conversation_profile


def test_accepted_manifest_matches_owner_approved_exact_profile_evidence():
    path = Path(__file__).parents[1] / 'docs/decisions/conversation-delivery-qualification-v1.json'
    document = json.loads(path.read_text())
    rows = document['rows']
    assert len(rows) == 28
    expected = {(backend, freedom, operation, cap)
        for backend in ('claude', 'vibe', 'codex', 'antigravity', 'cursor')
        for freedom in ('read_only', 'write_in_repo')
        for operation in ('start', 'resume')
        for cap in ((8, 100) if backend in ('claude', 'vibe') else (None,))}
    assert {(r['backend'], r['freedom'], r['session_operation'], r['max_turns']) for r in rows} == expected
    accepted = {(r['runtime_fingerprint'], r['profile_fingerprint'], r['evidence_sha256'])
                for r in q.ACCEPTED_PROFILES}
    assert len(accepted) == len(q.ACCEPTED_PROFILES) == 28
    assert accepted == {(r['runtime_fingerprint'], r['profile_fingerprint'], r['evidence_sha256']) for r in rows}
    for row in rows:
        assert row['requested_model'] is row['reasoning_effort'] is row['requested_network'] is None
        assert row['exit_code'] == 0 and row['output_drained'] is True
        assert row['profile_name'] == ('shipped-medium35-high' if row['backend'] == 'vibe' else 'native')
        assert row['evidence_sha256'] == q.fingerprint({key: value for key, value in row.items() if key != 'evidence_sha256'})
        for key in ('runtime_fingerprint', 'profile_fingerprint', 'raw_result_sha256', 'source_manifest_sha256'):
            assert len(row[key]) == 64 and set(row[key]) <= set('0123456789abcdef')


def original(repo, model=None):
    return ClaudeBackend().build_start_argv('hello', repo=repo, freedom='read_only',
        session_id='12345678-1234-1234-1234-123456789abc', model=model, max_turns=None, reasoning_effort=None)


def candidate(repo, model=None):
    backend = ClaudeBackend()
    invocation = original(repo, model)
    profile = conversation_profile(backend, invocation, version='2.1.318', freedom='read_only')
    profile['cwd'] = str(repo)
    profile['requested_network'] = None
    return q.canonical_profile(backend, invocation, profile, repo_path=repo)


def test_empty_accepted_matrix_refuses_without_provider_or_version_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ())
    def forbidden(*args, **kwargs):
        pytest.fail('closed profile must not run discovery or provider calls')
    monkeypatch.setattr(q, 'identify_runtime', forbidden)
    with pytest.raises(ConversationRefused, match='no accepted'):
        q.prepare_qualified_launch(ClaudeBackend(), original(tmp_path), repo_path=tmp_path, freedom='read_only')


def test_only_validated_repository_role_normalizes(tmp_path):
    one, two = tmp_path / 'one', tmp_path / 'two'
    assert q.fingerprint(candidate(one)) == q.fingerprint(candidate(two))
    assert q.fingerprint(candidate(one, 'model-a')) != q.fingerprint(candidate(one, 'model-b'))
    backend = ClaudeBackend()
    invocation = original(one)
    profile = conversation_profile(backend, invocation, version='2.1.318', freedom='read_only')
    profile['cwd'] = str(one)
    with pytest.raises(ConversationRefused, match='repository authority mismatch'):
        q.canonical_profile(backend, invocation, profile, repo_path=two)


def test_arbitrary_other_path_is_not_repository_placeholder(tmp_path):
    backend = ClaudeBackend()
    invocation = original(tmp_path)
    profile = conversation_profile(backend, invocation, version='2.1.318', freedom='read_only')
    profile['cwd'] = str(tmp_path)
    profile['additional_configuration_root'] = '/unrelated/configuration'
    canonical = q.canonical_profile(backend, invocation, profile, repo_path=tmp_path)
    assert canonical['profile']['additional_configuration_root'] == '/unrelated/configuration'
    assert canonical['profile']['cwd'] == {'harflow_path_role': 'repository'}


def test_accepted_receipt_requires_current_exact_runtime_identity(tmp_path, monkeypatch):
    backend = ClaudeBackend()
    invocation = original(tmp_path)
    profile = conversation_profile(backend, invocation, version='2.1.318', freedom='read_only')
    profile.update(cwd=str(tmp_path), requested_network=None)
    scope = q.fingerprint(q.canonical_profile(backend, invocation, profile, repo_path=tmp_path))
    monkeypatch.setattr(q, 'describe_candidate', lambda *a, **k: (profile, 'fresh-runtime', scope))
    monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ({'runtime_fingerprint': 'old-runtime', 'profile_fingerprint': scope, 'evidence_sha256': 'a' * 64},))
    with pytest.raises(ConversationRefused, match='no accepted qualification'):
        q.prepare_qualified_launch(backend, invocation, repo_path=tmp_path, freedom='read_only')
    monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ({'runtime_fingerprint': 'fresh-runtime', 'profile_fingerprint': scope, 'evidence_sha256': 'a' * 64},))
    launch, receipt = q.prepare_qualified_launch(backend, invocation, repo_path=tmp_path, freedom='read_only')
    assert launch.invocation.argv == invocation.argv
    assert receipt.launch_fingerprint == q.fingerprint(profile)
    assert receipt.runtime_fingerprint == 'fresh-runtime'


def test_model_and_environment_equal_repo_remain_literal_authority(tmp_path):
    one, two = tmp_path / 'one', tmp_path / 'two'
    # Model identifiers can be arbitrary strings; equality to a path does not
    # turn them into repository authority.
    assert q.fingerprint(candidate(one, str(one))) != q.fingerprint(candidate(two, str(two)))
    class EnvBackend(ClaudeBackend):
        def assert_safe(self, invocation, freedom, network=None):
            # Independent scripted backend shape validates this env deliberately.
            super().assert_safe(replace(invocation, env={}), freedom, network)
    def with_env(repo):
        backend = EnvBackend()
        invocation = replace(original(repo), env={'FIXTURE_MODEL_ALIAS': str(repo)})
        profile = conversation_profile(backend, invocation, version='2.1.318', freedom='read_only')
        profile['cwd'] = str(repo)
        return q.canonical_profile(backend, invocation, profile, repo_path=repo)
    assert q.fingerprint(with_env(one)) != q.fingerprint(with_env(two))
    assert with_env(one)['invocation_authority']['env']['FIXTURE_MODEL_ALIAS'] == str(one)


@pytest.mark.parametrize('name', ['claude', 'codex'])
async def test_owner_proven_managed_grants_have_stable_scope_across_task_ids(tmp_path, name):
    import uuid
    from harflow_mcp import backends, state_ownership
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.tasks import TaskRegistry
    from test_conversation_profile_portability import capture, profile
    layout = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(exist_ok=True)
        backend = backends.get(name)
        registry = TaskRegistry(log_dir=layout.tasks_directory)
        first = await capture(registry, backend, tmp_path, 'write_in_repo', task_id=str(uuid.uuid4()))
        second = await capture(registry, backend, tmp_path, 'write_in_repo', task_id=str(uuid.uuid4()))
        scopes = [q.canonical_profile(backend, item.final, profile(backend, item.final, tmp_path, 'write_in_repo'),
                    repo_path=tmp_path, managed_paths=item.policy['managed_paths']) for item in (first, second)]
        assert q.fingerprint(scopes[0]) == q.fingerprint(scopes[1])
        assert scopes[0]['managed_root_roles'] == [
            {'role': 'task_scratch', 'access': 'writable'}, {'role': 'chain_scratch', 'access': 'writable'}]
        assert str(first.final.scratch_directory) not in str(scopes[0])
        assert first.final.scratch_directory != second.final.scratch_directory
        if name == 'codex':
            replacements = []
            for item in (first, second):
                context = profile(backend, item.final, tmp_path, 'write_in_repo')
                context['runtime_workspace_roots'] = list(context['writable_roots'])
                replacements.append(q.canonical_profile(backend, item.final, context,
                    repo_path=tmp_path, managed_paths=item.policy['managed_paths']))
            assert q.fingerprint(replacements[0]) == q.fingerprint(replacements[1])
            assert replacements[0]['profile']['runtime_workspace_roots'] == [
                {'harflow_path_role':'repository'}, {'harflow_path_role':'task_scratch'},
                {'harflow_path_role':'chain_scratch'}]
        # Normalization is pure: actual provider permission roots remain literal.
        assert first.final.scratch_directory.startswith(str(layout.root))


@pytest.mark.parametrize('name', ['claude', 'codex', 'vibe', 'antigravity', 'cursor'])
def test_start_qualification_never_implies_retained_session_load(tmp_path, name):
    from harflow_mcp import backends
    from harflow_mcp.backends.conversation_native import VERSIONS
    backend = backends.get(name)
    context = '12345678-1234-1234-1234-123456789abc'
    common = dict(repo=tmp_path, freedom='read_only', model=None, max_turns=None, reasoning_effort=None)
    started = backend.build_start_argv('hello', session_id=context if backend.capabilities.chooses_session_id else None, **common)
    resumed = backend.build_resume_argv('hello', session_id=context, **common)
    scopes = []
    for invocation, operation in ((started, 'start'), (resumed, 'resume')):
        profile = conversation_profile(backend, invocation, version=VERSIONS[name], freedom='read_only',
            profile_name='shipped-medium35-high' if name == 'vibe' else None)
        profile['cwd'] = str(tmp_path)
        canonical = q.canonical_profile(backend, invocation, profile, repo_path=tmp_path)
        scopes.append(canonical)
    assert scopes[0]['profile']['session_operation'] == 'start'
    assert scopes[1]['profile']['session_operation'] == 'resume'
    assert q.fingerprint(scopes[0]) != q.fingerprint(scopes[1])


@pytest.mark.parametrize('version', ['2.1.318', '2.1.319'])
def test_claude_candidate_protocol_version_is_not_production_qualification(tmp_path, monkeypatch, version):
    monkeypatch.setattr(q, 'ACCEPTED_PROFILES', ())
    from harflow_mcp.backends.conversation_native import configure_conversation
    from harflow_mcp.backends.qualification import Identity
    backend = ClaudeBackend()
    invocation = original(tmp_path)
    monkeypatch.setattr(q.qualification, 'identify', lambda selected: Identity(
        selected.name, version, '/fixture/claude', 'a' * 64, 'darwin'))
    observed, _ = q.identify_runtime(backend)
    assert observed.version == version
    candidate = configure_conversation(backend, invocation, version=version, freedom='read_only', qualified=True)
    assert candidate.invocation.conversation_context['version'] == version
    with pytest.raises(ConversationRefused, match='explicit owner qualification'):
        configure_conversation(backend, invocation, version=version, freedom='read_only')
    with pytest.raises(ConversationRefused, match='no accepted'):
        q.prepare_qualified_launch(backend, invocation, repo_path=tmp_path, freedom='read_only')


def test_unknown_claude_protocol_version_stays_closed_before_model(tmp_path, monkeypatch):
    from harflow_mcp.backends.qualification import Identity
    backend = ClaudeBackend()
    monkeypatch.setattr(q.qualification, 'identify', lambda selected: Identity(
        selected.name, '2.1.320', '/fixture/claude', 'b' * 64, 'darwin'))
    with pytest.raises(ConversationRefused, match='runtime version is not supported'):
        q.identify_runtime(backend)
    with pytest.raises(ConversationRefused, match='version is not qualified'):
        conversation_profile(backend, original(tmp_path), version='2.1.320', freedom='read_only')


@pytest.mark.parametrize('factory_kind', ['production', 'private_probe'])
def test_launcher_retarget_refuses_with_old_binary_unchanged(tmp_path, monkeypatch, factory_kind):
    from harflow_mcp.backends.qualification import Identity
    from harflow_mcp.conversation_task_transport import _probe_factory
    launcher_dir = tmp_path / 'bin'
    launcher_dir.mkdir()
    old, new = tmp_path / 'runtime-old', tmp_path / 'runtime-new'
    for path in (old, new):
        path.write_text('audited fake launcher, never executed')
        path.chmod(0o700)
    launcher = launcher_dir / 'claude'
    launcher.symlink_to(old)
    monkeypatch.setenv('PATH', str(launcher_dir))
    backend = ClaudeBackend()
    monkeypatch.setattr(q, 'identify_runtime', lambda selected: (
        Identity(selected.name, '2.1.319', str(old), 'a' * 64, 'darwin'), 'runtime-old'))
    preflight = q._runtime_preflight(backend)
    q.verify_runtime_preflight(preflight, backend)
    before = old.stat()
    launcher.unlink()
    launcher.symlink_to(new)
    assert (old.stat().st_dev, old.stat().st_ino, old.stat().st_size, old.stat().st_mtime_ns) == (
        before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    # A changed launcher must refuse before any native profile/handshake frames.
    def forbidden(*args, **kwargs):
        pytest.fail('changed launcher reached native profile construction')
    if factory_kind == 'production':
        monkeypatch.setattr(q, 'conversation_profile', forbidden)
        run = lambda: q.prepare_from_preflight(preflight, backend, original(tmp_path),
            repo_path=tmp_path, freedom='read_only')
    else:
        from harflow_mcp.backends import conversation_native
        monkeypatch.setattr(conversation_native, 'conversation_profile', forbidden)
        factory = _probe_factory(None)
        run = lambda: factory.finalize(preflight, backend, original(tmp_path),
            {'repo_path': tmp_path, 'freedom': 'read_only'})
    with pytest.raises(ConversationRefused, match='launcher resolution changed'):
        run()
    assert launcher.is_symlink() and launcher.resolve() == new
    assert backend.binary == 'claude'  # No argv/PATH pin to the old target.


def test_launcher_resolution_check_does_not_probe_or_hash(tmp_path, monkeypatch):
    from harflow_mcp.backends.qualification import Identity
    launcher_dir = tmp_path / 'bin'
    launcher_dir.mkdir()
    target = tmp_path / 'runtime'
    target.write_text('never executed')
    target.chmod(0o700)
    (launcher_dir / 'claude').symlink_to(target)
    monkeypatch.setenv('PATH', str(launcher_dir))
    backend = ClaudeBackend()
    monkeypatch.setattr(q, 'identify_runtime', lambda selected: (
        Identity(selected.name, '2.1.319', str(target), 'b' * 64, 'darwin'), 'runtime'))
    preflight = q._runtime_preflight(backend)
    def forbidden(*args, **kwargs):
        pytest.fail('final check attempted fresh hashing/version discovery')
    monkeypatch.setattr(q, 'identify_runtime', forbidden)
    monkeypatch.setattr(q, 'fingerprint', forbidden)
    q.verify_runtime_preflight(preflight, backend)


def test_codex_resume_replacement_roots_are_exact_typed_authority(tmp_path):
    from harflow_mcp.backends.codex import CodexBackend
    backend = CodexBackend()
    invocation = backend.build_resume_argv('next', repo=tmp_path, freedom='write_in_repo',
        session_id='original-thread', model=None, max_turns=None, reasoning_effort=None, network=False)
    invocation = backend.with_writable_directory(invocation, tmp_path/'current-scratch', 'write_in_repo')
    profile = conversation_profile(backend, invocation, version='0.166.0', freedom='write_in_repo', network=False)
    assert profile['runtime_workspace_roots'] == [str(tmp_path), str(tmp_path/'current-scratch')]
    profile['requested_network'] = False
    # Explicit configured/caller slots remain literal even when equal to cwd.
    profile['runtime_workspace_roots'].append(str(tmp_path))
    canonical = q.canonical_profile(backend, invocation, profile, repo_path=tmp_path)
    assert canonical['profile']['session_operation'] == 'resume'
    assert canonical['profile']['runtime_workspace_roots'] == [
        {'harflow_path_role':'repository'}, str(tmp_path/'current-scratch'), str(tmp_path)]
