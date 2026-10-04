from datetime import datetime, timezone
from pathlib import Path

from polybridge import store
from polybridge.tasks import Task, TaskRegistry
from polybridge.backends.base import Accumulator
from polybridge.backends.claude import ClaudeBackend, ALLOWED_TOOLS


def test_complete_assignment_persisted(tmp_path):
    prompt = 'review payload ' + 'x' * 42000 + ' final SHA'
    task = Task(task_id='full-prompt', backend='claude', session_id='session', repo_path=tmp_path,
                prompt=prompt, max_turns=None, log_path=tmp_path/'full-prompt.jsonl',
                started_at=datetime.now(timezone.utc))
    TaskRegistry(log_dir=tmp_path).persist(task)
    assert store.read(tmp_path, task.task_id).prompt == prompt


def test_claude_safety_shape_denial_retains_reason():
    backend, acc = ClaudeBackend(), Accumulator()
    backend.ingest({'type':'assistant','message':{'content':[{'type':'tool_use','id':'tool-1','name':'Bash','input':{'command':'printf payload'}}]}}, acc)
    backend.ingest({'type':'user','message':{'content':[{'type':'tool_result','tool_use_id':'tool-1','is_error':True,'content':'Contains brace with quote character. This command requires approval.'}]}}, acc)
    assert len(acc.denials) == 1
    assert acc.denials[0]['reason'] == 'Contains brace with quote character. This command requires approval.'
    assert acc.denials[0]['tool_input']['command'] == 'printf payload'


def test_publish_does_not_allow_approval_or_raw_api():
    rules = ALLOWED_TOOLS['publish']
    assert 'Bash(gh pr review:*)' not in rules
    assert 'Bash(gh api' not in rules


def test_vibe_unified_session_model(tmp_path, monkeypatch):
    import json
    from polybridge.backends.vibe import VibeBackend
    monkeypatch.setenv('VIBE_HOME', str(tmp_path))
    directory = tmp_path/'logs/session/unified/native-session'
    generation = directory/'generations/0000000000000001'
    generation.mkdir(parents=True)
    (directory/'CURRENT').write_text(json.dumps({'generation':'0000000000000001','session_id':'native-session'}))
    (generation/'runtime-state.json').write_text(json.dumps({'session_id':'native-session','session_metadata':{'active_model':'glm-5-3','reasoning_effort':'medium'}}))
    result = VibeBackend.workflow_observed_metadata({'session_id':'native-session'})
    assert result['observed']['model'] == 'glm-5-3'
    assert result['observed']['reasoning_effort'] == 'medium'


def test_codex_large_prompt_uses_closed_one_shot_stdin(tmp_path):
    from polybridge.backends.codex import CodexBackend
    backend = CodexBackend()
    prompt = 'assignment ' + 'x' * 1_050_838
    for method, session in ((backend.build_start_argv, None), (backend.build_resume_argv, 'native-thread')):
        invocation = method(prompt, repo=tmp_path, freedom='read_only', session_id=session,
                            model=None, max_turns=None, reasoning_effort=None)
        assert max(len(arg) for arg in invocation.argv) < 10000
        assert invocation.stdin_mode == 'pipe_once'
        assert invocation.initial_input == prompt.encode()
        assert not invocation.live_input
        backend.assert_safe(invocation, 'read_only')


async def test_large_codex_stdin_delivers_all_bytes_then_eof(tmp_path, monkeypatch):
    import asyncio
    import sys
    from polybridge.backends.codex import CodexBackend
    import polybridge.tasks as tasks
    backend = CodexBackend()
    prompt = 'assignment ' + 'x' * 1_050_838
    invocation = backend.build_start_argv(prompt, repo=tmp_path, freedom='read_only', session_id=None,
                                          model=None, max_turns=None, reasoning_effort=None)
    create = asyncio.create_subprocess_exec
    received = tmp_path/'received.txt'
    async def fake_exec(*argv, **kwargs):
        assert max(len(arg) for arg in argv) < 10000
        code = 'import sys,pathlib;pathlib.Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read());print(\'{"type":"turn.completed"}\')'
        return await create(sys.executable, '-c', code, str(received), **kwargs)
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', fake_exec)
    registry = TaskRegistry(log_dir=tmp_path/'logs')
    task = await registry._spawn(invocation, backend=backend, prompt=prompt, repo_path=tmp_path,
                                 session_id=None, freedom='read_only', max_turns=None, model=None,
                                 reasoning_effort=None)
    await asyncio.wait_for(task.proc.wait(), timeout=5)
    assert received.read_bytes() == prompt.encode()
    assert task.input_closed
    assert task.pump is None
    if task.monitor:
        await task.monitor


def test_scratch_extension_only_matches_adapter_owned_path(tmp_path):
    from dataclasses import replace
    import pytest
    from polybridge.backends.codex import CodexBackend
    from polybridge.backends.claude import ClaudeBackend
    for backend in (CodexBackend(), ClaudeBackend()):
        invocation = backend.build_start_argv('assignment', repo=tmp_path, freedom='write_in_repo',
                                              session_id='scratch-session' if isinstance(backend, ClaudeBackend) else None, model=None, max_turns=None, reasoning_effort=None)
        extended = backend.with_writable_directory(invocation, tmp_path/'scratch', 'write_in_repo')
        assert extended.scratch_directory == str(tmp_path/'scratch')
        backend.assert_safe(extended, 'write_in_repo')
        with pytest.raises(RuntimeError):
            backend.assert_safe(replace(extended, scratch_directory=str(tmp_path/'other')), 'write_in_repo')
        with pytest.raises((RuntimeError, ValueError)):
            backend.with_writable_directory(invocation, tmp_path/'scratch', 'read_only')


async def test_scratch_spawn_scope_and_environment(tmp_path, monkeypatch):
    import asyncio
    import json
    import sys
    from polybridge.backends.codex import CodexBackend
    backend = CodexBackend()
    create = asyncio.create_subprocess_exec
    monkeypatch.setenv('PB_TASK_SCRATCH', '/inherited/other-task')
    captured = []
    async def fake_exec(*argv, **kwargs):
        captured.append((argv, kwargs['env'].get('PB_TASK_SCRATCH')))
        return await create(sys.executable, '-c', 'print(\'{"type":"turn.completed"}\')', **kwargs)
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', fake_exec)
    registry = TaskRegistry(log_dir=tmp_path/'tasks')
    for freedom in ('read_only', 'write_in_repo'):
        invocation = backend.build_start_argv('assignment', repo=tmp_path, freedom=freedom, session_id=None,
                                              model=None, max_turns=None, reasoning_effort=None)
        task = await registry._spawn(invocation, backend=backend, prompt='assignment', repo_path=tmp_path,
                                     session_id=None, freedom=freedom, max_turns=None, model=None, reasoning_effort=None)
        await asyncio.wait_for(task.proc.wait(), timeout=5)
        if task.monitor:
            await task.monitor
        argv, directory = captured[-1]
        if freedom == 'read_only':
            assert directory is None
            assert '--add-dir' not in argv
        else:
            assert directory == str(tmp_path/'scratch'/task.task_id)
            assert argv[argv.index('--add-dir')+1] == directory
            assert Path(directory).is_dir()
            assert Path(directory).stat().st_mode & 0o777 == 0o700
            assert directory in task.enforcement['writable_roots']


async def test_positive_not_started_spawn_cleans_scratch(tmp_path, monkeypatch):
    import asyncio
    import pytest
    from polybridge.backends.codex import CodexBackend
    backend = CodexBackend()
    invocation = backend.build_start_argv('assignment', repo=tmp_path, freedom='write_in_repo', session_id=None,
                                          model=None, max_turns=None, reasoning_effort=None)
    async def refused(*argv, **kwargs):
        assert Path(kwargs['env']['PB_TASK_SCRATCH']).is_dir()
        raise OSError('executable unavailable')
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', refused)
    with pytest.raises(OSError) as failure:
        await TaskRegistry(log_dir=tmp_path/'tasks')._spawn(invocation, backend=backend, prompt='assignment',
            repo_path=tmp_path, session_id=None, freedom='write_in_repo', max_turns=None, model=None,
            reasoning_effort=None, task_id='not-started')
    assert failure.value.polybridge_not_started
    assert not (tmp_path/'scratch/not-started').exists()


def test_retention_keeps_record_when_scratch_cleanup_fails(tmp_path, monkeypatch):
    from polybridge import retention, scratch
    task_id = 'retained-scratch'
    log_dir = tmp_path/'tasks'
    log_dir.mkdir()
    record = log_dir/f'{task_id}.meta.json'
    record.write_text('{}')
    def fail(*args):
        raise OSError('scratch busy')
    monkeypatch.setattr(scratch, 'remove', fail)
    assert not retention._delete_task_files(log_dir, task_id)
    assert record.exists()


async def test_uncertain_spawn_retains_scratch(tmp_path, monkeypatch):
    import asyncio
    import pytest
    from polybridge.backends.codex import CodexBackend
    backend = CodexBackend()
    invocation = backend.build_start_argv('assignment', repo=tmp_path, freedom='write_in_repo', session_id=None,
                                          model=None, max_turns=None, reasoning_effort=None)
    async def interrupted(*argv, **kwargs):
        raise asyncio.CancelledError()
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', interrupted)
    with pytest.raises(asyncio.CancelledError):
        await TaskRegistry(log_dir=tmp_path/'tasks')._spawn(invocation, backend=backend, prompt='assignment',
            repo_path=tmp_path, session_id=None, freedom='write_in_repo', max_turns=None, model=None,
            reasoning_effort=None, task_id='uncertain-spawn')
    assert (tmp_path/'scratch/uncertain-spawn').is_dir()


def test_safe_github_read_rules_available_without_publish():
    for freedom in ('read_only', 'write_in_repo'):
        rules = ALLOWED_TOOLS.get(freedom, '')
        assert 'Bash(gh pr view:*)' in rules
        assert 'Bash(gh pr diff:*)' in rules
        assert 'Bash(gh api' not in rules


def test_codex_reports_inherited_configured_writable_roots(tmp_path, monkeypatch):
    from polybridge.backends.codex import CodexBackend
    home = tmp_path/'codex-home'
    home.mkdir()
    (home/'config.toml').write_text('[sandbox_workspace_write]\nwritable_roots=["/extra/docs"]\n')
    monkeypatch.setenv('CODEX_HOME', str(home))
    report = CodexBackend().enforcement('write_in_repo')
    assert '/extra/docs' in report.writable_roots
    assert any('config.toml' in caveat for caveat in report.caveats)
    assert '/extra/docs' not in CodexBackend().enforcement('read_only').writable_roots


def test_codex_profile_writable_roots_override_global(tmp_path, monkeypatch):
    from polybridge.backends.codex import CodexBackend
    home = tmp_path/'codex-home'
    home.mkdir()
    (home/'config.toml').write_text('profile="work"\n[sandbox_workspace_write]\nwritable_roots=["/global"]\n[profiles.work.sandbox_workspace_write]\nwritable_roots=["/profile"]\n')
    monkeypatch.setenv('CODEX_HOME', str(home))
    report = CodexBackend().enforcement('publish')
    assert '/profile' in report.writable_roots
    assert '/global' not in report.writable_roots
