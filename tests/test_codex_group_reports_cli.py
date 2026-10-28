"""Opt-in actual native read-only profile qualification; localhost provider, no paid calls."""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import uuid

import pytest

from fake_responses_api import LocalResponses
from harflow_mcp.backends import get
from test_execution_config import isolate_configuration_environment

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    not os.environ.get('HARFLOW_CLI_INTEGRATION'), reason='opt in with HARFLOW_CLI_INTEGRATION=1')]


def test_native_group_output_fresh_resume_peer_and_isolation(tmp_path, monkeypatch):
    if sys.platform != 'darwin' or not shutil.which('codex'):
        pytest.skip('measured macOS Codex required')
    executable = shutil.which('codex')
    if subprocess.check_output([executable, '--version'], text=True).strip() != 'codex-cli 0.165.0':
        pytest.skip('qualification measured on codex-cli 0.165.0')
    paths = {name: (tmp_path / name).resolve() for name in ('repo', 'group', 'other', 'outside', 'home', 'codex-home', 'native-bin')}
    for path in paths.values(): path.mkdir()
    subprocess.run(['git', 'init', '-q', str(paths['repo'])], check=True)
    (paths['repo'] / '.codex').mkdir()
    (paths['group'] / 'escape').symlink_to(paths['outside'], target_is_directory=True)
    # Qualification's conservative runtime profile excludes ambient script launchers.
    for name, selected in [('codex', executable), *((name, shutil.which(name)) for name in ('git', 'ps', 'open', 'touch', 'cat'))]:
        if selected is None: pytest.skip('native command unavailable: ' + name)
        (paths['native-bin'] / name).symlink_to(selected)
    isolate_configuration_environment(monkeypatch, paths['home'])
    monkeypatch.setenv('CODEX_HOME', str(paths['codex-home']))
    monkeypatch.setenv('HARFLOW_STATE_ROOT', str(tmp_path / 'state'))
    monkeypatch.setenv('PATH', str(paths['native-bin']))
    backend = get('codex')
    q = lambda p: shlex.quote(str(p))
    session = None
    with LocalResponses(tmp_path).serve() as api:
        (paths['codex-home'] / 'config.toml').write_text(
            'model_provider="local"\nmodel="gpt-6.1-sol"\n[model_providers.local]\nname="local"\nwire_api="responses"\nrequires_openai_auth=false\nbase_url=' + json.dumps(api.url + '/v1') + '\n')
        for phase in ('fresh', 'resume', 'continuation', 'peer', 'other_group'):
            api.nonce = 'group_' + uuid.uuid4().hex
            group = paths['other'] if phase == 'other_group' else paths['group']
            api.commands = [('create', f'printf first > {q(group / "report.md")}'),
                            ('update', f'printf second > {q(group / "report.md")}'),
                            ('read', f'cat {q(group / "report.md")}'),
                            ('repo', f'touch {q(paths["repo"] / "denied")}'),
                            ('config', f'touch {q(paths["repo"] / ".codex/config.toml")}'),
                            ('other', f'touch {q((paths["group"] if phase == "other_group" else paths["other"]) / "denied")}'),
                            ('outside', f'touch {q(paths["outside"] / "denied")}'),
                            ('traversal', f'touch {q(group / ".." / "outside/traversal")}')]
            if phase != 'other_group': api.commands.append(('symlink', f'touch {q(group / "escape/symlink")}'))
            if phase == 'peer': api.commands.insert(0, ('peer_read', f'cat {q(group / "report.md")}'))
            builder = backend.build_resume_argv if phase == 'resume' else backend.build_start_argv
            invocation = builder(api.nonce, repo=paths['repo'], freedom='read_only', network=False,
                                 session_id=session if phase == 'resume' else None, model=None, reasoning_effort=None, max_turns=None)
            invocation = backend.configure_group_report_output(invocation, group, 'read_only', False)
            backend.assert_safe(invocation, 'read_only', False)
            result = subprocess.run(invocation.argv, cwd=paths['repo'], env=os.environ.copy(), stdin=subprocess.DEVNULL,
                                    capture_output=True, text=True, timeout=90)
            (tmp_path / (phase + '.jsonl')).write_text(result.stdout)
            (tmp_path / (phase + '.stderr')).write_text(result.stderr)
            assert result.returncode == 0, result.stderr
            # Native permission guidance exposes the exact authorized path without changing caller prose.
            request_text = json.dumps(api.requests[-2]['input'])
            assert str(group) in request_text and 'writable root' in request_text
            results = api.results()
            for label in ('create', 'update', 'read'): assert results[label]['exit_code'] == 0, results
            for label in ('repo', 'config', 'other', 'outside', 'traversal'):
                assert results[label]['exit_code'] != 0 and 'Operation not permitted' in results[label]['output'], results
            if phase != 'other_group': assert results['symlink']['exit_code'] != 0, results
            if phase == 'peer': assert 'second' in results['peer_read']['output'], results
            assert (group / 'report.md').read_text() == 'second'
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            current = next(event['thread_id'] for event in events if event.get('type') == 'thread.started')
            if phase == 'fresh': session = current
            elif phase == 'resume': assert current == session
            else: assert current != session
